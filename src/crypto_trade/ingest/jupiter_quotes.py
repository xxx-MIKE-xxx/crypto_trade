from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from crypto_trade.core.env import load_env
from crypto_trade.core.io import append_jsonl
from crypto_trade.core.jupiter import request_jupiter_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ONCHAIN_DIR
from crypto_trade.core.time import now_ms, now_ts

logger = logging.getLogger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"
QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
OUTPUT_FILENAME = "jupiter_quotes.jsonl"
LAMPORTS_PER_SOL = 1_000_000_000

DEFAULT_BUY_SOL_AMOUNTS = [0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]
DEFAULT_SELL_BASE_SOL_AMOUNTS = [0.1, 1.0]
DEFAULT_SELL_FRACTIONS = [0.25, 0.5, 1.0]


def output_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / OUTPUT_FILENAME
    return ONCHAIN_DIR / mint / OUTPUT_FILENAME


def sol_to_lamports(amount_sol: float) -> int:
    return int(amount_sol * LAMPORTS_PER_SOL)


def response_row(
    *,
    mint: str,
    tick_index: int,
    capture_start_ms: int,
    quote_type: str,
    direction: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    response: Any | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response_error_type = getattr(response, "error_type", None)
    response_error_message = getattr(response, "error_message", None)

    return {
        "timestamp": now_ts(),
        "local_received_at_ms": now_ms(),
        "elapsed_since_start_ms": now_ms() - capture_start_ms,
        "source": "jupiter",
        "method": "swap-v1-quote",
        "mint": mint,
        "tick_index": tick_index,
        "quote_type": quote_type,
        "direction": direction,
        "input_mint": input_mint,
        "output_mint": output_mint,
        "amount": str(amount),
        "http_status": getattr(response, "http_status", None),
        "elapsed_ms": getattr(response, "elapsed_ms", None),
        "rate_limit": getattr(response, "rate_limit", {}) if response is not None else {},
        "error_type": error_type or response_error_type,
        "error_message": error_message or response_error_message,
        "details": details or {},
        "data": getattr(response, "data", None) if response is not None else None,
    }


async def quote(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    restrict_intermediate_tokens: bool,
    only_direct_routes: bool,
) -> Any:
    params: dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": slippage_bps,
        "restrictIntermediateTokens": str(restrict_intermediate_tokens).lower(),
        "instructionVersion": "V2",
    }

    if only_direct_routes:
        params["onlyDirectRoutes"] = "true"

    return await request_jupiter_json("GET", QUOTE_URL, params=params)


async def capture_tick(
    *,
    path: Path,
    mint: str,
    tick_index: int,
    capture_start_ms: int,
    buy_sol_amounts: list[float],
    sell_base_sol_amounts: list[float],
    sell_fractions: list[float],
    slippage_bps: int,
    restrict_intermediate_tokens: bool,
    only_direct_routes: bool,
) -> None:
    buy_out_by_sol: dict[float, int] = {}

    for sol_amount in buy_sol_amounts:
        lamports = sol_to_lamports(sol_amount)
        response = await quote(
            input_mint=WSOL_MINT,
            output_mint=mint,
            amount=lamports,
            slippage_bps=slippage_bps,
            restrict_intermediate_tokens=restrict_intermediate_tokens,
            only_direct_routes=only_direct_routes,
        )
        append_jsonl(
            path,
            response_row(
                mint=mint,
                tick_index=tick_index,
                capture_start_ms=capture_start_ms,
                quote_type="buy",
                direction="sol_to_token",
                input_mint=WSOL_MINT,
                output_mint=mint,
                amount=lamports,
                response=response,
                details={"amount_sol": sol_amount, "only_direct_routes": only_direct_routes},
            ),
        )

        out_amount = (response.data or {}).get("outAmount") if not response.error_type else None
        if out_amount:
            buy_out_by_sol[sol_amount] = int(out_amount)

    for base_sol in sell_base_sol_amounts:
        base_token_amount = buy_out_by_sol.get(base_sol)
        for fraction in sell_fractions:
            if not base_token_amount:
                append_jsonl(
                    path,
                    response_row(
                        mint=mint,
                        tick_index=tick_index,
                        capture_start_ms=capture_start_ms,
                        quote_type="sell",
                        direction="token_to_sol",
                        input_mint=mint,
                        output_mint=WSOL_MINT,
                        amount=0,
                        error_type="missing_buy_quote",
                        error_message=f"Missing buy quote for base {base_sol} SOL",
                        details={
                            "base_sol_amount": base_sol,
                            "position_fraction": fraction,
                            "only_direct_routes": only_direct_routes,
                        },
                    ),
                )
                continue

            amount = max(1, int(base_token_amount * fraction))
            response = await quote(
                input_mint=mint,
                output_mint=WSOL_MINT,
                amount=amount,
                slippage_bps=slippage_bps,
                restrict_intermediate_tokens=restrict_intermediate_tokens,
                only_direct_routes=only_direct_routes,
            )
            append_jsonl(
                path,
                response_row(
                    mint=mint,
                    tick_index=tick_index,
                    capture_start_ms=capture_start_ms,
                    quote_type="sell",
                    direction="token_to_sol",
                    input_mint=mint,
                    output_mint=WSOL_MINT,
                    amount=amount,
                    response=response,
                    details={
                        "base_sol_amount": base_sol,
                        "position_fraction": fraction,
                        "only_direct_routes": only_direct_routes,
                    },
                ),
            )


async def main(
    mint: str,
    save_dir: Path | None = None,
    interval_seconds: int = 15,
    length_seconds: int = 1800,
    slippage_bps: int = 300,
    buy_sol_amounts: list[float] | None = None,
    sell_base_sol_amounts: list[float] | None = None,
    sell_fractions: list[float] | None = None,
    restrict_intermediate_tokens: bool = True,
    only_direct_routes: bool = False,
) -> Path:
    configure_logging()
    load_env()

    path = output_path(mint, save_dir)
    capture_start_ms = now_ms()
    tick_index = 0
    buy_sol_amounts = buy_sol_amounts or DEFAULT_BUY_SOL_AMOUNTS
    sell_base_sol_amounts = sell_base_sol_amounts or DEFAULT_SELL_BASE_SOL_AMOUNTS
    sell_fractions = sell_fractions or DEFAULT_SELL_FRACTIONS

    while now_ms() - capture_start_ms <= length_seconds * 1000:
        tick_started_ms = now_ms()
        await capture_tick(
            path=path,
            mint=mint,
            tick_index=tick_index,
            capture_start_ms=capture_start_ms,
            buy_sol_amounts=buy_sol_amounts,
            sell_base_sol_amounts=sell_base_sol_amounts,
            sell_fractions=sell_fractions,
            slippage_bps=slippage_bps,
            restrict_intermediate_tokens=restrict_intermediate_tokens,
            only_direct_routes=only_direct_routes,
        )
        tick_index += 1

        sleep_for = interval_seconds - ((now_ms() - tick_started_ms) / 1000)
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

    logger.info("Saved Jupiter quotes to %s", path)
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--interval-seconds", type=int, default=15)
    parser.add_argument("--length-seconds", type=int, default=1800)
    args = parser.parse_args()

    output = asyncio.run(
        main(
            mint=args.mint,
            save_dir=args.out_dir,
            interval_seconds=args.interval_seconds,
            length_seconds=args.length_seconds,
        )
    )
    print(json.dumps({"saved_to": str(output)}, indent=2))
