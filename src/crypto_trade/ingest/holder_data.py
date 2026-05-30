from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from crypto_trade.core.env import load_env
from crypto_trade.core.io import append_jsonl
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ONCHAIN_DIR
from crypto_trade.core.rpc import RPC
from crypto_trade.core.time import now_ms, now_ts

logger = logging.getLogger(__name__)

LARGEST_ACCOUNTS_FILENAME = "largest_accounts.jsonl"
TOKEN_ACCOUNTS_FILENAME = "token_accounts.jsonl"

DEFAULT_SCHEDULE: list[dict[str, Any]] = [
    {"label": "migration", "delay_seconds": 0},
    {"label": "migration_plus_5m", "delay_seconds": 300},
    {"label": "migration_plus_30m", "delay_seconds": 1800},
]

DEFAULT_TOKEN_ACCOUNTS_SCHEDULE: list[dict[str, Any]] = [
    {"label": "migration", "delay_seconds": 0},
    {"label": "migration_plus_1h", "delay_seconds": 3600},
]


def largest_accounts_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / LARGEST_ACCOUNTS_FILENAME
    return ONCHAIN_DIR / mint / LARGEST_ACCOUNTS_FILENAME


def token_accounts_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / TOKEN_ACCOUNTS_FILENAME
    return ONCHAIN_DIR / mint / TOKEN_ACCOUNTS_FILENAME


def normalize_schedule(schedule: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    raw_schedule = schedule or DEFAULT_SCHEDULE
    normalized: list[dict[str, Any]] = []

    for index, item in enumerate(raw_schedule):
        label = str(item.get("label") or f"snapshot_{index}")
        delay_seconds = max(0, int(item.get("delay_seconds") or 0))
        normalized.append({"label": label, "delay_seconds": delay_seconds})

    return normalized or list(DEFAULT_SCHEDULE)


def response_row(
    *,
    mint: str,
    snapshot_label: str,
    delay_seconds: int,
    method: str,
    response: Any | None = None,
    error: BaseException | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "timestamp": now_ts(),
        "local_received_at_ms": now_ms(),
        "source": "helius",
        "method": method,
        "mint": mint,
        "snapshot_label": snapshot_label,
        "delay_seconds": delay_seconds,
        "http_status": getattr(response, "http_status", None),
        "elapsed_ms": getattr(response, "elapsed_ms", None),
        "rate_limit": getattr(response, "rate_limit", {}) if response is not None else {},
        "error_type": type(error).__name__ if error else getattr(response, "error_type", None),
        "error_message": str(error) if error else getattr(response, "error_message", None),
        "meta": extra or {},
        "data": None if error else getattr(response, "data", None),
    }


async def snapshot_largest_accounts(
    *,
    mint: str,
    path: Path,
    snapshot_label: str,
    delay_seconds: int,
) -> Path:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)

    try:
        rpc = RPC()
        response = await rpc.call_rpc(
            "getTokenLargestAccounts",
            [mint, {"commitment": "confirmed"}],
        )
        row = response_row(
            mint=mint,
            snapshot_label=snapshot_label,
            delay_seconds=delay_seconds,
            method="getTokenLargestAccounts",
            response=response,
        )
    except Exception as exc:
        logger.warning(
            "Failed to collect getTokenLargestAccounts snapshot for %s %s: %s",
            mint,
            snapshot_label,
            exc,
        )
        row = response_row(
            mint=mint,
            snapshot_label=snapshot_label,
            delay_seconds=delay_seconds,
            method="getTokenLargestAccounts",
            error=exc,
        )

    append_jsonl(path, row)
    return path


async def snapshot_token_accounts(
    *,
    mint: str,
    path: Path,
    snapshot_label: str,
    delay_seconds: int,
    page_limit: int,
    max_pages: int,
    max_accounts: int,
) -> Path:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)

    rpc = RPC()
    cursor = None
    accounts_seen = 0

    for page_index in range(max_pages):
        params: dict[str, Any] = {
            "mint": mint,
            "limit": page_limit,
            "options": {"showZeroBalance": False},
        }
        if cursor:
            params["cursor"] = cursor

        try:
            response = await rpc.call_rpc("getTokenAccounts", params)
            data = response.data or {}
            result = data.get("result") if isinstance(data, dict) else None
            token_accounts = (result or {}).get("token_accounts") or []
            accounts_seen += len(token_accounts)
            cursor = (result or {}).get("cursor")

            row = response_row(
                mint=mint,
                snapshot_label=snapshot_label,
                delay_seconds=delay_seconds,
                method="getTokenAccounts",
                response=response,
                extra={
                    "page_index": page_index,
                    "page_limit": page_limit,
                    "max_pages": max_pages,
                    "max_accounts": max_accounts,
                    "accounts_seen": accounts_seen,
                    "has_next_page": bool(cursor),
                    "stopped_reason": None,
                },
            )
            append_jsonl(path, row)

            if not cursor:
                break
            if accounts_seen >= max_accounts:
                break

        except Exception as exc:
            logger.warning(
                "Failed to collect getTokenAccounts snapshot for %s %s page %s: %s",
                mint,
                snapshot_label,
                page_index,
                exc,
            )
            append_jsonl(
                path,
                response_row(
                    mint=mint,
                    snapshot_label=snapshot_label,
                    delay_seconds=delay_seconds,
                    method="getTokenAccounts",
                    error=exc,
                    extra={
                        "page_index": page_index,
                        "page_limit": page_limit,
                        "max_pages": max_pages,
                        "max_accounts": max_accounts,
                        "accounts_seen": accounts_seen,
                    },
                ),
            )
            break

    return path


async def main(
    mint: str,
    save_dir: Path | None = None,
    schedule: list[dict[str, Any]] | None = None,
    largest_accounts: bool = True,
    token_accounts: str | bool = "disabled",
    token_accounts_schedule: list[dict[str, Any]] | None = None,
    token_accounts_page_limit: int = 100,
    token_accounts_max_pages: int = 5,
    token_accounts_max_accounts: int = 500,
) -> Path | None:
    configure_logging()
    load_env()

    tasks = []

    if largest_accounts:
        largest_path = largest_accounts_path(mint, save_dir)
        for item in normalize_schedule(schedule):
            tasks.append(
                snapshot_largest_accounts(
                    mint=mint,
                    path=largest_path,
                    snapshot_label=str(item["label"]),
                    delay_seconds=int(item["delay_seconds"]),
                )
            )

    if token_accounts and token_accounts != "disabled":
        token_path = token_accounts_path(mint, save_dir)
        token_schedule = token_accounts_schedule or DEFAULT_TOKEN_ACCOUNTS_SCHEDULE
        for item in normalize_schedule(token_schedule):
            tasks.append(
                snapshot_token_accounts(
                    mint=mint,
                    path=token_path,
                    snapshot_label=str(item["label"]),
                    delay_seconds=int(item["delay_seconds"]),
                    page_limit=int(token_accounts_page_limit),
                    max_pages=int(token_accounts_max_pages),
                    max_accounts=int(token_accounts_max_accounts),
                )
            )

    if not tasks:
        logger.info("Holder snapshots disabled for %s", mint)
        return None

    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Saved holder snapshots for %s", mint)
    return save_dir or ONCHAIN_DIR / mint


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True, help="Solana token mint address")
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    output_path = asyncio.run(main(mint=args.mint, save_dir=args.out_dir))
    print(json.dumps({"saved_to": str(output_path) if output_path else None}, indent=2))
