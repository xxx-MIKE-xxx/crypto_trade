from __future__ import annotations

import argparse
import asyncio
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from crypto_trade.core.env import load_env
from crypto_trade.core.io import append_jsonl, save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ONCHAIN_DIR
from crypto_trade.core.rpc import RPC
from crypto_trade.core.time import now_ms, now_ts

logger = logging.getLogger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"
OUTPUT_DIR = ONCHAIN_DIR


def response_row(name: str, response: Any) -> dict[str, Any]:
    return {
        "timestamp": now_ts(),
        "local_received_at_ms": now_ms(),
        "name": name,
        "http_status": response.http_status,
        "elapsed_ms": response.elapsed_ms,
        "rate_limit": response.rate_limit,
        "error_type": response.error_type,
        "error_message": response.error_message,
        "data": response.data,
    }


def merge_accounts(*groups: Iterable[str | None]) -> list[str]:
    accounts: list[str] = []

    for group in groups:
        for account in group:
            if account and account not in accounts:
                accounts.append(account)

    return accounts


def tx_account_keys(tx: dict[str, Any]) -> list[str]:
    keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    out: list[str] = []

    for key in keys:
        out.append(key.get("pubkey", "") if isinstance(key, dict) else str(key))

    loaded = tx.get("meta", {}).get("loadedAddresses") or {}
    out.extend(loaded.get("writable") or [])
    out.extend(loaded.get("readonly") or [])
    return out


def token_amount(balance: dict[str, Any] | None) -> str:
    if not balance:
        return "0"
    return str(balance.get("uiTokenAmount", {}).get("amount") or "0")


def score_vault_candidates(
    txs: list[dict[str, Any]],
    target_mint: str,
) -> dict[str, dict[str, int]]:
    scores: dict[str, dict[str, int]] = {
        target_mint: defaultdict(int),
        WSOL_MINT: defaultdict(int),
    }

    for tx in txs:
        keys = tx_account_keys(tx)
        meta = tx.get("meta") or {}

        pre_by_index = {
            b.get("accountIndex"): b
            for b in meta.get("preTokenBalances") or []
            if b.get("mint") in scores
        }

        for post in meta.get("postTokenBalances") or []:
            mint = post.get("mint")
            index = post.get("accountIndex")

            if mint not in scores or index is None or index >= len(keys):
                continue

            before = token_amount(pre_by_index.get(index))
            after = token_amount(post)

            if before == after:
                continue

            token_account = keys[index]
            if token_account:
                scores[mint][token_account] += 1

    return {
        mint: dict(sorted(accounts.items(), key=lambda x: x[1], reverse=True))
        for mint, accounts in scores.items()
    }


async def fetch_signatures_for_address(
    rpc: RPC,
    address: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    max_signatures: int = 1000,
) -> list[str]:
    signatures: list[str] = []
    before = None
    start_s = start_ms // 1000 if start_ms else None
    end_s = end_ms // 1000 if end_ms else None

    while len(signatures) < max_signatures:
        config: dict[str, Any] = {
            "limit": min(1000, max_signatures - len(signatures)),
            "commitment": "confirmed",
        }

        if before:
            config["before"] = before

        response = await rpc.call_rpc("getSignaturesForAddress", [address, config])
        rows = (response.data or {}).get("result") or []

        if not rows:
            break

        reached_before_window = False

        for row in rows:
            block_time = row.get("blockTime")
            signature = row.get("signature")

            if not signature or row.get("err") is not None:
                continue

            if end_s is not None and block_time is not None and block_time > end_s:
                continue

            if start_s is not None and block_time is not None and block_time < start_s:
                reached_before_window = True
                continue

            signatures.append(signature)

            if len(signatures) >= max_signatures:
                break

        before = rows[-1].get("signature")

        if reached_before_window or not before:
            break

    return signatures


async def fetch_transaction(rpc: RPC, signature: str) -> dict[str, Any] | None:
    response = await rpc.call_rpc(
        "getTransaction",
        [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    )
    return (response.data or {}).get("result")


async def infer_vaults(
    rpc: RPC,
    mint: str,
    pair_address: str,
    limit: int,
) -> dict[str, Any]:
    signatures = await fetch_signatures_for_address(
        rpc=rpc,
        address=pair_address,
        max_signatures=limit,
    )

    txs: list[dict[str, Any]] = []
    for signature in signatures:
        tx = await fetch_transaction(rpc, signature)
        if tx:
            txs.append(tx)

    scores = score_vault_candidates(txs, mint)
    token_candidates = scores.get(mint, {})
    sol_candidates = scores.get(WSOL_MINT, {})

    return {
        "mint": mint,
        "pair_address": pair_address,
        "pool_state": pair_address,
        "token_vault": next(iter(token_candidates), None),
        "sol_vault": next(iter(sol_candidates), None),
        "signatures_requested": limit,
        "signatures_found": len(signatures),
        "transactions_loaded": len(txs),
        "estimated_credits": 1 + len(signatures),
        "scores": scores,
    }


async def poll_rpc_methods(
    rpc: RPC,
    specs: list[dict[str, Any]],
    interval: int,
    capture_time: int,
) -> None:
    if not specs:
        return

    start = now_ms()

    while now_ms() - start < capture_time * 1000:
        for spec in specs:
            response = await rpc.call_rpc(spec["method"], spec["params"])
            append_jsonl(spec["path"], response_row(spec["method"], response))

        await asyncio.sleep(interval)


async def stream_account(
    rpc: RPC,
    path: Path,
    account: str,
    capture_time: int,
) -> None:
    params = [
        account,
        {
            "encoding": "base64",
            "commitment": "processed",
        },
    ]

    try:
        await asyncio.wait_for(
            rpc.connect_websocket(params, "accountSubscribe", path),
            timeout=capture_time,
        )
    except asyncio.TimeoutError:
        logger.info("Finished accountSubscribe for %s", account)


async def backfill_transactions(
    rpc: RPC,
    path: Path,
    addresses: list[str],
    start_ms: int,
    end_ms: int,
    max_signatures_per_address: int,
    max_transactions_total: int,
) -> None:
    signatures: dict[str, set[str]] = {}

    for address in addresses:
        found = await fetch_signatures_for_address(
            rpc=rpc,
            address=address,
            start_ms=start_ms,
            end_ms=end_ms,
            max_signatures=max_signatures_per_address,
        )

        for signature in found:
            signatures.setdefault(signature, set()).add(address)

    txs: dict[str, dict[str, Any]] = {}
    start_s = start_ms // 1000
    end_s = end_ms // 1000

    for signature, source_addresses in list(signatures.items())[:max_transactions_total]:
        tx = await fetch_transaction(rpc, signature)
        if not tx:
            continue

        block_time = tx.get("blockTime")
        if block_time is not None and not (start_s <= block_time <= end_s):
            continue

        tx["_source_addresses"] = sorted(source_addresses)
        txs[signature] = tx

    transactions = sorted(
        txs.values(),
        key=lambda tx: (
            tx.get("slot") or 10**20,
            tx.get("transactionIndex") or 10**20,
        ),
    )

    save_json(
        path,
        {
            "start_ms": start_ms,
            "end_ms": end_ms,
            "addresses": addresses,
            "signature_count": len(signatures),
            "transaction_count": len(transactions),
            "estimated_credits": len(addresses) + min(len(signatures), max_transactions_total),
            "max_signatures_per_address": max_signatures_per_address,
            "max_transactions_total": max_transactions_total,
            "transactions": transactions,
        },
    )


def build_poll_specs(
    priority_fee_path: Path,
    performance_samples_path: Path,
    simulated_transactions_path: Path,
    fee_accounts: list[str],
    simulate_tx_base64: str | None,
    performance_sample_limit: int,
) -> list[dict[str, Any]]:
    specs = []

    if fee_accounts:
        specs.append(
            {
                "method": "getPriorityFeeEstimate",
                "params": [
                    {
                        "accountKeys": fee_accounts,
                        "options": {
                            "priorityLevel": "High",
                            "includeAllPriorityFeeLevels": True,
                            "lookbackSlots": 150,
                        },
                    }
                ],
                "path": priority_fee_path,
            }
        )

    specs.append(
        {
            "method": "getRecentPerformanceSamples",
            "params": [max(1, min(performance_sample_limit, 720))],
            "path": performance_samples_path,
        }
    )

    if simulate_tx_base64:
        specs.append(
            {
                "method": "simulateTransaction",
                "params": [
                    simulate_tx_base64,
                    {
                        "encoding": "base64",
                        "commitment": "confirmed",
                        "sigVerify": False,
                        "replaceRecentBlockhash": True,
                    },
                ],
                "path": simulated_transactions_path,
            }
        )

    return specs


async def main(
    mint: str,
    capture_time: int,
    rpc_interval: int,
    watch_accounts: list[str],
    pair_address: str | None,
    infer_vaults_limit: int,
    simulate_tx_base64: str | None,
    performance_sample_limit: int,
    max_signatures_per_address: int,
    max_transactions_total: int,
    save_dir: Path | None = None,
    window_start_ms: int | None = None,
    pool_state: str | None = None,
    token_vault: str | None = None,
    sol_vault: str | None = None,
    backfill_on_cancel: bool = True,
) -> None:
    configure_logging()
    load_env()

    rpc = RPC()

    out = save_dir if save_dir is not None else OUTPUT_DIR / mint
    out.mkdir(parents=True, exist_ok=True)

    transactions_path = out / "transactions.json"
    account_state_path = out / "account_state.jsonl"
    priority_fee_path = out / "priority_fee.jsonl"
    performance_samples_path = out / "performance_samples.jsonl"
    simulated_transactions_path = out / "simulated_transactions.jsonl"
    vault_inference_path = out / "vault_inference.json"

    resolved_watch_accounts = merge_accounts(
        watch_accounts,
        [pool_state, token_vault, sol_vault],
    )

    if pair_address and not (token_vault and sol_vault):
        inferred = await infer_vaults(
            rpc=rpc,
            mint=mint,
            pair_address=pair_address,
            limit=infer_vaults_limit,
        )
        save_json(vault_inference_path, inferred)

        resolved_watch_accounts = merge_accounts(
            resolved_watch_accounts,
            [
                inferred.get("pool_state"),
                inferred.get("token_vault"),
                inferred.get("sol_vault"),
            ],
        )

    addresses = list(dict.fromkeys([mint, *resolved_watch_accounts]))
    fee_accounts = resolved_watch_accounts or [mint]
    start_ms = window_start_ms or now_ms()

    tasks = [
        poll_rpc_methods(
            rpc=rpc,
            specs=build_poll_specs(
                priority_fee_path=priority_fee_path,
                performance_samples_path=performance_samples_path,
                simulated_transactions_path=simulated_transactions_path,
                fee_accounts=fee_accounts,
                simulate_tx_base64=simulate_tx_base64,
                performance_sample_limit=performance_sample_limit,
            ),
            interval=rpc_interval,
            capture_time=capture_time,
        )
    ]

    for account in resolved_watch_accounts:
        tasks.append(
            stream_account(
                rpc=rpc,
                path=account_state_path,
                account=account,
                capture_time=capture_time,
            )
        )

    cancelled = False

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if not cancelled or backfill_on_cancel:
            end_ms = now_ms()

            await backfill_transactions(
                rpc=rpc,
                path=transactions_path,
                addresses=addresses,
                start_ms=start_ms,
                end_ms=end_ms,
                max_signatures_per_address=max_signatures_per_address,
                max_transactions_total=max_transactions_total,
            )

    logger.info("Saved Helius capture to %s", out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True, help="Solana token mint address")
    parser.add_argument("--pair-address", default=None, help="Pool/pair address used to infer vaults")
    parser.add_argument("--infer-vaults-limit", type=int, default=50)
    parser.add_argument(
        "--watch-account",
        action="append",
        default=[],
        help="Pool/vault/pool-state account. Repeat for multiple accounts.",
    )
    parser.add_argument("--pool-state", default=None)
    parser.add_argument("--token-vault", default=None)
    parser.add_argument("--sol-vault", default=None)
    parser.add_argument("--window-start-ms", type=int, default=None)
    parser.add_argument("--backfill-on-cancel", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--out-dir", type=Path, default=None)
    parser.add_argument("--capture-time", type=int, default=1800)
    parser.add_argument("--rpc-interval", type=int, default=60)
    parser.add_argument("--simulate-tx-base64", default=None)
    parser.add_argument("--performance-sample-limit", type=int, default=60)
    parser.add_argument("--max-signatures-per-address", type=int, default=1000)
    parser.add_argument("--max-transactions-total", type=int, default=3000)
    args = parser.parse_args()

    asyncio.run(
        main(
            mint=args.mint,
            capture_time=args.capture_time,
            rpc_interval=args.rpc_interval,
            watch_accounts=args.watch_account,
            pair_address=args.pair_address,
            infer_vaults_limit=args.infer_vaults_limit,
            simulate_tx_base64=args.simulate_tx_base64,
            performance_sample_limit=args.performance_sample_limit,
            max_signatures_per_address=args.max_signatures_per_address,
            max_transactions_total=args.max_transactions_total,
            save_dir=args.out_dir,
            window_start_ms=args.window_start_ms,
            pool_state=args.pool_state,
            token_vault=args.token_vault,
            sol_vault=args.sol_vault,
            backfill_on_cancel=args.backfill_on_cancel,
        )
    )