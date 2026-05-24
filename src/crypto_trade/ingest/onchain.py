#!/usr/bin/env python3
"""
solana_coin_1h_capture.py

Goal:
  Leave this running ~1 hour for ONE Solana coin/pair and collect as much raw data
  as possible for later backtest viability assessment.

What it does:
  - logsSubscribe for mint + pair/pool/watch addresses
  - getSignaturesForAddress polling/backfill
  - persistent signature queue
  - slow getTransaction fetcher with retries
  - rotates across multiple RPC HTTP URLs if provided
  - respects HTTP 429 Retry-After when present
  - accountSubscribe for watched addresses
  - periodic getMultipleAccounts snapshots
  - discovers token accounts from fetched tx pre/post token balances
  - writes a final viability_report.json

Install:
  pip install requests websockets

Public RPC, 1 hour:
  python -m crypto_trade.ingest.onchain ^
    --mint 33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump ^
    --pair ETMhxtENfkMK85TAcveEbZdBv9htziWzDSddmShRP2wB ^
    --out ./capture ^
    --duration-seconds 3600

Recommended with Helius free RPC:
  set HELIUS_API_KEY=YOUR_KEY
  python -m crypto_trade.ingest.onchain ^
    --mint 33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump ^
    --pair ETMhxtENfkMK85TAcveEbZdBv9htziWzDSddmShRP2wB ^
    --rpc "https://mainnet.helius-rpc.com/?api-key=%HELIUS_API_KEY%" ^
    --ws "wss://mainnet.helius-rpc.com/?api-key=%HELIUS_API_KEY%" ^
    --out ./capture ^
    --duration-seconds 3600

Outputs:
  manifest.json
  signatures_seen.jsonl
  raw_transactions.jsonl
  tx_index.csv
  failed_attempts.jsonl
  account_notifications.jsonl
  account_snapshots.jsonl
  discovered_token_accounts.jsonl
  ws_log_notifications.jsonl
  viability_report.json

Important:
  For reconstruction, raw_transactions.jsonl is the source of truth.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from dotenv import load_dotenv

import websockets

from crypto_trade.core.io import (
    append_csv,
    append_jsonl,
    chunked,
    ensure_dir,
    iter_jsonl,
    read_csv_col,
    save_json,
)
from crypto_trade.core.logging import configure_logging
from crypto_trade.core.rpc import RpcPool, short_rpc_name
from crypto_trade.core.time import now_iso, now_ts, utc_now
from crypto_trade.ingest.solana_rpc import (
    PUBLIC_RPC,
    PUBLIC_WS,
    USDC_MINT,
    WSOL_MINT,
    get_multiple_accounts,
    get_signatures_for_address,
    get_transaction,
)
from crypto_trade.ingest.solana_tx import (
    discover_token_accounts_from_tx,
    summarize_tx_for_mint,
)





class Capture:
    def __init__(
        self,
        mint: str,
        watch_addresses: List[str],
        quote_mints: Set[str],
        rpc_urls: List[str],
        ws_url: str,
        out_dir: Path,
        duration_seconds: int,
        rpc_min_interval: float,
        tx_retry_seconds: float,
        max_tx_attempts: int,
        poll_seconds: float,
        backfill_limit: int,
        snapshot_seconds: float,
        max_queue_size: int,
        debug: bool,
    ):
        self.mint = mint
        self.watch_addresses = watch_addresses
        self.quote_mints = quote_mints
        self.out_dir = out_dir
        self.duration_seconds = duration_seconds
        self.ws_url = ws_url
        self.debug = debug

        self.rpc_pool = RpcPool(
            urls=rpc_urls,
            min_interval=rpc_min_interval,
            debug=debug,
            default_url=PUBLIC_RPC,
        )
        self.tx_retry_seconds = tx_retry_seconds
        self.max_tx_attempts = max_tx_attempts
        self.poll_seconds = poll_seconds
        self.backfill_limit = backfill_limit
        self.snapshot_seconds = snapshot_seconds
        self.max_queue_size = max_queue_size

        self.stop_event = asyncio.Event()
        self.started_at = now_ts()

        self.paths = {
            "manifest": out_dir / "manifest.json",
            "signatures": out_dir / "signatures_seen.jsonl",
            "raw": out_dir / "raw_transactions.jsonl",
            "failed": out_dir / "failed_attempts.jsonl",
            "tx_index": out_dir / "tx_index.csv",
            "ws_logs": out_dir / "ws_log_notifications.jsonl",
            "account_notifications": out_dir / "account_notifications.jsonl",
            "snapshots": out_dir / "account_snapshots.jsonl",
            "discovered_accounts": out_dir / "discovered_token_accounts.jsonl",
            "report": out_dir / "viability_report.json",
        }

        self.csv_fields = [
            "fetched_at", "signature", "attempt", "rpc", "slot", "block_time", "iso_time",
            "err", "seen_by", "mentions_mint_in_balances", "mint_accounts_changed",
            "mint_balance_delta_sum", "fee_lamports", "has_inner_instructions", "log_count",
        ]

        self.seen_sigs: Set[str] = set()
        self.fetched_sigs: Set[str] = set()
        self.signature_sources: Dict[str, Set[str]] = {}
        self.attempts: Dict[str, int] = {}
        self.pending: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue_size)

        self.snapshot_accounts: Set[str] = set(watch_addresses)
        self.discovered_token_accounts: Set[str] = set()

        self.stats = {
            "signatures_seen": 0,
            "signatures_new_this_run": 0,
            "transactions_fetched": 0,
            "transactions_failed": 0,
            "transactions_success_err_none": 0,
            "transactions_failed_onchain_err": 0,
            "account_notifications": 0,
            "account_snapshots": 0,
            "discovered_token_accounts": 0,
        }

        self.load_existing_state()

    def load_existing_state(self) -> None:
        for obj in iter_jsonl(self.paths["signatures"]) or []:
            sig = obj.get("signature")
            if sig:
                self.seen_sigs.add(sig)
                src = obj.get("source") or obj.get("seen_by") or "existing"
                self.signature_sources.setdefault(sig, set()).add(str(src))

        self.fetched_sigs |= read_csv_col(self.paths["tx_index"], "signature")

        for obj in iter_jsonl(self.paths["discovered_accounts"]) or []:
            acct = obj.get("token_account")
            if acct:
                self.discovered_token_accounts.add(acct)
                self.snapshot_accounts.add(acct)

    def write_manifest(self, rpc_urls: List[str]) -> None:
        save_json(self.paths["manifest"], {
            "created_at": now_iso(),
            "mint": self.mint,
            "watch_addresses": self.watch_addresses,
            "quote_mints": sorted(self.quote_mints),
            "rpc_urls": rpc_urls,
            "ws_url": self.ws_url,
            "duration_seconds": self.duration_seconds,
            "poll_seconds": self.poll_seconds,
            "backfill_limit": self.backfill_limit,
            "snapshot_seconds": self.snapshot_seconds,
            "tx_retry_seconds": self.tx_retry_seconds,
            "max_tx_attempts": self.max_tx_attempts,
            "outputs": {k: str(v.name) for k, v in self.paths.items()},
        })

    async def add_signature(self, sig: str, seen_by: str, source: str) -> None:
        if not sig:
            return

        self.signature_sources.setdefault(sig, set()).add(seen_by)

        is_new = sig not in self.seen_sigs
        if is_new:
            self.seen_sigs.add(sig)
            self.stats["signatures_new_this_run"] += 1
            append_jsonl(self.paths["signatures"], {
                "first_seen_at": now_iso(),
                "signature": sig,
                "seen_by": seen_by,
                "source": source,
            })

        if sig not in self.fetched_sigs and self.attempts.get(sig, 0) < self.max_tx_attempts:
            try:
                self.pending.put_nowait(sig)
            except asyncio.QueueFull:
                # Queue full is acceptable for high-volume coins; poller and retry will re-add later.
                append_jsonl(self.paths["failed"], {
                    "failed_at": now_iso(),
                    "signature": sig,
                    "error": "LOCAL_QUEUE_FULL",
                    "will_retry": True,
                })

    async def websocket_listener(self) -> None:
        """WebSocket does discovery only; if it disconnects, the poller covers gaps."""
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    print(f"[ws] connected {self.ws_url}")
                    pending_ids: Dict[int, Tuple[str, str]] = {}
                    req_id = 1

                    for addr in self.watch_addresses:
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "logsSubscribe",
                            "params": [
                                {"mentions": [addr]},
                                {"commitment": "confirmed"},
                            ],
                        }))
                        pending_ids[req_id] = ("logs", addr)
                        req_id += 1

                    for addr in self.watch_addresses:
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "accountSubscribe",
                            "params": [
                                addr,
                                {
                                    "encoding": "base64",
                                    "commitment": "confirmed",
                                },
                            ],
                        }))
                        pending_ids[req_id] = ("account", addr)
                        req_id += 1

                    sub_id_to_target: Dict[int, Tuple[str, str]] = {}

                    while not self.stop_event.is_set():
                        msg = json.loads(await ws.recv())

                        if "result" in msg and "id" in msg:
                            target = pending_ids.get(int(msg["id"]))
                            if target:
                                sub_id_to_target[int(msg["result"])] = target
                                print(f"[ws] subscribed {target[0]} {target[1]}")
                            continue

                        params = msg.get("params") or {}
                        sub_id = params.get("subscription")
                        kind, addr = sub_id_to_target.get(sub_id, ("unknown", f"sub_{sub_id}"))

                        if msg.get("method") == "logsNotification":
                            append_jsonl(self.paths["ws_logs"], {
                                "received_at": now_iso(),
                                "address": addr,
                                "message": msg,
                            })
                            value = (params.get("result") or {}).get("value") or {}
                            sig = value.get("signature")
                            if sig:
                                await self.add_signature(sig, addr, "websocket_logs")

                        elif msg.get("method") == "accountNotification":
                            self.stats["account_notifications"] += 1
                            append_jsonl(self.paths["account_notifications"], {
                                "received_at": now_iso(),
                                "address": addr,
                                "message": msg,
                            })

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[ws] error {e!r}; reconnect in 5s")
                await asyncio.sleep(5)

    async def poller(self) -> None:
        while not self.stop_event.is_set():
            for addr in self.watch_addresses:
                res, rpc_name = await asyncio.to_thread(
                    get_signatures_for_address, self.rpc_pool, addr, self.backfill_limit
                )
                if not res.ok:
                    append_jsonl(self.paths["failed"], {
                        "failed_at": now_iso(),
                        "signature": None,
                        "operation": "getSignaturesForAddress",
                        "address": addr,
                        "rpc": rpc_name,
                        "error": res.error,
                        "will_retry": True,
                    })
                    continue

                for row in res.result or []:
                    sig = row.get("signature")
                    if sig:
                        await self.add_signature(sig, addr, "poll_getSignaturesForAddress")

            await asyncio.sleep(self.poll_seconds)

    async def fetcher(self) -> None:
        while not self.stop_event.is_set():
            try:
                sig = await asyncio.wait_for(self.pending.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if sig in self.fetched_sigs:
                self.pending.task_done()
                continue

            attempt = self.attempts.get(sig, 0) + 1
            self.attempts[sig] = attempt

            res, rpc_name = await asyncio.to_thread(get_transaction, self.rpc_pool, sig)

            if res.ok:
                tx = res.result
                append_jsonl(self.paths["raw"], {
                    "fetched_at": now_iso(),
                    "signature": sig,
                    "attempt": attempt,
                    "rpc": rpc_name,
                    "seen_by": sorted(self.signature_sources.get(sig, set())),
                    "transaction": tx,
                })

                row = summarize_tx_for_mint(
                    sig, tx, self.mint, self.signature_sources.get(sig, set()), rpc_name, attempt
                )
                append_csv(self.paths["tx_index"], row, self.csv_fields)

                self.fetched_sigs.add(sig)
                self.stats["transactions_fetched"] += 1

                if row["err"]:
                    self.stats["transactions_failed_onchain_err"] += 1
                else:
                    self.stats["transactions_success_err_none"] += 1

                for acct in discover_token_accounts_from_tx(tx, self.mint, self.quote_mints):
                    token_account = acct.get("token_account")
                    if token_account and token_account not in self.discovered_token_accounts:
                        self.discovered_token_accounts.add(token_account)
                        self.snapshot_accounts.add(token_account)
                        self.stats["discovered_token_accounts"] += 1
                        append_jsonl(self.paths["discovered_accounts"], acct)

                if self.stats["transactions_fetched"] % 10 == 0:
                    print(
                        f"[fetch] fetched={self.stats['transactions_fetched']} "
                        f"queue={self.pending.qsize()} seen={len(self.seen_sigs)}"
                    )

            else:
                self.stats["transactions_failed"] += 1
                will_retry = attempt < self.max_tx_attempts
                append_jsonl(self.paths["failed"], {
                    "failed_at": now_iso(),
                    "signature": sig,
                    "attempt": attempt,
                    "operation": "getTransaction",
                    "rpc": rpc_name,
                    "error": res.error,
                    "retry_after": res.retry_after,
                    "will_retry": will_retry,
                })

                if will_retry:
                    delay = res.retry_after if res.retry_after is not None else self.tx_retry_seconds
                    asyncio.create_task(self.requeue_later(sig, delay))

            self.pending.task_done()

    async def requeue_later(self, sig: str, delay: float) -> None:
        await asyncio.sleep(max(1.0, delay))
        if not self.stop_event.is_set() and sig not in self.fetched_sigs:
            try:
                self.pending.put_nowait(sig)
            except asyncio.QueueFull:
                append_jsonl(self.paths["failed"], {
                    "failed_at": now_iso(),
                    "signature": sig,
                    "error": "LOCAL_QUEUE_FULL_ON_REQUEUE",
                    "will_retry": True,
                })

    async def snapshotter(self) -> None:
        while not self.stop_event.is_set():
            accounts = sorted(self.snapshot_accounts)
            for batch in chunked(accounts, 100):
                res, rpc_name = await asyncio.to_thread(
                    get_multiple_accounts, self.rpc_pool, batch
                )
                if res.ok:
                    self.stats["account_snapshots"] += 1
                    append_jsonl(self.paths["snapshots"], {
                        "snapshot_at": now_iso(),
                        "rpc": rpc_name,
                        "accounts": batch,
                        "result": res.result,
                    })
                else:
                    append_jsonl(self.paths["failed"], {
                        "failed_at": now_iso(),
                        "signature": None,
                        "operation": "getMultipleAccounts",
                        "rpc": rpc_name,
                        "error": res.error,
                        "will_retry": True,
                    })

            await asyncio.sleep(self.snapshot_seconds)

    async def reporter(self) -> None:
        while not self.stop_event.is_set():
            self.write_report(partial=True)
            await asyncio.sleep(60)

    def write_report(self, partial: bool = False) -> Dict[str, Any]:
        elapsed = max(1.0, now_ts() - self.started_at)
        fetched = len(self.fetched_sigs)
        seen = len(self.seen_sigs)
        coverage = fetched / seen if seen else 0.0

        report = {
            "generated_at": now_iso(),
            "partial": partial,
            "elapsed_seconds": elapsed,
            "mint": self.mint,
            "watch_addresses": self.watch_addresses,
            "signatures_seen_total": seen,
            "signatures_fetched_total": fetched,
            "raw_coverage_ratio": coverage,
            "pending_queue_size": self.pending.qsize(),
            "discovered_token_accounts": len(self.discovered_token_accounts),
            "snapshot_accounts": len(self.snapshot_accounts),
            "stats_this_run": self.stats,
            "rpc_stats": self.rpc_pool.stats(),
            "viability": {
                "usable_for_rough_timeline": coverage >= 0.5 and fetched >= 20,
                "usable_for_serious_backtest": coverage >= 0.9 and fetched >= 50,
                "reason": (
                    "Good raw transaction coverage."
                    if coverage >= 0.9 and fetched >= 50
                    else "Need higher raw transaction coverage; keep drain running or use a better RPC."
                ),
            },
        }
        save_json(self.paths["report"], report)
        return report

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self.websocket_listener()),
            asyncio.create_task(self.poller()),
            asyncio.create_task(self.fetcher()),
            asyncio.create_task(self.snapshotter()),
            asyncio.create_task(self.reporter()),
        ]

        async def stop_after() -> None:
            await asyncio.sleep(self.duration_seconds)
            self.stop_event.set()

        tasks.append(asyncio.create_task(stop_after()))

        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.5)
        finally:
            print("[main] stopping; draining queue briefly...")
            # Give fetcher a brief final chance, but do not hang forever.
            stop_deadline = now_ts() + 15
            while not self.pending.empty() and now_ts() < stop_deadline:
                await asyncio.sleep(0.5)

            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            report = self.write_report(partial=False)
            print(json.dumps(report["viability"], indent=2))


async def async_main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True, help="Target token mint")
    parser.add_argument("--pair", action="append", default=[], help="Pair/pool address to watch; can pass multiple")
    parser.add_argument("--watch", action="append", default=[], help="Extra address to watch; can pass multiple")
    parser.add_argument("--out", default="./capture")
    parser.add_argument("--duration-seconds", type=int, default=3600)

    parser.add_argument("--rpc", action="append", default=[], help="HTTP RPC URL; can pass multiple")
    parser.add_argument("--ws", default=os.getenv("SOLANA_WS_URL", PUBLIC_WS), help="WebSocket RPC URL")
    parser.add_argument("--rpc-min-interval", type=float, default=1.1, help="Minimum seconds between calls per RPC URL")
    parser.add_argument("--tx-retry-seconds", type=float, default=20.0)
    parser.add_argument("--max-tx-attempts", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=20.0)
    parser.add_argument("--backfill-limit", type=int, default=5)
    parser.add_argument("--snapshot-seconds", type=float, default=60.0)
    parser.add_argument("--max-queue-size", type=int, default=100000)
    parser.add_argument("--quote-mint", action="append", default=[WSOL_MINT, USDC_MINT])
    parser.add_argument("--debug", action="store_true")

    args = parser.parse_args()

    rpc_urls = args.rpc or []
    env_rpc = os.getenv("SOLANA_RPC_URLS")
    if env_rpc:
        rpc_urls.extend([x.strip() for x in env_rpc.split(",") if x.strip()])
    if not rpc_urls:
        rpc_urls = [PUBLIC_RPC]

    watch_addresses: List[str] = []
    for a in [args.mint] + args.pair + args.watch:
        if a and a not in watch_addresses:
            watch_addresses.append(a)

    out_dir = Path(args.out) / args.mint / utc_now().strftime("%Y%m%d_%H%M%S")
    ensure_dir(out_dir)

    capture = Capture(
        mint=args.mint,
        watch_addresses=watch_addresses,
        quote_mints=set(args.quote_mint),
        rpc_urls=rpc_urls,
        ws_url=args.ws,
        out_dir=out_dir,
        duration_seconds=args.duration_seconds,
        rpc_min_interval=args.rpc_min_interval,
        tx_retry_seconds=args.tx_retry_seconds,
        max_tx_attempts=args.max_tx_attempts,
        poll_seconds=args.poll_seconds,
        backfill_limit=args.backfill_limit,
        snapshot_seconds=args.snapshot_seconds,
        max_queue_size=args.max_queue_size,
        debug=args.debug,
    )
    capture.write_manifest(rpc_urls)

    print("Output:", out_dir)
    print("Watch addresses:")
    for a in watch_addresses:
        print(" ", a)
    print("HTTP RPC endpoints:")
    for u in rpc_urls:
        print(" ", short_rpc_name(u), u)
    print("WS:", args.ws)

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, sig_name), capture.stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    try:
        await capture.run()
    except KeyboardInterrupt:
        capture.stop_event.set()
        capture.write_report(partial=False)

    print("Done. Check viability_report.json")
    return 0


def main() -> int:
    configure_logging()
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
