from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from crypto_trade.core.env import load_env
from crypto_trade.core.io import append_jsonl, chunked, save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import RAW_DIR
from crypto_trade.core.time import now_ms, now_ts, utc_now_iso_ms_z
from crypto_trade.ingest import dexscreener
from crypto_trade.ingest.pumpportal import listen

logger = logging.getLogger(__name__)

DEX_FILENAME = "dexscreener_pumpportal.jsonl"
STATE_FILENAME = "pumpportal_listener_state.json"


def day() -> str:
    return utc_now_iso_ms_z()[:10]


def events_path(save_root: Path) -> Path:
    return save_root / "migrations" / f"{day()}.jsonl"


def dex_batch_path(save_root: Path) -> Path:
    return save_root / "onchain" / "dexscreener_pumpportal" / f"{day()}.jsonl"


def dex_mint_path(save_root: Path, mint: str) -> Path:
    return save_root / "onchain" / mint / DEX_FILENAME


def state_path(save_root: Path) -> Path:
    return save_root / "migrations" / STATE_FILENAME


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "seen_mints": [],
            "migrated_mints": [],
            "tracked_until_ms": {},
        }

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(save_root: Path, seen: set[str], migrated: set[str], tracked_until: dict[str, int]) -> None:
    save_json(
        state_path(save_root),
        {
            "updated_at": utc_now_iso_ms_z(),
            "seen_mints": sorted(seen),
            "migrated_mints": sorted(migrated),
            "tracked_until_ms": dict(sorted(tracked_until.items())),
        },
    )


def event_row(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "time": utc_now_iso_ms_z(),
        "local_received_at_ms": now_ms(),
        "event_type": event.get("type"),
        "mint": event.get("mint"),
        "source": "pumpportal",
        "event": event,
    }


def filter_pairs_for_mint(data: Any, mint: str) -> Any:
    if not isinstance(data, list):
        return data

    out = []
    for pair in data:
        if not isinstance(pair, dict):
            continue

        base = (pair.get("baseToken") or {}).get("address")
        quote = (pair.get("quoteToken") or {}).get("address")

        if mint in {base, quote}:
            out.append(pair)

    return out


async def poll_dexscreener(
    *,
    save_root: Path,
    tracked_until: dict[str, int],
    lock: asyncio.Lock,
    interval: int,
    batch_size: int,
) -> None:
    while True:
        current_ms = now_ms()

        async with lock:
            expired = [mint for mint, until_ms in tracked_until.items() if until_ms <= current_ms]
            for mint in expired:
                tracked_until.pop(mint, None)

            mints = sorted(tracked_until)

        if not mints:
            await asyncio.sleep(interval)
            continue

        for batch in chunked(mints, batch_size):
            response = await dexscreener.transactions_multiple_tokens(*batch)
            row = {
                "timestamp": now_ts(),
                "local_received_at_ms": now_ms(),
                "source": "dexscreener",
                "method": "tokens-v1",
                "mints": batch,
                "http_status": response.http_status,
                "elapsed_ms": response.elapsed_ms,
                "rate_limit": response.rate_limit,
                "error_type": response.error_type,
                "error_message": response.error_message,
                "data": response.data,
            }

            append_jsonl(dex_batch_path(save_root), row)

            for mint in batch:
                append_jsonl(
                    dex_mint_path(save_root, mint),
                    {
                        **row,
                        "mint": mint,
                        "data": filter_pairs_for_mint(response.data, mint),
                    },
                )

        await asyncio.sleep(interval)


async def main(args: argparse.Namespace) -> None:
    configure_logging()
    load_env()

    save_root = args.save_root
    state = load_state(state_path(save_root))

    seen_mints = set(state.get("seen_mints") or [])
    migrated_mints = set(state.get("migrated_mints") or [])
    tracked_until = {str(k): int(v) for k, v in (state.get("tracked_until_ms") or {}).items()}
    lock = asyncio.Lock()

    dex_task = None
    if not args.no_dex:
        dex_task = asyncio.create_task(
            poll_dexscreener(
                save_root=save_root,
                tracked_until=tracked_until,
                lock=lock,
                interval=args.dex_interval,
                batch_size=args.dex_batch_size,
            )
        )

    try:
        async for event in listen(mints=True, migrations=True, url=args.pumpportal_url):
            mint = event.get("mint")
            event_type = event.get("type")

            append_jsonl(events_path(save_root), event_row(event))

            if not mint:
                continue

            should_track = False

            if event_type == "mint":
                seen_mints.add(mint)
                should_track = args.dex_on_mint

            elif event_type == "migration":
                migrated_mints.add(mint)
                should_track = not args.require_seen_mint or mint in seen_mints

                if not should_track:
                    logger.info("Skipping DexScreener tracking for unseen migrated mint %s", mint)

            if should_track and not args.no_dex:
                async with lock:
                    tracked_until[mint] = max(
                        tracked_until.get(mint, 0),
                        now_ms() + args.dex_length * 1000,
                    )

                logger.info("Tracking DexScreener for %s after %s", mint, event_type)

            save_state(save_root, seen_mints, migrated_mints, tracked_until)

    finally:
        save_state(save_root, seen_mints, migrated_mints, tracked_until)
        if dex_task:
            dex_task.cancel()
            await asyncio.gather(dex_task, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Listen to PumpPortal and poll DexScreener for migrated coins.")
    parser.add_argument("--save-root", type=Path, default=RAW_DIR)
    parser.add_argument("--pumpportal-url", default=None)
    parser.add_argument("--dex-interval", type=int, default=60)
    parser.add_argument("--dex-length", type=int, default=24 * 60 * 60)
    parser.add_argument("--dex-batch-size", type=int, default=30)
    parser.add_argument("--dex-on-mint", action="store_true", help="Also poll DexScreener from mint/create events. Usually empty pre-migration.")
    parser.add_argument("--require-seen-mint", action="store_true", help="Only poll DexScreener for migrations whose mint event was seen by this script.")
    parser.add_argument("--no-dex", action="store_true")

    asyncio.run(main(parser.parse_args()))
