from __future__ import annotations

import argparse
import asyncio
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from crypto_trade.core.env import load_env
from crypto_trade.core.io import append_jsonl, chunked
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import RAW_DIR
from crypto_trade.core.text import compact_json_dumps, short_hash
from crypto_trade.core.time import now_ms, now_ts, utc_now_iso_ms_z
from crypto_trade.ingest import dexscreener
from crypto_trade.ingest.pumpportal import listen

ROOT = RAW_DIR / "premigration"
WSOL_MINT = "So11111111111111111111111111111111111111112"


def date_key() -> str:
    return utc_now_iso_ms_z()[:10]


def raw_event_path(root: Path) -> Path:
    return root / "pumpportal" / f"{date_key()}.jsonl"


def dex_path(root: Path) -> Path:
    return root / "dexscreener" / f"{date_key()}.jsonl"


def mint_dex_path(root: Path, mint: str) -> Path:
    return root / "dexscreener_by_mint" / mint / f"{date_key()}.jsonl"


def pair_mints(pair: dict[str, Any]) -> set[str]:
    return {
        str((pair.get("baseToken") or {}).get("address") or ""),
        str((pair.get("quoteToken") or {}).get("address") or ""),
    }


def pairs_for_mint(data: Any, mint: str) -> list[dict[str, Any]]:
    if not isinstance(data, list):
        return []
    return [pair for pair in data if isinstance(pair, dict) and mint in pair_mints(pair)]


def pair_score(pairs: list[dict[str, Any]]) -> float:
    if not pairs:
        return 0.0

    vol5 = max(float((p.get("volume") or {}).get("m5") or 0) for p in pairs)
    liq = max(float((p.get("liquidity") or {}).get("usd") or 0) for p in pairs)
    tx5 = max(
        float(((p.get("txns") or {}).get("m5") or {}).get("buys") or 0)
        + float(((p.get("txns") or {}).get("m5") or {}).get("sells") or 0)
        for p in pairs
    )

    score = (
        0.45 * math.log1p(vol5) / math.log1p(10_000)
        + 0.35 * math.log1p(tx5) / math.log1p(200)
        + 0.20 * math.log1p(liq) / math.log1p(100_000)
    )
    return max(0.0, min(score, 1.0))


def next_interval_ms(score: float, min_s: int, max_s: int) -> int:
    return int((max_s - (max_s - min_s) * score) * 1000)


class PreMigrationState:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS mints (
                mint TEXT PRIMARY KEY,
                first_seen_ts TEXT NOT NULL,
                first_seen_ms INTEGER NOT NULL,
                migrated_ts TEXT,
                status TEXT NOT NULL,
                last_dex_poll_ms INTEGER,
                next_dex_poll_ms INTEGER NOT NULL,
                dex_polls INTEGER NOT NULL DEFAULT 0,
                empty_polls INTEGER NOT NULL DEFAULT 0,
                priority_score REAL NOT NULL DEFAULT 0,
                dead_reason TEXT,
                updated_ts TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                event_hash TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                mint TEXT,
                event_ts TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                ingest_ts TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_mints_due ON mints(status, next_dex_poll_ms, priority_score);
            """
        )

    def record_event(self, event: dict[str, Any]) -> bool:
        event_hash = short_hash(compact_json_dumps(event))
        try:
            self.conn.execute(
                """
                INSERT INTO events(event_hash, event_type, mint, event_ts, payload_json, ingest_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_hash,
                    event.get("type"),
                    event.get("mint"),
                    event.get("time") or utc_now_iso_ms_z(),
                    compact_json_dumps(event),
                    utc_now_iso_ms_z(),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def upsert_mint(self, mint: str) -> None:
        ts = utc_now_iso_ms_z()
        ms = now_ms()
        self.conn.execute(
            """
            INSERT INTO mints(mint, first_seen_ts, first_seen_ms, status, next_dex_poll_ms, updated_ts)
            VALUES (?, ?, ?, 'active', ?, ?)
            ON CONFLICT(mint) DO UPDATE SET
                next_dex_poll_ms = MIN(mints.next_dex_poll_ms, excluded.next_dex_poll_ms),
                updated_ts = excluded.updated_ts
            """,
            (mint, ts, ms, ms, ts),
        )

    def mark_migrated(self, mint: str) -> None:
        self.upsert_mint(mint)
        self.conn.execute(
            """
            UPDATE mints
            SET migrated_ts = COALESCE(migrated_ts, ?),
                status = 'migrated',
                next_dex_poll_ms = ?,
                updated_ts = ?
            WHERE mint = ?
            """,
            (utc_now_iso_ms_z(), now_ms(), utc_now_iso_ms_z(), mint),
        )

    def due_mints(self, limit: int) -> list[str]:
        rows = self.conn.execute(
            """
            SELECT mint FROM mints
            WHERE status NOT IN ('dead', 'expired')
              AND next_dex_poll_ms <= ?
            ORDER BY last_dex_poll_ms IS NOT NULL, next_dex_poll_ms, priority_score DESC
            LIMIT ?
            """,
            (now_ms(), limit),
        ).fetchall()
        return [row[0] for row in rows]

    def save_snapshot(
        self,
        mint: str,
        pairs: list[dict[str, Any]],
        *,
        min_poll_s: int,
        max_poll_s: int,
        no_pair_dead_ms: int,
        max_track_ms: int,
    ) -> None:
        row = self.conn.execute(
            "SELECT first_seen_ms, migrated_ts, empty_polls FROM mints WHERE mint = ?",
            (mint,),
        ).fetchone()
        if not row:
            return

        current_ms = now_ms()
        age_ms = current_ms - int(row[0])
        migrated = row[1] is not None
        empty_polls = 0 if pairs else int(row[2]) + 1
        score = pair_score(pairs)
        status = "migrated" if migrated else "active"
        dead_reason = None
        next_poll_ms = current_ms + next_interval_ms(score, min_poll_s, max_poll_s)

        if age_ms >= max_track_ms:
            status, next_poll_ms, dead_reason = "expired", 0, "max_track_hours"
        elif not pairs and not migrated and age_ms >= no_pair_dead_ms:
            status, next_poll_ms, dead_reason = "dead", 0, "no_pair_after_threshold"

        self.conn.execute(
            """
            UPDATE mints
            SET status = ?, last_dex_poll_ms = ?, next_dex_poll_ms = ?,
                dex_polls = dex_polls + 1, empty_polls = ?, priority_score = ?,
                dead_reason = COALESCE(?, dead_reason), updated_ts = ?
            WHERE mint = ?
            """,
            (status, current_ms, next_poll_ms, empty_polls, score, dead_reason, utc_now_iso_ms_z(), mint),
        )


async def pumpportal_loop(state: PreMigrationState, root: Path, url: str | None) -> None:
    async for event in listen(mints=True, migrations=True, url=url):
        append_jsonl(raw_event_path(root), event)
        if not state.record_event(event):
            continue

        mint = event.get("mint")
        if not mint:
            continue
        if event.get("type") == "migration":
            state.mark_migrated(mint)
        else:
            state.upsert_mint(mint)


async def dexscreener_loop(args: argparse.Namespace, state: PreMigrationState) -> None:
    batch_size = min(args.batch_size, 30)
    request_delay = 60 / args.requests_per_minute

    while True:
        mints = state.due_mints(batch_size)
        if not mints:
            await asyncio.sleep(args.idle_sleep)
            continue

        response = await dexscreener.transactions_multiple_tokens(*mints)
        row = {
            "timestamp": now_ts(),
            "local_received_at_ms": now_ms(),
            "source": "dexscreener",
            "method": "tokens-v1",
            "mints": mints,
            "http_status": response.http_status,
            "elapsed_ms": response.elapsed_ms,
            "rate_limit": response.rate_limit,
            "error_type": response.error_type,
            "error_message": response.error_message,
            "data": response.data,
        }
        append_jsonl(dex_path(args.root), row)

        for mint in mints:
            pairs = pairs_for_mint(response.data, mint)
            append_jsonl(mint_dex_path(args.root, mint), {**row, "mint": mint, "data": pairs})
            state.save_snapshot(
                mint,
                pairs,
                min_poll_s=args.min_poll_s,
                max_poll_s=args.max_poll_s,
                no_pair_dead_ms=args.no_pair_dead_minutes * 60_000,
                max_track_ms=args.max_track_hours * 60 * 60_000,
            )

        await asyncio.sleep(request_delay)


async def main(args: argparse.Namespace) -> None:
    configure_logging()
    load_env()

    state = PreMigrationState(args.root / "state.sqlite3")
    await asyncio.gather(
        pumpportal_loop(state, args.root, args.pumpportal_url),
        dexscreener_loop(args, state),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--pumpportal-url", default=None)
    parser.add_argument("--batch-size", type=int, default=30)
    parser.add_argument("--requests-per-minute", type=int, default=240)
    parser.add_argument("--idle-sleep", type=int, default=2)
    parser.add_argument("--min-poll-s", type=int, default=10)
    parser.add_argument("--max-poll-s", type=int, default=300)
    parser.add_argument("--no-pair-dead-minutes", type=int, default=60)
    parser.add_argument("--max-track-hours", type=int, default=24)
    asyncio.run(main(parser.parse_args()))
