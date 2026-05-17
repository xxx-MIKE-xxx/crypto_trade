#!/usr/bin/env python3
"""
test_pumpportal_ws_env.py

Reads PumpPortal API key from local .env in the same folder.

Create .env next to this script:
  PUMPPORTAL_API_KEY=your_key_here

Run:
  pip install websockets
  python test_pumpportal_ws_env.py
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets


ENV_FILE = ".env"
DURATION_SECONDS = 120

RAW_SUBDIR = Path("data") / "raw" / "pump_portal"
SQLITE_PATH = Path("data") / "pipeline_state.sqlite3"


@dataclass(frozen=True)
class EventMeta:
    received_at_ms: int
    event_date: str
    event_type: str
    mint: str | None
    signature: str | None


class PumpPortalStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")

        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pumpportal_event_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at_ms INTEGER NOT NULL,
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                mint TEXT,
                signature TEXT,
                raw_file TEXT NOT NULL,
                raw_line INTEGER NOT NULL,
                UNIQUE(raw_file, raw_line)
            );

            CREATE INDEX IF NOT EXISTS idx_pumpportal_event_type_date
            ON pumpportal_event_index(event_type, event_date);

            CREATE INDEX IF NOT EXISTS idx_pumpportal_event_mint
            ON pumpportal_event_index(mint);

            CREATE TABLE IF NOT EXISTS pumpportal_mint_status (
                mint TEXT PRIMARY KEY,
                seen_new_token INTEGER NOT NULL DEFAULT 0,
                migrated INTEGER NOT NULL DEFAULT 0,
                first_seen_at_ms INTEGER,
                migrated_at_ms INTEGER,
                last_event_at_ms INTEGER NOT NULL,
                new_token_event_id INTEGER,
                migration_event_id INTEGER,
                FOREIGN KEY(new_token_event_id)
                    REFERENCES pumpportal_event_index(id),
                FOREIGN KEY(migration_event_id)
                    REFERENCES pumpportal_event_index(id)
            );

            CREATE INDEX IF NOT EXISTS idx_pumpportal_mint_status_migrated
            ON pumpportal_mint_status(migrated);

            CREATE INDEX IF NOT EXISTS idx_pumpportal_mint_status_first_seen
            ON pumpportal_mint_status(first_seen_at_ms);
            """
        )
        self.conn.commit()

    def insert_event_index(
        self,
        meta: EventMeta,
        raw_file: Path,
        raw_line: int,
    ) -> int:
        with self.conn:
            cursor = self.conn.execute(
                """
                INSERT INTO pumpportal_event_index (
                    received_at_ms,
                    event_date,
                    event_type,
                    mint,
                    signature,
                    raw_file,
                    raw_line
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    meta.received_at_ms,
                    meta.event_date,
                    meta.event_type,
                    meta.mint,
                    meta.signature,
                    str(raw_file),
                    raw_line,
                ),
            )

        return int(cursor.lastrowid)

    def update_mint_status(self, meta: EventMeta, event_id: int) -> None:
        if meta.mint is None:
            return

        if meta.event_type == "new_token":
            self._mark_new_token(meta, event_id)
            return

        if meta.event_type == "migration":
            self._mark_migration(meta, event_id)
            return

    def _mark_new_token(self, meta: EventMeta, event_id: int) -> None:
        assert meta.mint is not None

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO pumpportal_mint_status (
                    mint,
                    seen_new_token,
                    migrated,
                    first_seen_at_ms,
                    migrated_at_ms,
                    last_event_at_ms,
                    new_token_event_id,
                    migration_event_id
                )
                VALUES (?, 1, 0, ?, NULL, ?, ?, NULL)
                ON CONFLICT(mint) DO UPDATE SET
                    seen_new_token = 1,
                    first_seen_at_ms = COALESCE(
                        pumpportal_mint_status.first_seen_at_ms,
                        excluded.first_seen_at_ms
                    ),
                    last_event_at_ms = excluded.last_event_at_ms,
                    new_token_event_id = COALESCE(
                        pumpportal_mint_status.new_token_event_id,
                        excluded.new_token_event_id
                    )
                """,
                (
                    meta.mint,
                    meta.received_at_ms,
                    meta.received_at_ms,
                    event_id,
                ),
            )

    def _mark_migration(self, meta: EventMeta, event_id: int) -> None:
        assert meta.mint is not None

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO pumpportal_mint_status (
                    mint,
                    seen_new_token,
                    migrated,
                    first_seen_at_ms,
                    migrated_at_ms,
                    last_event_at_ms,
                    new_token_event_id,
                    migration_event_id
                )
                VALUES (?, 0, 1, NULL, ?, ?, NULL, ?)
                ON CONFLICT(mint) DO UPDATE SET
                    migrated = 1,
                    migrated_at_ms = COALESCE(
                        pumpportal_mint_status.migrated_at_ms,
                        excluded.migrated_at_ms
                    ),
                    last_event_at_ms = excluded.last_event_at_ms,
                    migration_event_id = COALESCE(
                        pumpportal_mint_status.migration_event_id,
                        excluded.migration_event_id
                    )
                """,
                (
                    meta.mint,
                    meta.received_at_ms,
                    meta.received_at_ms,
                    event_id,
                ),
            )

    def get_summary(self) -> dict[str, int | float]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_unique_mints,
                SUM(CASE WHEN seen_new_token = 1 THEN 1 ELSE 0 END)
                    AS seen_new_token_mints,
                SUM(CASE WHEN migrated = 1 THEN 1 ELSE 0 END)
                    AS migrated_mints,
                SUM(
                    CASE
                        WHEN seen_new_token = 1 AND migrated = 1 THEN 1
                        ELSE 0
                    END
                ) AS observed_then_migrated_mints
            FROM pumpportal_mint_status
            """
        ).fetchone()

        seen_new = int(row["seen_new_token_mints"] or 0)
        migrated_after_seen = int(row["observed_then_migrated_mints"] or 0)

        migration_rate = migrated_after_seen / seen_new if seen_new else 0.0

        return {
            "total_unique_mints_in_state": int(row["total_unique_mints"] or 0),
            "seen_new_token_mints": seen_new,
            "migrated_mints": int(row["migrated_mints"] or 0),
            "observed_then_migrated_mints": migrated_after_seen,
            "observed_migration_rate_pct": migration_rate * 100.0,
        }

    def close(self) -> None:
        self.conn.close()


def load_local_env(filename: str = ENV_FILE) -> dict[str, str]:
    path = Path(__file__).resolve().parent / filename
    values: dict[str, str] = {}

    if not path.exists():
        print(f"WARNING: {filename} not found next to script; connecting without API key.")
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def utc_date_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date().isoformat()


def build_raw_file_path(repo_root: Path) -> Path:
    raw_dir = repo_root / RAW_SUBDIR
    raw_dir.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return raw_dir / f"pumpportal_stream_{run_id}.jsonl"


def extract_mint(data: dict[str, Any]) -> str | None:
    candidates = (
        data.get("mint"),
        data.get("tokenMint"),
        data.get("token_mint"),
        data.get("mintAddress"),
        data.get("ca"),
    )

    for value in candidates:
        if isinstance(value, str) and value:
            return value

    return None


def extract_signature(data: dict[str, Any]) -> str | None:
    candidates = (
        data.get("signature"),
        data.get("txSignature"),
        data.get("transactionSignature"),
        data.get("sig"),
    )

    for value in candidates:
        if isinstance(value, str) and value:
            return value

    return None


def detect_event_type(data: dict[str, Any]) -> str:
    if "errors" in data or "error" in data:
        return "error"

    message = str(data.get("message", "")).strip().lower()

    if message:
        if "subscribed" in message:
            return "subscription_ack"
        if "minimum balance" in message:
            return "permission_error"
        return "control_message"

    raw_type = (
        data.get("txType")
        or data.get("type")
        or data.get("eventType")
        or data.get("method")
        or ""
    )

    tx_type = str(raw_type).strip().lower()

    if tx_type in {"create", "new_token", "newtoken"}:
        return "new_token"

    if tx_type in {"migrate", "migration"}:
        return "migration"

    if "migrat" in tx_type:
        return "migration"

    if tx_type:
        return tx_type

    return "unknown"

def build_event_meta(data: dict[str, Any], received_at_ms: int) -> EventMeta:
    return EventMeta(
        received_at_ms=received_at_ms,
        event_date=utc_date_from_ms(received_at_ms),
        event_type=detect_event_type(data),
        mint=extract_mint(data),
        signature=extract_signature(data),
    )


def build_raw_row(meta: EventMeta, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "received_at_ms": meta.received_at_ms,
        "received_at_iso_utc": datetime.fromtimestamp(
            meta.received_at_ms / 1000,
            tz=timezone.utc,
        ).isoformat(),
        "source": "pumpportal",
        "event_type": meta.event_type,
        "mint": meta.mint,
        "signature": meta.signature,
        "data": data,
    }


async def main() -> None:
    repo_root = Path(__file__).resolve().parent

    env = load_local_env()
    api_key = env.get("PUMPPORTAL_API_KEY", "")

    url = "wss://pumpportal.fun/api/data"
    if api_key:
        url += f"?api-key={api_key}"

    raw_path = build_raw_file_path(repo_root)
    db_path = repo_root / SQLITE_PATH

    store = PumpPortalStore(db_path)

    print("listening to PumpPortal WebSocket")
    print(f"raw JSONL: {raw_path}")
    print(f"sqlite:    {db_path}")

    try:
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({"method": "subscribeMigration"}))
            await ws.send(json.dumps({"method": "subscribeNewToken"}))

            start = time.time()
            raw_line = 0

            with raw_path.open("a", encoding="utf-8") as out:
                while time.time() - start < DURATION_SECONDS:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        print("no message in 30s; still connected")
                        continue

                    received_at_ms = now_ms()

                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        data = {
                            "_decode_error": True,
                            "raw_message": msg,
                        }

                    if not isinstance(data, dict):
                        data = {
                            "_non_object_message": True,
                            "raw_message": data,
                        }

                    meta = build_event_meta(data, received_at_ms)
                    raw_row = build_raw_row(meta, data)

                    raw_line += 1
                    out.write(json.dumps(raw_row, ensure_ascii=False, separators=(",", ":")))
                    out.write("\n")
                    out.flush()

                    event_id = store.insert_event_index(
                        meta=meta,
                        raw_file=raw_path,
                        raw_line=raw_line,
                    )
                    store.update_mint_status(meta=meta, event_id=event_id)

                    print(
                        json.dumps(
                            {
                                "event_type": meta.event_type,
                                "mint": meta.mint,
                                "signature": meta.signature,
                            },
                            ensure_ascii=False,
                        )
                    )

    finally:
        summary = store.get_summary()
        store.close()

    print("summary:")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())