from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from crypto_trade.core.text import compact_json_dumps, short_hash
from crypto_trade.core.time import now_ms, utc_now_iso_ms_z

ENRICHMENT_COLUMNS = {
    "security": "security_report_ts",
    "twitter_lite": "twitter_lite_ts",
    "telegram_lite": "telegram_lite_ts",
    "website": "website_report_ts",
}


class PreMigrationState:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
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
                inactive_polls INTEGER NOT NULL DEFAULT 0,
                priority_score REAL NOT NULL DEFAULT 0,
                dead_reason TEXT,
                dead_ts TEXT,
                security_report_ts TEXT,
                twitter_lite_ts TEXT,
                telegram_lite_ts TEXT,
                website_report_ts TEXT,
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

            CREATE INDEX IF NOT EXISTS ix_mints_due
            ON mints(status, next_dex_poll_ms, priority_score);
            """
        )

        self._ensure_column("mints", "inactive_polls", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("mints", "dead_ts", "TEXT")
        for column in ENRICHMENT_COLUMNS.values():
            self._ensure_column("mints", column, "TEXT")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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

    def mint_age_ms(self, mint: str) -> int:
        row = self.conn.execute(
            "SELECT first_seen_ms FROM mints WHERE mint = ?",
            (mint,),
        ).fetchone()
        return max(0, now_ms() - int(row[0])) if row else 0

    def enrichment_due(self, mint: str, kind: str) -> bool:
        column = ENRICHMENT_COLUMNS[kind]
        row = self.conn.execute(
            f"SELECT {column} FROM mints WHERE mint = ?",
            (mint,),
        ).fetchone()
        return bool(row and row[0] is None)

    def mark_enrichment_done(self, mint: str, kind: str) -> None:
        column = ENRICHMENT_COLUMNS[kind]
        self.conn.execute(
            f"UPDATE mints SET {column} = ?, updated_ts = ? WHERE mint = ?",
            (utc_now_iso_ms_z(), utc_now_iso_ms_z(), mint),
        )

    def update_after_dex_poll(
        self,
        mint: str,
        *,
        has_pairs: bool,
        inactive: bool,
        score: float,
        next_poll_ms: int,
        no_pair_dead_ms: int,
        inactive_dead_ms: int,
        inactive_confirmations: int,
        max_track_ms: int,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT first_seen_ms, migrated_ts, empty_polls, inactive_polls FROM mints WHERE mint = ?",
            (mint,),
        ).fetchone()
        if not row:
            return None

        age_ms = now_ms() - int(row[0])
        migrated = row[1] is not None
        empty_polls = 0 if has_pairs else int(row[2]) + 1
        inactive_polls = int(row[3]) + 1 if inactive else 0
        status = "migrated" if migrated else "active"
        dead_reason = None
        dead_ts = None

        if age_ms >= max_track_ms:
            status, next_poll_ms, dead_reason = "expired", 0, "max_track_hours"
        elif not migrated and age_ms >= no_pair_dead_ms and not has_pairs:
            status, next_poll_ms, dead_reason, dead_ts = "dead", 0, "no_pair_after_threshold", utc_now_iso_ms_z()
        elif (
            not migrated
            and has_pairs
            and age_ms >= inactive_dead_ms
            and inactive_polls >= inactive_confirmations
        ):
            status, next_poll_ms, dead_reason, dead_ts = "dead", 0, "inactive_after_threshold", utc_now_iso_ms_z()

        self.conn.execute(
            """
            UPDATE mints
            SET status = ?, last_dex_poll_ms = ?, next_dex_poll_ms = ?,
                dex_polls = dex_polls + 1, empty_polls = ?, inactive_polls = ?,
                priority_score = ?, dead_reason = COALESCE(?, dead_reason),
                dead_ts = COALESCE(?, dead_ts), updated_ts = ?
            WHERE mint = ?
            """,
            (
                status,
                now_ms(),
                next_poll_ms,
                empty_polls,
                inactive_polls,
                score,
                dead_reason,
                dead_ts,
                utc_now_iso_ms_z(),
                mint,
            ),
        )
        return {"status": status, "dead_reason": dead_reason, "inactive_polls": inactive_polls}

    def close(self) -> None:
        self.conn.close()
