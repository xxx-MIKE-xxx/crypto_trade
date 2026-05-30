"""SQLite-backed state store for the data-acquisition pipeline."""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

from crypto_trade.core.text import compact_json_dumps, short_hash
from crypto_trade.core.time import utc_now_iso_ms_z


class StateStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.closed = False
        self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tokens (
                mint TEXT PRIMARY KEY,
                first_seen_ts TEXT,
                migrated_ts TEXT,
                dex_visible_ts TEXT,
                status TEXT NOT NULL,
                token_dir TEXT,
                migration_event_json TEXT,
                dex_snapshot_json TEXT,
                updated_ts TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                mint TEXT,
                stage TEXT NOT NULL,
                command_json TEXT NOT NULL,
                started_ts TEXT NOT NULL,
                ended_ts TEXT,
                status TEXT NOT NULL,
                return_code INTEGER,
                stdout_log TEXT,
                stderr_log TEXT,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS seen_events (
                event_hash TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                mint TEXT,
                event_ts TEXT NOT NULL,
                ingest_ts TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_tokens_status ON tokens(status);
            CREATE INDEX IF NOT EXISTS ix_jobs_mint_stage ON jobs(mint, stage);
            """
        )

    def record_seen_event(
        self,
        *,
        source: str,
        event_type: str,
        mint: str | None,
        event_ts: str,
        payload: Any,
    ) -> bool:
        if self.closed:
            return False

        event_hash = short_hash(
            f"{source}|{event_type}|{mint or ''}|{compact_json_dumps(payload)}"
        )
        try:
            self.conn.execute(
                """
                INSERT INTO seen_events(event_hash, source, event_type, mint, event_ts, ingest_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_hash, source, event_type, mint, event_ts, utc_now_iso_ms_z()),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def upsert_new_token(self, mint: str, token_dir: Path) -> None:
        if self.closed:
            return

        now = utc_now_iso_ms_z()
        self.conn.execute(
            """
            INSERT INTO tokens(mint, first_seen_ts, status, token_dir, updated_ts)
            VALUES (?, ?, 'seen', ?, ?)
            ON CONFLICT(mint) DO UPDATE SET
                first_seen_ts = COALESCE(tokens.first_seen_ts, excluded.first_seen_ts),
                token_dir = COALESCE(tokens.token_dir, excluded.token_dir),
                updated_ts = excluded.updated_ts
            """,
            (mint, now, str(token_dir), now),
        )

    def mark_migrated(self, mint: str, token_dir: Path, event: Mapping[str, Any]) -> bool:
        if self.closed:
            return False

        now = utc_now_iso_ms_z()
        cur = self.conn.execute(
            "SELECT migrated_ts FROM tokens WHERE mint = ?", (mint,)
        )
        row = cur.fetchone()
        first_migration = row is None or row[0] is None
        self.conn.execute(
            """
            INSERT INTO tokens(mint, migrated_ts, status, token_dir, migration_event_json, updated_ts)
            VALUES (?, ?, 'migrated', ?, ?, ?)
            ON CONFLICT(mint) DO UPDATE SET
                migrated_ts = COALESCE(tokens.migrated_ts, excluded.migrated_ts),
                status = CASE
                    WHEN tokens.status IN ('reports_done', 'failed') THEN tokens.status
                    ELSE 'migrated'
                END,
                token_dir = COALESCE(tokens.token_dir, excluded.token_dir),
                migration_event_json = COALESCE(tokens.migration_event_json, excluded.migration_event_json),
                updated_ts = excluded.updated_ts
            """,
            (mint, now, str(token_dir), compact_json_dumps(event), now),
        )
        return first_migration

    def mark_dex_visible(self, mint: str, snapshot: Any) -> None:
        if self.closed:
            return

        now = utc_now_iso_ms_z()
        self.conn.execute(
            """
            UPDATE tokens
            SET dex_visible_ts = COALESCE(dex_visible_ts, ?),
                status = CASE WHEN status = 'failed' THEN status ELSE 'dex_visible' END,
                dex_snapshot_json = ?,
                updated_ts = ?
            WHERE mint = ?
            """,
            (now, compact_json_dumps(snapshot), now, mint),
        )

    def mark_status(self, mint: str, status: str) -> None:
        if self.closed:
            return

        self.conn.execute(
            "UPDATE tokens SET status = ?, updated_ts = ? WHERE mint = ?",
            (status, utc_now_iso_ms_z(), mint),
        )

    def start_job(self, mint: str | None, stage: str, command: Sequence[str]) -> str:
        job_id = str(uuid.uuid4())
        if self.closed:
            return job_id

        self.conn.execute(
            """
            INSERT INTO jobs(job_id, mint, stage, command_json, started_ts, status)
            VALUES (?, ?, ?, ?, ?, 'running')
            """,
            (job_id, mint, stage, compact_json_dumps(list(command)), utc_now_iso_ms_z()),
        )
        return job_id

    def finish_job(
        self,
        job_id: str,
        *,
        status: str,
        return_code: int | None = None,
        stdout_log: Path | None = None,
        stderr_log: Path | None = None,
        error: str | None = None,
    ) -> None:
        if self.closed:
            return

        self.conn.execute(
            """
            UPDATE jobs
            SET ended_ts = ?, status = ?, return_code = ?, stdout_log = ?, stderr_log = ?, error = ?
            WHERE job_id = ?
            """,
            (
                utc_now_iso_ms_z(),
                status,
                return_code,
                str(stdout_log) if stdout_log else None,
                str(stderr_log) if stderr_log else None,
                error,
                job_id,
            ),
        )

    def close(self) -> None:
        if self.closed:
            return

        self.closed = True
        self.conn.close()
