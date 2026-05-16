"""Append-only bronze event sink.

Schema-stable Parquet with raw JSON strings; if pyarrow is missing, falls back
to JSONL with the same fields. Partitions by ``source/date/token_mint`` so
downstream readers can scan a single token quickly.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, MutableMapping

from crypto_trade.core.text import compact_json_dumps, safe_part
from crypto_trade.core.time import parse_event_ts, utc_now_iso_ms_z

try:  # pyarrow is an optional dependency at runtime.
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - exercised only on bare installs
    pa = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]


def event_timestamp(payload: Mapping[str, Any]) -> str:
    """Best-effort timestamp for a payload, falling back to ``utc_now_iso_ms_z``."""
    for key in (
        "timestamp",
        "time",
        "createdAt",
        "created_at",
        "blockTime",
        "block_time",
        "migrationTimestamp",
    ):
        ts = parse_event_ts(payload.get(key))
        if ts:
            return ts
    return utc_now_iso_ms_z()


class EventSink:
    """Asynchronous batched bronze writer."""

    def __init__(self, root: Path, batch_size: int = 500) -> None:
        self.root = root
        self.batch_size = batch_size
        self.buffers: MutableMapping[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        self.lock = asyncio.Lock()
        self.schema = None
        if pa is not None:
            self.schema = pa.schema(
                [
                    ("ingest_ts", pa.string()),
                    ("event_ts", pa.string()),
                    ("source", pa.string()),
                    ("event_type", pa.string()),
                    ("token_mint", pa.string()),
                    ("stream", pa.string()),
                    ("level", pa.string()),
                    ("raw_text", pa.string()),
                    ("event_json", pa.string()),
                ]
            )

    async def write(
        self,
        *,
        source: str,
        event_type: str,
        payload: Any,
        token_mint: str | None = None,
        raw_text: str | None = None,
        stream: str = "",
        level: str = "info",
        event_ts: str | None = None,
    ) -> None:
        event_ts = event_ts or (
            event_timestamp(payload) if isinstance(payload, Mapping) else utc_now_iso_ms_z()
        )
        row = {
            "ingest_ts": utc_now_iso_ms_z(),
            "event_ts": event_ts,
            "source": source,
            "event_type": event_type,
            "token_mint": token_mint or "",
            "stream": stream,
            "level": level,
            "raw_text": raw_text or "",
            "event_json": compact_json_dumps(payload),
        }
        date_part = event_ts[:10] if len(event_ts) >= 10 else utc_now_iso_ms_z()[:10]
        key = (source, date_part, safe_part(token_mint, "_global"))
        async with self.lock:
            self.buffers[key].append(row)
            if len(self.buffers[key]) >= self.batch_size:
                self._flush_key(key)

    async def flush(self) -> None:
        async with self.lock:
            for key in list(self.buffers):
                self._flush_key(key)

    def _flush_key(self, key: tuple[str, str, str]) -> None:
        rows = self.buffers.pop(key, [])
        if not rows:
            return
        source, date_part, token_part = key
        out_dir = (
            self.root
            / "bronze"
            / f"source={safe_part(source)}"
            / f"date={date_part}"
            / f"token_mint={token_part}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        part = out_dir / f"part-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        if pa is not None and pq is not None:
            table = pa.Table.from_pylist(rows, schema=self.schema)
            pq.write_table(table, part.with_suffix(".parquet"), compression="zstd")
        else:
            with part.with_suffix(".jsonl").open("a", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(compact_json_dumps(row) + "\n")
