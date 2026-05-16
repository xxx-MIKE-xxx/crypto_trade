"""Time helpers used across acquisition, features, research, and execution.

Multiple ISO-8601 shapes coexist in the codebase because downstream consumers
parse them differently. Each helper documents which call sites depend on its
exact output, so call sites stay byte-stable after the consolidation.
"""

from __future__ import annotations

import datetime as dt
import time
from typing import Any

UTC = dt.timezone.utc


def utc_now() -> dt.datetime:
    """Return current time as a timezone-aware UTC datetime."""
    return dt.datetime.now(tz=UTC)


def now_ts() -> float:
    """Current Unix timestamp in seconds (monotonic-ish wall clock)."""
    return time.time()


def now_ms() -> int:
    """Current Unix timestamp in milliseconds."""
    return int(time.time() * 1000)


def now_iso() -> str:
    """Microsecond-precision UTC ISO-8601 with ``+00:00`` offset.

    Used by on-chain JSONL capture (``ingest/onchain.py``) where existing
    parquet/JSON files already contain this exact shape.
    """
    return utc_now().isoformat()


def utc_now_iso_z() -> str:
    """Second-precision UTC ISO-8601 with ``Z`` suffix.

    Used by risk report timestamps (``features/risk``).
    """
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_now_iso_ms_z() -> str:
    """Millisecond-precision UTC ISO-8601 with ``Z`` suffix.

    Used by bronze sink ``ingest_ts``/``event_ts`` (``ingest/bronze.py``).
    """
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ts_iso(ts: int | float | None) -> str | None:
    """Convert a Unix timestamp (seconds) to UTC ISO-8601 with ``+00:00`` offset.

    Used for on-chain block times. Returns ``None`` for ``None`` input.
    """
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(ts, tz=UTC).isoformat()


def parse_event_ts(value: Any) -> str | None:
    """Best-effort parse of mixed-format timestamps to ms-precision ``Z`` ISO.

    Accepts:
      * Unix seconds or milliseconds (``int`` / ``float``)
      * Digit strings
      * ISO-8601 strings (with or without ``Z`` suffix / offset)

    Returns ``None`` when the value cannot be interpreted.
    """
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            seconds = value / 1000.0 if value > 10_000_000_000 else float(value)
            return (
                dt.datetime.fromtimestamp(seconds, tz=UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            if s.isdigit():
                return parse_event_ts(int(s))
            parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return (
                parsed.astimezone(UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
    except Exception:
        return None
    return None
