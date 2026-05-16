"""Time helpers used across acquisition, features, research, and execution."""

from __future__ import annotations

import time
from datetime import datetime, timezone


def now_ts() -> float:
    """Return current Unix timestamp in seconds."""
    return time.time()


def now_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def ts_iso(ts: int | float | None) -> str | None:
    """Convert Unix timestamp to UTC ISO-8601 string."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()