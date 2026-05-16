"""Small text / serialization helpers shared across modules."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


def compact_json_dumps(obj: Any) -> str:
    """Deterministic, compact JSON suitable for hashing and Parquet rows."""
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def safe_part(value: str | None, fallback: str = "_none") -> str:
    """Sanitize a string for use as a filesystem partition segment."""
    if not value:
        return fallback
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value)[:128]


def short_hash(text: str) -> str:
    """16-char SHA-256 prefix; collision-tolerant for dedup keys."""
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:16]
