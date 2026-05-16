"""Pure helpers shared across the risk package."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from crypto_trade.core.time import utc_now_iso_z

from .constants import RISK_LEVEL_CODES


def clamp(x: Optional[float], lo: float = 0.0, hi: float = 100.0) -> Optional[float]:
    if x is None:
        return None
    if math.isnan(x):
        return None
    return max(lo, min(hi, float(x)))


def as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def as_bool(x: Any) -> Optional[bool]:
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        v = x.strip().lower()
        if v in {"true", "1", "yes", "y"}:
            return True
        if v in {"false", "0", "no", "n"}:
            return False
    return None


def risk_level(score: Optional[float]) -> str:
    if score is None:
        return "UNKNOWN"
    if score <= 25:
        return "LOW"
    if score <= 50:
        return "MEDIUM"
    if score <= 75:
        return "HIGH"
    return "CRITICAL"


def risk_level_code(level: str) -> Optional[int]:
    return RISK_LEVEL_CODES.get(level.upper() if level else "UNKNOWN")


def risk_score_from_level(level: Any) -> Optional[float]:
    if level is None:
        return None
    v = str(level).strip().lower()
    mapping = {
        "none": 0,
        "low": 20,
        "info": 20,
        "medium": 45,
        "moderate": 45,
        "warn": 55,
        "warning": 55,
        "high": 75,
        "danger": 85,
        "critical": 95,
        "severe": 95,
    }
    return mapping.get(v)


def weighted_average(items: List[Tuple[Optional[float], float]]) -> Optional[float]:
    valid = [(s, w) for s, w in items if s is not None]
    if not valid:
        return None
    total_w = sum(w for _, w in valid)
    if total_w <= 0:
        return None
    return clamp(sum(float(s) * w for s, w in valid) / total_w)


def utc_now_iso() -> str:
    """Backwards-compatible alias for :func:`crypto_trade.core.time.utc_now_iso_z`."""
    return utc_now_iso_z()


def empty_metrics(keys: Sequence[str]) -> Dict[str, None]:
    return {k: None for k in keys}


def bool_to_ml(v: Optional[bool]) -> Optional[int]:
    if v is None:
        return None
    return 1 if v else 0


def json_default(obj: Any) -> Any:
    if isinstance(obj, Path):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
