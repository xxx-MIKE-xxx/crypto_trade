#!/usr/bin/env python3
"""
Solana token risk/security report — production data component.

Risk score convention: 0 = lowest risk, 100 = highest risk.

Examples:
    # Standard pipeline output:
    #   data/raw/analysis/<MINT>/security_report
    #   data/raw/analysis/<MINT>/security_analysis
    python solana_risk_report.py --mint <MINT>

    # Machine-friendly outputs for pipelines
    python solana_risk_report.py --mint <MINT> --machine --format json
    python solana_risk_report.py --mint <MINT> --format jsonl --out data/solana_risk_reports.jsonl --append
    python solana_risk_report.py --mint <MINT> --format csv --out data/features.csv --append
    python solana_risk_report.py --mint <MINT> --format parquet --out data/features.parquet

Environment (optional; missing keys mark sources unavailable):
    DEFADE_API_KEY, GOPLUS_API_KEY, GOPLUS_API_SECRET, JUPITER_API_KEY, RUGCHECK_API_KEY
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from io import StringIO
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0.0"
MODEL_VERSION = "heuristic_v1"
CHAIN = "solana"

DEFAULT_ANALYSIS_ROOT = Path("data/raw/analytics")
SECURITY_REPORT_FILENAME = "security_report"
SECURITY_ANALYSIS_FILENAME = "security_analytics"

RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"
DEXSCREENER_BASE = "https://api.dexscreener.com"
DEFADE_BASE = "https://api.defade.org"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"
JUPITER_BASE = "https://api.jup.ag/tokens/v2"

SOURCE_NAMES: Tuple[str, ...] = (
    "rugcheck",
    "dexscreener",
    "defade",
    "goplus",
    "jupiter",
)

SOURCES_REQUIRING_KEY = frozenset({"defade", "goplus", "jupiter"})

CATEGORY_NAMES: Tuple[str, ...] = (
    "external_vendor_risk",
    "contract_permissions",
    "holder_distribution",
    "liquidity_health",
    "trading_behavior",
    "verification_identity",
)

CATEGORY_WEIGHTS: Dict[str, float] = {
    "external_vendor_risk": 0.25,
    "contract_permissions": 0.20,
    "holder_distribution": 0.15,
    "liquidity_health": 0.20,
    "trading_behavior": 0.10,
    "verification_identity": 0.10,
}

EXTERNAL_VENDOR_METRIC_KEYS: Tuple[str, ...] = (
    "rugcheck_score",
    "defade_rug_score",
    "goplus_risky_flag_count",
)

CONTRACT_PERMISSION_METRIC_KEYS: Tuple[str, ...] = (
    "mint_authority_disabled",
    "freeze_authority_disabled",
    "metadata_mutable",
    "token_2022_detected",
    "non_transferable",
    "transfer_fee_upgradable",
)

HOLDER_DISTRIBUTION_METRIC_KEYS: Tuple[str, ...] = (
    "holder_count",
    "top_holders_pct",
    "insider_score",
    "bundle_score",
    "sniper_score",
)

LIQUIDITY_HEALTH_METRIC_KEYS: Tuple[str, ...] = (
    "total_liquidity_usd",
    "top_pair_liquidity_usd",
    "lp_locked_pct",
    "pair_count",
    "newest_pair_age_hours",
)

TRADING_BEHAVIOR_METRIC_KEYS: Tuple[str, ...] = (
    "h24_buys",
    "h24_sells",
    "h24_volume_usd",
    "h24_price_change_pct",
    "h24_sell_buy_ratio",
    "h24_volume_liquidity_ratio",
)

VERIFICATION_IDENTITY_METRIC_KEYS: Tuple[str, ...] = (
    "jupiter_verified",
    "jupiter_organic_score",
    "website_count",
    "social_count",
)

CATEGORY_METRIC_KEYS: Dict[str, Tuple[str, ...]] = {
    "external_vendor_risk": EXTERNAL_VENDOR_METRIC_KEYS,
    "contract_permissions": CONTRACT_PERMISSION_METRIC_KEYS,
    "holder_distribution": HOLDER_DISTRIBUTION_METRIC_KEYS,
    "liquidity_health": LIQUIDITY_HEALTH_METRIC_KEYS,
    "trading_behavior": TRADING_BEHAVIOR_METRIC_KEYS,
    "verification_identity": VERIFICATION_IDENTITY_METRIC_KEYS,
}

RISK_LEVEL_CODES: Dict[str, Optional[int]] = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
    "CRITICAL": 4,
    "UNKNOWN": None,
}

SEVERITY_ORDER = ("LOW", "MEDIUM", "HIGH", "CRITICAL")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


# ---------------------------------------------------------------------------
# Configuration & data classes
# ---------------------------------------------------------------------------


@dataclass
class ReportConfig:
    """Runtime configuration for report generation."""

    timeout: int = 15
    include_raw: bool = True
    defade_api_key: Optional[str] = field(default_factory=lambda: os.getenv("DEFADE_API_KEY"))
    goplus_api_key: Optional[str] = field(default_factory=lambda: os.getenv("GOPLUS_API_KEY"))
    goplus_api_secret: Optional[str] = field(default_factory=lambda: os.getenv("GOPLUS_API_SECRET"))
    jupiter_api_key: Optional[str] = field(default_factory=lambda: os.getenv("JUPITER_API_KEY"))
    rugcheck_api_key: Optional[str] = field(default_factory=lambda: os.getenv("RUGCHECK_API_KEY"))

    @classmethod
    def from_env(cls, timeout: int = 15, include_raw: bool = True) -> "ReportConfig":
        return cls(timeout=timeout, include_raw=include_raw)


@dataclass
class SourceResult:
    """Result wrapper for a single data source fetch."""

    source: str
    attempted: bool
    success: bool
    available: bool
    requires_key: bool
    latency_ms: Optional[float] = None
    http_status: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    data: Any = None
    raw: Any = None

    def to_status_dict(self) -> Dict[str, Any]:
        return {
            "attempted": self.attempted,
            "success": self.success,
            "available": self.available,
            "requires_key": self.requires_key,
            "latency_ms": self.latency_ms,
            "http_status": self.http_status,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }


FeatureRow = Dict[str, Any]


@dataclass
class StandardRiskReport:
    """Standardized machine-readable risk report."""

    schema_version: str
    generated_at_utc: str
    token: Dict[str, Any]
    overall: Dict[str, Any]
    source_status: Dict[str, Dict[str, Any]]
    categories: Dict[str, Dict[str, Any]]
    feature_row: FeatureRow
    warnings: List[Dict[str, Any]]
    raw: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at_utc": self.generated_at_utc,
            "token": self.token,
            "overall": self.overall,
            "source_status": self.source_status,
            "categories": self.categories,
            "feature_row": self.feature_row,
            "warnings": self.warnings,
            "raw": self.raw,
        }


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


def http_get_json(
    name: str,
    url: str,
    *,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: int = 15,
) -> Tuple[Optional[Any], Optional[int], Optional[str], Optional[str], float]:
    """GET JSON with timing. Returns (data, http_status, error_type, error_message, latency_ms)."""
    start = time.perf_counter()
    try:
        r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        latency_ms = (time.perf_counter() - start) * 1000.0
        status = r.status_code
        if status == 404:
            return None, status, "not_found", f"{name}: 404 not found", latency_ms
        if status == 401:
            return None, status, "unauthorized", f"{name}: 401 unauthorized", latency_ms
        if status == 403:
            return None, status, "forbidden", f"{name}: 403 forbidden", latency_ms
        if status == 429:
            return None, status, "rate_limited", f"{name}: 429 rate limited", latency_ms
        r.raise_for_status()
        return r.json(), status, None, None, latency_ms
    except requests.Timeout as e:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return None, None, "timeout", f"{name}: timeout: {e}", latency_ms
    except requests.RequestException as e:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return None, None, "request_error", f"{name}: request failed: {e}", latency_ms
    except ValueError as e:
        latency_ms = (time.perf_counter() - start) * 1000.0
        return None, None, "invalid_json", f"{name}: invalid JSON: {e}", latency_ms


def _missing_key_result(source: str) -> SourceResult:
    return SourceResult(
        source=source,
        attempted=False,
        success=False,
        available=False,
        requires_key=True,
        error_type="missing_api_key",
        error_message=f"{source}: API key not configured",
        data=None,
        raw=None,
    )


def defade_analyze_error_message(payload: Mapping[str, Any]) -> Optional[str]:
    """Return a human-readable error from a Defade /v1/analyze JSON body, if any."""
    err = payload.get("error")
    if isinstance(err, str) and err.strip():
        return err.strip()
    if payload.get("success") is False:
        return "analysis unsuccessful"
    return None


def normalize_defade_analyze_response(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Map Defade GET /v1/analyze/:mint JSON to the flat fields used by scoring.

    Documented response: rugScore, riskLevel, token, analysis.{holders,bundles,...}
    Also accepts legacy envelopes with risk.score and holders.bundles.bundlePct.
    """
    if not isinstance(raw, dict):
        return None
    if defade_analyze_error_message(raw):
        return None

    analysis = raw.get("analysis") if isinstance(raw.get("analysis"), dict) else {}
    token = raw.get("token") if isinstance(raw.get("token"), dict) else {}
    raw_holders = raw.get("holders") if isinstance(raw.get("holders"), dict) else {}

    rug_score = as_float(raw.get("rugScore"))
    risk_level = raw.get("riskLevel")
    risk_block = raw.get("risk") if isinstance(raw.get("risk"), dict) else {}
    if rug_score is None:
        rug_score = as_float(risk_block.get("score"))
    if risk_level is None:
        rating = risk_block.get("rating")
        if isinstance(rating, str) and rating.strip():
            risk_level = rating.strip().split()[0].upper()

    holders = analysis.get("holders") if isinstance(analysis.get("holders"), dict) else {}
    bundles = analysis.get("bundles") if isinstance(analysis.get("bundles"), dict) else {}
    if not bundles and isinstance(raw_holders.get("bundles"), dict):
        bundles = raw_holders["bundles"]
    insider = (
        analysis.get("insiderNetwork")
        if isinstance(analysis.get("insiderNetwork"), dict)
        else {}
    )
    snipers = analysis.get("snipers") if isinstance(analysis.get("snipers"), dict) else {}

    insider_score = as_float(insider.get("networkScore"))
    bundle_score = as_float(bundles.get("bundledPct"))
    if bundle_score is None:
        bundle_score = as_float(bundles.get("bundlePct"))
    sniper_score = as_float(snipers.get("pct"))

    holder_score = as_float(holders.get("top10Pct"))
    if holder_score is None:
        concentration = raw_holders.get("concentration")
        if isinstance(concentration, dict):
            holder_score = as_float(concentration.get("top10"))
    holder_count = as_float(holders.get("total"))
    if holder_count is None:
        holder_count = as_float(raw_holders.get("totalHolders"))

    if rug_score is None:
        return None

    return {
        "rugScore": rug_score,
        "riskLevel": risk_level,
        "insiderScore": insider_score,
        "bundleScore": bundle_score,
        "sniperScore": sniper_score,
        "holderScore": holder_score,
        "holderCount": holder_count,
        "token": token,
        "analysis": analysis or None,
    }


# ---------------------------------------------------------------------------
# RiskReportClient
# ---------------------------------------------------------------------------


class RiskReportClient:
    """Fetches and normalizes data from all configured risk sources."""

    def __init__(self, config: Optional[ReportConfig] = None) -> None:
        self.config = config or ReportConfig.from_env()

    def fetch_all(self, mint: str) -> Dict[str, SourceResult]:
        return {name: getattr(self, f"fetch_{name}")(mint) for name in SOURCE_NAMES}

    def fetch_rugcheck(self, mint: str) -> SourceResult:
        headers: Dict[str, str] = {}
        if self.config.rugcheck_api_key:
            headers["Authorization"] = f"Bearer {self.config.rugcheck_api_key}"
        url = f"{RUGCHECK_BASE}/tokens/{mint}/report"
        data, status, err_type, err_msg, latency = http_get_json(
            "rugcheck", url, headers=headers, timeout=self.config.timeout
        )
        ok = data is not None
        return SourceResult(
            source="rugcheck",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=False,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=data,
            raw=data,
        )

    def fetch_dexscreener(self, mint: str) -> SourceResult:
        url = f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{mint}"
        data, status, err_type, err_msg, latency = http_get_json(
            "dexscreener", url, timeout=self.config.timeout
        )
        if isinstance(data, dict) and "pairs" in data:
            pairs = data.get("pairs") or []
        elif isinstance(data, list):
            pairs = data
        else:
            pairs = []
        ok = data is not None
        return SourceResult(
            source="dexscreener",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=False,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=pairs,
            raw=data,
        )

    def fetch_defade(self, mint: str) -> SourceResult:
        if not self.config.defade_api_key:
            return _missing_key_result("defade")
        url = f"{DEFADE_BASE}/v1/analyze/{mint}"
        raw, status, err_type, err_msg, latency = http_get_json(
            "defade",
            url,
            headers={"x-api-key": self.config.defade_api_key},
            timeout=self.config.timeout,
        )
        normalized = normalize_defade_analyze_response(raw)
        if raw is not None and normalized is None and err_type is None:
            if isinstance(raw, dict):
                api_err = defade_analyze_error_message(raw)
                err_msg = f"defade: {api_err}" if api_err else "defade: incomplete analyze response"
            else:
                err_msg = "defade: invalid analyze response"
            err_type = "api_error"
        ok = normalized is not None
        return SourceResult(
            source="defade",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=True,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=normalized,
            raw=raw,
        )

    def fetch_goplus(self, mint: str) -> SourceResult:
        if not self.config.goplus_api_key or not self.config.goplus_api_secret:
            return _missing_key_result("goplus")

        now = int(time.time())
        sign = hashlib.sha1(
            f"{self.config.goplus_api_key}{now}{self.config.goplus_api_secret}".encode("utf-8")
        ).hexdigest()
        token_payload = {
            "app_key": self.config.goplus_api_key,
            "time": now,
            "sign": sign,
        }
        start = time.perf_counter()
        try:
            token_resp = requests.post(
                f"{GOPLUS_BASE}/token",
                json=token_payload,
                timeout=self.config.timeout,
            )
            token_latency = (time.perf_counter() - start) * 1000.0
            token_resp.raise_for_status()
            token_json = token_resp.json()
        except requests.RequestException as e:
            token_latency = (time.perf_counter() - start) * 1000.0
            return SourceResult(
                source="goplus",
                attempted=True,
                success=False,
                available=False,
                requires_key=True,
                latency_ms=round(token_latency, 2),
                http_status=getattr(getattr(e, "response", None), "status_code", None),
                error_type="request_error",
                error_message=f"goplus token request failed: {e}",
                data=None,
                raw=None,
            )
        except ValueError as e:
            token_latency = (time.perf_counter() - start) * 1000.0
            return SourceResult(
                source="goplus",
                attempted=True,
                success=False,
                available=False,
                requires_key=True,
                latency_ms=round(token_latency, 2),
                error_type="invalid_json",
                error_message=f"goplus token invalid JSON: {e}",
                data=None,
                raw=None,
            )

        access_token = token_json.get("result", {}).get("access_token") or token_json.get(
            "access_token"
        )
        if not access_token:
            return SourceResult(
                source="goplus",
                attempted=True,
                success=False,
                available=False,
                requires_key=True,
                latency_ms=round(token_latency, 2),
                error_type="auth_error",
                error_message="goplus: access_token missing in token response",
                data=None,
                raw=token_json,
            )

        url = f"{GOPLUS_BASE}/solana/token_security"
        data, status, err_type, err_msg, latency = http_get_json(
            "goplus",
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"contract_addresses": mint},
            timeout=self.config.timeout,
        )
        parsed = None
        if isinstance(data, dict):
            result = data.get("result", data)
            if isinstance(result, dict):
                parsed = result.get(mint) or result.get(mint.lower()) or result
            else:
                parsed = result
        ok = data is not None and parsed is not None
        total_latency = token_latency + latency
        return SourceResult(
            source="goplus",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=True,
            latency_ms=round(total_latency, 2),
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=parsed,
            raw=data,
        )

    def fetch_jupiter(self, mint: str) -> SourceResult:
        if not self.config.jupiter_api_key:
            return _missing_key_result("jupiter")
        url = f"{JUPITER_BASE}/search"
        data, status, err_type, err_msg, latency = http_get_json(
            "jupiter",
            url,
            headers={"x-api-key": self.config.jupiter_api_key},
            params={"query": mint},
            timeout=self.config.timeout,
        )
        item = None
        if isinstance(data, list):
            for row in data:
                if str(row.get("id", "")).lower() == mint.lower():
                    item = row
                    break
            if item is None and data:
                item = data[0]
        ok = item is not None
        return SourceResult(
            source="jupiter",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=True,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type if not ok else None,
            error_message=err_msg if not ok else None,
            data=item,
            raw=data,
        )


# ---------------------------------------------------------------------------
# Normalizers / extractors (preserved scoring inputs)
# ---------------------------------------------------------------------------


def rugcheck_risks(rug: Optional[dict]) -> List[dict]:
    if not isinstance(rug, dict):
        return []
    risks = rug.get("risks") or rug.get("risk") or []
    return risks if isinstance(risks, list) else []


def rugcheck_score(rug: Optional[dict]) -> Optional[float]:
    if not isinstance(rug, dict):
        return None
    for key in ("score_normalised", "score_normalized", "normalizedScore", "riskScore"):
        v = as_float(rug.get(key))
        if v is not None:
            return clamp(v)
    raw = as_float(rug.get("score"))
    if raw is not None:
        return clamp(raw if raw <= 100 else min(100, raw / 10))
    risks = rugcheck_risks(rug)
    risk_scores = []
    for r in risks:
        if not isinstance(r, dict):
            continue
        s = as_float(r.get("score"))
        if s is None:
            s = risk_score_from_level(r.get("level"))
        if s is not None:
            risk_scores.append(clamp(s))
    return weighted_average([(s, 1.0) for s in risk_scores])


def extract_lp_locked_pct(rug: Optional[dict]) -> Optional[float]:
    if not isinstance(rug, dict):
        return None
    for key in ("lpLockedPct", "lp_locked_pct", "lpLockPct", "lockedLiquidityPct"):
        v = as_float(rug.get(key))
        if v is not None:
            return clamp(v)
    markets = rug.get("markets")
    if isinstance(markets, list):
        vals = []
        for m in markets:
            if not isinstance(m, dict):
                continue
            for key in ("lpLockedPct", "lp_locked_pct", "lockedLiquidityPct"):
                v = as_float(m.get(key))
                if v is not None:
                    vals.append(clamp(v))
        if vals:
            return max(vals)
    return None


def aggregate_dex_pairs(pairs: List[dict]) -> Dict[str, Any]:
    valid = [p for p in pairs if isinstance(p, dict) and p.get("chainId") == "solana"]
    if not valid:
        valid = [p for p in pairs if isinstance(p, dict)]

    def liquidity_usd(p: dict) -> float:
        return as_float((p.get("liquidity") or {}).get("usd")) or 0.0

    total_liquidity = sum(liquidity_usd(p) for p in valid)
    top_pair = max(valid, key=liquidity_usd, default=None)

    created_ts: List[float] = []
    for p in valid:
        ms = as_float(p.get("pairCreatedAt"))
        if ms:
            created_ts.append(ms / 1000.0 if ms > 10_000_000_000 else ms)

    now = time.time()
    min_age_hours = None
    if created_ts:
        min_age_hours = max(0.0, (now - max(created_ts)) / 3600.0)

    buys24 = sells24 = 0
    volume24 = 0.0
    price_change24 = None
    if top_pair:
        tx24 = (top_pair.get("txns") or {}).get("h24") or {}
        buys24 = int(as_float(tx24.get("buys")) or 0)
        sells24 = int(as_float(tx24.get("sells")) or 0)
        volume24 = as_float((top_pair.get("volume") or {}).get("h24")) or 0.0
        price_change24 = as_float((top_pair.get("priceChange") or {}).get("h24"))

    sell_buy_ratio = None
    if buys24 > 0:
        sell_buy_ratio = sells24 / buys24
    elif sells24 > 0:
        sell_buy_ratio = None

    vol_liq_ratio = None
    if total_liquidity > 0 and volume24 > 0:
        vol_liq_ratio = volume24 / total_liquidity

    websites = ((top_pair or {}).get("info") or {}).get("websites") or []
    socials = ((top_pair or {}).get("info") or {}).get("socials") or []

    return {
        "pair_count": len(valid),
        "total_liquidity_usd": total_liquidity if valid else None,
        "top_pair_liquidity_usd": liquidity_usd(top_pair) if top_pair else None,
        "newest_pair_age_hours": min_age_hours,
        "h24_buys": buys24 if (buys24 or sells24) else None,
        "h24_sells": sells24 if (buys24 or sells24) else None,
        "h24_volume_usd": volume24 if volume24 else None,
        "h24_price_change_pct": price_change24,
        "h24_sell_buy_ratio": sell_buy_ratio,
        "h24_volume_liquidity_ratio": vol_liq_ratio,
        "websites": websites,
        "socials": socials,
    }


def count_goplus_risky_flags(goplus: Optional[dict]) -> Optional[int]:
    if not isinstance(goplus, dict):
        return None
    risky_keys = [
        "is_mintable",
        "mintable",
        "is_freezable",
        "freezable",
        "metadata_mutable",
        "is_honeypot",
        "is_blacklisted",
        "is_proxy",
        "transfer_fee_upgradable",
        "is_non_transferable",
    ]
    count = 0
    for k in risky_keys:
        if k not in goplus:
            continue
        b = as_bool(goplus.get(k))
        if k == "is_open_source":
            if b is False:
                count += 1
        elif b is True:
            count += 1
    return count


def extract_token_info(
    mint: str,
    rug: Optional[dict],
    jup: Optional[dict],
    top_pair: Optional[dict],
) -> Dict[str, Any]:
    symbol = name = decimals = None
    if isinstance(jup, dict):
        symbol = jup.get("symbol") or symbol
        name = jup.get("name") or name
        decimals = jup.get("decimals") if jup.get("decimals") is not None else decimals
    if isinstance(rug, dict):
        token = rug.get("token") or rug.get("tokenMeta") or {}
        if isinstance(token, dict):
            symbol = symbol or token.get("symbol")
            name = name or token.get("name")
    if isinstance(top_pair, dict):
        base = top_pair.get("baseToken") or {}
        if isinstance(base, dict):
            symbol = symbol or base.get("symbol")
            name = name or base.get("name")
    return {
        "chain": CHAIN,
        "mint": mint,
        "symbol": symbol,
        "name": name,
        "decimals": decimals,
    }


def build_category_metrics(
    rug: Optional[dict],
    dex: Dict[str, Any],
    defade: Optional[dict],
    goplus: Optional[dict],
    jup: Optional[dict],
) -> Dict[str, Dict[str, Any]]:
    """Populate fixed metric dicts per category."""
    external = empty_metrics(EXTERNAL_VENDOR_METRIC_KEYS)
    external["rugcheck_score"] = rugcheck_score(rug)
    if isinstance(defade, dict):
        external["defade_rug_score"] = as_float(defade.get("rugScore"))
    external["goplus_risky_flag_count"] = count_goplus_risky_flags(goplus)

    contract = empty_metrics(CONTRACT_PERMISSION_METRIC_KEYS)
    if isinstance(jup, dict):
        audit = jup.get("audit") or {}
        contract["mint_authority_disabled"] = as_bool(audit.get("mintAuthorityDisabled"))
        contract["freeze_authority_disabled"] = as_bool(audit.get("freezeAuthorityDisabled"))
        token_program = jup.get("tokenProgram")
        if token_program is not None:
            contract["token_2022_detected"] = "TokenzQd" in str(token_program)
    if isinstance(goplus, dict):
        mintable = as_bool(goplus.get("is_mintable")) or as_bool(goplus.get("mintable"))
        freezable = as_bool(goplus.get("is_freezable")) or as_bool(goplus.get("freezable"))
        if mintable is not None and contract["mint_authority_disabled"] is None:
            contract["mint_authority_disabled"] = not mintable if mintable is not None else None
        if freezable is not None and contract["freeze_authority_disabled"] is None:
            contract["freeze_authority_disabled"] = not freezable if freezable is not None else None
        contract["metadata_mutable"] = as_bool(goplus.get("metadata_mutable"))
        contract["non_transferable"] = as_bool(goplus.get("is_non_transferable"))
        contract["transfer_fee_upgradable"] = as_bool(goplus.get("transfer_fee_upgradable"))

    holder = empty_metrics(HOLDER_DISTRIBUTION_METRIC_KEYS)
    if isinstance(jup, dict):
        audit = jup.get("audit") or {}
        holder["top_holders_pct"] = as_float(audit.get("topHoldersPercentage"))
        holder["holder_count"] = as_float(jup.get("holderCount"))
    if isinstance(defade, dict):
        if holder["top_holders_pct"] is None:
            holder["top_holders_pct"] = as_float(defade.get("holderScore"))
        if holder["holder_count"] is None:
            holder["holder_count"] = as_float(defade.get("holderCount"))
        holder["insider_score"] = as_float(defade.get("insiderScore"))
        holder["bundle_score"] = as_float(defade.get("bundleScore"))
        sniper = as_float(defade.get("sniperScore"))
        if sniper is not None:
            holder["sniper_score"] = sniper

    liquidity = empty_metrics(LIQUIDITY_HEALTH_METRIC_KEYS)
    liquidity["total_liquidity_usd"] = as_float(dex.get("total_liquidity_usd"))
    liquidity["top_pair_liquidity_usd"] = as_float(dex.get("top_pair_liquidity_usd"))
    liquidity["lp_locked_pct"] = extract_lp_locked_pct(rug)
    liquidity["pair_count"] = (
        int(dex["pair_count"]) if dex.get("pair_count") is not None else None
    )
    liquidity["newest_pair_age_hours"] = as_float(dex.get("newest_pair_age_hours"))

    trading = empty_metrics(TRADING_BEHAVIOR_METRIC_KEYS)
    trading["h24_buys"] = dex.get("h24_buys")
    trading["h24_sells"] = dex.get("h24_sells")
    trading["h24_volume_usd"] = as_float(dex.get("h24_volume_usd"))
    trading["h24_price_change_pct"] = as_float(dex.get("h24_price_change_pct"))
    trading["h24_sell_buy_ratio"] = as_float(dex.get("h24_sell_buy_ratio"))
    trading["h24_volume_liquidity_ratio"] = as_float(dex.get("h24_volume_liquidity_ratio"))

    verification = empty_metrics(VERIFICATION_IDENTITY_METRIC_KEYS)
    websites = dex.get("websites") or []
    socials = dex.get("socials") or []
    verification["website_count"] = len(websites) if isinstance(websites, list) else None
    verification["social_count"] = len(socials) if isinstance(socials, list) else None
    if isinstance(jup, dict):
        verification["jupiter_verified"] = as_bool(jup.get("isVerified"))
        verification["jupiter_organic_score"] = as_float(jup.get("organicScore"))

    return {
        "external_vendor_risk": external,
        "contract_permissions": contract,
        "holder_distribution": holder,
        "liquidity_health": liquidity,
        "trading_behavior": trading,
        "verification_identity": verification,
    }


# ---------------------------------------------------------------------------
# Scoring categories (logic preserved from prototype)
# ---------------------------------------------------------------------------


def score_external_vendor_risk(
    rug: Optional[dict],
    defade: Optional[dict],
    goplus: Optional[dict],
) -> Dict[str, Any]:
    scores: List[Tuple[Optional[float], float]] = []
    rscore = rugcheck_score(rug)
    if rscore is not None:
        scores.append((rscore, 0.55))
    if isinstance(defade, dict):
        ds = as_float(defade.get("rugScore"))
        if ds is not None:
            scores.append((clamp(ds), 0.45))
    if isinstance(goplus, dict):
        risky_keys = [
            "is_mintable",
            "mintable",
            "is_freezable",
            "freezable",
            "metadata_mutable",
            "is_honeypot",
            "is_blacklisted",
            "is_proxy",
            "is_open_source",
            "transfer_fee_upgradable",
            "is_non_transferable",
        ]
        gp_penalties = []
        for k in risky_keys:
            if k not in goplus:
                continue
            b = as_bool(goplus.get(k))
            if k == "is_open_source":
                if b is False:
                    gp_penalties.append(25)
            elif b is True:
                gp_penalties.append(60 if "honeypot" in k or "blacklisted" in k else 35)
        if gp_penalties:
            scores.append((clamp(sum(gp_penalties)), 0.25))
    score = weighted_average(scores)
    return {"score": score, "level": risk_level(score)}


def score_contract_permissions(
    rug: Optional[dict],
    goplus: Optional[dict],
    jup: Optional[dict],
) -> Dict[str, Any]:
    penalties: List[float] = []
    has_audit_signal = False
    if isinstance(jup, dict):
        audit = jup.get("audit") or {}
        mint_disabled = as_bool(audit.get("mintAuthorityDisabled"))
        freeze_disabled = as_bool(audit.get("freezeAuthorityDisabled"))
        if mint_disabled is not None or freeze_disabled is not None:
            has_audit_signal = True
        if mint_disabled is False:
            penalties.append(35)
        if freeze_disabled is False:
            penalties.append(35)
        token_program = jup.get("tokenProgram")
        if token_program and "TokenzQd" in str(token_program):
            penalties.append(10)
    if isinstance(goplus, dict):
        checks = [
            ("is_mintable", 35),
            ("mintable", 35),
            ("is_freezable", 35),
            ("freezable", 35),
            ("metadata_mutable", 20),
            ("transfer_fee_upgradable", 25),
            ("is_non_transferable", 50),
        ]
        for key, penalty in checks:
            if as_bool(goplus.get(key)) is True:
                penalties.append(penalty)
    for r in rugcheck_risks(rug):
        text = " ".join(str(r.get(k, "")) for k in ("name", "description", "value")).lower()
        if "mint" in text and "authority" in text:
            penalties.append(35)
        if "freeze" in text and "authority" in text:
            penalties.append(35)
        if "mutable" in text:
            penalties.append(20)
    if penalties:
        score = clamp(sum(penalties))
    elif has_audit_signal or isinstance(goplus, dict):
        score = 10.0
    else:
        score = None
    return {"score": score, "level": risk_level(score)}


def score_holder_distribution(
    rug: Optional[dict],
    jup: Optional[dict],
    defade: Optional[dict],
) -> Dict[str, Any]:
    scores: List[Tuple[Optional[float], float]] = []
    if isinstance(jup, dict):
        audit = jup.get("audit") or {}
        top_pct = as_float(audit.get("topHoldersPercentage"))
        if top_pct is not None:
            if top_pct >= 30:
                s = 100
            elif top_pct >= 20:
                s = 80
            elif top_pct >= 10:
                s = 55
            elif top_pct >= 5:
                s = 30
            else:
                s = 10
            scores.append((s, 0.55))
        holders = as_float(jup.get("holderCount"))
        if holders is not None:
            if holders < 50:
                s = 75
            elif holders < 200:
                s = 55
            elif holders < 1000:
                s = 30
            else:
                s = 10
            scores.append((s, 0.25))
    if isinstance(defade, dict):
        for key in (
            "holderRiskScore",
            "holderScore",
            "insiderScore",
            "bundleScore",
            "sniperScore",
        ):
            v = as_float(defade.get(key))
            if v is not None:
                scores.append((clamp(v), 0.25))
    risk_hits = []
    for r in rugcheck_risks(rug):
        text = " ".join(str(r.get(k, "")) for k in ("name", "description", "value")).lower()
        if any(w in text for w in ("holder", "concentration", "insider", "sniper", "top 10")):
            s = as_float(r.get("score"))
            if s is None:
                s = risk_score_from_level(r.get("level")) or 50
            risk_hits.append(clamp(s))
    if risk_hits:
        scores.append((max(risk_hits), 0.45))
    score = weighted_average(scores)
    return {"score": score, "level": risk_level(score)}


def score_liquidity_health(
    rug: Optional[dict],
    dex: Dict[str, Any],
    jup: Optional[dict],
) -> Dict[str, Any]:
    scores: List[Tuple[Optional[float], float]] = []
    liq = as_float(dex.get("total_liquidity_usd"))
    if liq is not None:
        if liq <= 0:
            s = 100
        elif liq < 1_000:
            s = 95
        elif liq < 5_000:
            s = 80
        elif liq < 20_000:
            s = 55
        elif liq < 100_000:
            s = 30
        else:
            s = 10
        scores.append((s, 0.55))
    age = as_float(dex.get("newest_pair_age_hours"))
    if age is not None:
        if age < 1:
            s = 70
        elif age < 6:
            s = 45
        elif age < 24:
            s = 25
        else:
            s = 10
        scores.append((s, 0.15))
    lp_locked = extract_lp_locked_pct(rug)
    if lp_locked is not None:
        if lp_locked < 20:
            s = 90
        elif lp_locked < 50:
            s = 65
        elif lp_locked < 80:
            s = 35
        else:
            s = 10
        scores.append((s, 0.30))
    score = weighted_average(scores)
    return {"score": score, "level": risk_level(score)}


def score_trading_behavior(dex: Dict[str, Any], jup: Optional[dict]) -> Dict[str, Any]:
    scores: List[Tuple[Optional[float], float]] = []
    buys = int(as_float(dex.get("h24_buys")) or 0)
    sells = int(as_float(dex.get("h24_sells")) or 0)
    if buys or sells:
        if buys >= 10 and sells == 0:
            s = 95
        elif buys > 0:
            sell_buy_ratio = sells / buys
            if sell_buy_ratio < 0.05:
                s = 85
            elif sell_buy_ratio < 0.20:
                s = 60
            elif sell_buy_ratio > 5:
                s = 65
            else:
                s = 20
        else:
            s = 35
        scores.append((s, 0.60))
    pc = as_float(dex.get("h24_price_change_pct"))
    if pc is not None:
        if pc <= -80:
            s = 90
        elif pc >= 1000:
            s = 75
        elif pc >= 300:
            s = 55
        elif pc <= -50:
            s = 60
        else:
            s = 20
        scores.append((s, 0.20))
    liq = as_float(dex.get("total_liquidity_usd")) or 0.0
    vol = as_float(dex.get("h24_volume_usd")) or 0.0
    if liq > 0 and vol > 0:
        vol_liq = vol / liq
        if vol_liq > 100:
            s = 80
        elif vol_liq > 30:
            s = 60
        elif vol_liq > 10:
            s = 40
        else:
            s = 20
        scores.append((s, 0.20))
    score = weighted_average(scores)
    return {"score": score, "level": risk_level(score)}


def score_verification_identity(dex: Dict[str, Any], jup: Optional[dict]) -> Dict[str, Any]:
    scores: List[Tuple[Optional[float], float]] = []
    websites = dex.get("websites") or []
    socials = dex.get("socials") or []
    if not websites and not socials:
        scores.append((45, 0.25))
    else:
        scores.append((15, 0.25))
    if isinstance(jup, dict):
        is_verified = as_bool(jup.get("isVerified"))
        organic = as_float(jup.get("organicScore"))
        if is_verified is True:
            scores.append((5, 0.35))
        elif is_verified is False:
            scores.append((55, 0.35))
        if organic is not None:
            scores.append((100 - clamp(organic), 0.30))
    score = weighted_average(scores)
    return {"score": score, "level": risk_level(score)}


def combine_category_scores(
    categories: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[float], float, float]:
    weighted_items: List[Tuple[float, float]] = []
    available_weight = 0.0
    for name, cat in categories.items():
        s = cat.get("score")
        w = CATEGORY_WEIGHTS.get(name, 0.0)
        if s is not None:
            weighted_items.append((float(s), w))
            available_weight += w
    if not weighted_items or available_weight <= 0:
        return None, 0.0, 0.0
    overall = sum(s * w for s, w in weighted_items) / available_weight
    confidence = clamp(available_weight * 100.0) or 0.0
    coverage = available_weight / sum(CATEGORY_WEIGHTS.values())
    return clamp(overall), float(confidence), round(coverage, 4)


# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------


def build_warnings(
    categories: Dict[str, Dict[str, Any]],
    metrics_by_category: Dict[str, Dict[str, Any]],
    source_results: Dict[str, SourceResult],
) -> List[Dict[str, Any]]:
    warnings: List[Dict[str, Any]] = []

    for cat_name, cat in categories.items():
        score = cat.get("score")
        if score is not None and score >= 60:
            warnings.append(
                {
                    "category": cat_name,
                    "severity": cat.get("level", "HIGH"),
                    "code": "CATEGORY_ELEVATED_RISK",
                    "message": f"{cat_name} score is elevated",
                    "value": round(float(score), 2),
                }
            )

    contract = metrics_by_category.get("contract_permissions", {})
    if contract.get("mint_authority_disabled") is False:
        warnings.append(
            {
                "category": "contract_permissions",
                "severity": "HIGH",
                "code": "MINT_AUTHORITY_ENABLED",
                "message": "Mint authority is not disabled",
                "value": False,
            }
        )
    if contract.get("freeze_authority_disabled") is False:
        warnings.append(
            {
                "category": "contract_permissions",
                "severity": "HIGH",
                "code": "FREEZE_AUTHORITY_ENABLED",
                "message": "Freeze authority is not disabled",
                "value": False,
            }
        )

    liquidity = metrics_by_category.get("liquidity_health", {})
    liq = as_float(liquidity.get("total_liquidity_usd"))
    if liq is not None and liq < 5_000:
        warnings.append(
            {
                "category": "liquidity_health",
                "severity": "HIGH" if liq < 1_000 else "MEDIUM",
                "code": "LOW_LIQUIDITY",
                "message": "Total liquidity is low",
                "value": liq,
            }
        )

    lp_locked = as_float(liquidity.get("lp_locked_pct"))
    if lp_locked is not None and lp_locked < 50:
        warnings.append(
            {
                "category": "liquidity_health",
                "severity": "HIGH" if lp_locked < 20 else "MEDIUM",
                "code": "LOW_LP_LOCK",
                "message": "LP lock percentage is low",
                "value": lp_locked,
            }
        )

    for src_name, result in source_results.items():
        if result.requires_key and not result.attempted:
            warnings.append(
                {
                    "category": "data_quality",
                    "severity": "LOW",
                    "code": "SOURCE_SKIPPED_NO_KEY",
                    "message": f"{src_name} skipped: API key not configured",
                    "value": src_name,
                }
            )
        elif result.attempted and not result.success:
            warnings.append(
                {
                    "category": "data_quality",
                    "severity": "MEDIUM",
                    "code": "SOURCE_FETCH_FAILED",
                    "message": f"{src_name} fetch failed",
                    "value": src_name,
                }
            )

    return warnings


def warning_counts(warnings: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {s.lower(): 0 for s in SEVERITY_ORDER}
    counts["total"] = len(warnings)
    for w in warnings:
        sev = str(w.get("severity", "LOW")).upper()
        key = sev.lower() if sev.lower() in counts else "low"
        counts[key] = counts.get(key, 0) + 1
    return {
        "warning_count_total": counts["total"],
        "warning_count_low": counts.get("low", 0),
        "warning_count_medium": counts.get("medium", 0),
        "warning_count_high": counts.get("high", 0),
        "warning_count_critical": counts.get("critical", 0),
    }


# ---------------------------------------------------------------------------
# Report assembly
# ---------------------------------------------------------------------------


def build_report(mint: str, config: Optional[ReportConfig] = None) -> StandardRiskReport:
    """Build a complete standardized risk report for a Solana token mint."""
    cfg = config or ReportConfig.from_env()
    client = RiskReportClient(cfg)
    source_results = client.fetch_all(mint)

    rug = source_results["rugcheck"].data
    dex_pairs = source_results["dexscreener"].data or []
    dex = aggregate_dex_pairs(dex_pairs if isinstance(dex_pairs, list) else [])
    defade = source_results["defade"].data
    goplus = source_results["goplus"].data
    jup = source_results["jupiter"].data

    metrics_by_category = build_category_metrics(rug, dex, defade, goplus, jup)

    raw_scores = {
        "external_vendor_risk": score_external_vendor_risk(rug, defade, goplus),
        "contract_permissions": score_contract_permissions(rug, goplus, jup),
        "holder_distribution": score_holder_distribution(rug, jup, defade),
        "liquidity_health": score_liquidity_health(rug, dex, jup),
        "trading_behavior": score_trading_behavior(dex, jup),
        "verification_identity": score_verification_identity(dex, jup),
    }

    categories: Dict[str, Dict[str, Any]] = {}
    for cat_name in CATEGORY_NAMES:
        scored = raw_scores[cat_name]
        score = scored.get("score")
        categories[cat_name] = {
            "score": round(score, 2) if score is not None else None,
            "level": scored.get("level", "UNKNOWN"),
            "metrics": metrics_by_category[cat_name],
        }

    overall_score, confidence, coverage = combine_category_scores(raw_scores)
    overall_level = risk_level(overall_score)

    source_status = {
        name: source_results[name].to_status_dict() for name in SOURCE_NAMES
    }

    warnings = build_warnings(categories, metrics_by_category, source_results)

    top_pair = None
    if isinstance(dex_pairs, list) and dex_pairs:
        def liq(p: dict) -> float:
            return as_float((p.get("liquidity") or {}).get("usd")) or 0.0

        valid = [p for p in dex_pairs if isinstance(p, dict)]
        if valid:
            top_pair = max(valid, key=liq)

    token = extract_token_info(mint, rug, jup, top_pair)

    raw_section: Dict[str, Any] = {name: None for name in SOURCE_NAMES}
    if cfg.include_raw:
        for name, result in source_results.items():
            raw_section[name] = result.raw if result.success else result.raw

    report_stub = StandardRiskReport(
        schema_version=SCHEMA_VERSION,
        generated_at_utc=utc_now_iso(),
        token=token,
        overall={
            "risk_score": round(overall_score, 2) if overall_score is not None else None,
            "risk_level": overall_level,
            "confidence_score": round(confidence, 1),
            "coverage_ratio": coverage,
            "model_version": MODEL_VERSION,
        },
        source_status=source_status,
        categories=categories,
        feature_row={},
        warnings=warnings,
        raw=raw_section,
    )

    feature_row = flatten_report(report_stub)
    report_stub.feature_row = feature_row
    return report_stub


def flatten_report(report: StandardRiskReport) -> FeatureRow:
    """Flatten a standardized report into an ML-friendly feature dict."""
    row: FeatureRow = {
        "schema_version": report.schema_version,
        "generated_at_utc": report.generated_at_utc,
        "chain": report.token.get("chain"),
        "mint": report.token.get("mint"),
        "token_symbol": report.token.get("symbol"),
        "token_name": report.token.get("name"),
        "token_decimals": report.token.get("decimals"),
        "overall_risk_score": report.overall.get("risk_score"),
        "overall_risk_level": report.overall.get("risk_level"),
        "overall_risk_level_code": risk_level_code(str(report.overall.get("risk_level", "UNKNOWN"))),
        "confidence_score": report.overall.get("confidence_score"),
        "coverage_ratio": report.overall.get("coverage_ratio"),
        "model_version": report.overall.get("model_version"),
    }

    for src in SOURCE_NAMES:
        status = report.source_status.get(src, {})
        row[f"source_{src}_attempted"] = bool_to_ml(status.get("attempted"))
        row[f"source_{src}_success"] = bool_to_ml(status.get("success"))
        row[f"source_{src}_available"] = bool_to_ml(status.get("available"))
        row[f"source_{src}_latency_ms"] = status.get("latency_ms")
        row[f"source_{src}_http_status"] = status.get("http_status")

    for cat_name in CATEGORY_NAMES:
        cat = report.categories.get(cat_name, {})
        row[f"{cat_name}_score"] = cat.get("score")
        row[f"{cat_name}_level_code"] = risk_level_code(str(cat.get("level", "UNKNOWN")))
        metrics = cat.get("metrics") or {}
        for metric_key, value in metrics.items():
            field_name = f"{cat_name}__{metric_key}"
            if isinstance(value, bool):
                row[field_name] = bool_to_ml(value)
            else:
                row[field_name] = value

    row.update(warning_counts(report.warnings))
    return row


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_report_schema(report: Union[StandardRiskReport, Dict[str, Any]]) -> None:
    """Validate required sections and fixed metric keys. Raises ValueError on failure."""
    data = report.to_dict() if isinstance(report, StandardRiskReport) else report

    required_top = (
        "schema_version",
        "generated_at_utc",
        "token",
        "overall",
        "source_status",
        "categories",
        "feature_row",
        "warnings",
        "raw",
    )
    for key in required_top:
        if key not in data:
            raise ValueError(f"Missing top-level key: {key}")

    for src in SOURCE_NAMES:
        if src not in data["source_status"]:
            raise ValueError(f"Missing source_status entry: {src}")
        status = data["source_status"][src]
        for field_name in (
            "attempted",
            "success",
            "available",
            "requires_key",
            "latency_ms",
            "http_status",
            "error_type",
            "error_message",
        ):
            if field_name not in status:
                raise ValueError(f"source_status.{src} missing field: {field_name}")

    for cat_name in CATEGORY_NAMES:
        if cat_name not in data["categories"]:
            raise ValueError(f"Missing category: {cat_name}")
        cat = data["categories"][cat_name]
        for field_name in ("score", "level", "metrics"):
            if field_name not in cat:
                raise ValueError(f"categories.{cat_name} missing field: {field_name}")
        expected_metrics = CATEGORY_METRIC_KEYS[cat_name]
        metrics = cat.get("metrics") or {}
        for mk in expected_metrics:
            if mk not in metrics:
                raise ValueError(f"categories.{cat_name}.metrics missing key: {mk}")


# ---------------------------------------------------------------------------
# File output
# ---------------------------------------------------------------------------


def write_report(
    report: StandardRiskReport,
    path: Union[str, Path],
    fmt: str,
    *,
    append: bool = False,
    pretty: bool = False,
) -> None:
    """Write a report to disk in the requested format."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fmt_lower = fmt.lower()

    if fmt_lower == "json":
        payload = report.to_dict()
        indent = 2 if pretty else None
        text = json.dumps(payload, indent=indent, ensure_ascii=False, default=json_default)
        out_path.write_text(text + ("\n" if pretty else ""), encoding="utf-8")
        return

    if fmt_lower == "jsonl":
        line = json.dumps(report.to_dict(), ensure_ascii=False, default=json_default)
        mode = "a" if append else "w"
        needs_sep = False
        if append and out_path.exists() and out_path.stat().st_size > 0:
            with out_path.open("rb") as rf:
                rf.seek(-1, os.SEEK_END)
                needs_sep = rf.read(1) != b"\n"
        with out_path.open(mode, encoding="utf-8") as f:
            if needs_sep:
                f.write("\n")
            f.write(line + "\n")
        return

    if fmt_lower == "csv":
        row = flatten_report(report)
        _write_csv_row(out_path, row, append=append)
        return

    if fmt_lower == "parquet":
        if append:
            raise NotImplementedError(
                "Parquet append is not supported. Use --format jsonl --append for "
                "streaming append logs, then batch-convert to Parquet."
            )
        _write_parquet_row(out_path, flatten_report(report))
        return

    raise ValueError(f"Unsupported format: {fmt}")


def _write_csv_row(path: Path, row: FeatureRow, *, append: bool) -> None:
    fieldnames: List[str]
    if append and path.exists() and path.stat().st_size > 0:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    else:
        fieldnames = list(row.keys())

    mode = "a" if append else "w"
    write_header = not (append and path.exists() and path.stat().st_size > 0)
    with path.open(mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k) for k in fieldnames})


def _write_parquet_row(path: Path, row: FeatureRow) -> None:
    try:
        import pandas as pd
    except ImportError as e:
        raise ImportError(
            "Parquet output requires pandas and pyarrow. "
            "Install with: pip install pandas pyarrow"
        ) from e
    try:
        import pyarrow  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Parquet output requires pyarrow. Install with: pip install pyarrow"
        ) from e

    df = pd.DataFrame([row])
    df.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Human-readable output
# ---------------------------------------------------------------------------


def render_human_report(report: StandardRiskReport) -> str:
    """Render the short human-readable security report."""
    buf = StringIO()

    print("\n=== Solana Token Risk Report ===", file=buf)

    symbol = report.token.get("symbol")
    name = report.token.get("name")
    label = f"{symbol} ({name})" if symbol and name else symbol or name

    print(f"Token: {report.token.get('mint')}", file=buf)
    if label:
        print(f"Name: {label}", file=buf)

    overall = report.overall
    print(
        f"Overall risk: {overall.get('risk_score')} / 100 "
        f"({overall.get('risk_level')})",
        file=buf,
    )
    print(f"Confidence: {overall.get('confidence_score')}%", file=buf)
    print(f"Coverage ratio: {overall.get('coverage_ratio')}", file=buf)

    print("\nSource status:", file=buf)
    for src, status in report.source_status.items():
        if status.get("success"):
            print(
                f"  - {src}: ok "
                f"(latency_ms={status.get('latency_ms')}, http_status={status.get('http_status')})",
                file=buf,
            )
        elif status.get("attempted"):
            print(
                f"  - {src}: failed "
                f"({status.get('error_type')}: {status.get('error_message')})",
                file=buf,
            )
        else:
            print(f"  - {src}: skipped ({status.get('error_message')})", file=buf)

    print("\nSub-category scores:", file=buf)
    for name, cat in report.categories.items():
        print(f"\n  {name}: {cat.get('score')} / 100 ({cat.get('level')})", file=buf)
        metrics = cat.get("metrics") or {}
        for metric_name, value in metrics.items():
            print(f"    - {metric_name}: {value}", file=buf)

    if report.warnings:
        print("\nTop warnings:", file=buf)
        for w in report.warnings[:12]:
            print(
                f"  - [{w.get('severity')}] {w.get('code')}: "
                f"{w.get('message')} (value={w.get('value')})",
                file=buf,
            )

    liq = report.categories.get("liquidity_health", {}).get("metrics", {})
    trading = report.categories.get("trading_behavior", {}).get("metrics", {})

    print("\nMarket snapshot:", file=buf)
    print(f"  Pair count: {liq.get('pair_count')}", file=buf)
    print(f"  Total liquidity: {liq.get('total_liquidity_usd')}", file=buf)
    print(f"  Top pair liquidity: {liq.get('top_pair_liquidity_usd')}", file=buf)
    print(f"  Newest pair age hours: {liq.get('newest_pair_age_hours')}", file=buf)
    print(f"  24h buys/sells: {trading.get('h24_buys')}/{trading.get('h24_sells')}", file=buf)
    print(f"  24h volume: {trading.get('h24_volume_usd')}", file=buf)
    print(f"  24h price change: {trading.get('h24_price_change_pct')}%", file=buf)

    print("\nSchema:", report.schema_version, file=buf)
    print(
        "\nDisclaimer: This is a heuristic risk screen, not financial advice "
        "and not a guarantee. Always inspect raw API reports, liquidity lockers, "
        "deployer wallets, and recent transactions.",
        file=buf,
    )

    return buf.getvalue()


def print_human(report: StandardRiskReport) -> None:
    """Print a human-readable summary."""
    print(render_human_report(report), end="")


def write_standard_security_outputs(
    report: StandardRiskReport,
    analysis_root: Union[str, Path] = DEFAULT_ANALYSIS_ROOT,
) -> Dict[str, Path]:
    """Write standardized per-mint security outputs.

    Layout:
        data/raw/analysis/<mint>/security_report
        data/raw/analysis/<mint>/security_analysis

    security_report is human-readable text.
    security_analysis is a compact flat JSON feature row for downstream analysis.
    """
    mint = str(report.token.get("mint") or "").strip()
    if not mint:
        raise ValueError("Cannot write standard outputs without token.mint")

    out_dir = Path(analysis_root) / mint
    out_dir.mkdir(parents=True, exist_ok=True)

    report_path = out_dir / SECURITY_REPORT_FILENAME
    analysis_path = out_dir / SECURITY_ANALYSIS_FILENAME

    report_path.write_text(render_human_report(report), encoding="utf-8")

    analysis_row = flatten_report(report)
    analysis_path.write_text(
        json.dumps(
            analysis_row,
            ensure_ascii=False,
            separators=(",", ":"),
            default=json_default,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "directory": out_dir,
        "security_report": report_path,
        "security_analysis": analysis_path,
    }


def _print_standard_save_paths(paths: Mapping[str, Path], *, stream: Any = sys.stdout) -> None:
    print(f"\nSaved security_report: {paths['security_report']}", file=stream)
    print(f"Saved security_analysis: {paths['security_analysis']}", file=stream)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _resolve_mint(args: argparse.Namespace) -> str:
    mint = args.mint or getattr(args, "mint_positional", None)
    if not mint:
        raise SystemExit("Error: provide a mint address as argument or --mint <MINT>")
    return mint.strip()


def _format_was_explicit(argv: Sequence[str]) -> bool:
    return any(arg == "--format" or arg.startswith("--format=") for arg in argv)


def _print_machine_stdout(report: StandardRiskReport, fmt: str, *, pretty: bool) -> int:
    fmt_lower = fmt.lower()

    if fmt_lower == "json":
        indent = 2 if pretty else None
        text = json.dumps(report.to_dict(), indent=indent, ensure_ascii=False, default=json_default)
        try:
            sys.stdout.buffer.write(text.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")
            sys.stdout.buffer.flush()
        except (AttributeError, OSError):
            print(text)
        return 0

    if fmt_lower == "jsonl":
        text = json.dumps(report.to_dict(), ensure_ascii=False, default=json_default)
        print(text)
        return 0

    if fmt_lower == "csv":
        row = flatten_report(report)
        writer = csv.DictWriter(sys.stdout, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)
        return 0

    if fmt_lower == "parquet":
        print("Parquet cannot be written to stdout. Use --out <path>.", file=sys.stderr)
        return 1

    print(f"Unsupported format: {fmt}", file=sys.stderr)
    return 1


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Solana token risk report with human display and machine-readable exports.",
    )
    parser.add_argument("mint_positional", nargs="?", help="Solana token mint address")
    parser.add_argument("--mint", dest="mint", help="Solana token mint address")
    parser.add_argument(
        "--format",
        choices=["json", "jsonl", "csv", "parquet"],
        default=None,
        help="Machine-readable output format. Default is json when --out or --machine is used.",
    )
    parser.add_argument("--out", help="Output file path for machine-readable export")
    parser.add_argument(
        "--analysis-root",
        default=str(DEFAULT_ANALYSIS_ROOT),
        help="Base directory for standard per-mint outputs. Default: data/raw/analysis",
    )
    parser.add_argument(
        "--no-standard-save",
        action="store_true",
        help="Do not write data/raw/analysis/<mint>/security_report and security_analysis.",
    )
    parser.add_argument("--append", action="store_true", help="Append to JSONL/CSV output")
    parser.add_argument("--no-raw", action="store_true", help="Exclude raw vendor payloads")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout in seconds")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate schema before writing output",
    )
    parser.add_argument(
        "--human",
        action="store_true",
        help="Force human-readable summary. This is already the default when not exporting.",
    )
    parser.add_argument(
        "--machine",
        action="store_true",
        help="Print machine-readable output to stdout instead of the human summary.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]

    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    mint = _resolve_mint(args)
    output_format = args.format or "json"
    explicit_machine_request = args.machine or _format_was_explicit(raw_argv)

    config = ReportConfig.from_env(timeout=args.timeout, include_raw=not args.no_raw)

    logger.info("Building risk report for mint=%s", mint)
    report = build_report(mint, config=config)

    if args.validate:
        validate_report_schema(report)
        logger.info("Schema validation passed")

    standard_paths: Optional[Dict[str, Path]] = None
    if not args.no_standard_save:
        standard_paths = write_standard_security_outputs(report, args.analysis_root)
        logger.info(
            "Wrote standard security outputs to %s",
            standard_paths["directory"],
        )

    if args.out:
        try:
            write_report(
                report,
                args.out,
                output_format,
                append=args.append,
                pretty=args.pretty,
            )
        except (ImportError, NotImplementedError) as e:
            print(str(e), file=sys.stderr)
            return 1

        logger.info("Wrote %s report to %s", output_format, args.out)
        print(f"Wrote {output_format.upper()} report to {args.out}")

        if standard_paths is not None:
            _print_standard_save_paths(standard_paths)

        if args.human:
            print_human(report)

        return 0

    if args.human or not explicit_machine_request:
        print_human(report)
        if standard_paths is not None:
            _print_standard_save_paths(standard_paths)
        return 0

    return _print_machine_stdout(report, output_format, pretty=args.pretty)


if __name__ == "__main__":
    raise SystemExit(main())