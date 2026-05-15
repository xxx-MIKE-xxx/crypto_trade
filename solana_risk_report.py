#!/usr/bin/env python3
"""
Solana small-cap token risk/security report.

Risk score convention:
    0   = lowest observed risk
    100 = highest observed risk

Default no-key sources:
    - RugCheck
    - DEX Screener

Optional key-gated sources:
    - DEFADE_API_KEY
    - GOPLUS_BEARER
    - JUPITER_API_KEY

Usage:
    pip install requests python-dotenv
    python solana_risk_report.py <SOLANA_TOKEN_MINT>
    python solana_risk_report.py <SOLANA_TOKEN_MINT> --json
    python solana_risk_report.py <SOLANA_TOKEN_MINT> --out report.json

Environment variables:
    export DEFADE_API_KEY="df_..."
    export GOPLUS_BEARER="eyJ..."
    export JUPITER_API_KEY="..."
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv
import requests

load_dotenv()

RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"
DEXSCREENER_BASE = "https://api.dexscreener.com"
DEFADE_BASE = "https://api.defade.org"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"
JUPITER_BASE = "https://api.jup.ag/tokens/v2"


CATEGORY_WEIGHTS = {
    "external_vendor_risk": 0.25,
    "contract_permissions": 0.20,
    "holder_distribution": 0.15,
    "liquidity_health": 0.20,
    "trading_behavior": 0.10,
    "verification_identity": 0.10,
}


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


def http_get_json(
    name: str,
    url: str,
    *,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: int = 15,
) -> Tuple[Optional[Any], Optional[str]]:
    try:
        r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if r.status_code == 404:
            return None, f"{name}: 404 not found"
        if r.status_code == 401:
            return None, f"{name}: 401 unauthorized; check API key/token"
        if r.status_code == 403:
            return None, f"{name}: 403 forbidden; key may not have access"
        if r.status_code == 429:
            return None, f"{name}: 429 rate limited"
        r.raise_for_status()
        return r.json(), None
    except requests.RequestException as e:
        return None, f"{name}: request failed: {e}"
    except ValueError as e:
        return None, f"{name}: invalid JSON: {e}"


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


def add_evidence(evidence: List[str], text: str) -> None:
    if text and text not in evidence:
        evidence.append(text)


# -------------------------
# Data fetchers
# -------------------------

def fetch_rugcheck(mint: str) -> Dict[str, Any]:
    headers = {}
    maybe_key = os.getenv("RUGCHECK_API_KEY")
    if maybe_key:
        headers["Authorization"] = f"Bearer {maybe_key}"

    url = f"{RUGCHECK_BASE}/tokens/{mint}/report"
    data, err = http_get_json("rugcheck", url, headers=headers)
    return {
        "ok": data is not None,
        "error": err,
        "data": data,
    }


def fetch_dexscreener(mint: str) -> Dict[str, Any]:
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{mint}"
    data, err = http_get_json("dexscreener", url)
    if isinstance(data, dict) and "pairs" in data:
        pairs = data.get("pairs") or []
    elif isinstance(data, list):
        pairs = data
    else:
        pairs = []
    return {
        "ok": data is not None,
        "error": err,
        "data": pairs,
    }


def fetch_defade(mint: str) -> Dict[str, Any]:
    key = os.getenv("DEFADE_API_KEY")
    if not key:
        return {"ok": False, "error": "DEFADE_API_KEY not set", "data": None}

    url = f"{DEFADE_BASE}/v1/analyze/{mint}"
    data, err = http_get_json("defade", url, headers={"x-api-key": key})
    return {
        "ok": data is not None,
        "error": err,
        "data": data,
    }


def fetch_goplus(mint: str) -> Dict[str, Any]:
    bearer = os.getenv("GOPLUS_BEARER")
    if not bearer:
        return {"ok": False, "error": "GOPLUS_BEARER not set", "data": None}

    url = f"{GOPLUS_BASE}/solana/token_security"
    headers = {"Authorization": f"Bearer {bearer}"}
    params = {"contract_addresses": mint}
    data, err = http_get_json("goplus", url, headers=headers, params=params)

    parsed = None
    if isinstance(data, dict):
        result = data.get("result", data)
        if isinstance(result, dict):
            parsed = result.get(mint) or result.get(mint.lower()) or result
        else:
            parsed = result

    return {
        "ok": data is not None,
        "error": err,
        "data": parsed,
        "raw": data,
    }


def fetch_jupiter(mint: str) -> Dict[str, Any]:
    key = os.getenv("JUPITER_API_KEY")
    if not key:
        return {"ok": False, "error": "JUPITER_API_KEY not set", "data": None}

    url = f"{JUPITER_BASE}/search"
    params = {"query": mint}
    headers = {"x-api-key": key}
    data, err = http_get_json("jupiter", url, headers=headers, params=params)

    item = None
    if isinstance(data, list):
        for row in data:
            if str(row.get("id", "")).lower() == mint.lower():
                item = row
                break
        if item is None and data:
            item = data[0]

    return {
        "ok": item is not None,
        "error": err if item is None else None,
        "data": item,
        "raw": data,
    }


# -------------------------
# Normalizers / extractors
# -------------------------

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
        # RugCheck score scales have changed across wrappers/docs.
        # If it already looks 0-100, use it. If larger, squash it.
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

    created_ts = []
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

    return {
        "pair_count": len(valid),
        "total_liquidity_usd": total_liquidity,
        "top_pair_liquidity_usd": liquidity_usd(top_pair) if top_pair else None,
        "top_pair_url": top_pair.get("url") if top_pair else None,
        "top_pair_dex": top_pair.get("dexId") if top_pair else None,
        "newest_pair_age_hours": min_age_hours,
        "h24_buys": buys24,
        "h24_sells": sells24,
        "h24_volume_usd": volume24,
        "h24_price_change_pct": price_change24,
        "websites": ((top_pair or {}).get("info") or {}).get("websites") or [],
        "socials": ((top_pair or {}).get("info") or {}).get("socials") or [],
    }


# -------------------------
# Scoring categories
# -------------------------

def score_external_vendor_risk(
    rug: Optional[dict],
    defade: Optional[dict],
    goplus: Optional[dict],
) -> Dict[str, Any]:
    evidence: List[str] = []
    scores: List[Tuple[Optional[float], float]] = []

    rscore = rugcheck_score(rug)
    if rscore is not None:
        scores.append((rscore, 0.55))
        add_evidence(evidence, f"RugCheck normalized/derived risk score: {rscore:.1f}/100")

    if isinstance(defade, dict):
        ds = as_float(defade.get("rugScore"))
        if ds is not None:
            scores.append((clamp(ds), 0.45))
            add_evidence(evidence, f"DeFade rugScore: {ds:.1f}/100")
        if defade.get("riskLevel"):
            add_evidence(evidence, f"DeFade riskLevel: {defade.get('riskLevel')}")

    if isinstance(goplus, dict):
        # GoPlus may not return a single score. Penalize explicit risky booleans if present.
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
                    add_evidence(evidence, "GoPlus: source/metadata transparency warning")
            elif b is True:
                gp_penalties.append(60 if "honeypot" in k or "blacklisted" in k else 35)
                add_evidence(evidence, f"GoPlus risky flag: {k}={goplus.get(k)}")

        if gp_penalties:
            scores.append((clamp(sum(gp_penalties)), 0.25))

    score = weighted_average(scores)
    return {
        "score": score,
        "level": risk_level(score),
        "evidence": evidence,
    }


def score_contract_permissions(
    rug: Optional[dict],
    goplus: Optional[dict],
    jup: Optional[dict],
) -> Dict[str, Any]:
    evidence: List[str] = []
    penalties: List[float] = []

    if isinstance(jup, dict):
        audit = jup.get("audit") or {}
        mint_disabled = as_bool(audit.get("mintAuthorityDisabled"))
        freeze_disabled = as_bool(audit.get("freezeAuthorityDisabled"))

        if mint_disabled is False:
            penalties.append(35)
            add_evidence(evidence, "Jupiter audit: mint authority is not disabled")
        elif mint_disabled is True:
            add_evidence(evidence, "Jupiter audit: mint authority disabled")

        if freeze_disabled is False:
            penalties.append(35)
            add_evidence(evidence, "Jupiter audit: freeze authority is not disabled")
        elif freeze_disabled is True:
            add_evidence(evidence, "Jupiter audit: freeze authority disabled")

        token_program = jup.get("tokenProgram")
        if token_program and "TokenzQd" in str(token_program):
            penalties.append(10)
            add_evidence(evidence, "Jupiter: Token-2022 program detected; review extensions")

    if isinstance(goplus, dict):
        checks = [
            ("is_mintable", 35, "GoPlus: token appears mintable"),
            ("mintable", 35, "GoPlus: token appears mintable"),
            ("is_freezable", 35, "GoPlus: token appears freezable"),
            ("freezable", 35, "GoPlus: token appears freezable"),
            ("metadata_mutable", 20, "GoPlus: metadata appears mutable"),
            ("transfer_fee_upgradable", 25, "GoPlus: transfer fee may be upgradable"),
            ("is_non_transferable", 50, "GoPlus: token may be non-transferable"),
        ]
        for key, penalty, msg in checks:
            if as_bool(goplus.get(key)) is True:
                penalties.append(penalty)
                add_evidence(evidence, msg)

    for r in rugcheck_risks(rug):
        text = " ".join(str(r.get(k, "")) for k in ("name", "description", "value")).lower()
        if "mint" in text and "authority" in text:
            penalties.append(35)
            add_evidence(evidence, "RugCheck risk: mint authority issue")
        if "freeze" in text and "authority" in text:
            penalties.append(35)
            add_evidence(evidence, "RugCheck risk: freeze authority issue")
        if "mutable" in text:
            penalties.append(20)
            add_evidence(evidence, "RugCheck risk: mutable metadata")

    score = clamp(sum(penalties)) if penalties else (10 if evidence else None)
    return {
        "score": score,
        "level": risk_level(score),
        "evidence": evidence,
    }


def score_holder_distribution(
    rug: Optional[dict],
    jup: Optional[dict],
    defade: Optional[dict],
) -> Dict[str, Any]:
    evidence: List[str] = []
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
            add_evidence(evidence, f"Jupiter audit: top holders own {top_pct:.2f}%")

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
            add_evidence(evidence, f"Jupiter holderCount: {int(holders)}")

    if isinstance(defade, dict):
        for key in ("holderRiskScore", "holderScore", "insiderScore", "bundleScore"):
            v = as_float(defade.get(key))
            if v is not None:
                scores.append((clamp(v), 0.25))
                add_evidence(evidence, f"DeFade {key}: {v:.1f}/100")

    risk_hits = []
    for r in rugcheck_risks(rug):
        text = " ".join(str(r.get(k, "")) for k in ("name", "description", "value")).lower()
        if any(w in text for w in ("holder", "concentration", "insider", "sniper", "top 10")):
            s = as_float(r.get("score"))
            if s is None:
                s = risk_score_from_level(r.get("level")) or 50
            risk_hits.append(clamp(s))
            add_evidence(evidence, f"RugCheck holder-related risk: {r.get('name') or r.get('description')}")
    if risk_hits:
        scores.append((max(risk_hits), 0.45))

    score = weighted_average(scores)
    return {
        "score": score,
        "level": risk_level(score),
        "evidence": evidence,
    }


def score_liquidity_health(
    rug: Optional[dict],
    dex: Dict[str, Any],
    jup: Optional[dict],
) -> Dict[str, Any]:
    evidence: List[str] = []
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
        add_evidence(evidence, f"DEX Screener total liquidity: ${liq:,.0f}")

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
        add_evidence(evidence, f"Newest DEX pair age: {age:.2f} hours")

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
        add_evidence(evidence, f"RugCheck LP locked estimate: {lp_locked:.1f}%")

    if isinstance(jup, dict):
        jliq = as_float(jup.get("liquidity"))
        if jliq is not None:
            add_evidence(evidence, f"Jupiter liquidity: ${jliq:,.0f}")

    score = weighted_average(scores)
    return {
        "score": score,
        "level": risk_level(score),
        "evidence": evidence,
    }


def score_trading_behavior(dex: Dict[str, Any], jup: Optional[dict]) -> Dict[str, Any]:
    evidence: List[str] = []
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
        add_evidence(evidence, f"DEX Screener h24 buys/sells: {buys}/{sells}")

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
        add_evidence(evidence, f"DEX Screener h24 price change: {pc:.2f}%")

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
        add_evidence(evidence, f"Volume/liquidity ratio h24: {vol_liq:.2f}")

    if isinstance(jup, dict):
        stats24 = jup.get("stats24h") or {}
        nb = as_float(stats24.get("numBuys"))
        ns = as_float(stats24.get("numSells"))
        if nb is not None and ns is not None:
            add_evidence(evidence, f"Jupiter stats24h numBuys/numSells: {int(nb)}/{int(ns)}")

    score = weighted_average(scores)
    return {
        "score": score,
        "level": risk_level(score),
        "evidence": evidence,
    }


def score_verification_identity(dex: Dict[str, Any], jup: Optional[dict]) -> Dict[str, Any]:
    evidence: List[str] = []
    scores: List[Tuple[Optional[float], float]] = []

    websites = dex.get("websites") or []
    socials = dex.get("socials") or []
    if not websites and not socials:
        scores.append((45, 0.25))
        add_evidence(evidence, "DEX Screener: no website/social metadata on top pair")
    else:
        scores.append((15, 0.25))
        add_evidence(evidence, f"DEX Screener metadata: {len(websites)} website(s), {len(socials)} social(s)")

    if isinstance(jup, dict):
        is_verified = as_bool(jup.get("isVerified"))
        tags = jup.get("tags") or []
        organic = as_float(jup.get("organicScore"))
        organic_label = jup.get("organicScoreLabel")

        if is_verified is True:
            scores.append((5, 0.35))
            add_evidence(evidence, "Jupiter: token is verified")
        elif is_verified is False:
            scores.append((55, 0.35))
            add_evidence(evidence, "Jupiter: token is not verified")

        if organic is not None:
            # Higher organic score is better, so invert it.
            scores.append((100 - clamp(organic), 0.30))
            add_evidence(evidence, f"Jupiter organicScore: {organic:.1f}/100")

        if organic_label:
            add_evidence(evidence, f"Jupiter organicScoreLabel: {organic_label}")
        if tags:
            add_evidence(evidence, f"Jupiter tags: {', '.join(map(str, tags))}")

    score = weighted_average(scores)
    return {
        "score": score,
        "level": risk_level(score),
        "evidence": evidence,
    }


def combine_category_scores(categories: Dict[str, Dict[str, Any]]) -> Tuple[Optional[float], float]:
    weighted_items = []
    available_weight = 0.0

    for name, cat in categories.items():
        s = cat.get("score")
        w = CATEGORY_WEIGHTS.get(name, 0.0)
        if s is not None:
            weighted_items.append((float(s), w))
            available_weight += w

    if not weighted_items or available_weight <= 0:
        return None, 0.0

    overall = sum(s * w for s, w in weighted_items) / available_weight
    confidence = clamp(available_weight * 100.0)
    return clamp(overall), float(confidence or 0.0)


def build_report(mint: str) -> Dict[str, Any]:
    sources = {
        "rugcheck": fetch_rugcheck(mint),
        "dexscreener": fetch_dexscreener(mint),
        "defade": fetch_defade(mint),
        "goplus": fetch_goplus(mint),
        "jupiter": fetch_jupiter(mint),
    }

    rug = sources["rugcheck"]["data"]
    dex = aggregate_dex_pairs(sources["dexscreener"]["data"] or [])
    defade = sources["defade"]["data"]
    goplus = sources["goplus"]["data"]
    jup = sources["jupiter"]["data"]

    categories = {
        "external_vendor_risk": score_external_vendor_risk(rug, defade, goplus),
        "contract_permissions": score_contract_permissions(rug, goplus, jup),
        "holder_distribution": score_holder_distribution(rug, jup, defade),
        "liquidity_health": score_liquidity_health(rug, dex, jup),
        "trading_behavior": score_trading_behavior(dex, jup),
        "verification_identity": score_verification_identity(dex, jup),
    }

    overall, confidence = combine_category_scores(categories)

    successful_sources = [k for k, v in sources.items() if v.get("ok")]
    skipped_or_failed = {k: v.get("error") for k, v in sources.items() if not v.get("ok")}

    top_warnings = []
    for cat_name, cat in categories.items():
        if cat.get("score") is not None and cat["score"] >= 60:
            top_warnings.append({
                "category": cat_name,
                "score": round(cat["score"], 2),
                "level": cat["level"],
                "evidence": cat.get("evidence", [])[:3],
            })

    return {
        "token_mint": mint,
        "overall_risk_score": round(overall, 2) if overall is not None else None,
        "overall_risk_level": risk_level(overall),
        "confidence_from_available_sources_pct": round(confidence, 1),
        "successful_sources": successful_sources,
        "skipped_or_failed_sources": skipped_or_failed,
        "categories": {
            k: {
                "score": round(v["score"], 2) if v.get("score") is not None else None,
                "level": v.get("level"),
                "evidence": v.get("evidence", []),
            }
            for k, v in categories.items()
        },
        "top_warnings": top_warnings,
        "market_snapshot": dex,
        "raw_summary": {
            "rugcheck_score": rugcheck_score(rug),
            "defade_rugScore": as_float(defade.get("rugScore")) if isinstance(defade, dict) else None,
            "jupiter_isVerified": jup.get("isVerified") if isinstance(jup, dict) else None,
            "jupiter_organicScore": jup.get("organicScore") if isinstance(jup, dict) else None,
        },
        "disclaimer": (
            "This is a heuristic risk screen, not financial advice and not a guarantee. "
            "Always inspect raw API reports, liquidity lockers, deployer wallets, and recent transactions."
        ),
    }


def print_human(report: Dict[str, Any]) -> None:
    print("\n=== Solana Token Risk Report ===")
    print(f"Token: {report['token_mint']}")
    print(
        f"Overall risk: {report['overall_risk_score']} / 100 "
        f"({report['overall_risk_level']})"
    )
    print(f"Confidence from available sources: {report['confidence_from_available_sources_pct']}%")
    print(f"Successful sources: {', '.join(report['successful_sources']) or 'none'}")

    if report["skipped_or_failed_sources"]:
        print("\nSkipped/failed sources:")
        for src, err in report["skipped_or_failed_sources"].items():
            print(f"  - {src}: {err}")

    print("\nSub-category scores:")
    for name, cat in report["categories"].items():
        score = cat["score"]
        level = cat["level"]
        print(f"\n  {name}: {score} / 100 ({level})")
        for ev in cat["evidence"][:6]:
            print(f"    - {ev}")

    if report["top_warnings"]:
        print("\nTop warnings:")
        for warning in report["top_warnings"]:
            print(f"  - {warning['category']}: {warning['score']} / 100 ({warning['level']})")
            for ev in warning["evidence"]:
                print(f"      * {ev}")

    ms = report["market_snapshot"]
    print("\nMarket snapshot:")
    print(f"  DEX pair count: {ms.get('pair_count')}")
    print(f"  Total liquidity: ${ms.get('total_liquidity_usd', 0):,.0f}")
    print(f"  Top DEX: {ms.get('top_pair_dex')}")
    print(f"  Top pair URL: {ms.get('top_pair_url')}")
    print(f"  24h buys/sells: {ms.get('h24_buys')}/{ms.get('h24_sells')}")
    print(f"  24h volume: ${ms.get('h24_volume_usd', 0):,.0f}")
    print(f"  24h price change: {ms.get('h24_price_change_pct')}%")

    print("\nDisclaimer:")
    print(f"  {report['disclaimer']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mint", help="Solana token mint address")
    parser.add_argument("--json", action="store_true", help="Print raw JSON report")
    parser.add_argument("--out", help="Write JSON report to file")
    args = parser.parse_args()

    report = build_report(args.mint)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"Wrote JSON report to {args.out}")

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print_human(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())