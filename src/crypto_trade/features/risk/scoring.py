"""Per-source parsers, per-category scorers, and warning builders."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from .constants import (
    CATEGORY_WEIGHTS,
    CHAIN,
    CONTRACT_PERMISSION_METRIC_KEYS,
    EXTERNAL_VENDOR_METRIC_KEYS,
    HOLDER_DISTRIBUTION_METRIC_KEYS,
    LIQUIDITY_HEALTH_METRIC_KEYS,
    SEVERITY_ORDER,
    TRADING_BEHAVIOR_METRIC_KEYS,
    VERIFICATION_IDENTITY_METRIC_KEYS,
)
from .types import SourceResult
from .utils import as_bool, as_float, clamp, empty_metrics, risk_level, risk_score_from_level, weighted_average


# ---------------------------------------------------------------------------
# Normalizers / extractors
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
# Scoring categories
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
        for key in ("holderRiskScore", "holderScore", "insiderScore", "bundleScore"):
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
