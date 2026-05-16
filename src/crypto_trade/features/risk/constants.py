"""Shared constants for the Solana token risk package."""

from __future__ import annotations

from typing import Dict, Optional, Tuple

SCHEMA_VERSION = "1.0.0"
MODEL_VERSION = "heuristic_v1"
CHAIN = "solana"

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
