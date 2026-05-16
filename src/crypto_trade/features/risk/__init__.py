"""Solana token risk scoring package.

Split from the former ``crypto_trade.features.token_risk`` monolith:

* :mod:`.constants` -- schema/source/category constants
* :mod:`.types` -- ``ReportConfig``, ``SourceResult``, ``StandardRiskReport``
* :mod:`.utils` -- pure helpers (``clamp``, ``as_float``, ``risk_level`` ...)
* :mod:`.http_client` -- HTTP wrapper and :class:`RiskReportClient`
* :mod:`.scoring` -- per-category scorers, parsers, warnings
* :mod:`.report` -- :func:`build_report`, :func:`flatten_report`, writers
* :mod:`.cli` -- argparse + :func:`main`

The public API matches the old module so existing callers and entry points
continue to work via :mod:`crypto_trade.features.token_risk`.
"""

from .constants import (
    CATEGORY_METRIC_KEYS,
    CATEGORY_NAMES,
    CATEGORY_WEIGHTS,
    CHAIN,
    DEFADE_BASE,
    DEXSCREENER_BASE,
    GOPLUS_BASE,
    JUPITER_BASE,
    MODEL_VERSION,
    RISK_LEVEL_CODES,
    RUGCHECK_BASE,
    SCHEMA_VERSION,
    SEVERITY_ORDER,
    SOURCE_NAMES,
    SOURCES_REQUIRING_KEY,
)
from .http_client import RiskReportClient, http_get_json
from .report import (
    FeatureRow,
    build_report,
    flatten_report,
    print_human,
    validate_report_schema,
    write_report,
)
from .scoring import (
    aggregate_dex_pairs,
    build_category_metrics,
    build_warnings,
    combine_category_scores,
    count_goplus_risky_flags,
    extract_lp_locked_pct,
    extract_token_info,
    rugcheck_risks,
    rugcheck_score,
    score_contract_permissions,
    score_external_vendor_risk,
    score_holder_distribution,
    score_liquidity_health,
    score_trading_behavior,
    score_verification_identity,
    warning_counts,
)
from .types import ReportConfig, SourceResult, StandardRiskReport
from .utils import (
    as_bool,
    as_float,
    bool_to_ml,
    clamp,
    empty_metrics,
    json_default,
    risk_level,
    risk_level_code,
    risk_score_from_level,
    utc_now_iso,
    weighted_average,
)

__all__ = [
    "CATEGORY_METRIC_KEYS",
    "CATEGORY_NAMES",
    "CATEGORY_WEIGHTS",
    "CHAIN",
    "DEFADE_BASE",
    "DEXSCREENER_BASE",
    "FeatureRow",
    "GOPLUS_BASE",
    "JUPITER_BASE",
    "MODEL_VERSION",
    "RISK_LEVEL_CODES",
    "ReportConfig",
    "RiskReportClient",
    "RUGCHECK_BASE",
    "SCHEMA_VERSION",
    "SEVERITY_ORDER",
    "SOURCE_NAMES",
    "SOURCES_REQUIRING_KEY",
    "SourceResult",
    "StandardRiskReport",
    "aggregate_dex_pairs",
    "as_bool",
    "as_float",
    "bool_to_ml",
    "build_category_metrics",
    "build_report",
    "build_warnings",
    "clamp",
    "combine_category_scores",
    "count_goplus_risky_flags",
    "empty_metrics",
    "extract_lp_locked_pct",
    "extract_token_info",
    "flatten_report",
    "http_get_json",
    "json_default",
    "print_human",
    "risk_level",
    "risk_level_code",
    "risk_score_from_level",
    "rugcheck_risks",
    "rugcheck_score",
    "score_contract_permissions",
    "score_external_vendor_risk",
    "score_holder_distribution",
    "score_liquidity_health",
    "score_trading_behavior",
    "score_verification_identity",
    "utc_now_iso",
    "validate_report_schema",
    "warning_counts",
    "weighted_average",
    "write_report",
]
