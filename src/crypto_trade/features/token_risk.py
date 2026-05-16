"""Backward-compatible facade for the split risk package.

The implementation now lives in :mod:`crypto_trade.features.risk`. Existing
callers and the ``score-token-risk`` console entry point keep working through
this module.
"""

from __future__ import annotations

from crypto_trade.features.risk import (  # noqa: F401
    CATEGORY_METRIC_KEYS,
    CATEGORY_NAMES,
    CATEGORY_WEIGHTS,
    CHAIN,
    DEFADE_BASE,
    DEXSCREENER_BASE,
    FeatureRow,
    GOPLUS_BASE,
    JUPITER_BASE,
    MODEL_VERSION,
    RISK_LEVEL_CODES,
    ReportConfig,
    RiskReportClient,
    RUGCHECK_BASE,
    SCHEMA_VERSION,
    SEVERITY_ORDER,
    SOURCE_NAMES,
    SOURCES_REQUIRING_KEY,
    SourceResult,
    StandardRiskReport,
    aggregate_dex_pairs,
    as_bool,
    as_float,
    bool_to_ml,
    build_category_metrics,
    build_report,
    build_warnings,
    clamp,
    combine_category_scores,
    count_goplus_risky_flags,
    empty_metrics,
    extract_lp_locked_pct,
    extract_token_info,
    flatten_report,
    http_get_json,
    json_default,
    print_human,
    risk_level,
    risk_level_code,
    risk_score_from_level,
    rugcheck_risks,
    rugcheck_score,
    score_contract_permissions,
    score_external_vendor_risk,
    score_holder_distribution,
    score_liquidity_health,
    score_trading_behavior,
    score_verification_identity,
    utc_now_iso,
    validate_report_schema,
    warning_counts,
    weighted_average,
    write_report,
)
from crypto_trade.features.risk.cli import main  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(main())
