from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

from crypto_trade.core.io import load_json, save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ANALYTICS_DIR, CONFIG_DIR
from crypto_trade.core.time import now_ts
from crypto_trade.core.yaml import load_yaml

logger = logging.getLogger(__name__)

MISSING = object()
OUTPUT_FILENAME = "red_flags.json"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "red_flags.yaml"
SYSTEM_AUTHORITY = "11111111111111111111111111111111"


DEFAULT_RULE_PARAMS: dict[str, dict[str, Any]] = {
    "liquidity_below_threshold": {
        "min_liquidity_usd": 5000,
    },
    "risk_score_exceeds_threshold": {
        "max_risk_score": 90,
    },
    "no_sells_after_activity_window": {
        "minimum_buys": 10,
        "window": "h1",
    },
    "holder_concentration": {
        "max_single_holder_pct": 20,
        "max_top5_pct": 40,
        "max_top10_pct": 55,
        "max_top20_pct": 70,
        "max_bundle_pct": 60,
        "max_bundle_group_pct": 25,
    },
}


def get_one_report_value(report: dict[str, Any], path: list[Any], default: Any = MISSING):
    name = path[0]
    keys = path[1:]
    data: Any = report

    for key in keys:
        if isinstance(data, dict):
            if key not in data:
                return name, default
            data = data[key]
        elif isinstance(data, list):
            if not isinstance(key, int) or key < 0 or key >= len(data):
                return name, default
            data = data[key]
        else:
            return name, default

    return name, data


def get_report_values(report: dict[str, Any], *paths: list[Any], default: Any = MISSING):
    return [get_one_report_value(report, path, default=default) for path in paths]


def is_allowed_value(value: Any, allowed: Any) -> bool:
    if isinstance(allowed, (list, tuple, set)):
        return value in allowed
    return value == allowed


def any_wrong(report_values: list[tuple[str, Any]], good_values: dict[str, Any]) -> bool:
    for name, value in report_values:
        if value is MISSING:
            continue

        if name not in good_values:
            continue

        if not is_allowed_value(value, good_values[name]):
            return True

    return False


def to_float(value: Any, default: float = 0.0) -> float:
    if value in (MISSING, None, ""):
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_report_mint(security_report: dict[str, Any]) -> str:
    paths = [
        ["top_level_mint", "mint"],
        ["rugcheck_mint", "rugcheck", "data", "mint"],
        ["rugcheck_token_mint", "rugcheck", "data", "token", "mint"],
        ["dexscreener_base_token", "dexscreener", "data", 0, "baseToken", "address"],
        ["jupiter_id", "jupiter", "data", 0, "id"],
        ["jupiter_address", "jupiter", "data", 0, "address"],
    ]

    for _name, value in get_report_values(security_report, *paths):
        if value not in (MISSING, None, ""):
            return str(value)

    raise ValueError("Missing mint in security report")


def missing_result(reason: str) -> dict[str, Any]:
    return {
        "status": True,
        "reason": reason,
        "missing_data": True,
    }


def passed_result() -> dict[str, Any]:
    return {
        "status": False,
        "reason": None,
        "missing_data": False,
    }


def mint_authority_active(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    paths = [
        ["rugcheck_token_mint_authority", "rugcheck", "data", "token", "mintAuthority"],
        ["rugcheck_root_mint_authority", "rugcheck", "data", "mintAuthority"],
        ["goplus_mintable_authority", "goplus", "data", "result", mint, "mintable", "authority"],
        ["goplus_mintable_status", "goplus", "data", "result", mint, "mintable", "status"],
        ["jupiter_mint_authority_disabled", "jupiter", "data", 0, "audit", "mintAuthorityDisabled"],
        ["defade_mint_authority", "defade", "data", "token", "mintAuthority"],
    ]

    good_values = {
        "rugcheck_token_mint_authority": [None, SYSTEM_AUTHORITY],
        "rugcheck_root_mint_authority": [None, SYSTEM_AUTHORITY],
        "goplus_mintable_authority": [False, "0", 0, None, [], ""],
        "goplus_mintable_status": [False, "0", 0, None, ""],
        "jupiter_mint_authority_disabled": True,
        "defade_mint_authority": [0, "0", None, False, SYSTEM_AUTHORITY],
    }

    failed = any_wrong(get_report_values(security_report, *paths), good_values)

    return {
        "status": failed,
        "reason": "Mint authority active" if failed else None,
        "missing_data": False,
    }


def freeze_authority_active(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    paths = [
        ["rugcheck_token_freeze_authority", "rugcheck", "data", "token", "freezeAuthority"],
        ["rugcheck_root_freeze_authority", "rugcheck", "data", "freezeAuthority"],
        ["rugcheck_market_freeze_authority", "rugcheck", "data", "markets", 0, "mintAAccount", "freezeAuthority"],
        ["defade_freeze_authority", "defade", "data", "token", "freezeAuthority"],
        ["goplus_freezable_status", "goplus", "data", "result", mint, "freezable", "status"],
        ["goplus_freezable_authority", "goplus", "data", "result", mint, "freezable", "authority"],
        ["jupiter_freeze_authority_disabled", "jupiter", "data", 0, "audit", "freezeAuthorityDisabled"],
    ]

    good_values = {
        "rugcheck_token_freeze_authority": [None, SYSTEM_AUTHORITY],
        "rugcheck_root_freeze_authority": [None, SYSTEM_AUTHORITY],
        "rugcheck_market_freeze_authority": [None, SYSTEM_AUTHORITY],
        "defade_freeze_authority": [None, SYSTEM_AUTHORITY],
        "goplus_freezable_status": ["0", 0, False, None],
        "goplus_freezable_authority": [[], None, ""],
        "jupiter_freeze_authority_disabled": True,
    }

    failed = any_wrong(get_report_values(security_report, *paths), good_values)

    return {
        "status": failed,
        "reason": "Freeze authority active" if failed else None,
        "missing_data": False,
    }


def liquidity_below_threshold(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    paths = [
        ["dexscreener_liquidity", "dexscreener", "data", 0, "liquidity", "usd"],
        ["defade_liquidity", "defade", "data", "token", "liquidity"],
        ["jupiter_liquidity", "jupiter", "data", 0, "liquidity"],
        ["rugcheck_total_market_liquidity", "rugcheck", "data", "totalMarketLiquidity"],
        ["rugcheck_total_stable_liquidity", "rugcheck", "data", "totalStableLiquidity"],
        ["rugcheck_lp_locked_usd", "rugcheck", "data", "markets", 0, "lp", "lpLockedUSD"],
        ["rugcheck_quote_usd", "rugcheck", "data", "markets", 0, "lp", "quoteUSD"],
        ["rugcheck_base_usd", "rugcheck", "data", "markets", 0, "lp", "baseUSD"],
    ]

    liquidity_usd = None
    liquidity_source = None

    for source, value in get_report_values(security_report, *paths):
        if value not in (MISSING, None, ""):
            liquidity_usd = to_float(value)
            liquidity_source = source
            break

    if liquidity_usd is None:
        result = missing_result("Liquidity data missing")
        result.update({"value": None, "threshold": None, "source": None})
        return result

    min_liquidity_usd = float(params.get("min_liquidity_usd", 5000))
    threshold = min_liquidity_usd
    failed = liquidity_usd < threshold

    return {
        "status": failed,
        "reason": f"Liquidity below threshold: {liquidity_usd} < {threshold}" if failed else None,
        "missing_data": False,
        "value": liquidity_usd,
        "threshold": threshold,
        "source": liquidity_source,
    }


def risk_score_exceeds_threshold(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    paths = [
        ["rugcheck_score_normalised", "rugcheck", "data", "score_normalised"],
        ["rugcheck_score", "rugcheck", "data", "score"],
    ]

    score = None
    score_source = None

    for source, value in get_report_values(security_report, *paths):
        if value not in (MISSING, None, ""):
            score = to_float(value)
            score_source = source
            break

    if score is None:
        result = missing_result("Risk score data missing")
        result.update({"value": None, "threshold": None, "source": None})
        return result

    max_risk_score = float(params.get("max_risk_score", 90))
    failed = score > max_risk_score

    return {
        "status": failed,
        "reason": f"Risk score exceeds threshold: {score} > {max_risk_score}" if failed else None,
        "missing_data": False,
        "value": score,
        "threshold": max_risk_score,
        "source": score_source,
    }


def critical_risk_level(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    return passed_result()


def non_transferable(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    paths = [
        ["rugcheck_non_transferable", "rugcheck", "data", "token_extensions", "nonTransferable"],
        ["goplus_non_transferable", "goplus", "data", "result", mint, "non_transferable"],
    ]

    good_values = {
        "rugcheck_non_transferable": [False, None],
        "goplus_non_transferable": ["0", 0, False, None],
    }

    failed = any_wrong(get_report_values(security_report, *paths), good_values)

    return {
        "status": failed,
        "reason": "Token is non-transferable" if failed else None,
        "missing_data": False,
    }


def transfer_fee_upgradable(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    paths = [
        ["goplus_transfer_fee_status", "goplus", "data", "result", mint, "transfer_fee_upgradable", "status"],
        ["goplus_transfer_fee_authority", "goplus", "data", "result", mint, "transfer_fee_upgradable", "authority"],
        ["rugcheck_transfer_fee_config", "rugcheck", "data", "token_extensions", "transferFeeConfig"],
        ["rugcheck_transfer_fee_authority", "rugcheck", "data", "transferFee", "authority"],
    ]

    good_values = {
        "goplus_transfer_fee_status": ["0", 0, False, None],
        "goplus_transfer_fee_authority": [[], None, ""],
        "rugcheck_transfer_fee_config": None,
        "rugcheck_transfer_fee_authority": [SYSTEM_AUTHORITY, None, ""],
    }

    failed = any_wrong(get_report_values(security_report, *paths), good_values)

    return {
        "status": failed,
        "reason": "Transfer fee is upgradable" if failed else None,
        "missing_data": False,
    }


def no_sells_after_activity_window(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    minimum_buys = int(params.get("minimum_buys", 10))
    window = str(params.get("window", "h1"))

    paths = [
        ["dex_m5_buys", "dexscreener", "data", 0, "txns", "m5", "buys"],
        ["dex_m5_sells", "dexscreener", "data", 0, "txns", "m5", "sells"],
        ["dex_h1_buys", "dexscreener", "data", 0, "txns", "h1", "buys"],
        ["dex_h1_sells", "dexscreener", "data", 0, "txns", "h1", "sells"],
        ["dex_h6_buys", "dexscreener", "data", 0, "txns", "h6", "buys"],
        ["dex_h6_sells", "dexscreener", "data", 0, "txns", "h6", "sells"],
        ["dex_h24_buys", "dexscreener", "data", 0, "txns", "h24", "buys"],
        ["dex_h24_sells", "dexscreener", "data", 0, "txns", "h24", "sells"],
        ["jup_h24_buys", "jupiter", "data", 0, "stats24h", "numBuys"],
        ["jup_h24_sells", "jupiter", "data", 0, "stats24h", "numSells"],
        ["jup_h6_sells", "jupiter", "data", 0, "stats6h", "numSells"],
    ]

    values = dict(get_report_values(security_report, *paths))

    dex_buys = values.get(f"dex_{window}_buys", MISSING)
    dex_sells = values.get(f"dex_{window}_sells", MISSING)

    failed = (
        dex_buys not in (MISSING, None)
        and dex_sells not in (MISSING, None)
        and to_float(dex_buys) >= minimum_buys
        and to_float(dex_sells) == 0
    )

    return {
        "status": failed,
        "reason": f"No sells after {dex_buys} buys in {window} window" if failed else None,
        "missing_data": False,
    }


def holder_concentration(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    def val(path: list[Any], default: Any = 0):
        _name, value = get_one_report_value(security_report, path)
        return default if value is MISSING or value is None else value

    def max_list_pct(path: list[Any], field: str, multiplier: float = 1.0) -> float:
        items = val(path, default=[])
        if not isinstance(items, list):
            return 0.0

        values = []
        for item in items:
            if isinstance(item, dict) and field in item:
                values.append(to_float(item[field]) * multiplier)

        return max(values, default=0.0)

    defade_top5 = to_float(val(["defade", "data", "holders", "concentration", "top5"]))
    defade_top10 = to_float(val(["defade", "data", "holders", "concentration", "top10"]))
    defade_top20 = to_float(val(["defade", "data", "holders", "concentration", "top20"]))
    defade_bundle_detected = bool(val(["defade", "data", "holders", "bundles", "detected"], default=False))
    defade_bundle_pct = to_float(val(["defade", "data", "holders", "bundles", "bundlePct"]))
    jupiter_top_holders_pct = to_float(val(["jupiter", "data", 0, "audit", "topHoldersPercentage"]))

    max_single_holder_pct = float(params.get("max_single_holder_pct", 20))
    max_top5_pct = float(params.get("max_top5_pct", 40))
    max_top10_pct = float(params.get("max_top10_pct", 55))
    max_top20_pct = float(params.get("max_top20_pct", 70))
    max_bundle_pct = float(params.get("max_bundle_pct", 60))
    max_bundle_group_pct = float(params.get("max_bundle_group_pct", 25))

    single_holder_pct = max(
        max_list_pct(["rugcheck", "data", "topHolders"], "pct"),
        max_list_pct(["defade", "data", "holders", "topHolders"], "percentage"),
        max_list_pct(["goplus", "data", "result", mint, "holders"], "percent", 100),
    )

    max_bundle_group = max_list_pct(
        ["defade", "data", "holders", "bundles", "groups"],
        "totalPct",
    )

    failed_reasons = []

    if single_holder_pct > max_single_holder_pct:
        failed_reasons.append(
            f"Single holder too concentrated: {single_holder_pct}% > {max_single_holder_pct}%"
        )

    if defade_top5 > max_top5_pct:
        failed_reasons.append(f"Top 5 holders too concentrated: {defade_top5}% > {max_top5_pct}%")

    if defade_top10 > max_top10_pct:
        failed_reasons.append(f"Top 10 holders too concentrated: {defade_top10}% > {max_top10_pct}%")

    if defade_top20 > max_top20_pct:
        failed_reasons.append(f"Top 20 holders too concentrated: {defade_top20}% > {max_top20_pct}%")

    if jupiter_top_holders_pct > max_top20_pct:
        failed_reasons.append(
            f"Jupiter top holders too concentrated: {jupiter_top_holders_pct}% > {max_top20_pct}%"
        )

    if defade_bundle_detected and defade_bundle_pct > max_bundle_pct:
        failed_reasons.append(f"Bundle ownership too high: {defade_bundle_pct}% > {max_bundle_pct}%")

    if max_bundle_group > max_bundle_group_pct:
        failed_reasons.append(
            f"Bundle group too concentrated: {max_bundle_group}% > {max_bundle_group_pct}%"
        )

    failed = bool(failed_reasons)

    return {
        "status": failed,
        "reason": "; ".join(failed_reasons) if failed else None,
        "missing_data": False,
    }


def permanent_delegate_enabled(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    paths = [
        ["rugcheck_permanent_delegate", "rugcheck", "data", "token_extensions", "permanentDelegate"],
        ["goplus_permanent_delegate", "goplus", "data", "result", mint, "permanent_delegate"],
        ["goplus_permanent_delegate_authority", "goplus", "data", "result", mint, "permanent_delegate_authority"],
    ]

    good_values = {
        "rugcheck_permanent_delegate": [None, "", []],
        "goplus_permanent_delegate": [None, "", [], "0", 0, False],
        "goplus_permanent_delegate_authority": [[], None, ""],
    }

    failed = any_wrong(get_report_values(security_report, *paths), good_values)

    return {
        "status": failed,
        "reason": "Permanent delegate enabled" if failed else None,
        "missing_data": False,
    }


def dangerous_transfer_hook_detected(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)

    paths = [
        ["goplus_transfer_hook", "goplus", "data", "result", mint, "transfer_hook"],
        ["goplus_transfer_hook_status", "goplus", "data", "result", mint, "transfer_hook_upgradable", "status"],
        ["goplus_transfer_hook_authority", "goplus", "data", "result", mint, "transfer_hook_upgradable", "authority"],
        ["rugcheck_transfer_hook", "rugcheck", "data", "token_extensions", "transfer_hook"],
    ]

    good_values = {
        "goplus_transfer_hook": [[], None, "", "0", 0, False],
        "goplus_transfer_hook_status": ["0", 0, False, None],
        "goplus_transfer_hook_authority": [[], None, ""],
        "rugcheck_transfer_hook": [None, "", []],
    }

    failed = any_wrong(get_report_values(security_report, *paths), good_values)

    return {
        "status": failed,
        "reason": "Dangerous transfer hook detected" if failed else None,
        "missing_data": False,
    }


def cannot_sell_test_amount(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params: Any,
) -> dict[str, Any]:
    return passed_result()


RULE_FUNCTIONS = {
    "mint_authority_active": mint_authority_active,
    "freeze_authority_active": freeze_authority_active,
    "liquidity_below_threshold": liquidity_below_threshold,
    "risk_score_exceeds_threshold": risk_score_exceeds_threshold,
    "critical_risk_level": critical_risk_level,
    "non_transferable": non_transferable,
    "transfer_fee_upgradable": transfer_fee_upgradable,
    "no_sells_after_activity_window": no_sells_after_activity_window,
    "holder_concentration": holder_concentration,
    "permanent_delegate_enabled": permanent_delegate_enabled,
    "dangerous_transfer_hook_detected": dangerous_transfer_hook_detected,
    "cannot_sell_test_amount": cannot_sell_test_amount,
}


def load_rule_params(config_path: Path | None = None) -> dict[str, dict[str, Any]]:
    params = {name: dict(values) for name, values in DEFAULT_RULE_PARAMS.items()}

    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        return params

    try:
        raw = load_yaml(path) or {}
    except Exception as exc:
        logger.warning("Failed to load red-flag config %s: %s", path, exc)
        return params

    configured_rules = raw.get("rules", raw)
    if not isinstance(configured_rules, dict):
        return params

    for rule_name, rule_params in configured_rules.items():
        if not isinstance(rule_params, dict):
            continue

        params.setdefault(str(rule_name), {}).update(rule_params)

    return params


def evaluate_red_flags(
    security_report: dict[str, Any],
    dex_features: dict[str, Any] | None = None,
    *,
    rule_params: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dex_features = dex_features or {}
    rule_params = rule_params or {}

    results: dict[str, Any] = {}

    try:
        mint = get_report_mint(security_report)
    except Exception:
        mint = None

    for rule_name, fn in RULE_FUNCTIONS.items():
        try:
            params = rule_params.get(rule_name, {})
            result = fn(security_report, dex_features, **params)
        except Exception as exc:
            result = {
                "status": True,
                "reason": f"Rule evaluation error: {type(exc).__name__}: {exc}",
                "missing_data": False,
                "error": True,
            }

        results[rule_name] = result

    failed_rules = [
        rule_name
        for rule_name, result in results.items()
        if isinstance(result, dict) and result.get("status")
    ]

    missing_data_rules = [
        rule_name
        for rule_name, result in results.items()
        if isinstance(result, dict) and result.get("missing_data")
    ]

    return {
        **results,
        "mint": mint,
        "generated_at": now_ts(),
        "failed": bool(failed_rules),
        "failed_rules": failed_rules,
        "missing_data_rules": missing_data_rules,
    }


def red_flags_path(
    mint: str | None,
    save_dir: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    if output_path is not None:
        return output_path

    if save_dir is not None:
        return save_dir / OUTPUT_FILENAME

    if mint:
        return ANALYTICS_DIR / mint / OUTPUT_FILENAME

    return ANALYTICS_DIR / "unknown_mint" / OUTPUT_FILENAME


def main(
    security_report_path: str | Path,
    dexscreener_report_path: str | Path | None = None,
    *,
    mint: str | None = None,
    save_dir: Path | None = None,
    output_path: Path | None = None,
    config_path: Path | None = None,
) -> dict[str, Any]:
    configure_logging()

    security_report = load_json(str(security_report_path))

    if dexscreener_report_path:
        dex_features = load_json(str(dexscreener_report_path))
    else:
        dex_features = security_report.get("dexscreener", {}) if isinstance(security_report, dict) else {}

    rule_params = load_rule_params(config_path)
    output = evaluate_red_flags(
        security_report=security_report,
        dex_features=dex_features,
        rule_params=rule_params,
    )

    resolved_mint = mint or output.get("mint")
    path = red_flags_path(resolved_mint, save_dir=save_dir, output_path=output_path)

    save_json(path, output)
    logger.info("Saved red-flag evaluation to %s; failed=%s", path, output["failed"])

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--security-report-path", "--security_report_path", required=True)
    parser.add_argument("--dexscreener-report-path", "--dexscreener_report_path", default=None)
    parser.add_argument("--mint", default=None)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to data/raw/analytics/<mint>/",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Exact output path. Overrides --out-dir.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional red flag config YAML. Defaults to config/red_flags.yaml if it exists.",
    )
    args = parser.parse_args()

    results = main(
        security_report_path=args.security_report_path,
        dexscreener_report_path=args.dexscreener_report_path,
        mint=args.mint,
        save_dir=args.out_dir,
        output_path=args.out,
        config_path=args.config,
    )

    print(
        json.dumps(
            {
                "mint": results.get("mint"),
                "failed": results["failed"],
                "failed_rules": results["failed_rules"],
                "missing_data_rules": results["missing_data_rules"],
            },
            indent=2,
            ensure_ascii=False,
        )
    )