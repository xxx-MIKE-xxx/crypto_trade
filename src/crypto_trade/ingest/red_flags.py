import argparse
import json
import logging
from typing import Any

from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.io import load_json
from crypto_trade.core.yaml import get_yaml_value

logger = logging.getLogger(__name__)

MISSING = object()

def get_one_report_value(report, path, default=MISSING):
    name = path[0]
    keys = path[1:]
    data = report

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


def get_report_values(report, *paths, default=MISSING):
    return [get_one_report_value(report, path, default=default) for path in paths]


def any_wrong(report_values, good_values):
    for name, val in report_values:
        if val is MISSING:
            continue
        if val != good_values[name]:
            return True
    return False


def get_report_mint(security_report: dict[str, Any]) -> str:
    _name, mint = get_one_report_value(
        security_report,
        ["rugcheck_mint", "rugcheck", "data", "mint"],
    )

    if mint in (MISSING, None, ""):
        raise ValueError("Missing mint at rugcheck.data.mint")

    return mint


def mint_authority_active(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
) -> dict[str, Any]:
    mint = get_report_mint(security_report)
    paths = [
        ["rugcheck", "data", "token", "mintAuthority"],
        ["rugcheck", "data", "mintAuthority"],
        ["goplus", "data", "result", mint, "mintable", "authority"],
        ["goplus", "data", "result", mint, "mintable", "status"],
        ["jupiter", "data", 0, "audit", "mintAuthorityDisabled"],
        ["defade", "data", "token", "mintAuthority"],
    ]
    good_values = {"rugcheck": None, "goplus": False, "jupiter": True, "defade": 0, "dexscreener": "11111111111111111111111111111111"}
    report_values = get_report_values(security_report, *paths)
    failed = any_wrong(report_values, good_values=good_values)  
    return {
        "status": failed,
        "reason": "Mint authority active" if failed else None,
    }


def freeze_authority_active(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
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
        "rugcheck_token_freeze_authority": None,
        "rugcheck_root_freeze_authority": None,
        "rugcheck_market_freeze_authority": None,
        "defade_freeze_authority": None,
        "goplus_freezable_status": "0",
        "goplus_freezable_authority": [],
        "jupiter_freeze_authority_disabled": True,
    }

    report_values = get_report_values(security_report, *paths)

    failed = any_wrong(report_values, good_values=good_values)

    return {
        "status": failed,
        "reason": "Freeze authority active" if failed else None,
    }


def liquidity_below_threshold(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params,
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

    report_values = get_report_values(security_report, *paths)

    liquidity_usd = None
    liquidity_source = None

    for source, value in report_values:
        if value not in (MISSING, None):
            liquidity_usd = float(value)
            liquidity_source = source
            break

    if liquidity_usd is None:
        return {
            "status": True,
            "reason": "Liquidity data missing",
            "value": None,
            "threshold": None,
            "source": None,
        }

    min_liquidity_usd = float(params.get("min_liquidity_usd", 5000))
    "The position size x multiplier is currently replaced by just min. liq. because trade sizes can vary"
    intended_position_size_usd = 0 #float(params.get("intended_position_size_usd", 30))
    position_size_multiplier = 0 #float(params.get("position_size_multiplier", 50))

    threshold = max(
        min_liquidity_usd,
        intended_position_size_usd * position_size_multiplier,
    )

    failed = liquidity_usd < threshold

    return {
        "status": failed,
        "reason": f"Liquidity below threshold: {liquidity_usd} < {threshold}" if failed else None,
        "value": liquidity_usd,
        "threshold": threshold,
        "source": liquidity_source,
    }


def risk_score_exceeds_threshold(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params,
) -> dict[str, Any]:
    paths = [
        ["rugcheck_score_normalised", "rugcheck", "data", "score_normalised"],
        ["rugcheck_score", "rugcheck", "data", "score"],
    ]

    report_values = get_report_values(security_report, *paths)

    score = None
    source = None

    for source, value in report_values:
        if value not in (MISSING, None):
            source = source
            score = float(value)
            break

    if score is None:
        return {
            "status": True,
            "reason": "Risk score data missing"
        }

    max_risk_score = params.get("max_risk_score", 90)

    failed = score > max_risk_score

    
    return {
        "status": failed,
        "reason": f"Risk score exceeds threshold: {score} > {max_risk_score}" if failed else None,
        "value": score,
        "threshold": max_risk_score,
        "source": source,
    }


def critical_risk_level(security_report: dict[str, Any], dex_features: dict[str, Any]) -> dict[str, Any]:
    return {"status": False, "reason": None}


def non_transferable(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
) -> dict[str, Any]:
    mint = get_report_mint(security_report)
    paths = [
        ["rugcheck_non_transferable", "rugcheck", "data", "token_extensions", "nonTransferable"],
        ["goplus_non_transferable", "goplus", "data", "result", mint, "non_transferable"],
    ]

    good_values = {
        "rugcheck_non_transferable": False,
        "goplus_non_transferable": "0",
    }

    report_values = get_report_values(security_report, *paths)

    failed = any_wrong(report_values, good_values=good_values)


    return {
        "status": failed,
        "reason": "Token is non-transferable" if failed else None,
    }


def transfer_fee_upgradable(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
) -> dict[str, Any]:
    mint = get_report_mint(security_report)
    paths = [
        ["goplus_transfer_fee_status", "goplus", "data", "result", mint, "transfer_fee_upgradable", "status"],
        ["goplus_transfer_fee_authority", "goplus", "data", "result", mint, "transfer_fee_upgradable", "authority"],
        ["rugcheck_transfer_fee_config", "rugcheck", "data", "token_extensions", "transferFeeConfig"],
        ["rugcheck_transfer_fee_authority", "rugcheck", "data", "transferFee", "authority"],
    ]

    good_values = {
        "goplus_transfer_fee_status": "0",
        "goplus_transfer_fee_authority": [],
        "rugcheck_transfer_fee_config": None,
        "rugcheck_transfer_fee_authority": "11111111111111111111111111111111",
    }

    report_values = get_report_values(security_report, *paths)

    failed = any_wrong(report_values, good_values=good_values)

    return {
        "status": failed,
        "reason": "Transfer fee is upgradable" if failed else None,
    }


def no_sells_after_activity_window(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params,
) -> dict[str, Any]:
    minimum_buys = params.get("minimum_buys", 10)
    window = params.get("window", "h1")

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
        and dex_buys >= minimum_buys
        and dex_sells == 0
    )

    return {
        "status": failed,
        "reason": f"No sells after {dex_buys} buys in {window} window" if failed else None,
    }


def holder_concentration(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
    **params,
) -> dict[str, Any]:
    mint = get_report_mint(security_report)
    def val(path, default=0):
        _name, value = get_one_report_value(security_report, path)
        return default if value is MISSING or value is None else value

    def max_list_pct(path, field, multiplier=1):
        items = val(path, default=[])
        return max((float(item[field]) * multiplier for item in items), default=0)

    defade_top5 = val(["defade", "data", "holders", "concentration", "top5"])
    defade_top10 = val(["defade", "data", "holders", "concentration", "top10"])
    defade_top20 = val(["defade", "data", "holders", "concentration", "top20"])
    defade_bundle_detected = val(["defade", "data", "holders", "bundles", "detected"], default=False)
    defade_bundle_pct = float(val(["defade", "data", "holders", "bundles", "bundlePct"]))
    jupiter_top_holders_pct = val(["jupiter", "data", 0, "audit", "topHoldersPercentage"])

    max_single_holder_pct = params.get("max_single_holder_pct", 20)
    max_top5_pct = params.get("max_top5_pct", 40)
    max_top10_pct = params.get("max_top10_pct", 55)
    max_top20_pct = params.get("max_top20_pct", 70)
    max_bundle_pct = params.get("max_bundle_pct", params.get("max_bundle_score", 60))
    max_bundle_group_pct = params.get("max_bundle_group_pct", 25)

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
        failed_reasons.append(f"Single holder too concentrated: {single_holder_pct}% > {max_single_holder_pct}%")

    if defade_top5 > max_top5_pct:
        failed_reasons.append(f"Top 5 holders too concentrated: {defade_top5}% > {max_top5_pct}%")

    if defade_top10 > max_top10_pct:
        failed_reasons.append(f"Top 10 holders too concentrated: {defade_top10}% > {max_top10_pct}%")

    if defade_top20 > max_top20_pct:
        failed_reasons.append(f"Top 20 holders too concentrated: {defade_top20}% > {max_top20_pct}%")

    if jupiter_top_holders_pct > max_top20_pct:
        failed_reasons.append(f"Jupiter top holders too concentrated: {jupiter_top_holders_pct}% > {max_top20_pct}%")

    if defade_bundle_detected and defade_bundle_pct > max_bundle_pct:
        failed_reasons.append(f"Bundle ownership too high: {defade_bundle_pct}% > {max_bundle_pct}%")

    if max_bundle_group > max_bundle_group_pct:
        failed_reasons.append(f"Bundle group too concentrated: {max_bundle_group}% > {max_bundle_group_pct}%")

    failed = bool(failed_reasons)

    return {
        "status": failed,
        "reason": "; ".join(failed_reasons) if failed else None,
    }


def permanent_delegate_enabled(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
) -> dict[str, Any]:
    mint = get_report_mint(security_report)
    paths = [
        ["rugcheck_permanent_delegate", "rugcheck", "data", "token_extensions", "permanentDelegate"],
        ["goplus_permanent_delegate", "goplus", "data", "result", mint, "permanent_delegate"],
        ["goplus_permanent_delegate_authority", "goplus", "data", "result", mint, "permanent_delegate_authority"],
    ]

    good_values = {
        "rugcheck_permanent_delegate": None,
        "goplus_permanent_delegate": None,
        "goplus_permanent_delegate_authority": [],
    }

    report_values = get_report_values(security_report, *paths)

    failed = any_wrong(report_values, good_values=good_values)

    return {
        "status": failed,
        "reason": "Permanent delegate enabled" if failed else None,
    }


def dangerous_transfer_hook_detected(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
) -> dict[str, Any]:
    mint = get_report_mint(security_report)
    paths = [
        ["goplus_transfer_hook", "goplus", "data", "result", mint, "transfer_hook"],
        ["goplus_transfer_hook_status", "goplus", "data", "result", mint, "transfer_hook_upgradable", "status"],
        ["goplus_transfer_hook_authority", "goplus", "data", "result", mint, "transfer_hook_upgradable", "authority"],
        ["rugcheck_transfer_hook", "rugcheck", "data", "token_extensions", "transfer_hook"],
    ]

    good_values = {
        "goplus_transfer_hook": [],
        "goplus_transfer_hook_status": "0",
        "goplus_transfer_hook_authority": [],
        "rugcheck_transfer_hook": None,
    }

    report_values = get_report_values(security_report, *paths)
    failed = any_wrong(report_values, good_values=good_values)

    return {
        "status": failed,
        "reason": "Dangerous transfer hook detected" if failed else None,
    }


def cannot_sell_test_amount(security_report: dict[str, Any], dex_features: dict[str, Any]) -> dict[str, Any]:
    return {"status": False, "reason": None}


RULE_FUNCTIONS = {
    "mint_authority_active": mint_authority_active,
    "freeze_authority_active": freeze_authority_active,
    "liquidity_below_threshold": liquidity_below_threshold,
    "risk_score_exceeds_threshold": risk_score_exceeds_threshold,
    "critical_risk_level": critical_risk_level,
    "non_transferable": non_transferable,
    "transfer_fee_upgradable": transfer_fee_upgradable,
    #"metadata_mutable": metadata_mutable,
    "no_sells_after_activity_window": no_sells_after_activity_window,
    "holder_concentration": holder_concentration,
    "permanent_delegate_enabled": permanent_delegate_enabled,
    "dangerous_transfer_hook_detected": dangerous_transfer_hook_detected,
    "cannot_sell_test_amount": cannot_sell_test_amount,
}

def evaluate_red_flags(
    security_report: dict[str, Any],
    dex_features: dict[str, Any],
) -> dict[str, Any]:
    results = {}
    for rule_name, fn in RULE_FUNCTIONS.items():
        result = fn(security_report, dex_features)
        results[rule_name] = result
    results["failed"] = any(result.get("status") for result in results.values())
    return results


def main(security_report_path: str, dexscreener_report_path: str) -> dict[str, Any]:
    configure_logging()
    security_report = load_json(security_report_path)
    dex_features = load_json(dexscreener_report_path)
    output = evaluate_red_flags(security_report, dex_features)
    logger.info("Finished flag evaluations, tests: %s", output["failed"])
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--security_report_path")
    parser.add_argument("--dexscreener_report_path")
    args = parser.parse_args()
    results = main(args.security_report_path, args.dexscreener_report_path)
    print(results)
    with open("tmp/red_flags.json", "w", encoding="utf-8") as f:
        json.dump(results, fp=f, indent=2, ensure_ascii=False)
