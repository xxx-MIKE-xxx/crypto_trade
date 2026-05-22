"""Pure hard-stop red flag evaluation for migrated Solana coins.

This module intentionally performs no API calls and no filesystem writes or
deletes. It only evaluates supplied data against supplied rule configuration.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional


MISSING = object()


@dataclass(frozen=True)
class RedFlagRuleConfig:
    enabled: bool = True
    required: bool = False
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "RedFlagRuleConfig":
        params = data.get("params") if isinstance(data.get("params"), Mapping) else {}
        return cls(
            enabled=bool(data.get("enabled", True)),
            required=bool(data.get("required", False)),
            params=dict(params),
        )


@dataclass(frozen=True)
class RedFlagConfig:
    rules: dict[str, RedFlagRuleConfig] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "RedFlagConfig":
        raw_rules = data.get("rules") if isinstance(data.get("rules"), Mapping) else {}
        rules: dict[str, RedFlagRuleConfig] = {}
        for name, raw_rule in raw_rules.items():
            if isinstance(raw_rule, Mapping):
                rules[str(name)] = RedFlagRuleConfig.from_mapping(raw_rule)
        return cls(rules=rules)


@dataclass(frozen=True)
class RedFlagDecision:
    accepted: bool
    rejected: bool
    reject_reasons: list[dict]
    passed_rules: list[str]
    skipped_rules: list[dict]
    missing_required_data: list[str]
    evaluated_at_utc: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "rejected": self.rejected,
            "reject_reasons": self.reject_reasons,
            "passed_rules": self.passed_rules,
            "skipped_rules": self.skipped_rules,
            "missing_required_data": self.missing_required_data,
            "evaluated_at_utc": self.evaluated_at_utc,
        }


def load_red_flag_config(path: Path) -> RedFlagConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping):
        raise ValueError(f"Red flag config must be a JSON object: {path}")
    return RedFlagConfig.from_mapping(data)


def evaluate_red_flags(
    *,
    security_report: Mapping[str, Any],
    config: RedFlagConfig | Mapping[str, Any],
    dex_features: Mapping[str, Any] | None = None,
    evaluated_at_utc: str | None = None,
) -> RedFlagDecision:
    cfg = config if isinstance(config, RedFlagConfig) else RedFlagConfig.from_mapping(config)
    context = {
        "security_report": security_report,
        "dex_features": dex_features or {},
    }
    evaluated_at = evaluated_at_utc or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    reject_reasons: list[dict] = []
    passed_rules: list[str] = []
    skipped_rules: list[dict] = []
    missing_required_data: list[str] = []

    for rule_name in sorted(cfg.rules):
        rule = cfg.rules[rule_name]
        if not rule.enabled:
            skipped_rules.append({"rule": rule_name, "reason": "disabled"})
            continue

        evaluator = RULE_EVALUATORS.get(rule_name)
        if evaluator is None:
            skipped_rules.append({"rule": rule_name, "reason": "unsupported"})
            continue

        result = evaluator(context, rule.params)
        if result.missing:
            missing_key = f"{rule_name}:{','.join(result.missing)}"
            if rule.required:
                missing_required_data.append(missing_key)
                reject_reasons.append(
                    {
                        "rule": rule_name,
                        "code": "MISSING_REQUIRED_DATA",
                        "message": "Required data for red-flag rule is missing",
                        "missing": result.missing,
                    }
                )
            else:
                skipped_rules.append({"rule": rule_name, "reason": "missing_data", "missing": result.missing})
            continue

        if result.failed:
            reject_reasons.append(
                {
                    "rule": rule_name,
                    "code": result.code,
                    "message": result.message,
                    "value": result.value,
                    "threshold": result.threshold,
                }
            )
        else:
            passed_rules.append(rule_name)

    rejected = bool(reject_reasons)
    return RedFlagDecision(
        accepted=not rejected,
        rejected=rejected,
        reject_reasons=reject_reasons,
        passed_rules=passed_rules,
        skipped_rules=skipped_rules,
        missing_required_data=missing_required_data,
        evaluated_at_utc=evaluated_at,
    )


@dataclass(frozen=True)
class _RuleResult:
    failed: bool = False
    code: str = ""
    message: str = ""
    value: Any = None
    threshold: Any = None
    missing: list[str] = field(default_factory=list)


def _get(data: Mapping[str, Any], path: str) -> Any:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return MISSING
        current = current[part]
    return current


def _first_present(data: Mapping[str, Any], paths: tuple[str, ...]) -> tuple[Any, Optional[str]]:
    for path in paths:
        value = _get(data, path)
        if value is not MISSING and value is not None:
            return value, path
    return MISSING, None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return None


def _numeric_param(params: Mapping[str, Any], name: str) -> Optional[float]:
    return _as_float(params.get(name))


def _boolean_field_rule(
    data: Mapping[str, Any],
    *,
    paths: tuple[str, ...],
    reject_when: bool,
    code: str,
    message: str,
) -> _RuleResult:
    value, path = _first_present(data, paths)
    if value is MISSING:
        return _RuleResult(missing=[paths[0]])
    parsed = _as_bool(value)
    if parsed is None:
        return _RuleResult(missing=[path or paths[0]])
    if parsed is reject_when:
        return _RuleResult(True, code, message, parsed, reject_when)
    return _RuleResult()


def _risk_score_exceeds_threshold(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    report = context["security_report"]
    value = _get(report, "overall.risk_score")
    threshold = _numeric_param(params, "max_risk_score")
    missing = []
    if value is MISSING or _as_float(value) is None:
        missing.append("overall.risk_score")
    if threshold is None:
        missing.append("params.max_risk_score")
    if missing:
        return _RuleResult(missing=missing)
    score = float(_as_float(value))
    if score > float(threshold):
        return _RuleResult(True, "RISK_SCORE_EXCEEDS_THRESHOLD", "Risk score exceeds configured maximum", score, threshold)
    return _RuleResult()


def _critical_risk_level(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    report = context["security_report"]
    value = _get(report, "overall.risk_level")
    if value is MISSING or value is None:
        return _RuleResult(missing=["overall.risk_level"])
    blocked = params.get("blocked_levels")
    if not isinstance(blocked, list) or not blocked:
        return _RuleResult(missing=["params.blocked_levels"])
    blocked_levels = {str(item).upper() for item in blocked}
    level = str(value).upper()
    if level in blocked_levels:
        return _RuleResult(True, "CRITICAL_RISK_LEVEL", "Risk level is blocked", level, sorted(blocked_levels))
    return _RuleResult()


def _liquidity_below_threshold(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    report = context["security_report"]
    dex = context["dex_features"]
    value, path = _first_present(
        {"report": report, "dex": dex},
        (
            "report.categories.liquidity_health.metrics.total_liquidity_usd",
            "report.categories.liquidity_health.metrics.top_pair_liquidity_usd",
            "dex.liquidity_usd",
        ),
    )
    min_liquidity = _numeric_param(params, "min_liquidity_usd")
    intended_size = _numeric_param(params, "intended_position_size_usd")
    multiplier = _numeric_param(params, "position_size_multiplier")
    missing = []
    liquidity = _as_float(value)
    if value is MISSING or liquidity is None:
        missing.append(path or "liquidity_usd")
    if min_liquidity is None:
        missing.append("params.min_liquidity_usd")
    if intended_size is None:
        missing.append("params.intended_position_size_usd")
    if multiplier is None:
        missing.append("params.position_size_multiplier")
    if missing:
        return _RuleResult(missing=missing)
    threshold = max(float(min_liquidity), float(intended_size) * float(multiplier))
    if float(liquidity) < threshold:
        return _RuleResult(True, "LIQUIDITY_BELOW_THRESHOLD", "Liquidity is below configured hard-stop threshold", liquidity, threshold)
    return _RuleResult()


def _mint_authority_active(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _boolean_field_rule(
        context["security_report"],
        paths=("categories.contract_permissions.metrics.mint_authority_disabled",),
        reject_when=False,
        code="MINT_AUTHORITY_ACTIVE",
        message="Mint authority is active",
    )


def _freeze_authority_active(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _boolean_field_rule(
        context["security_report"],
        paths=("categories.contract_permissions.metrics.freeze_authority_disabled",),
        reject_when=False,
        code="FREEZE_AUTHORITY_ACTIVE",
        message="Freeze authority is active",
    )


def _non_transferable(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _boolean_field_rule(
        context["security_report"],
        paths=("categories.contract_permissions.metrics.non_transferable",),
        reject_when=True,
        code="NON_TRANSFERABLE",
        message="Token is marked non-transferable",
    )


def _transfer_fee_upgradable(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _boolean_field_rule(
        context["security_report"],
        paths=("categories.contract_permissions.metrics.transfer_fee_upgradable",),
        reject_when=True,
        code="TRANSFER_FEE_UPGRADABLE",
        message="Transfer fee is upgradable",
    )


def _metadata_mutable(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _boolean_field_rule(
        context["security_report"],
        paths=("categories.contract_permissions.metrics.metadata_mutable",),
        reject_when=True,
        code="METADATA_MUTABLE",
        message="Metadata is mutable",
    )


def _truthy_or_present_field_rule(
    data: Mapping[str, Any],
    *,
    paths: tuple[str, ...],
    code: str,
    message: str,
) -> _RuleResult:
    """Reject if the first present field is truthy or contains a non-empty authority/program value.

    This is useful for fields that may be returned as:
    - boolean true/false
    - an authority address string
    - a program id string
    - an object/list describing an enabled extension
    """
    value, path = _first_present(data, paths)
    if value is MISSING:
        return _RuleResult(missing=[paths[0]])

    parsed_bool = _as_bool(value)
    if parsed_bool is not None:
        active = parsed_bool
    elif isinstance(value, str):
        normalized = value.strip().lower()
        active = normalized not in {
            "",
            "none",
            "null",
            "false",
            "0",
            "disabled",
            "not_set",
            "not set",
            "11111111111111111111111111111111",
        }
    elif isinstance(value, (list, tuple, set, dict)):
        active = bool(value)
    else:
        active = bool(value)

    if active:
        return _RuleResult(True, code, message, value, True)

    return _RuleResult()


def _permanent_delegate_enabled(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _truthy_or_present_field_rule(
        context["security_report"],
        paths=(
            "categories.contract_permissions.metrics.permanent_delegate_enabled",
            "categories.contract_permissions.metrics.has_permanent_delegate",
            "categories.contract_permissions.metrics.permanent_delegate",
            "categories.contract_permissions.metrics.permanent_delegate_authority",
            "raw.rugcheck.token.permanentDelegate",
            "raw.rugcheck.token_extensions.permanent_delegate",
            "raw.defade.analysis.permanentDelegate",
        ),
        code="PERMANENT_DELEGATE_ENABLED",
        message="Permanent delegate is enabled",
    )


def _dangerous_transfer_hook_detected(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    return _truthy_or_present_field_rule(
        context["security_report"],
        paths=(
            "categories.contract_permissions.metrics.dangerous_transfer_hook_detected",
            "categories.contract_permissions.metrics.transfer_hook_enabled",
            "categories.contract_permissions.metrics.has_transfer_hook",
            "categories.contract_permissions.metrics.transfer_hook_program",
            "raw.rugcheck.token_extensions.transfer_hook",
            "raw.defade.analysis.transferHook",
        ),
        code="DANGEROUS_TRANSFER_HOOK_DETECTED",
        message="Dangerous or unsupported transfer hook is detected",
    )


def _cannot_sell_test_amount(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    report = context["security_report"]

    cannot_sell_paths = (
        "categories.trading_behavior.metrics.cannot_sell_test_amount",
        "categories.trading_behavior.metrics.sell_test_failed",
        "categories.trading_behavior.metrics.cannot_sell",
        "categories.sellability.metrics.cannot_sell_test_amount",
        "categories.sellability.metrics.sell_test_failed",
        "raw.jupiter.sell_test.cannot_sell_test_amount",
        "raw.jupiter.sell_test.sell_test_failed",
    )
    value, path = _first_present(report, cannot_sell_paths)
    if value is not MISSING:
        parsed = _as_bool(value)
        if parsed is None:
            return _RuleResult(missing=[path or cannot_sell_paths[0]])
        if parsed:
            return _RuleResult(
                True,
                "CANNOT_SELL_TEST_AMOUNT",
                "Sell simulation failed for configured test amount",
                parsed,
                False,
            )
        return _RuleResult()

    can_sell_paths = (
        "categories.trading_behavior.metrics.can_sell_test_amount",
        "categories.trading_behavior.metrics.can_sell",
        "categories.sellability.metrics.can_sell_test_amount",
        "categories.sellability.metrics.can_sell",
        "raw.jupiter.sell_test.can_sell_test_amount",
        "raw.jupiter.sell_test.can_sell",
    )
    value, path = _first_present(report, can_sell_paths)
    if value is MISSING:
        return _RuleResult(missing=[cannot_sell_paths[0]])

    parsed = _as_bool(value)
    if parsed is None:
        return _RuleResult(missing=[path or can_sell_paths[0]])

    if not parsed:
        return _RuleResult(
            True,
            "CANNOT_SELL_TEST_AMOUNT",
            "Sell simulation failed for configured test amount",
            parsed,
            True,
        )

    return _RuleResult()


def _no_sells_after_activity_window(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    report = context["security_report"]
    dex = context["dex_features"]
    minimum_buys = _numeric_param(params, "minimum_buys")
    buys, buy_path = _first_present(
        {"report": report, "dex": dex},
        (
            "dex.txns_m5_buys",
            "dex.txns_h1_buys",
            "report.categories.trading_behavior.metrics.h24_buys",
        ),
    )
    sells, sell_path = _first_present(
        {"report": report, "dex": dex},
        (
            "dex.txns_m5_sells",
            "dex.txns_h1_sells",
            "report.categories.trading_behavior.metrics.h24_sells",
        ),
    )
    missing = []
    buy_count = _as_float(buys)
    sell_count = _as_float(sells)
    if buys is MISSING or buy_count is None:
        missing.append(buy_path or "txns_buys")
    if sells is MISSING or sell_count is None:
        missing.append(sell_path or "txns_sells")
    if minimum_buys is None:
        missing.append("params.minimum_buys")
    if missing:
        return _RuleResult(missing=missing)
    if float(buy_count) >= float(minimum_buys) and float(sell_count) <= 0:
        return _RuleResult(
            True,
            "NO_SELLS_AFTER_ACTIVITY_WINDOW",
            "Configured buy activity threshold was reached with no sells",
            {"buys": buy_count, "sells": sell_count},
            {"minimum_buys": minimum_buys, "minimum_sells": 1},
        )
    return _RuleResult()


def _holder_concentration(context: Mapping[str, Any], params: Mapping[str, Any]) -> _RuleResult:
    report = context["security_report"]
    paths_and_thresholds = (
        ("categories.holder_distribution.metrics.top_holders_pct", "max_top_holders_pct"),
        ("categories.holder_distribution.metrics.insider_score", "max_insider_score"),
        ("categories.holder_distribution.metrics.bundle_score", "max_bundle_score"),
        ("categories.holder_distribution.metrics.sniper_score", "max_sniper_score"),
        ("raw.defade.analysis.devTracker.rugHistory", "max_dev_rug_history"),
    )
    present = False
    failures: list[dict[str, Any]] = []
    missing_thresholds: list[str] = []
    for path, threshold_name in paths_and_thresholds:
        threshold = _numeric_param(params, threshold_name)
        if threshold is None:
            missing_thresholds.append(f"params.{threshold_name}")
            continue
        value = _get(report, path)
        numeric_value = _as_float(value)
        if value is MISSING or numeric_value is None:
            continue
        present = True
        if float(numeric_value) > float(threshold):
            failures.append({"field": path, "value": numeric_value, "threshold": threshold})
    if not present:
        return _RuleResult(missing=["holder_concentration_metrics"])
    if failures:
        return _RuleResult(
            True,
            "HOLDER_CONCENTRATION",
            "Holder, bundle, insider, sniper, or dev-history concentration exceeded threshold",
            failures,
            {item["field"]: item["threshold"] for item in failures},
        )
    return _RuleResult(missing=missing_thresholds) if len(missing_thresholds) == len(paths_and_thresholds) else _RuleResult()


RULE_EVALUATORS = {
    "cannot_sell_test_amount": _cannot_sell_test_amount,
    "critical_risk_level": _critical_risk_level,
    "dangerous_transfer_hook_detected": _dangerous_transfer_hook_detected,
    "freeze_authority_active": _freeze_authority_active,
    "holder_concentration": _holder_concentration,
    "liquidity_below_threshold": _liquidity_below_threshold,
    "metadata_mutable": _metadata_mutable,
    "mint_authority_active": _mint_authority_active,
    "no_sells_after_activity_window": _no_sells_after_activity_window,
    "non_transferable": _non_transferable,
    "permanent_delegate_enabled": _permanent_delegate_enabled,
    "risk_score_exceeds_threshold": _risk_score_exceeds_threshold,
    "transfer_fee_upgradable": _transfer_fee_upgradable,
}
