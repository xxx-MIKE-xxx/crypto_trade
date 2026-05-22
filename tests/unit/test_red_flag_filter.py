from pathlib import Path

import red_flag_filter as rff


def base_report() -> dict:
    return {
        "overall": {"risk_score": 25, "risk_level": "LOW"},
        "categories": {
            "contract_permissions": {
                "metrics": {
                    "mint_authority_disabled": True,
                    "freeze_authority_disabled": True,
                    "non_transferable": False,
                    "transfer_fee_upgradable": False,
                    "metadata_mutable": False,
                }
            },
            "liquidity_health": {
                "metrics": {
                    "total_liquidity_usd": 50_000,
                    "top_pair_liquidity_usd": 45_000,
                }
            },
            "trading_behavior": {"metrics": {"h24_buys": 50, "h24_sells": 20}},
            "holder_distribution": {
                "metrics": {
                    "top_holders_pct": 10,
                    "insider_score": 10,
                    "bundle_score": 5,
                    "sniper_score": 5,
                }
            },
        },
        "raw": {"defade": {"analysis": {"devTracker": {"rugHistory": 0}}}},
    }


def config(**overrides) -> dict:
    rules = {
        "mint_authority_active": {"enabled": True, "required": True, "params": {}},
        "freeze_authority_active": {"enabled": True, "required": True, "params": {}},
        "liquidity_below_threshold": {
            "enabled": True,
            "required": False,
            "params": {
                "min_liquidity_usd": 10_000,
                "intended_position_size_usd": 250,
                "position_size_multiplier": 30,
            },
        },
        "risk_score_exceeds_threshold": {
            "enabled": True,
            "required": False,
            "params": {"max_risk_score": 85},
        },
        "critical_risk_level": {
            "enabled": True,
            "required": False,
            "params": {"blocked_levels": ["CRITICAL"]},
        },
        "holder_concentration": {
            "enabled": True,
            "required": False,
            "params": {
                "max_top_holders_pct": 30,
                "max_insider_score": 80,
                "max_bundle_score": 25,
                "max_sniper_score": 25,
                "max_dev_rug_history": 0,
            },
        },
    }
    rules.update(overrides)
    return {"rules": rules}


def evaluate(report=None, cfg=None):
    return rff.evaluate_red_flags(
        security_report=report or base_report(),
        config=cfg or config(),
        evaluated_at_utc="2026-05-22T00:00:00Z",
    )


def reason_codes(decision: rff.RedFlagDecision) -> set[str]:
    return {str(reason["code"]) for reason in decision.reject_reasons}


def test_accepts_coin_when_all_enabled_rules_pass():
    decision = evaluate()

    assert decision.accepted is True
    assert decision.rejected is False
    assert decision.reject_reasons == []
    assert "liquidity_below_threshold" in decision.passed_rules


def test_rejects_coin_when_liquidity_is_below_threshold():
    report = base_report()
    report["categories"]["liquidity_health"]["metrics"]["total_liquidity_usd"] = 1_000

    decision = evaluate(report)

    assert decision.rejected
    assert "LIQUIDITY_BELOW_THRESHOLD" in reason_codes(decision)


def test_rejects_coin_when_risk_score_exceeds_threshold():
    report = base_report()
    report["overall"]["risk_score"] = 99

    decision = evaluate(report)

    assert decision.rejected
    assert "RISK_SCORE_EXCEEDS_THRESHOLD" in reason_codes(decision)


def test_rejects_coin_when_mint_authority_is_enabled():
    report = base_report()
    report["categories"]["contract_permissions"]["metrics"]["mint_authority_disabled"] = False

    decision = evaluate(report)

    assert decision.rejected
    assert "MINT_AUTHORITY_ACTIVE" in reason_codes(decision)


def test_rejects_coin_when_freeze_authority_is_enabled():
    report = base_report()
    report["categories"]["contract_permissions"]["metrics"]["freeze_authority_disabled"] = False

    decision = evaluate(report)

    assert decision.rejected
    assert "FREEZE_AUTHORITY_ACTIVE" in reason_codes(decision)


def test_missing_optional_data_skips_rule_and_accepts():
    report = base_report()
    del report["categories"]["liquidity_health"]["metrics"]["total_liquidity_usd"]
    del report["categories"]["liquidity_health"]["metrics"]["top_pair_liquidity_usd"]

    decision = evaluate(report)

    assert decision.accepted
    assert {"rule": "liquidity_below_threshold", "reason": "missing_data", "missing": ["liquidity_usd"]} in decision.skipped_rules


def test_missing_required_data_rejects_and_records_missing_required_data():
    report = base_report()
    del report["categories"]["contract_permissions"]["metrics"]["mint_authority_disabled"]

    decision = evaluate(report)

    assert decision.rejected
    assert "MISSING_REQUIRED_DATA" in reason_codes(decision)
    assert decision.missing_required_data == [
        "mint_authority_active:categories.contract_permissions.metrics.mint_authority_disabled"
    ]


def test_disabled_rules_do_not_affect_decision():
    report = base_report()
    report["overall"]["risk_score"] = 99
    cfg = config(risk_score_exceeds_threshold={"enabled": False, "required": False, "params": {"max_risk_score": 85}})

    decision = evaluate(report, cfg)

    assert decision.accepted
    assert {"rule": "risk_score_exceeds_threshold", "reason": "disabled"} in decision.skipped_rules


def test_multiple_failed_rules_return_multiple_reject_reasons():
    report = base_report()
    report["overall"]["risk_score"] = 99
    report["categories"]["contract_permissions"]["metrics"]["freeze_authority_disabled"] = False
    report["categories"]["liquidity_health"]["metrics"]["total_liquidity_usd"] = 1_000

    decision = evaluate(report)

    assert {"RISK_SCORE_EXCEEDS_THRESHOLD", "FREEZE_AUTHORITY_ACTIVE", "LIQUIDITY_BELOW_THRESHOLD"} <= reason_codes(decision)


def test_module_does_not_write_or_delete_files(monkeypatch, tmp_path):
    writes: list[str] = []
    deletes: list[str] = []
    monkeypatch.setattr(Path, "write_text", lambda self, *args, **kwargs: writes.append(str(self)))
    monkeypatch.setattr(Path, "unlink", lambda self, *args, **kwargs: deletes.append(str(self)))

    decision = evaluate()

    assert decision.accepted
    assert writes == []
    assert deletes == []
