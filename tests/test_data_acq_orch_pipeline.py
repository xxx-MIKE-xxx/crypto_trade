from pathlib import Path

import data_acq_orch as orch


def test_orchestrator_defaults_follow_pipeline_layout():
    args = orch.build_parser().parse_args([])
    config = orch.config_from_args(args)

    assert config.run_pumpportal is True
    assert config.pumpportal_script_path == Path("pumpportal_ws.py")
    assert config.dexscreener_script_path == Path("dexscreener_api.py")
    assert config.telegram_info_script_path == Path("telegram_info.py")
    assert config.risk_analysis_root == Path("data/raw/analytics")
    assert config.dexscreener_out_root == Path("data/raw/analytics")
    assert config.website_output_root == Path("data/raw/analytics")


def test_dex_features_path_is_under_analytics_mint_dexscreener(tmp_path):
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        dexscreener_out_root=tmp_path / "data" / "raw" / "analytics",
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    assert orchestrator.dex_features_path("Mint111") == (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / "Mint111"
        / "dexscreener"
        / "features.json"
    )


def test_extract_social_urls_from_dexscreener_features():
    features = {
        "socials": [
            {"type": "twitter", "url": "https://x.com/examplecoin"},
            {"type": "telegram", "url": "t.me/examplecoin"},
        ]
    }

    assert orch.extract_social_url(features, {"telegram", "tg"}) == "https://t.me/examplecoin"
    assert orch.extract_social_url(features, {"twitter", "x"}) == "https://x.com/examplecoin"
