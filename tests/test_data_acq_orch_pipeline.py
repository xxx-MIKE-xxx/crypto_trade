from pathlib import Path
import asyncio
import dataclasses
import io
import logging
import sys

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
    assert config.capture_network_sample_seconds == 60.0
    assert config.capture_network_sample_fee_addresses is False
    assert config.max_post_migration_tracked_coins == 6
    assert config.post_migration_dex_tracking_hours == 24.0
    assert config.post_migration_dex_requests_per_minute == 50.0
    assert config.post_migration_dex_endpoint_profile == "market"
    assert config.post_migration_dex_out_root == Path("data/raw/onchain")


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


def test_capture_command_passes_network_sampling_knobs(tmp_path, monkeypatch):
    commands = []

    async def fake_run_subprocess(command, **kwargs):
        commands.append(command)
        return orch.SubprocessResult(returncode=0, elapsed_seconds=0.0, command=tuple(command))

    monkeypatch.setattr(orch, "run_subprocess", fake_run_subprocess)
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        capture_network_sample_seconds=90.0,
        capture_network_sample_fee_addresses=True,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    ctx = orch.CoinContext(mint="Mint111", pair_addresses=("Pair111",), migration_event={})
    asyncio.run(orchestrator.run_capture(ctx))

    command = commands[0]
    assert "--network-sample-seconds" in command
    assert command[command.index("--network-sample-seconds") + 1] == "90.0"
    assert "--network-sample-fee-addresses" in command


def test_capture_command_filters_non_address_pairs(tmp_path, monkeypatch):
    commands = []

    async def fake_run_subprocess(command, **kwargs):
        commands.append(command)
        return orch.SubprocessResult(returncode=0, elapsed_seconds=0.0, command=tuple(command))

    monkeypatch.setattr(orch, "run_subprocess", fake_run_subprocess)
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    ctx = orch.CoinContext(
        mint="1tMyNUUnCL6aeFAWnQDCj5ok3CFTatgehBkodxVpump",
        pair_addresses=("pump-amm", "So11111111111111111111111111111111111111112"),
        migration_event={},
    )
    asyncio.run(orchestrator.run_capture(ctx))

    command = commands[0]
    assert "pump-amm" not in command
    assert "--pair" in command
    assert "So11111111111111111111111111111111111111112" in command


class _FakeTty(io.StringIO):
    def isatty(self) -> bool:
        return True


def _metrics_record(**extra: object) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="metrics",
        args=(),
        exc_info=None,
    )
    record.event = "orchestrator_metrics"
    record.rows_read = 1
    record.currently_tracked_mints = 2
    record.migrated_coins_detected = 3
    record.eligible_migrations = 4
    record.active_coin_pipelines = 1
    record.post_migration_dex_tracked_coins = 0
    record.completed_coin_analyses = 0
    record.failed_coin_analyses = 0
    record.red_flag_rejected_coin_analyses = 0
    record.current_stage = {}
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_format_metrics_line_includes_red_flag_counter():
    state = orch.ConsoleDashboardState(
        rows_read=10,
        eligible_migrations=4,
        active_coin_pipelines=0,
        max_concurrent_coins=3,
        red_flag_rejected_coin_analyses=4,
    )
    line = orch.format_metrics_line(state, columns=200)
    assert "redflag=4" in line


def test_format_metrics_line_truncates():
    state = orch.ConsoleDashboardState(
        rows_read=999_999,
        migrated_coins_detected=99,
        eligible_migrations=88,
        active_coin_pipelines=7,
        max_concurrent_coins=5,
        post_migration_dex_tracked_coins=6,
        completed_coin_analyses=5,
        failed_coin_analyses=4,
        current_stage={"CuAgWRcsKuNBoXLvytiRc1BQ4yF19gcxkX8ufmHNpump": "solana_1h_capture"},
    )
    state.coins["CuAgWRcsKuNBoXLvytiRc1BQ4yF19gcxkX8ufmHNpump"] = orch.CoinConsoleState(
        mint="CuAgWRcsKuNBoXLvytiRc1BQ4yF19gcxkX8ufmHNpump",
        postdex_samples=212,
        next_interval_s=2.4,
    )
    line = orch.format_metrics_line(state, columns=40)
    assert len(line) <= 40
    assert line.endswith("…")


def test_dashboard_noisy_events_do_not_add_newlines():
    stream = _FakeTty()
    handler = orch.PrettyConsoleHandler(stream, console_display="dashboard")
    coin = "CuAgWRcsKuNBoXLvytiRc1BQ4yF19gcxkX8ufmHNpump"

    handler.emit(_metrics_record(max_concurrent_coins=5))
    assert stream.getvalue().count("\n") == 1

    for _ in range(5):
        start = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="start",
            args=(),
            exc_info=None,
        )
        start.event = "subprocess_start"
        start.coin = coin
        start.stage = "post_migration_dexscreener"
        handler.emit(start)

        done = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="done",
            args=(),
            exc_info=None,
        )
        done.event = "subprocess_complete"
        done.coin = coin
        done.stage = "post_migration_dexscreener"
        done.elapsed_seconds = 0.44
        handler.emit(done)

        snap = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="snap",
            args=(),
            exc_info=None,
        )
        snap.event = "post_migration_dex_snapshot"
        snap.coin = coin
        snap.samples = 186
        snap.next_interval_seconds = 2.4
        handler.emit(snap)

    output = stream.getvalue()
    assert "post_migration_dexscreener started" not in output
    assert "post_migration_dexscreener finished" not in output
    assert "post-migration Dex snapshot" not in output
    assert "dex#186" in output
    assert "\x1b[2A" in output


def test_pretty_console_handler_verbose_uses_single_line_progress():
    stream = _FakeTty()
    handler = orch.PrettyConsoleHandler(stream, console_display="verbose")
    handler.emit(_metrics_record())
    output = stream.getvalue()
    assert "\x1b[2A" not in output
    assert "\r" in output


def test_pretty_console_handler_shows_fatal_error_detail():
    stream = io.StringIO()
    handler = orch.PrettyConsoleHandler(stream, console_display="verbose")
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="orchestrator_fatal_error",
        args=(),
        exc_info=None,
    )
    record.event = "orchestrator_fatal_error"
    record.error = "RuntimeError('boom')"

    handler.emit(record)

    assert "orchestrator_fatal_error RuntimeError('boom')" in stream.getvalue()


def test_run_subprocess_cancellation_terminates_child(tmp_path):
    stage_log = tmp_path / "stage.log"

    async def runner() -> None:
        task = asyncio.create_task(
            orch.run_subprocess(
                [
                    sys.executable,
                    "-c",
                    "import time; time.sleep(30)",
                ],
                logger=orch.setup_logging("ERROR"),
                mint="Mint111",
                stage="long_running",
                timeout_seconds=60,
                stage_log_path=stage_log,
            )
        )
        await asyncio.sleep(0.25)
        task.cancel()
        results = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError)

    asyncio.run(runner())


def test_post_migration_dex_tracking_command_uses_onchain_history(tmp_path):
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        post_migration_dex_out_root=tmp_path / "data" / "raw" / "onchain",
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )
    track = orch.PostMigrationDexTrack(mint="Mint111")

    command = orchestrator.build_post_migration_dex_command(track)

    assert "--token" in command
    assert command[command.index("--token") + 1] == "Mint111"
    assert "--out" in command
    assert command[command.index("--out") + 1] == str(tmp_path / "data" / "raw" / "onchain")
    assert "--append-history" in command
    assert "--timestamped-raw" in command
    assert "--quiet" in command
    assert "--endpoint-profile" in command
    assert command[command.index("--endpoint-profile") + 1] == "market"
    assert command[command.index("--sleep") + 1] == "0.0"


def test_post_migration_dex_interval_splits_global_request_budget(tmp_path):
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        post_migration_dex_requests_per_minute=50.0,
        post_migration_dex_requests_per_snapshot=1,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    assert orchestrator.post_migration_dex_interval_seconds(active_count=2) == 2.4


def test_post_migration_dex_tracking_respects_capacity(tmp_path):
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        max_post_migration_tracked_coins=1,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    orchestrator.start_post_migration_dex_tracking(
        orch.CoinContext(mint="Mint111", pair_addresses=(), migration_event={})
    )
    orchestrator.start_post_migration_dex_tracking(
        orch.CoinContext(mint="Mint222", pair_addresses=(), migration_event={})
    )

    assert sorted(orchestrator.post_migration_dex_tracks) == ["Mint111"]


def test_post_migration_dex_tracking_starts_at_migration(tmp_path, monkeypatch):
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )
    events: list[str] = []

    async def fake_capture(ctx: orch.CoinContext) -> None:
        events.append("capture_started")
        await asyncio.sleep(0.01)
        events.append("capture_finished")

    async def fake_risk(ctx: orch.CoinContext) -> None:
        events.append("risk_finished")

    async def fake_analytics(ctx: orch.CoinContext) -> None:
        events.append("analytics_started")
        await asyncio.sleep(0.02)
        events.append("analytics_finished")

    def fake_start_postdex(ctx: orch.CoinContext) -> None:
        events.append("postdex_started")

    monkeypatch.setattr(orchestrator, "run_capture", fake_capture)
    monkeypatch.setattr(orchestrator, "run_risk_report", fake_risk)
    monkeypatch.setattr(orchestrator, "load_security_report", lambda mint: {"token": {"mint": mint}})
    monkeypatch.setattr(orchestrator, "evaluate_red_flag_filter", lambda ctx, report, dex_features=None: _accepted_red_flag_decision())
    monkeypatch.setattr(orchestrator, "run_analytics_branch", fake_analytics)
    monkeypatch.setattr(orchestrator, "start_post_migration_dex_tracking", fake_start_postdex)

    ctx = orch.CoinContext(mint="Mint111", pair_addresses=(), migration_event={})
    asyncio.run(orchestrator.run_coin_pipeline(ctx))

    assert events.index("risk_finished") < events.index("postdex_started")
    assert events.index("postdex_started") < events.index("capture_finished")
    assert events.index("analytics_started") < events.index("capture_finished")


def test_migration_capacity_skips_instead_of_queueing(tmp_path, monkeypatch):
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        status_jsonl_path=tmp_path / "status.jsonl",
        max_concurrent_coins=1,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )
    started: list[str] = []
    monkeypatch.setattr(orchestrator, "start_coin_task", lambda ctx: started.append(ctx.mint))

    async def runner() -> None:
        blocker = asyncio.create_task(asyncio.sleep(60))
        orchestrator.active_tasks["MintActive"] = blocker
        try:
            orchestrator.seen_mints.add("MintSkip")
            await orchestrator.handle_migration_event({"mint": "MintSkip"}, "MintSkip")

            assert started == []
            assert orchestrator.metrics.eligible_migrations == 1
            assert orchestrator.metrics.skipped_due_to_concurrency == 1
            assert orchestrator.metrics.coins_started == 0
            assert orchestrator.skipped_mints == {"MintSkip"}
            assert orchestrator.current_stage["MintSkip"] == "skipped_capacity"

            orchestrator.active_tasks.clear()
            await orchestrator.handle_migration_event({"mint": "MintSkip"}, "MintSkip")
            assert started == []
            assert orchestrator.metrics.duplicate_migrations == 1

            orchestrator.seen_mints.add("MintNext")
            await orchestrator.handle_migration_event({"mint": "MintNext"}, "MintNext")
            assert started == ["MintNext"]
            assert orchestrator.metrics.coins_started == 1
        finally:
            blocker.cancel()
            await asyncio.gather(blocker, return_exceptions=True)

    asyncio.run(runner())


def test_validate_requires_jsonl_when_tail_only(tmp_path):
    missing = tmp_path / "missing.jsonl"
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=missing,
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        run_pumpportal=False,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    try:
        orchestrator.validate_pumpportal_jsonl_setup()
        raised = False
    except FileNotFoundError:
        raised = True

    assert raised


def test_wait_for_pumpportal_jsonl(tmp_path):
    jsonl = tmp_path / "stream.jsonl"

    async def runner() -> None:
        config = orch.OrchestratorConfig(
            pumpportal_jsonl_path=jsonl,
            pumpportal_script_path=Path("pumpportal_ws.py"),
            capture_script_path=Path("solana_coin_1h_capture.py"),
            risk_report_script_path=Path("solana_risk_report.py"),
            dexscreener_script_path=Path("dexscreener_api.py"),
            website_grader_script_path=Path("website_grader_v2.py"),
            telegram_info_script_path=Path("telegram_info.py"),
            state_path=tmp_path / "state.json",
            pumpportal_jsonl_wait_seconds=2.0,
        )
        orchestrator = orch.MemeCoinPipelineOrchestrator(
            config=config,
            logger=orch.setup_logging("ERROR"),
        )

        async def touch_file() -> None:
            await asyncio.sleep(0.05)
            jsonl.write_text('{"event_type":"new_token","mint":"Mint111"}\n', encoding="utf-8")

        touch_task = asyncio.create_task(touch_file())
        await orchestrator.wait_for_pumpportal_jsonl(None)
        await touch_task
        assert orchestrator._pumpportal_jsonl_ready

    asyncio.run(runner())


def test_pumpportal_command_passes_raw_jsonl(tmp_path, monkeypatch):
    jsonl = tmp_path / "migrations" / "day.jsonl"
    script = tmp_path / "pumpportal_ws.py"
    script.write_text("", encoding="utf-8")

    captured: list[list[str]] = []

    async def fake_exec(*args, **kwargs):
        captured.append([str(a) for a in args])

        class Proc:
            returncode = None

            stdout = None
            stderr = None

        return Proc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=jsonl,
        pumpportal_script_path=script,
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        stage_log_root=None,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )

    asyncio.run(orchestrator.start_pumpportal_process())

    command = captured[0]
    assert "--raw-jsonl" in command
    assert command[command.index("--raw-jsonl") + 1] == str(jsonl.resolve())


def _pipeline_orchestrator(tmp_path) -> orch.MemeCoinPipelineOrchestrator:
    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=tmp_path / "migrations.jsonl",
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        status_jsonl_path=tmp_path / "status.jsonl",
    )
    return orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )


def _accepted_red_flag_decision() -> orch.red_flag_filter.RedFlagDecision:
    return orch.red_flag_filter.RedFlagDecision(
        accepted=True,
        rejected=False,
        reject_reasons=[],
        passed_rules=["test"],
        skipped_rules=[],
        missing_required_data=[],
        evaluated_at_utc="2026-05-22T00:00:00Z",
    )


def _rejected_red_flag_decision() -> orch.red_flag_filter.RedFlagDecision:
    return orch.red_flag_filter.RedFlagDecision(
        accepted=False,
        rejected=True,
        reject_reasons=[{"rule": "liquidity_below_threshold", "code": "LIQUIDITY_BELOW_THRESHOLD"}],
        passed_rules=[],
        skipped_rules=[],
        missing_required_data=[],
        evaluated_at_utc="2026-05-22T00:00:00Z",
    )


def test_run_coin_pipeline_runs_capture_then_risk_gate_then_analytics(tmp_path):
    orchestrator = _pipeline_orchestrator(tmp_path)
    ctx = orch.CoinContext(mint="Mint111", pair_addresses=("pump-amm",), migration_event={})

    capture_entered = asyncio.Event()
    capture_release = asyncio.Event()
    risk_started = asyncio.Event()
    dex_entered = asyncio.Event()
    postdex_started = asyncio.Event()
    order: list[str] = []

    async def fake_capture(coin_ctx: orch.CoinContext) -> None:
        order.append("capture_enter")
        capture_entered.set()
        await capture_release.wait()
        order.append("capture_done")

    async def fake_risk(coin_ctx: orch.CoinContext) -> None:
        order.append("risk_start")
        risk_started.set()

    async def fake_poll(coin_ctx: orch.CoinContext) -> orch.DexScreenerResult:
        order.append("dex_enter")
        dex_entered.set()
        await capture_entered.wait()
        return orch.DexScreenerResult(
            features_path=tmp_path / "features.json",
            features={},
            website_url=None,
            telegram_url=None,
        )

    def fake_postdex(coin_ctx: orch.CoinContext) -> None:
        order.append("postdex_start")
        postdex_started.set()

    orchestrator.start_post_migration_dex_tracking = fake_postdex  # type: ignore[method-assign]
    orchestrator.run_capture = fake_capture  # type: ignore[method-assign]
    orchestrator.run_risk_report = fake_risk  # type: ignore[method-assign]
    orchestrator.load_security_report = lambda mint: {"token": {"mint": mint}}  # type: ignore[method-assign]
    orchestrator.evaluate_red_flag_filter = lambda coin_ctx, report, dex_features=None: _accepted_red_flag_decision()  # type: ignore[method-assign]
    orchestrator.poll_dexscreener = fake_poll  # type: ignore[method-assign]
    orchestrator.run_website_grader = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]
    orchestrator.run_telegram_info = lambda *args, **kwargs: asyncio.sleep(0)  # type: ignore[method-assign]

    async def runner() -> None:
        pipeline_task = asyncio.create_task(orchestrator.run_coin_pipeline(ctx))
        await asyncio.wait_for(capture_entered.wait(), timeout=2.0)
        await asyncio.wait_for(risk_started.wait(), timeout=2.0)
        await asyncio.wait_for(dex_entered.wait(), timeout=2.0)
        await asyncio.wait_for(postdex_started.wait(), timeout=2.0)
        assert "capture_done" not in order
        capture_release.set()
        await pipeline_task

    asyncio.run(runner())

    assert order.index("postdex_start") < order.index("capture_done")
    assert order.index("risk_start") < order.index("capture_done")
    assert order.index("dex_enter") < order.index("capture_done")
    assert order.index("risk_start") < order.index("dex_enter")


def test_analytics_failure_does_not_fail_capture(tmp_path):
    orchestrator = _pipeline_orchestrator(tmp_path)
    ctx = orch.CoinContext(mint="Mint111", pair_addresses=("pump-amm",), migration_event={})
    status_events: list[dict] = []

    def capture_status(event_type: str, **kwargs: object) -> None:
        status_events.append({"event_type": event_type, **kwargs})

    orchestrator.emit_status_event = capture_status  # type: ignore[method-assign]
    orchestrator.start_post_migration_dex_tracking = lambda coin_ctx: None  # type: ignore[method-assign]

    async def fake_capture(coin_ctx: orch.CoinContext) -> None:
        return None

    async def fake_risk(coin_ctx: orch.CoinContext) -> None:
        return None

    async def fake_poll(coin_ctx: orch.CoinContext) -> orch.DexScreenerResult:
        raise orch.PipelineError("dex unavailable")

    orchestrator.run_capture = fake_capture  # type: ignore[method-assign]
    orchestrator.run_risk_report = fake_risk  # type: ignore[method-assign]
    orchestrator.load_security_report = lambda mint: {"token": {"mint": mint}}  # type: ignore[method-assign]
    orchestrator.evaluate_red_flag_filter = lambda coin_ctx, report, dex_features=None: _accepted_red_flag_decision()  # type: ignore[method-assign]
    orchestrator.poll_dexscreener = fake_poll  # type: ignore[method-assign]

    asyncio.run(orchestrator.run_coin_pipeline(ctx))

    analytics_events = [e for e in status_events if e["event_type"] == "coin_analytics_failed"]
    assert len(analytics_events) == 1
    assert analytics_events[0]["mint"] == "Mint111"
    assert "dex unavailable" in str(analytics_events[0]["error"])


def test_red_flag_rejection_cancels_capture_deletes_data_and_marks_rejected(tmp_path):
    orchestrator = _pipeline_orchestrator(tmp_path)
    orchestrator.config = dataclasses.replace(
        orchestrator.config,
        capture_out_root=tmp_path / "onchain",
        red_flag_delete_rejected_capture_data=True,
    )
    ctx = orch.CoinContext(mint="Mint111", pair_addresses=("pump-amm",), migration_event={})
    capture_dir = orchestrator.config.capture_out_root / ctx.mint
    capture_dir.mkdir(parents=True)
    (capture_dir / "partial.jsonl").write_text("partial\n", encoding="utf-8")
    capture_cancelled = asyncio.Event()

    async def fake_capture(coin_ctx: orch.CoinContext) -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            capture_cancelled.set()
            raise

    async def fake_risk(coin_ctx: orch.CoinContext) -> None:
        return None

    orchestrator.run_capture = fake_capture  # type: ignore[method-assign]
    orchestrator.run_risk_report = fake_risk  # type: ignore[method-assign]
    orchestrator.load_security_report = lambda mint: {"token": {"mint": mint}}  # type: ignore[method-assign]
    orchestrator.evaluate_red_flag_filter = lambda coin_ctx, report, dex_features=None: _rejected_red_flag_decision()  # type: ignore[method-assign]

    asyncio.run(orchestrator.run_coin_pipeline_task(ctx))

    assert capture_cancelled.is_set()
    assert not capture_dir.exists()
    assert orchestrator.red_flag_rejected_mints == {"Mint111"}
    assert orchestrator.failed_mints == set()
    assert orchestrator.current_stage["Mint111"] == "red_flag_rejected"


def test_red_flag_rejected_cleanup_only_removes_capture_mint_dir(tmp_path):
    orchestrator = _pipeline_orchestrator(tmp_path)
    root = tmp_path / "onchain"
    target = root / "Mint111"
    sibling = root / "Mint222"
    target.mkdir(parents=True)
    sibling.mkdir(parents=True)
    (target / "partial.jsonl").write_text("partial\n", encoding="utf-8")
    (sibling / "keep.jsonl").write_text("keep\n", encoding="utf-8")
    orchestrator.config = dataclasses.replace(orchestrator.config, capture_out_root=root)

    deleted = orchestrator.delete_rejected_capture_data("Mint111")

    assert deleted == target.resolve()
    assert not target.exists()
    assert sibling.exists()


def test_red_flag_rejected_cleanup_refuses_outside_capture_root(tmp_path):
    orchestrator = _pipeline_orchestrator(tmp_path)
    root = tmp_path / "onchain"
    root.mkdir()
    orchestrator.config = dataclasses.replace(orchestrator.config, capture_out_root=root)

    try:
        orchestrator.delete_rejected_capture_data("../outside")
        raised = False
    except orch.PipelineError:
        raised = True

    assert raised


def test_security_report_failure_cancels_capture_and_marks_failed_not_rejected(tmp_path):
    orchestrator = _pipeline_orchestrator(tmp_path)
    ctx = orch.CoinContext(mint="Mint111", pair_addresses=("pump-amm",), migration_event={})
    capture_cancelled = asyncio.Event()

    async def fake_capture(coin_ctx: orch.CoinContext) -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            capture_cancelled.set()
            raise

    async def fake_risk(coin_ctx: orch.CoinContext) -> None:
        raise orch.PipelineError("risk failed")

    orchestrator.run_capture = fake_capture  # type: ignore[method-assign]
    orchestrator.run_risk_report = fake_risk  # type: ignore[method-assign]

    asyncio.run(orchestrator.run_coin_pipeline_task(ctx))

    assert capture_cancelled.is_set()
    assert orchestrator.failed_mints == {"Mint111"}
    assert orchestrator.red_flag_rejected_mints == set()
    assert orchestrator.current_stage["Mint111"] == "failed"


def test_supervise_pumpportal_restarts_after_exit(tmp_path, monkeypatch):
    jsonl = tmp_path / "migrations.jsonl"
    jsonl.write_text('{"event_type":"new_token","mint":"Mint111"}\n', encoding="utf-8")

    class DeadProc:
        returncode = 7
        pid = 4242

        async def wait(self) -> int:
            return 7

    class LiveProc:
        returncode = None
        pid = 4243

        async def wait(self) -> int:
            return 0

    start_calls = {"count": 0}

    async def fake_start() -> tuple[object, list, None]:
        start_calls["count"] += 1
        if start_calls["count"] == 1:
            return DeadProc(), [], None
        orchestrator.shutdown_event.set()
        return LiveProc(), [], None

    async def fake_stop(proc, tasks, log_file) -> None:
        return None

    async def fake_ensure(proc) -> None:
        return None

    config = orch.OrchestratorConfig(
        pumpportal_jsonl_path=jsonl,
        pumpportal_script_path=Path("pumpportal_ws.py"),
        capture_script_path=Path("solana_coin_1h_capture.py"),
        risk_report_script_path=Path("solana_risk_report.py"),
        dexscreener_script_path=Path("dexscreener_api.py"),
        website_grader_script_path=Path("website_grader_v2.py"),
        telegram_info_script_path=Path("telegram_info.py"),
        state_path=tmp_path / "state.json",
        pumpportal_auto_restart=True,
        pumpportal_restart_delay_seconds=0.0,
    )
    orchestrator = orch.MemeCoinPipelineOrchestrator(
        config=config,
        logger=orch.setup_logging("ERROR"),
    )
    orchestrator._pumpportal_jsonl_ready = True
    orchestrator.start_pumpportal_process = fake_start  # type: ignore[method-assign]
    orchestrator.stop_pumpportal_worker = fake_stop  # type: ignore[method-assign]
    orchestrator.ensure_pumpportal_jsonl_ready = fake_ensure  # type: ignore[method-assign]

    asyncio.run(orchestrator.supervise_pumpportal())

    assert start_calls["count"] == 2


def test_config_pumpportal_auto_restart_default(tmp_path):
    args = orch.build_parser().parse_args([])
    config = orch.config_from_args(args)
    assert config.pumpportal_auto_restart is True
    assert config.pumpportal_restart_delay_seconds == 5.0
    assert config.pumpportal_jsonl_stale_restart_seconds == 180.0
