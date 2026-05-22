import asyncio
import sys

import solana_coin_1h_capture as capture


def make_capture(tmp_path, **overrides):
    defaults = {
        "mint": "Mint111",
        "watch_addresses": ["Mint111"],
        "quote_mints": {capture.WSOL_MINT},
        "rpc_urls": ["https://example.invalid"],
        "ws_url": "wss://example.invalid",
        "out_dir": tmp_path,
        "duration_seconds": 60,
        "rpc_min_interval": 0.0,
        "tx_retry_seconds": 1.0,
        "max_tx_attempts": 1,
        "poll_seconds": 60.0,
        "backfill_limit": 1,
        "snapshot_seconds": 0.0,
        "poll_seconds_connected": 60.0,
        "network_sample_seconds": 0.0,
        "network_sample_fee_addresses": False,
        "max_queue_size": 100,
        "prefetch_filter_mode": "pump-first",
        "noise_fetch_sample_per_minute": 2,
        "write_dropped_ws_full_logs": False,
        "display_seconds": 0.0,
        "enable_account_notifications": False,
        "debug": False,
    }
    defaults.update(overrides)
    return capture.Capture(**defaults)


def log_message(signature, logs, err=None, slot=123):
    return {
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {
            "result": {
                "context": {"slot": slot},
                "value": {
                    "err": err,
                    "logs": logs,
                    "signature": signature,
                },
            }
        },
    }


def test_parse_args_credit_saving_defaults(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "solana_coin_1h_capture.py",
            "--mint",
            "Mint111",
        ],
    )

    args = capture.parse_args()

    assert args.poll_seconds == 20.0
    assert args.poll_seconds_connected == 60.0
    assert args.snapshot_seconds == 0.0
    assert args.network_sample_seconds == 60.0
    assert args.network_sample_fee_addresses is False
    assert args.enable_account_notifications is False
    assert args.prefetch_filter_mode == "pump-first"
    assert args.noise_fetch_sample_per_minute == 2
    assert args.write_dropped_ws_full_logs is False


def test_compute_budget_priority_fee_from_compiled_instruction():
    tx = {
        "transaction": {
            "message": {
                "instructions": [
                    {
                        "programId": capture.COMPUTE_BUDGET_PROGRAM_ID,
                        "parsed": {
                            "type": "setComputeUnitPrice",
                            "info": {"microLamports": 1_000},
                        },
                    }
                ]
            }
        },
        "meta": {"computeUnitsConsumed": 10_000},
    }

    features = capture.extract_compute_budget_features(tx)

    assert features["compute_unit_price_micro_lamports"] == 1_000
    assert features["compute_units_consumed"] == 10_000
    assert features["priority_fee_lamports_est"] == 10


def test_pool_event_classifier_uses_logs_and_balance_changes():
    tx = {
        "meta": {
            "logMessages": ["Program log: Instruction: Swap"],
            "preTokenBalances": [{"mint": "Mint111", "accountIndex": 0}],
            "postTokenBalances": [{"mint": capture.WSOL_MINT, "accountIndex": 1}],
        }
    }

    event = capture.classify_pool_events(tx, "Mint111", {capture.WSOL_MINT})

    assert event["primary"] == "swap"
    assert "swap" in event["labels"]
    assert "target_and_quote_balance_change" in event["evidence"]


def test_is_valid_solana_address_rejects_symbolic_pair_name():
    assert capture.is_valid_solana_address("So11111111111111111111111111111111111111112") is True
    assert capture.is_valid_solana_address("pump-amm") is False


def test_prefetch_classifier_keeps_pump_trade_logs():
    cls = capture.classify_prefetch_logs(
        [
            "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]",
            "Program log: Instruction: Buy",
        ]
    )

    assert cls.category == "trade_relevant"


def test_prefetch_classifier_keeps_liquidity_logs():
    cls = capture.classify_prefetch_logs(["Program log: Instruction: AddLiquidity"])

    assert cls.category == "liquidity_relevant"


def test_prefetch_classifier_keeps_failed_trade_logs():
    cls = capture.classify_prefetch_logs(
        [
            "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]",
            "Program log: Instruction: Buy",
            "Program log: AnchorError slippage below min amount out",
        ],
        err={"InstructionError": [1, "Custom"]},
    )

    assert cls.category == "failed_trade_relevant"


def test_prefetch_classifier_samples_no_arb_noise():
    cls = capture.classify_prefetch_logs(["Program log: No arb opportunity found"])

    assert cls.category == "bot_noise_sample"


def test_prefetch_classifier_drops_generic_mint_mentions():
    cls = capture.classify_prefetch_logs(["Program log: Mint111"])

    assert cls.category == "irrelevant_noise"


def test_prefetch_classifier_does_not_treat_pump_suffix_as_pump_amm():
    cls = capture.classify_prefetch_logs(
        [
            "Program log: SomeMint111111111111111111111111111111pump",
            "Program log: Instruction: Swap",
        ]
    )

    assert cls.category == "irrelevant_noise"


def test_noise_bucket_limits_noise_but_not_trades(tmp_path):
    cap = make_capture(tmp_path, noise_fetch_sample_per_minute=2)
    noise = capture.PrefetchClassification("bot_noise_sample", "test")
    trade = capture.PrefetchClassification("trade_relevant", "test")

    assert cap.decide_ws_prefetch(noise)[0] is True
    assert cap.decide_ws_prefetch(noise)[0] is True
    assert cap.decide_ws_prefetch(noise)[0] is False
    assert cap.decide_ws_prefetch(trade)[0] is True


def test_websocket_prefetch_filter_controls_queue_and_outputs(tmp_path):
    cap = make_capture(tmp_path, noise_fetch_sample_per_minute=1)

    async def runner():
        await cap.process_websocket_log_message(
            "Mint111",
            log_message(
                "trade_sig",
                [
                    "Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]",
                    "Program log: Instruction: Sell",
                ],
            ),
        )
        await cap.process_websocket_log_message(
            "Mint111",
            log_message("noise_sig_1", ["Program log: No profitable route"]),
        )
        await cap.process_websocket_log_message(
            "Mint111",
            log_message("noise_sig_2", ["Program log: No arb opportunity found"]),
        )
        await cap.process_websocket_log_message(
            "Mint111",
            log_message("generic_sig", ["Program log: Mint111"]),
        )

    asyncio.run(runner())
    cap.flush_ws_event_counts()

    assert cap.pending.qsize() == 2

    signatures = list(capture.iter_jsonl(cap.paths["signatures"]))
    by_sig = {row["signature"]: row for row in signatures}
    assert by_sig["trade_sig"]["prefetch_fetch_decision"] == "queued"
    assert by_sig["noise_sig_1"]["prefetch_fetch_decision"] == "queued_noise_sample"
    assert by_sig["noise_sig_2"]["prefetch_fetch_decision"] == "dropped_noise_sample_limit"
    assert by_sig["generic_sig"]["prefetch_fetch_decision"] == "dropped_filter"

    raw_ws = list(capture.iter_jsonl(cap.paths["ws_logs"]))
    compact_ws = list(capture.iter_jsonl(cap.paths["ws_events"]))
    counts = list(capture.iter_jsonl(cap.paths["ws_event_counts"]))

    assert len(raw_ws) == 2
    assert len(compact_ws) == 4
    assert counts[-1]["counts"]["total"] == 4
    assert counts[-1]["counts"]["decision_dropped_filter"] == 1
