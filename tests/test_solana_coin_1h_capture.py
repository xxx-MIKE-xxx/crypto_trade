import sys

import solana_coin_1h_capture as capture


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
