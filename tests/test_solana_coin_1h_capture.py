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
    assert args.enable_account_notifications is False
