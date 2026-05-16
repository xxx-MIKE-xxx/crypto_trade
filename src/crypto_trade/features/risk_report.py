"""Backward-compatible alias for crypto_trade.features.token_risk."""

from crypto_trade.features.token_risk import *  # noqa: F403

if __name__ == "__main__":
    from runpy import run_module

    run_module("crypto_trade.features.token_risk", run_name="__main__")