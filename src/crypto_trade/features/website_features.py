"""Backward-compatible alias for crypto_trade.features.alt_data."""

from crypto_trade.features.alt_data import *  # noqa: F403

if __name__ == "__main__":
    from runpy import run_module

    run_module("crypto_trade.features.alt_data", run_name="__main__")