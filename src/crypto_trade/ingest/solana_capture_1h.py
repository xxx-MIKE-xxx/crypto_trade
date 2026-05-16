"""Backward-compatible alias for crypto_trade.ingest.onchain."""

from crypto_trade.ingest.onchain import *  # noqa: F403

if __name__ == "__main__":
    from runpy import run_module

    run_module("crypto_trade.ingest.onchain", run_name="__main__")