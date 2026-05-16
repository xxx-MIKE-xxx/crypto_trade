"""Compatibility shim: invoke ``python -m crypto_trade.pipeline``."""

from runpy import run_module

if __name__ == "__main__":
    run_module("crypto_trade.pipeline", run_name="__main__")
