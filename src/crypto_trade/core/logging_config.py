"""Logging setup helpers."""

from __future__ import annotations

import logging

from crypto_trade.core.io import ensure_dir
from crypto_trade.core.paths import LOGS_DIR


NOISY_LOGGERS = ["httpx", "httpcore", "crypto_trade.ingest.dexscreener"]


def configure_logging(level: str = "INFO") -> None:
    ensure_dir(LOGS_DIR)
    log_file = LOGS_DIR / "crypto_trade.logs"

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    for name in NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
