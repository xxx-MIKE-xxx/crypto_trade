"""Logging setup helpers."""

from __future__ import annotations

import logging
from crypto_trade.core.paths import LOGS_DIR
from crypto_trade.core.io import ensure_dir


def configure_logging(level: str = "INFO") -> None:
    ensure_dir(LOGS_DIR)
    log_file = LOGS_DIR / "crypto_trade.logs"

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers = [
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )