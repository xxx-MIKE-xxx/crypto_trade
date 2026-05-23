"""PumpPortal websocket loop: ingest events, dedupe, and launch token workers."""

from __future__ import annotations

import asyncio
from pathlib import Path
import logging
from crypto_trade.ingest.pumpportal import listen
from crypto_trade.core.io import append_jsonl
from crypto_trade.core.time import utc_now
from crypto_trade.core.paths import MIGRATIONS_DIR
from crypto_trade.core.logging_config import configure_logging


logger = logging.getLogger(__name__)

def output_file() -> Path:
    now = utc_now()
    return MIGRATIONS_DIR / f"{now:%Y-%m-%d}.jsonl"

async def stream_pumpportal_events(file_path):
    async for event in listen():
        await asyncio.to_thread(append_jsonl, file_path, event)

async def main():
    configure_logging()
    logger.info(f"Creating output file")
    file_path = output_file()
    logger.info(f"created file {file_path}")
    await stream_pumpportal_events(file_path)


if __name__ == "__main__":
    asyncio.run(main())

    

