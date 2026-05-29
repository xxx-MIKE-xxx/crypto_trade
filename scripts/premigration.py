from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from crypto_trade.pipeline.premigration_tracker import CONFIG_PATH, main


if __name__ == "__main__":
    asyncio.run(main(CONFIG_PATH))
