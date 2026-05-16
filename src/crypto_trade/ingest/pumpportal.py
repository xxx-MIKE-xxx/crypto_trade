#!/usr/bin/env python3
"""PumpPortal websocket sample writer.

Reads ``PUMPPORTAL_API_KEY`` either from the process environment or from a
``.env`` file next to this script. Writes raw migration/new-token events to
JSONL for a fixed duration so the streaming side of the pipeline can be sanity
checked without standing up the full orchestrator.

Run:
    pip install websockets
    python -m crypto_trade.ingest.pumpportal
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import websockets

from crypto_trade.core.env import load_env
from crypto_trade.core.logging import configure_logging
from crypto_trade.core.time import now_ts

ENV_FILE = ".env"
OUT_FILE = "pumpportal_migrations_sample.jsonl"
DURATION_SECONDS = 120


async def _stream() -> None:
    here = Path(__file__).resolve().parent
    load_env(candidates=[here / ENV_FILE])

    api_key = os.environ.get("PUMPPORTAL_API_KEY", "")

    url = "wss://pumpportal.fun/api/data"
    if api_key:
        url += f"?api-key={api_key}"

    out_path = here / OUT_FILE

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"method": "subscribeMigration"}))
        await ws.send(json.dumps({"method": "subscribeNewToken"}))

        print("listening to PumpPortal WebSocket")
        print(f"writing to: {out_path}")

        start = now_ts()
        with out_path.open("a", encoding="utf-8") as out:
            while now_ts() - start < DURATION_SECONDS:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    print("no message in 30s; still connected")
                    continue

                data = json.loads(msg)
                row = {"received_at": now_ts(), "data": data}
                print(json.dumps(data, ensure_ascii=False))
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                out.flush()


def main() -> None:
    configure_logging()
    asyncio.run(_stream())


if __name__ == "__main__":
    main()
