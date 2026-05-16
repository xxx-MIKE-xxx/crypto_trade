#!/usr/bin/env python3
"""
test_pumpportal_ws_env.py

Reads PumpPortal API key from local .pumpportal.env in the same folder.

Create .pumpportal.env next to this script:
  PUMPPORTAL_API_KEY=your_key_here

Run:
  pip install websockets
  python test_pumpportal_ws_env.py
"""

import asyncio
import json
import time
from pathlib import Path

import websockets


ENV_FILE = ".env"
OUT_FILE = "pumpportal_migrations_sample.jsonl"
DURATION_SECONDS = 120


def load_local_env(filename: str = ENV_FILE) -> dict:
    path = Path(__file__).resolve().parent / filename
    values = {}

    if not path.exists():
        print(f"WARNING: {filename} not found next to script; connecting without API key.")
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


async def main():
    env = load_local_env()
    api_key = env.get("PUMPPORTAL_API_KEY", "")

    url = "wss://pumpportal.fun/api/data"
    if api_key:
        url += f"?api-key={api_key}"

    out_path = Path(__file__).resolve().parent / OUT_FILE

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"method": "subscribeMigration"}))
        await ws.send(json.dumps({"method": "subscribeNewToken"}))

        print("listening to PumpPortal WebSocket")
        print(f"writing to: {out_path}")

        start = time.time()
        with out_path.open("a", encoding="utf-8") as out:
            while time.time() - start < DURATION_SECONDS:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    print("no message in 30s; still connected")
                    continue

                data = json.loads(msg)
                row = {"received_at": time.time(), "data": data}
                print(json.dumps(data, ensure_ascii=False))
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                out.flush()


if __name__ == "__main__":
    asyncio.run(main())
