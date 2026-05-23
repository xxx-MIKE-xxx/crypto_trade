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

from pathlib import Path
import json
from websockets.asyncio.client import connect
import asyncio
import os
import requests
from crypto_trade.core.env import load_env, get_env
from crypto_trade.core.io import ensure_dir, append_jsonl
from crypto_trade.core.time import utc_now_iso_ms_z

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "migrations" 
env_dir = BASE_DIR / ".env"

load_env()

PUMPPORTAL_API_KEY = get_env("PUMPPORTAL_API_KEY") 

BASE_URL = f"wss://pumpportal.fun/api/data?api-key={PUMPPORTAL_API_KEY}"


def type_of_msg(msg: dict) -> str:
    """Detect type of PumpPortal event - either MINT | MIGRATION | OTHER
    """
    if "message" in msg:
        return "system"
    if msg.get("txType") == "create":
        return "mint"
    if msg.get("txType") == "migrate":
        return "migration"
    return None


async def subscribe_new_token(websocket):
    payload = {
        "method": "subscribeNewToken"
    }
    await websocket.send(json.dumps(payload))


async def subscribe_migration(websocket):
    payload = {
        "method": "subscribeMigration"
    }
    await websocket.send(json.dumps(payload))


async def listen(mints=True, migrations=True):
    async with connect(BASE_URL) as websocket:
        if mints:
            await subscribe_new_token(websocket)
        if migrations:
            await subscribe_migration(websocket)

        async for message in websocket:
            msg = json.loads(message)
            msg_type = type_of_msg(msg)
            if msg_type in {"mint", "migration"}:
                yield {
                    "time": utc_now_iso_ms_z(),
                    "type": msg_type,
                    **msg
                    
                }



            

async def main():
    await listen(
        mints=True,
        migrations=True
    )


            
if __name__ == "__main__":
    asyncio.run(main())