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
import websockets
import asyncio
from crypto_trade.core.env import load_env, get_env
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.core.yaml import get_yaml_value
from crypto_trade.core.paths import PUMPPORTAL_WS_CONFIG
from crypto_trade.core.logging_config import configure_logging
import logging

load_env()

logger = logging.getLogger(__name__)

PUMPPORTAL_API_KEY = get_env("PUMPPORTAL_API_KEY") 

BASE_URL = f"wss://pumpportal.fun/api/data?api-key={PUMPPORTAL_API_KEY}"

RETRY_INITIAL_DELAY = get_yaml_value(PUMPPORTAL_WS_CONFIG, "retry", "initial_delay_s")
RETRY_MAX_DELAY = get_yaml_value(PUMPPORTAL_WS_CONFIG, "retry", "max_delay_s")

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
    retry_delay = RETRY_INITIAL_DELAY
    while True:
            try:
                async with connect(BASE_URL) as websocket:
                    retry_delay = RETRY_INITIAL_DELAY
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
            except (OSError, websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidHandshake,
                                websockets.exceptions.InvalidStatus) as e:
                logger.warning("Connection lost - reconnecting %s", e)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, RETRY_MAX_DELAY)

async def main():
    configure_logging()
    async for event in listen(mints=True, migrations=True):
        print(event)
            
if __name__ == "__main__":
    asyncio.run(main())