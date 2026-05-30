#!/usr/bin/env python3
"""PumpPortal websocket helpers."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, AsyncIterator

import websockets
from websockets.asyncio.client import connect

from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.io import append_jsonl
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import PUMPPORTAL_WS_CONFIG
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.core.yaml import get_yaml_value

load_env()

logger = logging.getLogger(__name__)

PUMPPORTAL_API_KEY = get_env("PUMPPORTAL_API_KEY")
BASE_URL = f"wss://pumpportal.fun/api/data?api-key={PUMPPORTAL_API_KEY}"

RETRY_INITIAL_DELAY = get_yaml_value(PUMPPORTAL_WS_CONFIG, "retry", "initial_delay_s")
RETRY_MAX_DELAY = get_yaml_value(PUMPPORTAL_WS_CONFIG, "retry", "max_delay_s")
DEFAULT_SHARED_LOCK_STALE_SECONDS = 120
DEFAULT_SHARED_LOCK_TOUCH_SECONDS = 15


def type_of_msg(msg: dict) -> str | None:
    """Detect type of PumpPortal event - either MINT | MIGRATION | OTHER."""
    if "message" in msg:
        return "system"
    if msg.get("txType") == "create":
        return "mint"
    if msg.get("txType") == "migrate":
        return "migration"
    return None


async def subscribe_new_token(websocket):
    await websocket.send(json.dumps({"method": "subscribeNewToken"}))


async def subscribe_migration(websocket):
    await websocket.send(json.dumps({"method": "subscribeMigration"}))


async def listen(
    mints: bool = True,
    migrations: bool = True,
    url: str | None = None,
):
    retry_delay = RETRY_INITIAL_DELAY
    ws_url = url or BASE_URL

    while True:
        try:
            async with connect(ws_url) as websocket:
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
                            **msg,
                        }

        except (
            OSError,
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.InvalidHandshake,
            websockets.exceptions.InvalidStatus,
        ) as exc:
            logger.warning("Connection lost - reconnecting %s", exc)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, RETRY_MAX_DELAY)


def shared_lock_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".lock")


def lock_is_stale(lock_path: Path, stale_seconds: float) -> bool:
    if not lock_path.exists():
        return True
    return time.time() - lock_path.stat().st_mtime > stale_seconds


def touch_shared_lock(lock_path: Path, owner: str) -> None:
    lock_path.write_text(
        json.dumps({"owner": owner, "pid": os.getpid(), "time": time.time()}),
        encoding="utf-8",
    )


def try_acquire_shared_lock(path: Path, owner: str, stale_seconds: float) -> Path | None:
    lock_path = shared_lock_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists() and lock_is_stale(lock_path, stale_seconds):
        try:
            lock_path.unlink()
        except OSError:
            pass

    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return None

    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"owner": owner, "pid": os.getpid(), "time": time.time()}))
    return lock_path


async def keep_shared_lock_alive(lock_path: Path, owner: str) -> None:
    while True:
        touch_shared_lock(lock_path, owner)
        await asyncio.sleep(DEFAULT_SHARED_LOCK_TOUCH_SECONDS)


async def read_shared_file(
    path: Path,
    offset: int,
    poll_seconds: float,
) -> AsyncIterator[tuple[dict[str, Any], int]]:
    while True:
        if not path.exists():
            await asyncio.sleep(poll_seconds)
            continue

        with path.open("r", encoding="utf-8") as fh:
            fh.seek(offset)
            while line := fh.readline():
                offset = fh.tell()
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                yield event, offset

        await asyncio.sleep(poll_seconds)


async def shared_listen(
    *,
    path: Path,
    owner: str,
    url: str | None = None,
    initial_offset: int = 0,
    poll_seconds: float = 1,
    lock_stale_seconds: float = DEFAULT_SHARED_LOCK_STALE_SECONDS,
) -> AsyncIterator[tuple[dict[str, Any], int | None]]:
    """Yield PumpPortal events from a shared file, becoming the writer if needed.

    The first running process owns the websocket and appends to ``path``. Other
    processes tail the same file. If no writer is alive, this process takes over.
    """
    offset = initial_offset

    while True:
        lock_path = try_acquire_shared_lock(path, owner, lock_stale_seconds)
        if lock_path is not None:
            logger.info("PumpPortal shared writer acquired by %s", owner)
            keepalive = asyncio.create_task(keep_shared_lock_alive(lock_path, owner))
            try:
                async for event in listen(mints=True, migrations=True, url=url):
                    append_jsonl(path, event)
                    yield event, None
            finally:
                keepalive.cancel()
                await asyncio.gather(keepalive, return_exceptions=True)
                try:
                    lock_path.unlink()
                except OSError:
                    pass
        else:
            async for event, offset in read_shared_file(path, offset, poll_seconds):
                yield event, offset
                if lock_is_stale(shared_lock_path(path), lock_stale_seconds):
                    break


async def main():
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--shared-path", type=Path, default=None)
    args = parser.parse_args()

    if args.shared_path:
        async for event, _offset in shared_listen(path=args.shared_path, owner="pumpportal_cli"):
            print(event)
    else:
        async for event in listen(mints=True, migrations=True):
            print(event)


if __name__ == "__main__":
    asyncio.run(main())
