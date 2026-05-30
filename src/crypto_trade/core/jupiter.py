from __future__ import annotations

import asyncio
from typing import Any

from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.http import HTTPResponse, request_json

load_env()

JUPITER_API_KEY = get_env("JUPITER_API_KEY")
JUPITER_MIN_INTERVAL_SECONDS = float(get_env("JUPITER_MIN_INTERVAL_SECONDS", "1.05"))

_lock = asyncio.Lock()
_next_request_at = 0.0


async def wait_jupiter_rate_limit() -> None:
    global _next_request_at

    async with _lock:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now < _next_request_at:
            await asyncio.sleep(_next_request_at - now)
        _next_request_at = asyncio.get_running_loop().time() + JUPITER_MIN_INTERVAL_SECONDS


async def request_jupiter_json(
    method: str,
    url: str,
    *,
    timeout: float = 10,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: Any | None = None,
    json: Any | None = None,
) -> HTTPResponse:
    await wait_jupiter_rate_limit()

    request_headers = dict(headers or {})
    if JUPITER_API_KEY:
        request_headers.setdefault("x-api-key", JUPITER_API_KEY)

    return await request_json(
        method,
        url,
        timeout=timeout,
        headers=request_headers,
        params=params,
        data=data,
        json=json,
    )
