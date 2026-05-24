"""HTTP helpers shared across ingest and feature clients.

Provides a reusable async HTTP JSON helper that captures status, timing,
parse errors, and rate-limit response headers.
"""

from dataclasses import dataclass
from typing import Any

import httpx
import time
import json


@dataclass
class HTTPResponse:
    data: Any | None
    http_status: int | None
    error_type: str | None
    error_message: str | None
    elapsed_ms: float | None
    rate_limit: dict[str, str | None]


async def request_json(
    method: str,
    url: str,
    timeout: float = 10,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: Any | None = None,
    json: Any | None = None,
) -> HTTPResponse:
    start = time.perf_counter()

    try:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                data=data,
                json=json,
                timeout=timeout,
            )

        elapsed_ms = (time.perf_counter() - start) * 1000
        status = response.status_code

        rate_limit = {
            "limit": response.headers.get("x-ratelimit-limit"),
            "remaining": response.headers.get("x-ratelimit-remaining"),
            "reset": response.headers.get("x-ratelimit-reset"),
            "retry_after": response.headers.get("retry-after"),
        }

        if status == 401:
            return HTTPResponse(None, status, "unauthorized", "Unauthorized request", elapsed_ms, rate_limit)

        if status == 403:
            return HTTPResponse(None, status, "forbidden", "Forbidden request", elapsed_ms, rate_limit)

        if status == 404:
            return HTTPResponse(None, status, "not_found", "Resource not found", elapsed_ms, rate_limit)

        if status == 429:
            return HTTPResponse(None, status, "rate_limited", "Rate limit exceeded", elapsed_ms, rate_limit)

        if status >= 400:
            return HTTPResponse(
                None,
                status,
                "http_error",
                f"HTTP error {status}: {response.text[:300]}",
                elapsed_ms,
                rate_limit,
            )

        try:
            parsed_data = response.json()
        except json.JSONDecodeError as exc:
            return HTTPResponse(
                None,
                status,
                "json_decode_error",
                str(exc),
                elapsed_ms,
                rate_limit,
            )

        return HTTPResponse(
            parsed_data,
            status,
            None,
            None,
            elapsed_ms,
            rate_limit,
        )

    except httpx.TimeoutException as exc:
        return HTTPResponse(None, None, "timeout", str(exc), None, {})

    except httpx.ConnectError as exc:
        return HTTPResponse(None, None, "connection_error", str(exc), None, {})

    except httpx.RequestError as exc:
        return HTTPResponse(None, None, "request_error", str(exc), None, {})

    except Exception as exc:
        return HTTPResponse(None, None, "unknown_error", str(exc), None, {})