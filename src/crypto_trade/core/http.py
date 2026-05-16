"""HTTP helpers shared across ingest and feature clients.

Provides a single reusable ``requests.Session`` factory and a small
``get_json`` wrapper that captures status, timing, parse errors, and the
rate-limit response headers we commonly inspect.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

import requests

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "crypto-trade/0.1 (+https://example.invalid)"

RATE_LIMIT_HEADERS = (
    "retry-after",
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
)


@dataclass
class HttpResponse:
    """Outcome of a single HTTP request.

    ``error_type`` is set for transport/parse failures and HTTP statuses we
    treat as soft errors (404, 401, 403, 429). Successful 2xx responses leave
    ``error_type`` as ``None``.
    """

    url: str
    status_code: Optional[int]
    elapsed_seconds: float
    body: Any = None
    text: Optional[str] = None
    headers: dict[str, Optional[str]] = field(default_factory=dict)
    error_type: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def ok(self) -> bool:
        return (
            self.error_type is None
            and self.status_code is not None
            and 200 <= self.status_code < 400
        )


def make_session(
    user_agent: str = DEFAULT_USER_AGENT,
    extra_headers: Optional[Mapping[str, str]] = None,
) -> requests.Session:
    """Create a ``requests.Session`` pre-populated with default headers."""
    session = requests.Session()
    session.headers.update(
        {
            "user-agent": user_agent,
            "accept": "application/json, text/plain;q=0.9, */*;q=0.5",
        }
    )
    if extra_headers:
        session.headers.update(dict(extra_headers))
    return session


def _rate_limit_snapshot(headers: Mapping[str, str]) -> dict[str, Optional[str]]:
    lower = {k.lower(): v for k, v in headers.items()}
    snapshot: dict[str, Optional[str]] = {
        "content-type": lower.get("content-type"),
    }
    for key in RATE_LIMIT_HEADERS:
        snapshot[key] = lower.get(key)
    return snapshot


def _classify_status(name: str, status: int) -> tuple[Optional[str], Optional[str]]:
    if status == 404:
        return "not_found", f"{name}: 404 not found"
    if status == 401:
        return "unauthorized", f"{name}: 401 unauthorized"
    if status == 403:
        return "forbidden", f"{name}: 403 forbidden"
    if status == 429:
        return "rate_limited", f"{name}: 429 rate limited"
    if 400 <= status < 600:
        return "http_error", f"{name}: HTTP {status}"
    return None, None


def get_json(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    headers: Optional[Mapping[str, str]] = None,
    params: Optional[Mapping[str, Any]] = None,
    timeout: float = DEFAULT_TIMEOUT,
    name: Optional[str] = None,
    capture_text_on_parse_error: bool = True,
) -> HttpResponse:
    """Perform a GET and decode JSON, returning an :class:`HttpResponse`.

    ``session`` is optional; for repeated calls callers should pass a shared
    session from :func:`make_session` to enable connection reuse.
    """
    label = name or url
    sess = session or make_session()
    start = time.perf_counter()
    try:
        r = sess.get(url, headers=dict(headers) if headers else None, params=dict(params) if params else None, timeout=timeout)
    except requests.Timeout as e:
        return HttpResponse(
            url=url,
            status_code=None,
            elapsed_seconds=time.perf_counter() - start,
            error_type="timeout",
            error_message=f"{label}: timeout: {e}",
        )
    except requests.RequestException as e:
        return HttpResponse(
            url=url,
            status_code=None,
            elapsed_seconds=time.perf_counter() - start,
            error_type="request_error",
            error_message=f"{label}: request failed: {e}",
        )

    elapsed = time.perf_counter() - start
    headers_snapshot = _rate_limit_snapshot(r.headers)
    error_type, error_message = _classify_status(label, r.status_code)

    body: Any = None
    text: Optional[str] = None
    if error_type is None:
        try:
            body = r.json()
        except ValueError as e:
            error_type = "invalid_json"
            error_message = f"{label}: invalid JSON: {e}"
            if capture_text_on_parse_error:
                text = (r.text or "")[:4000]
    elif capture_text_on_parse_error:
        text = (r.text or "")[:4000]

    return HttpResponse(
        url=url,
        status_code=r.status_code,
        elapsed_seconds=elapsed,
        body=body,
        text=text,
        headers=headers_snapshot,
        error_type=error_type,
        error_message=error_message,
    )
