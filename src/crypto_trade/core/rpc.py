"""Shared JSON-RPC client primitives.

Solana on-chain capture rotates across multiple HTTP RPC providers and respects
per-endpoint throttling and ``Retry-After`` semantics. The same primitives can
back any future JSON-RPC integration (e.g. EVM chains), so they live in ``core``
rather than in ``ingest``.
"""

from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests

from crypto_trade.core.time import now_ts


def short_rpc_name(url: str) -> str:
    """Return a short, provider-aware identifier for an RPC URL."""
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    if "helius" in url:
        return f"helius_{h}"
    if "alchemy" in url:
        return f"alchemy_{h}"
    if "quicknode" in url or "quiknode" in url:
        return f"quicknode_{h}"
    if "solana.com" in url or "mainnet-beta" in url:
        return f"public_{h}"
    return f"rpc_{h}"


@dataclass
class RpcResult:
    ok: bool
    result: Any = None
    error: Optional[str] = None
    retry_after: Optional[float] = None
    status_code: Optional[int] = None


@dataclass
class RpcStats:
    calls: int = 0
    ok: int = 0
    rate_limited: int = 0
    errors: int = 0
    last_error: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "ok": self.ok,
            "rate_limited": self.rate_limited,
            "errors": self.errors,
            "last_error": self.last_error,
        }


class RpcEndpoint:
    """Single JSON-RPC endpoint with per-call throttling and 429 handling."""

    def __init__(self, url: str, min_interval: float, debug: bool = False) -> None:
        self.url = url
        self.name = short_rpc_name(url)
        self.min_interval = min_interval
        self.debug = debug
        self.session = requests.Session()
        self.req_id = random.randint(1000, 1_000_000)
        self.next_allowed_at = 0.0
        self.stats = RpcStats()

    def call(self, method: str, params: list[Any]) -> RpcResult:
        wait = self.next_allowed_at - now_ts()
        if wait > 0:
            time.sleep(wait)

        if self.min_interval > 0:
            self.next_allowed_at = max(self.next_allowed_at, now_ts()) + self.min_interval

        self.req_id += 1
        body = {
            "jsonrpc": "2.0",
            "id": self.req_id,
            "method": method,
            "params": params,
        }

        self.stats.calls += 1

        try:
            if self.debug:
                print(f"[{self.name}] RPC {method}")

            r = self.session.post(self.url, json=body, timeout=60)
            retry_after: Optional[float] = None
            if r.headers.get("Retry-After"):
                try:
                    retry_after = float(r.headers["Retry-After"])
                except Exception:
                    retry_after = None

            if r.status_code == 429:
                self.stats.rate_limited += 1
                self.stats.last_error = "HTTP_429_RATE_LIMIT"
                if retry_after is None:
                    retry_after = 10.0
                self.next_allowed_at = max(self.next_allowed_at, now_ts() + retry_after)
                return RpcResult(False, error="HTTP_429_RATE_LIMIT", retry_after=retry_after, status_code=429)

            if r.status_code == 403:
                self.stats.errors += 1
                self.stats.last_error = "HTTP_403_FORBIDDEN"
                return RpcResult(False, error="HTTP_403_FORBIDDEN", status_code=403)

            if r.status_code >= 400:
                self.stats.errors += 1
                msg = f"HTTP_{r.status_code}: {r.text[:500]}"
                self.stats.last_error = msg
                return RpcResult(False, error=msg, status_code=r.status_code)

            data = r.json()
            if "error" in data:
                self.stats.errors += 1
                msg = f"RPC_ERROR_{method}: {data['error']}"
                self.stats.last_error = msg
                msg_lower = str(data["error"]).lower()
                if "rate" in msg_lower or "limit" in msg_lower or "too many" in msg_lower:
                    self.stats.rate_limited += 1
                    self.next_allowed_at = max(self.next_allowed_at, now_ts() + 10.0)
                    return RpcResult(False, error="RPC_RATE_LIMIT", retry_after=10.0)
                return RpcResult(False, error=msg)

            self.stats.ok += 1
            return RpcResult(True, result=data.get("result"))

        except requests.RequestException as e:
            self.stats.errors += 1
            msg = f"REQUEST_EXCEPTION: {repr(e)}"
            self.stats.last_error = msg
            self.next_allowed_at = max(self.next_allowed_at, now_ts() + 3.0)
            return RpcResult(False, error=msg)
        except Exception as e:
            self.stats.errors += 1
            msg = f"EXCEPTION: {repr(e)}"
            self.stats.last_error = msg
            return RpcResult(False, error=msg)


@dataclass
class RpcPool:
    """Round-robin pool of ``RpcEndpoint`` instances with fallback retries."""

    urls: list[str]
    min_interval: float = 1.1
    debug: bool = False
    default_url: Optional[str] = None
    endpoints: list[RpcEndpoint] = field(init=False)
    idx: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        urls = self.urls or ([self.default_url] if self.default_url else [])
        if not urls:
            raise ValueError("RpcPool requires at least one URL")
        self.endpoints = [
            RpcEndpoint(u, min_interval=self.min_interval, debug=self.debug) for u in urls
        ]

    def next_endpoint(self) -> RpcEndpoint:
        ep = self.endpoints[self.idx % len(self.endpoints)]
        self.idx += 1
        return ep

    def call_any(
        self, method: str, params: list[Any], tries: Optional[int] = None
    ) -> tuple[RpcResult, str]:
        if tries is None:
            tries = max(1, len(self.endpoints))
        last: Optional[tuple[RpcResult, str]] = None
        for _ in range(tries):
            ep = self.next_endpoint()
            res = ep.call(method, params)
            if res.ok:
                return res, ep.name
            last = (res, ep.name)
        assert last is not None
        return last

    def stats(self) -> dict[str, Any]:
        return {ep.name: ep.stats.as_dict() | {"url": ep.url} for ep in self.endpoints}
