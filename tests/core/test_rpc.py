"""Tests for :mod:`crypto_trade.core.rpc`.

``requests.post`` is monkeypatched so we exercise the throttling, error mapping,
and pool round-robin logic without touching the network.
"""

from __future__ import annotations

from typing import Any

import pytest

from crypto_trade.core import rpc


class _Response:
    def __init__(self, status_code: int = 200, payload: Any = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {"result": {"ok": True}}
        self.headers = headers or {}
        self.text = str(self._payload)

    def json(self) -> Any:
        return self._payload


def _patch_session(monkeypatch, response_or_factory):
    if callable(response_or_factory):
        get_response = response_or_factory
    else:
        def get_response(*_a, **_kw):
            return response_or_factory

    def fake_post(self, url, json=None, timeout=None):
        return get_response(url=url, body=json)

    import requests

    monkeypatch.setattr(requests.Session, "post", fake_post, raising=True)


def test_short_rpc_name_picks_provider_prefix():
    assert rpc.short_rpc_name("https://mainnet.helius-rpc.com/x").startswith("helius_")
    assert rpc.short_rpc_name("https://solana-mainnet.alchemy.com/x").startswith("alchemy_")
    assert rpc.short_rpc_name("https://example.quicknode.pro/x").startswith("quicknode_")
    assert rpc.short_rpc_name("https://api.mainnet-beta.solana.com").startswith("public_")
    assert rpc.short_rpc_name("https://other.example/rpc").startswith("rpc_")


def test_endpoint_call_success_returns_result(monkeypatch):
    _patch_session(monkeypatch, _Response(200, {"result": {"value": 42}}))
    ep = rpc.RpcEndpoint("https://example/rpc", min_interval=0.0)
    out = ep.call("getThing", [])
    assert out.ok and out.result == {"value": 42}
    assert ep.stats.ok == 1 and ep.stats.calls == 1


def test_endpoint_call_rate_limit_records_retry_after(monkeypatch):
    _patch_session(monkeypatch, _Response(429, {}, headers={"Retry-After": "2.5"}))
    ep = rpc.RpcEndpoint("https://example/rpc", min_interval=0.0)
    out = ep.call("getThing", [])
    assert not out.ok
    assert out.status_code == 429
    assert out.retry_after == 2.5
    assert ep.stats.rate_limited == 1


def test_endpoint_call_http_error_propagates(monkeypatch):
    _patch_session(monkeypatch, _Response(500, {"oops": True}))
    ep = rpc.RpcEndpoint("https://example/rpc", min_interval=0.0)
    out = ep.call("getThing", [])
    assert not out.ok
    assert out.status_code == 500
    assert ep.stats.errors == 1


def test_endpoint_call_rpc_error_payload(monkeypatch):
    _patch_session(monkeypatch, _Response(200, {"error": {"code": -32000, "message": "boom"}}))
    ep = rpc.RpcEndpoint("https://example/rpc", min_interval=0.0)
    out = ep.call("getThing", [])
    assert not out.ok
    assert ep.stats.errors == 1
    assert "RPC_ERROR_getThing" in (out.error or "")


def test_endpoint_call_rpc_error_rate_limit_message(monkeypatch):
    _patch_session(monkeypatch, _Response(200, {"error": {"message": "Too Many Requests"}}))
    ep = rpc.RpcEndpoint("https://example/rpc", min_interval=0.0)
    out = ep.call("getThing", [])
    assert not out.ok
    assert out.error == "RPC_RATE_LIMIT"
    assert ep.stats.rate_limited == 1


def test_endpoint_call_request_exception(monkeypatch):
    import requests

    def raise_exc(*_a, **_kw):
        raise requests.ConnectionError("nope")

    monkeypatch.setattr(requests.Session, "post", raise_exc, raising=True)
    ep = rpc.RpcEndpoint("https://example/rpc", min_interval=0.0)
    out = ep.call("getThing", [])
    assert not out.ok
    assert ep.stats.errors == 1
    assert "REQUEST_EXCEPTION" in (out.error or "")


def test_pool_requires_at_least_one_url():
    with pytest.raises(ValueError):
        rpc.RpcPool(urls=[])


def test_pool_rotates_endpoints_and_returns_first_ok(monkeypatch):
    calls: list[str] = []

    def factory(url, body):
        calls.append(url)
        if "good" in url:
            return _Response(200, {"result": "ok"})
        return _Response(500, {})

    _patch_session(monkeypatch, factory)

    pool = rpc.RpcPool(urls=["https://bad.example/rpc", "https://good.example/rpc"], min_interval=0.0)
    result, name = pool.call_any("getThing", [])
    assert result.ok and result.result == "ok"
    assert name.startswith("rpc_")
    assert calls == ["https://bad.example/rpc", "https://good.example/rpc"]


def test_pool_returns_last_failure_when_all_fail(monkeypatch):
    _patch_session(monkeypatch, _Response(500, {}))
    pool = rpc.RpcPool(urls=["https://a.example/rpc", "https://b.example/rpc"], min_interval=0.0)
    result, _name = pool.call_any("getThing", [])
    assert not result.ok
    assert result.status_code == 500
