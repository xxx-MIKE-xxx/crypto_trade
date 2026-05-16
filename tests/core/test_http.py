"""Tests for :mod:`crypto_trade.core.http`.

``requests.Session.get`` is monkeypatched so we cover the status classification,
JSON decode, and timeout paths without touching the network.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from crypto_trade.core import http as cht


class _Resp:
    def __init__(
        self,
        status_code: int = 200,
        json_payload: Any = None,
        text: str = "",
        headers: dict[str, str] | None = None,
        json_raises: bool = False,
    ) -> None:
        self.status_code = status_code
        self._payload = json_payload if json_payload is not None else {"ok": True}
        self.text = text or "body"
        self.headers = headers or {"content-type": "application/json"}
        self._json_raises = json_raises

    def json(self) -> Any:
        if self._json_raises:
            raise ValueError("not json")
        return self._payload


def _install(monkeypatch, response_or_factory):
    if callable(response_or_factory):
        get_response = response_or_factory
    else:
        def get_response(*_a, **_kw):
            return response_or_factory

    def fake_get(self, url, headers=None, params=None, timeout=None):
        return get_response(url=url, headers=headers, params=params, timeout=timeout)

    monkeypatch.setattr(requests.Session, "get", fake_get, raising=True)


def test_make_session_sets_default_headers():
    s = cht.make_session(user_agent="ua/1.0")
    assert s.headers["user-agent"] == "ua/1.0"
    assert "application/json" in s.headers["accept"]


def test_make_session_merges_extra_headers():
    s = cht.make_session(extra_headers={"x-test": "1"})
    assert s.headers["x-test"] == "1"


def test_get_json_success_returns_body(monkeypatch):
    _install(monkeypatch, _Resp(200, {"value": 7}))
    out = cht.get_json("https://example/x")
    assert out.ok and out.body == {"value": 7}
    assert out.status_code == 200
    assert out.error_type is None
    assert out.headers.get("content-type") == "application/json"


@pytest.mark.parametrize(
    "status, expected",
    [
        (404, "not_found"),
        (401, "unauthorized"),
        (403, "forbidden"),
        (429, "rate_limited"),
        (500, "http_error"),
    ],
)
def test_get_json_classifies_status(monkeypatch, status, expected):
    _install(monkeypatch, _Resp(status_code=status, text="oops"))
    out = cht.get_json("https://example/x", name="probe")
    assert not out.ok
    assert out.error_type == expected
    assert out.text == "oops"


def test_get_json_invalid_json_records_text(monkeypatch):
    _install(monkeypatch, _Resp(status_code=200, text="<html>", json_raises=True))
    out = cht.get_json("https://example/x")
    assert not out.ok
    assert out.error_type == "invalid_json"
    assert out.text == "<html>"


def test_get_json_handles_timeout(monkeypatch):
    def raise_timeout(self, url, headers=None, params=None, timeout=None):
        raise requests.Timeout("slow")

    monkeypatch.setattr(requests.Session, "get", raise_timeout, raising=True)
    out = cht.get_json("https://example/x", name="probe")
    assert not out.ok
    assert out.error_type == "timeout"
    assert "timeout" in (out.error_message or "")


def test_get_json_handles_request_exception(monkeypatch):
    def raise_conn(self, url, headers=None, params=None, timeout=None):
        raise requests.ConnectionError("nope")

    monkeypatch.setattr(requests.Session, "get", raise_conn, raising=True)
    out = cht.get_json("https://example/x")
    assert not out.ok
    assert out.error_type == "request_error"


def test_get_json_captures_rate_limit_headers(monkeypatch):
    _install(monkeypatch, _Resp(429, headers={"Retry-After": "5", "RateLimit-Remaining": "0"}))
    out = cht.get_json("https://example/x")
    assert out.headers.get("retry-after") == "5"
    assert out.headers.get("ratelimit-remaining") == "0"
