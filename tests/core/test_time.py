"""Tests for :mod:`crypto_trade.core.time`.

The codebase intentionally keeps several ISO-8601 shapes alive because parquet
and JSON artifacts written by previous runs already contain those exact strings.
These tests pin the format of each helper so future "let's just standardize"
refactors do not silently break replay.
"""

from __future__ import annotations

import datetime as dt
import re

import pytest

from crypto_trade.core import time as ct


def test_utc_now_is_timezone_aware_utc():
    now = ct.utc_now()
    assert now.tzinfo is not None
    assert now.utcoffset() == dt.timedelta(0)


def test_now_ts_and_now_ms_are_consistent():
    ts = ct.now_ts()
    ms = ct.now_ms()
    assert isinstance(ts, float)
    assert isinstance(ms, int)
    assert abs(ms - ts * 1000) < 5_000


def test_now_iso_uses_microsecond_offset():
    s = ct.now_iso()
    assert s.endswith("+00:00")
    assert re.search(r"\.\d{6}\+00:00$", s)


def test_utc_now_iso_z_is_seconds_with_z():
    s = ct.utc_now_iso_z()
    assert s.endswith("Z")
    assert "." not in s
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", s)


def test_utc_now_iso_ms_z_is_milliseconds_with_z():
    s = ct.utc_now_iso_ms_z()
    assert s.endswith("Z")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", s)


def test_ts_iso_handles_none_and_unix():
    assert ct.ts_iso(None) is None
    out = ct.ts_iso(0)
    assert out == "1970-01-01T00:00:00+00:00"


@pytest.mark.parametrize(
    "value, expected",
    [
        (0, "1970-01-01T00:00:00.000Z"),
        (1_700_000_000, "2023-11-14T22:13:20.000Z"),
        (1_700_000_000_000, "2023-11-14T22:13:20.000Z"),
        ("1700000000", "2023-11-14T22:13:20.000Z"),
        ("2023-11-14T22:13:20Z", "2023-11-14T22:13:20.000Z"),
        ("2023-11-14T22:13:20+00:00", "2023-11-14T22:13:20.000Z"),
        ("2023-11-14T23:13:20+01:00", "2023-11-14T22:13:20.000Z"),
    ],
)
def test_parse_event_ts_normalizes_to_ms_z(value, expected):
    assert ct.parse_event_ts(value) == expected


@pytest.mark.parametrize("value", [None, "", "not-a-timestamp", "  "])
def test_parse_event_ts_returns_none_for_garbage(value):
    assert ct.parse_event_ts(value) is None
