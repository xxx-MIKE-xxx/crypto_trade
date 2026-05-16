"""Tests for :mod:`crypto_trade.core.text`."""

from __future__ import annotations

from crypto_trade.core import text


def test_compact_json_dumps_is_deterministic():
    a = text.compact_json_dumps({"b": 2, "a": 1})
    b = text.compact_json_dumps({"a": 1, "b": 2})
    assert a == b == '{"a":1,"b":2}'


def test_compact_json_dumps_handles_non_serializable_via_str():
    from pathlib import PurePosixPath

    out = text.compact_json_dumps({"p": PurePosixPath("/x/y")})
    assert out == '{"p":"/x/y"}'


def test_safe_part_falls_back_when_empty():
    assert text.safe_part("") == "_none"
    assert text.safe_part(None) == "_none"
    assert text.safe_part(None, fallback="missing") == "missing"


def test_safe_part_strips_disallowed_characters():
    assert text.safe_part("hello world!") == "hello_world_"
    assert text.safe_part("token=abc/def") == "token=abc_def"


def test_safe_part_truncates_to_128_chars():
    assert len(text.safe_part("a" * 500)) == 128


def test_short_hash_is_stable_and_short():
    h = text.short_hash("hello")
    assert len(h) == 16
    assert h == text.short_hash("hello")
    assert h != text.short_hash("hellp")
