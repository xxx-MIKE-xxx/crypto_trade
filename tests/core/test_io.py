"""Tests for :mod:`crypto_trade.core.io`."""

from __future__ import annotations

import json
from pathlib import Path

from crypto_trade.core import io as cio


def test_append_jsonl_and_iter_roundtrip(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    payloads = [{"a": 1}, {"b": [1, 2, 3]}, {"c": "x"}]
    for obj in payloads:
        cio.append_jsonl(path, obj)
    assert list(cio.iter_jsonl(path)) == payloads


def test_iter_jsonl_tolerates_blank_and_malformed_lines(tmp_path: Path):
    path = tmp_path / "events.jsonl"
    path.write_text('{"a":1}\n\n{not json}\n{"b":2}\n', encoding="utf-8")
    assert list(cio.iter_jsonl(path)) == [{"a": 1}, {"b": 2}]


def test_iter_jsonl_missing_file_returns_empty(tmp_path: Path):
    assert list(cio.iter_jsonl(tmp_path / "missing.jsonl")) == []


def test_save_json_is_atomic(tmp_path: Path):
    path = tmp_path / "out.json"
    cio.save_json(path, {"x": 1, "y": [2, 3]})
    assert json.loads(path.read_text(encoding="utf-8")) == {"x": 1, "y": [2, 3]}
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_append_csv_writes_header_once(tmp_path: Path):
    path = tmp_path / "rows.csv"
    cio.append_csv(path, {"a": 1, "b": 2}, ["a", "b"])
    cio.append_csv(path, {"a": 3, "b": 4}, ["a", "b"])
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "a,b"
    assert lines[1] == "1,2"
    assert lines[2] == "3,4"


def test_read_csv_col_collects_unique_values(tmp_path: Path):
    path = tmp_path / "mints.csv"
    path.write_text("mint,note\nA,first\nB,second\nA,dup\n,empty\n", encoding="utf-8")
    assert cio.read_csv_col(path, "mint") == {"A", "B"}


def test_read_csv_col_missing_file_returns_empty(tmp_path: Path):
    assert cio.read_csv_col(tmp_path / "missing.csv", "mint") == set()


def test_chunked_yields_expected_chunks():
    items = ["a", "b", "c", "d", "e"]
    assert list(cio.chunked(items, 2)) == [["a", "b"], ["c", "d"], ["e"]]
    assert list(cio.chunked(items, 10)) == [items]
    assert list(cio.chunked([], 2)) == []
