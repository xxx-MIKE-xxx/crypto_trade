"""Tests for :mod:`crypto_trade.core.env`."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from crypto_trade.core import env


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("CT_TEST_KEY", raising=False)
    yield


def test_load_env_loads_explicit_path(tmp_path: Path, monkeypatch):
    target = tmp_path / "explicit.env"
    target.write_text("CT_TEST_KEY=from_explicit\n", encoding="utf-8")
    loaded = env.load_env(target)
    assert loaded == target
    assert os.environ.get("CT_TEST_KEY") == "from_explicit"


def test_load_env_returns_none_when_explicit_path_missing(tmp_path: Path):
    assert env.load_env(tmp_path / "missing.env") is None


def test_load_env_uses_first_existing_candidate(tmp_path: Path):
    a = tmp_path / "a.env"
    b = tmp_path / "b.env"
    b.write_text("CT_TEST_KEY=from_b\n", encoding="utf-8")
    loaded = env.load_env(candidates=[a, b])
    assert loaded == b
    assert os.environ.get("CT_TEST_KEY") == "from_b"


def test_load_env_override_replaces_existing(tmp_path: Path, monkeypatch):
    target = tmp_path / "override.env"
    target.write_text("CT_TEST_KEY=new\n", encoding="utf-8")
    monkeypatch.setenv("CT_TEST_KEY", "old")
    env.load_env(target, override=True)
    assert os.environ["CT_TEST_KEY"] == "new"


def test_get_env_returns_default(monkeypatch):
    assert env.get_env("CT_TEST_KEY", default="fallback") == "fallback"
    monkeypatch.setenv("CT_TEST_KEY", "actual")
    assert env.get_env("CT_TEST_KEY", default="fallback") == "actual"
