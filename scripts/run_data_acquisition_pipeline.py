"""Compatibility shim for acquisition-pipeline smoke tests."""

from __future__ import annotations

import os
import shlex
import sys
from pathlib import Path
from runpy import run_module


def _load_dotenv_fallback(repo_root: Path) -> None:
    """Load .env without requiring python-dotenv.

    Does not override already-exported shell variables.
    """
    env_path = repo_root / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key:
            os.environ.setdefault(key, value)


def _quote(path: Path | str) -> str:
    # shlex.quote is POSIX-oriented; for Windows subprocess strings,
    # simple double-quoting is safer when paths may contain spaces.
    text = str(path)
    if " " in text or "\t" in text:
        return f'"{text}"'
    return text


def _set_smoke_test_defaults() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    _load_dotenv_fallback(repo_root)

    python_exe = _quote(sys.executable)
    capture_script = _quote(repo_root / "scripts" / "capture_coin_1h.py")

    os.environ.setdefault(
        "PIPELINE_CAPTURE_CMD_TEMPLATE",
        (
            f"{python_exe} {capture_script} "
            "--mint {mint} "
            "--duration-seconds {duration_sec} "
            "--out {onchain_dir}"
        ),
    )

    # Smoke-test mode: validate acquisition only.
    # Override these env vars when you want full enrichment.
    for env_name in (
        "PIPELINE_WEBSITE_CMD_TEMPLATE",
        "PIPELINE_RISK_REPORT_CMD_TEMPLATE",
        "PIPELINE_TOKEN_RISK_CMD_TEMPLATE",
        "PIPELINE_DEX_REPORT_CMD_TEMPLATE",
        "PIPELINE_TELEGRAM_CMD_TEMPLATE",
    ):
        os.environ.setdefault(env_name, f"{python_exe} -c \"pass\"")


if __name__ == "__main__":
    _set_smoke_test_defaults()
    run_module("crypto_trade.pipeline", run_name="__main__")