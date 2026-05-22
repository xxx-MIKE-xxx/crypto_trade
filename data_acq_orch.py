#!/usr/bin/env python3
"""
meme_coin_pipeline_orchestrator.py

Production orchestrator for a PumpPortal -> migrated meme coin acquisition pipeline.

It tails the PumpPortal JSONL file, accepts only migrations whose mint was seen in a
previous new-token/mint event, and runs the downstream scripts for at most N coins
concurrently.

When run_pumpportal is enabled, pumpportal_ws.py is supervised and restarted on
unexpected exits (see pumpportal_auto_restart / pumpportal_restart_delay_seconds /
pumpportal_jsonl_stale_restart_seconds).

Per migrated coin, Solana 1h capture starts immediately while the security report
is fetched. The red-flag filter then decides whether to stop and clean up that
coin, or let DexScreener, website grading, Telegram, and post-migration Dex label
tracking continue without waiting for capture to finish.

Post-migration Dex label sampling starts only after the hard-stop filter passes.

Analytics failures are logged but do not cancel an in-flight capture. Capture failure
still fails the coin pipeline.

The orchestrator never uses shell=True. All script calls are executed as argv lists.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import hashlib
import json
import logging
import os
import random
import signal
import shlex
import shutil
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, TextIO
from urllib.parse import urlparse

import red_flag_filter


LOGGER_NAME = "meme_coin_orchestrator"
DEFAULT_ORCH_CONFIG_PATH = Path("config/data_acq_orch.json")
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


# -----------------------------
# Logging
# -----------------------------


class JsonFormatter(logging.Formatter):
    """Small structured JSON formatter with support for Logger.extra fields."""

    _reserved = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key not in self._reserved and not key.startswith("_"):
                payload[key] = make_json_safe(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def format_stage_map(stage_map: Mapping[str, Any]) -> str:
    if not stage_map:
        return "-"
    parts = []
    for mint, stage in sorted(stage_map.items()):
        short_mint = short_mint_label(str(mint))
        parts.append(f"{short_mint}:{stage}")
    return ", ".join(parts)


def short_mint_label(mint: str) -> str:
    text = mint.strip()
    return text[:6] if text else "?"


def truncate_line(text: str, columns: int) -> str:
    if columns <= 0 or len(text) <= columns:
        return text
    if columns <= 1:
        return text[:columns]
    return text[: columns - 1] + "…"


def format_elapsed_badge(started_at: float) -> str:
    elapsed = max(0.0, time.time() - started_at)
    if elapsed >= 3600:
        return f"{elapsed / 3600:.0f}h"
    if elapsed >= 60:
        return f"{elapsed / 60:.0f}m"
    return f"{elapsed:.0f}s"


@dataclass
class CoinConsoleState:
    mint: str
    pipeline_stage: str = ""
    running_stage: str = ""
    postdex_samples: int = 0
    next_interval_s: Optional[float] = None
    last_elapsed_s: Optional[float] = None


@dataclass
class ConsoleDashboardState:
    started_at: float = field(default_factory=time.time)
    spinner_tick: int = 0
    rows_read: int = 0
    currently_tracked_mints: int = 0
    migrated_coins_detected: int = 0
    eligible_migrations: int = 0
    active_coin_pipelines: int = 0
    post_migration_dex_tracked_coins: int = 0
    skipped_due_to_concurrency: int = 0
    completed_coin_analyses: int = 0
    failed_coin_analyses: int = 0
    red_flag_rejected_coin_analyses: int = 0
    max_concurrent_coins: int = 0
    current_stage: dict[str, str] = field(default_factory=dict)
    coins: dict[str, CoinConsoleState] = field(default_factory=dict)
    last_activity: str = "starting…"


def format_coin_badges(state: ConsoleDashboardState) -> str:
    mints = sorted(set(state.current_stage) | set(state.coins))
    if not mints:
        return "-"
    parts: list[str] = []
    for mint in mints:
        short = short_mint_label(mint)
        coin_state = state.coins.get(mint)
        if coin_state and coin_state.postdex_samples > 0:
            next_part = ""
            if coin_state.next_interval_s is not None:
                next_part = f"~{coin_state.next_interval_s:g}s"
            parts.append(f"{short}:dex#{coin_state.postdex_samples}{next_part}")
            continue
        if coin_state and coin_state.running_stage:
            parts.append(f"{short}:{coin_state.running_stage}…")
            continue
        stage = state.current_stage.get(mint) or (coin_state.pipeline_stage if coin_state else "")
        if stage:
            parts.append(f"{short}:{stage}")
        else:
            parts.append(short)
    return " ".join(parts)


def format_metrics_line(state: ConsoleDashboardState, columns: int) -> str:
    spinner = "|/-\\"[state.spinner_tick % 4]
    elapsed = format_elapsed_badge(state.started_at)
    max_coins = state.max_concurrent_coins or "?"
    text = (
        f"[{spinner}] {elapsed} "
        f"rows={state.rows_read} "
        f"mig={state.migrated_coins_detected} "
        f"elig={state.eligible_migrations} "
        f"act={state.active_coin_pipelines}/{max_coins} "
        f"postdex={state.post_migration_dex_tracked_coins} "
        f"skip={state.skipped_due_to_concurrency} "
        f"done={state.completed_coin_analyses} "
        f"fail={state.failed_coin_analyses} "
        f"redflag={state.red_flag_rejected_coin_analyses} "
        f"| {format_coin_badges(state)}"
    )
    return truncate_line(text, columns)


def format_activity_line(state: ConsoleDashboardState, columns: int) -> str:
    activity = state.last_activity.strip() or "…"
    return truncate_line(f"> {activity}", columns)


class PrettyConsoleHandler(logging.StreamHandler):
    """Human-friendly console logs: 2-line TTY dashboard or verbose line-per-event mode."""

    suppressed_events = {
        "mint_recorded",
        "subprocess_output",
        "status_event_write_failed",
        "state_loaded",
        "subprocess_dry_run",
    }

    silent_dashboard_events = {
        "subprocess_start",
        "subprocess_complete",
        "post_migration_dex_snapshot",
        "dexscreener_retry_wait",
        "dexscreener_attempt_result",
    }

    banner_dashboard_events = {
        "scan_started",
        "orchestrator_stopped",
        "coin_pipeline_red_flag_rejected",
        "red_flag_audit_saved",
    }

    def __init__(
        self,
        stream: Optional[TextIO] = None,
        *,
        console_display: str = "dashboard",
    ):
        super().__init__(stream or sys.stdout)
        self.console_display = console_display
        self.state = ConsoleDashboardState()
        self._dashboard_drawn = False
        self._last_dashboard_width = 0
        self._last_progress_text = ""
        self._last_progress_width = 0
        self._progress_active = False
        self._supports_live_dashboard = bool(getattr(self.stream, "isatty", lambda: False)())

    @property
    def _use_dashboard(self) -> bool:
        return self.console_display == "dashboard" and self._supports_live_dashboard

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event = str(getattr(record, "event", "") or "")
            if event in self.suppressed_events:
                return

            if self._use_dashboard:
                self._emit_dashboard(record, event)
                return

            self._emit_verbose(record, event)
        except Exception:
            self.handleError(record)

    def _emit_verbose(self, record: logging.LogRecord, event: str) -> None:
        rendered = self.render_record(record, event)
        if rendered is None:
            return

        if event == "orchestrator_metrics" and self._supports_live_dashboard:
            self._write_single_progress(rendered)
            return

        self._clear_single_progress_line()
        self.stream.write(rendered + self.terminator)
        self.flush()

    def _emit_dashboard(self, record: logging.LogRecord, event: str) -> None:
        self._update_dashboard_state(record, event)

        if event in self.silent_dashboard_events or event == "orchestrator_metrics":
            self._refresh_dashboard()
            return

        rendered = self.render_record(record, event)
        if rendered is None:
            self._refresh_dashboard()
            return

        if event in self.banner_dashboard_events or record.levelno >= logging.ERROR:
            self._print_banner_line(rendered)
            return

        self.state.last_activity = rendered
        self._refresh_dashboard()

    def _ensure_coin(self, mint: str) -> CoinConsoleState:
        if mint not in self.state.coins:
            self.state.coins[mint] = CoinConsoleState(mint=mint)
        return self.state.coins[mint]

    def _update_dashboard_state(self, record: logging.LogRecord, event: str) -> None:
        self.state.spinner_tick += 1
        coin = str(getattr(record, "coin", "") or getattr(record, "mint", "") or "")
        stage = str(getattr(record, "stage", "") or "")

        if event == "scan_started":
            self.state.max_concurrent_coins = int(getattr(record, "max_concurrent_coins", 0) or 0)
        elif event == "orchestrator_metrics":
            self.state.rows_read = int(getattr(record, "rows_read", 0) or 0)
            self.state.currently_tracked_mints = int(
                getattr(record, "currently_tracked_mints", 0) or 0
            )
            self.state.migrated_coins_detected = int(
                getattr(record, "migrated_coins_detected", 0) or 0
            )
            self.state.eligible_migrations = int(getattr(record, "eligible_migrations", 0) or 0)
            self.state.active_coin_pipelines = int(
                getattr(record, "active_coin_pipelines", 0) or 0
            )
            self.state.post_migration_dex_tracked_coins = int(
                getattr(record, "post_migration_dex_tracked_coins", 0) or 0
            )
            self.state.skipped_due_to_concurrency = int(
                getattr(record, "skipped_due_to_concurrency", 0) or 0
            )
            self.state.completed_coin_analyses = int(
                getattr(record, "completed_coin_analyses", 0) or 0
            )
            self.state.failed_coin_analyses = int(
                getattr(record, "failed_coin_analyses", 0) or 0
            )
            self.state.red_flag_rejected_coin_analyses = int(
                getattr(record, "red_flag_rejected_coin_analyses", 0) or 0
            )
            stage_map = getattr(record, "current_stage", {}) or {}
            if isinstance(stage_map, Mapping):
                self.state.current_stage = {str(k): str(v) for k, v in stage_map.items()}
                for mint, pipeline_stage in self.state.current_stage.items():
                    self._ensure_coin(mint).pipeline_stage = pipeline_stage
        elif event == "subprocess_start" and coin:
            self._ensure_coin(coin).running_stage = stage
        elif event == "subprocess_complete" and coin:
            coin_state = self._ensure_coin(coin)
            coin_state.running_stage = ""
            elapsed = getattr(record, "elapsed_seconds", None)
            if elapsed is not None:
                coin_state.last_elapsed_s = float(elapsed)
        elif event == "post_migration_dex_snapshot" and coin:
            coin_state = self._ensure_coin(coin)
            coin_state.postdex_samples = int(getattr(record, "samples", 0) or 0)
            next_interval = getattr(record, "next_interval_seconds", None)
            if next_interval is not None:
                coin_state.next_interval_s = float(next_interval)
        elif event == "post_migration_dex_tracking_started" and coin:
            coin_state = self._ensure_coin(coin)
            coin_state.postdex_samples = 0
        elif event == "post_migration_dex_tracking_completed" and coin:
            coin_state = self._ensure_coin(coin)
            coin_state.postdex_samples = int(getattr(record, "samples", 0) or 0)
            coin_state.running_stage = ""
        elif event == "coin_pipeline_started" and coin:
            self._ensure_coin(coin)
        elif event == "coin_pipeline_red_flag_rejected":
            self.state.red_flag_rejected_coin_analyses = int(
                getattr(record, "red_flag_rejected_coin_analyses", 0)
                or self.state.red_flag_rejected_coin_analyses
            )
            if coin:
                coin_state = self.state.coins.get(coin)
                if coin_state:
                    coin_state.running_stage = ""
        elif event in {"coin_pipeline_completed", "coin_pipeline_failed", "coin_pipeline_cancelled"} and coin:
            coin_state = self.state.coins.get(coin)
            if coin_state:
                coin_state.running_stage = ""

    def _refresh_dashboard(self) -> None:
        columns = shutil.get_terminal_size((100, 20)).columns
        line1 = format_metrics_line(self.state, columns)
        line2 = format_activity_line(self.state, columns)
        self._write_dashboard(line1, line2)

    def _print_banner_line(self, text: str) -> None:
        if self._dashboard_drawn:
            self.stream.write("\x1b[2A\x1b[2K\r\x1b[2K\r")
            self._dashboard_drawn = False
            self._last_dashboard_width = 0
        self.stream.write(text + self.terminator)
        self.flush()
        self.state.last_activity = text
        self._refresh_dashboard()

    def render_record(self, record: logging.LogRecord, event: str) -> Optional[str]:
        if event in self.suppressed_events:
            return None

        coin = str(getattr(record, "coin", "") or "")
        stage = str(getattr(record, "stage", "") or "")
        level = record.levelname

        if event == "scan_started":
            return f"Watching {getattr(record, 'pumpportal_jsonl_path', '?')}  concurrency={getattr(record, 'max_concurrent_coins', '?')}"
        if event == "pumpportal_jsonl_waiting":
            return (
                f"Waiting for PumpPortal JSONL: {getattr(record, 'path', '?')} "
                f"(timeout={getattr(record, 'timeout_seconds', '?')}s)"
            )
        if event == "pumpportal_jsonl_ready":
            return f"PumpPortal JSONL ready: {getattr(record, 'path', '?')}"
        if event == "pumpportal_tail_at_eof":
            return (
                f"Tail at end of {getattr(record, 'path', '?')} "
                f"(offset={getattr(record, 'file_offset', '?')}/{getattr(record, 'file_size', '?')}); "
                "waiting for new migrations only"
            )
        if event == "pumpportal_process_exited":
            return (
                f"PumpPortal writer exited (code={getattr(record, 'return_code', '?')}); "
                f"restarting in {getattr(record, 'restart_delay_seconds', '?')}s "
                f"(restart #{getattr(record, 'restart_count', '?')})"
            )
        if event == "pumpportal_process_exited_no_restart":
            return (
                f"PumpPortal writer exited (code={getattr(record, 'return_code', '?')}); "
                "auto-restart disabled"
            )
        if event == "orchestrator_metrics":
            return (
                f"rows={getattr(record, 'rows_read', 0)} "
                f"tracked={getattr(record, 'currently_tracked_mints', 0)} "
                f"migrations={getattr(record, 'migrated_coins_detected', 0)} "
                f"eligible={getattr(record, 'eligible_migrations', 0)} "
                f"active={getattr(record, 'active_coin_pipelines', 0)} "
                f"postdex={getattr(record, 'post_migration_dex_tracked_coins', 0)} "
                f"skipped={getattr(record, 'skipped_due_to_concurrency', 0)} "
                f"done={getattr(record, 'completed_coin_analyses', 0)} "
                f"failed={getattr(record, 'failed_coin_analyses', 0)} "
                f"redflag={getattr(record, 'red_flag_rejected_coin_analyses', 0)} "
                f"stages={format_stage_map(getattr(record, 'current_stage', {}) or {})}"
            )
        if event == "migration_rejected_without_prior_mint":
            return f"Skipping migration for unseen mint {coin}"
        if event == "migration_skipped_due_to_concurrency":
            return (
                f"Skipped {coin}  capacity full "
                f"active={getattr(record, 'active_coin_pipelines', '?')}/"
                f"{getattr(record, 'max_concurrent_coins', '?')}"
            )
        if event == "coin_pipeline_started":
            return f"Started {coin}  pairs={len(getattr(record, 'pair_addresses', []) or [])}"
        if event == "subprocess_start":
            return f"[{coin}] {stage} started"
        if event == "subprocess_complete":
            return f"[{coin}] {stage} finished in {getattr(record, 'elapsed_seconds', '?')}s"
        if event == "dexscreener_dry_run_available":
            return (
                f"[{coin}] DexScreener dry-run "
                f"website={getattr(record, 'website_url', None) or '-'}"
            )
        if event == "subprocess_timeout":
            return f"[{coin}] {stage} timed out after {getattr(record, 'timeout_seconds', '?')}s"
        if event == "dexscreener_attempt":
            return f"[{coin}] DexScreener attempt {getattr(record, 'attempt', '?')}/{getattr(record, 'max_attempts', '?')}"
        if event == "dexscreener_retry_wait":
            return f"[{coin}] waiting {getattr(record, 'wait_seconds', '?')}s before next DexScreener attempt"
        if event == "dexscreener_attempt_result":
            return (
                f"[{coin}] DexScreener available={getattr(record, 'available', '?')} "
                f"website={getattr(record, 'website_url', None) or '-'} "
                f"telegram={getattr(record, 'telegram_url', None) or '-'} "
                f"x={getattr(record, 'x_url', None) or '-'}"
            )
        if event == "post_migration_dex_tracking_started":
            return (
                f"[{coin}] post-migration Dex tracking started "
                f"active={getattr(record, 'active_tracks', '?')}/{getattr(record, 'max_tracks', '?')}"
            )
        if event == "post_migration_dex_tracking_skipped_capacity":
            return f"[{coin}] post-migration Dex tracking skipped  tracker capacity full"
        if event == "post_migration_dex_snapshot":
            return (
                f"[{coin}] post-migration Dex snapshot "
                f"samples={getattr(record, 'samples', '?')} "
                f"next~{getattr(record, 'next_interval_seconds', '?')}s"
            )
        if event == "post_migration_dex_tracking_completed":
            return f"[{coin}] post-migration Dex tracking completed"
        if event == "website_grader_skipped_no_website":
            return f"[{coin}] website grader skipped  no website found"
        if event == "telegram_info_skipped_no_telegram":
            return f"[{coin}] telegram stage skipped  no telegram found"
        if event == "coin_pipeline_completed":
            return f"Completed {coin}"
        if event == "coin_pipeline_failed":
            return f"Failed {coin}: {getattr(record, 'error', record.getMessage())}"
        if event == "coin_pipeline_cancelled":
            return f"Cancelled {coin}"
        if event == "coin_pipeline_red_flag_rejected":
            return (
                f"Red-flag rejected {coin} "
                f"(total={getattr(record, 'red_flag_rejected_coin_analyses', '?')})"
            )
        if event == "red_flag_audit_saved":
            return f"Red-flag audit saved for {coin}: {getattr(record, 'audit_dir', '?')}"
        if event == "coin_analytics_failed":
            return f"[{coin}] analytics failed (capture may continue): {getattr(record, 'error', record.getMessage())}"
        if event == "orchestrator_fatal_error":
            return f"ERR: orchestrator_fatal_error {getattr(record, 'error', record.getMessage())}"
        if event == "orchestrator_stopped":
            return (
                f"Stopped  rows={getattr(record, 'rows_read', 0)} "
                f"eligible={getattr(record, 'eligible_migrations', 0)} "
                f"completed={getattr(record, 'completed_coin_analyses', 0)} "
                f"failed={getattr(record, 'failed_coin_analyses', 0)} "
                f"redflag={getattr(record, 'red_flag_rejected_coin_analyses', 0)}"
            )

        prefix = "WARN" if level == "WARNING" else ("ERR" if level == "ERROR" else level)
        return f"{prefix}: {record.getMessage()}"

    def _write_dashboard(self, line1: str, line2: str) -> None:
        width = max(len(line1), len(line2))
        if not self._dashboard_drawn:
            self.stream.write(line1 + "\n" + line2)
            self._dashboard_drawn = True
        else:
            pad1 = line1
            pad2 = line2
            if self._last_dashboard_width > len(line1):
                pad1 = line1 + (" " * (self._last_dashboard_width - len(line1)))
            if self._last_dashboard_width > len(line2):
                pad2 = line2 + (" " * (self._last_dashboard_width - len(line2)))
            self.stream.write("\x1b[2A\x1b[2K\r" + pad1 + "\n\x1b[2K\r" + pad2)
        self.stream.flush()
        self._last_dashboard_width = max(self._last_dashboard_width, width)

    def _write_single_progress(self, text: str) -> None:
        if not hasattr(self, "_last_progress_text"):
            self._last_progress_text = ""
            self._last_progress_width = 0
            self._progress_active = False
        padded = text
        if self._last_progress_width > len(text):
            padded = text + (" " * (self._last_progress_width - len(text)))
        self.stream.write("\r" + padded)
        self.flush()
        self._progress_active = True
        self._last_progress_text = text
        self._last_progress_width = max(self._last_progress_width, len(text))

    def _clear_single_progress_line(self) -> None:
        if not getattr(self, "_progress_active", False):
            return
        self.stream.write("\r" + (" " * getattr(self, "_last_progress_width", 0)) + "\r")
        self.flush()
        self._progress_active = False


def enable_ansi_stdout() -> None:
    if os.name != "nt":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            enable_vt = 0x0004
            kernel32.SetConsoleMode(handle, mode.value | enable_vt)
    except Exception:
        return


def setup_logging(
    level: str,
    console_format: str = "pretty",
    *,
    console_display: str = "dashboard",
) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    if console_format == "json":
        handler: logging.Handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
    else:
        if console_display == "dashboard":
            enable_ansi_stdout()
        handler = PrettyConsoleHandler(
            sys.stdout,
            console_display=console_display if console_format == "pretty" else "verbose",
        )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def make_json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, Mapping):
        return {str(k): make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [make_json_safe(v) for v in value]
    return value


# -----------------------------
# Configuration
# -----------------------------


PUMPPORTAL_RAW_SUBDIR = Path("data") / "raw" / "migrations"
SECURITY_REPORT_FILENAME = "security_report"
RED_FLAG_REJECTIONS_INDEX = "rejections.jsonl"


def default_pumpportal_jsonl() -> Path:
    """Same layout as pumpportal_ws.py default_raw_jsonl_path (UTC date file)."""
    day = datetime.now(timezone.utc).date().isoformat()
    return PUMPPORTAL_RAW_SUBDIR / f"{day}.jsonl"


def resolve_pumpportal_jsonl_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def subprocess_pumpportal_display(display: str) -> str:
    """Bar mode uses TTY cursor controls; the orchestrator captures PumpPortal stdout via pipe."""
    normalized = (display or "metrics").strip().lower()
    if normalized == "bar":
        return "metrics"
    return normalized


def load_json_config_defaults(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = load_json_file(path)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a JSON object: {path}")
    return data


def coerce_argparse_defaults(defaults: Mapping[str, Any], path_keys: set[str]) -> dict[str, Any]:
    coerced: dict[str, Any] = {}
    for key, value in defaults.items():
        if value is None:
            coerced[key] = None
        elif key in path_keys:
            coerced[key] = Path(value)
        else:
            coerced[key] = value
    return coerced


def b58decode(value: str) -> bytes:
    num = 0
    for char in value:
        num *= 58
        idx = BASE58_ALPHABET.find(char)
        if idx < 0:
            raise ValueError("invalid base58 character")
        num += idx

    raw = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
    pad = len(value) - len(value.lstrip("1"))
    return b"\x00" * pad + raw


def is_valid_solana_address(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    try:
        return len(b58decode(text)) == 32
    except Exception:
        return False


@dataclass(frozen=True)
class OrchestratorConfig:
    pumpportal_jsonl_path: Path
    pumpportal_script_path: Path
    capture_script_path: Path
    risk_report_script_path: Path
    dexscreener_script_path: Path
    website_grader_script_path: Path
    telegram_info_script_path: Path

    python_executable: str = sys.executable
    state_path: Path = Path("data/raw/orchestrator/state.json")

    run_pumpportal: bool = True
    pumpportal_duration_seconds: int = 0
    pumpportal_display: str = "metrics"
    pumpportal_jsonl_wait_seconds: float = 120.0
    pumpportal_auto_restart: bool = True
    pumpportal_restart_delay_seconds: float = 5.0
    pumpportal_jsonl_stale_restart_seconds: float = 180.0
    pumpportal_command_template: Optional[str] = None

    max_concurrent_coins: int = 2
    scan_poll_seconds: float = 2.0
    metrics_log_seconds: float = 30.0
    start_at_end: bool = False
    run_once: bool = False
    dry_run: bool = False
    dry_run_dex_website: Optional[str] = None
    retry_failed: bool = False

    status_jsonl_path: Optional[Path] = Path("data/raw/orchestrator/status_events.jsonl")
    stage_log_root: Optional[Path] = Path("data/raw/orchestrator/stage_logs")

    capture_command_template: Optional[str] = None
    risk_command_template: Optional[str] = None
    dexscreener_command_template: Optional[str] = None
    website_command_template: Optional[str] = None
    telegram_command_template: Optional[str] = None

    capture_duration_seconds: int = 3600
    capture_timeout_grace_seconds: int = 900
    capture_out_root: Path = Path("data/raw/onchain")
    capture_network_sample_seconds: float = 60.0
    capture_network_sample_fee_addresses: bool = False

    risk_analysis_root: Path = Path("data/raw/analytics")
    risk_export_root: Path = Path("data/raw/orchestrator/risk_reports")
    risk_http_timeout_seconds: int = 15
    risk_subprocess_timeout_seconds: int = 240
    red_flag_filter_enabled: bool = True
    red_flag_config_path: Path = Path("config/red_flags.json")
    red_flag_delete_rejected_capture_data: bool = True
    red_flag_audit_root: Path = Path("data/raw/red_flags")
    red_flag_save_audit_artifacts: bool = True

    dexscreener_out_root: Path = Path("data/raw/analytics")
    dexscreener_chain: str = "solana"
    dexscreener_initial_wait_seconds: float = 60.0
    dexscreener_max_wait_seconds: float = 300.0
    dexscreener_backoff_multiplier: float = 1.35
    dexscreener_max_attempts: int = 30
    dexscreener_script_sleep_seconds: float = 1.1
    dexscreener_subprocess_timeout_seconds: int = 180

    max_post_migration_tracked_coins: int = 6
    post_migration_dex_tracking_hours: float = 24.0
    post_migration_dex_requests_per_minute: float = 50.0
    post_migration_dex_requests_per_snapshot: int = 1
    post_migration_dex_endpoint_profile: str = "market"
    post_migration_dex_request_sleep_seconds: float = 0.0
    post_migration_dex_out_root: Path = Path("data/raw/onchain")
    post_migration_dex_timestamped_raw: bool = True

    website_output_root: Path = Path("data/raw/analytics")
    website_subprocess_timeout_seconds: int = 900
    telegram_subprocess_timeout_seconds: int = 900
    telegram_join: bool = True

    pumpportal_extra_args: tuple[str, ...] = ()
    capture_extra_args: tuple[str, ...] = ()
    risk_extra_args: tuple[str, ...] = ()
    dexscreener_extra_args: tuple[str, ...] = ()
    website_extra_args: tuple[str, ...] = ()
    telegram_extra_args: tuple[str, ...] = ()

    require_pair_for_capture: bool = False


@dataclass
class Metrics:
    rows_read: int = 0
    invalid_json_rows: int = 0
    new_token_events: int = 0
    migrated_coins_detected: int = 0
    eligible_migrations: int = 0
    ineligible_migrations: int = 0
    duplicate_migrations: int = 0
    skipped_due_to_concurrency: int = 0
    coins_started: int = 0
    completed_coin_analyses: int = 0
    failed_coin_analyses: int = 0
    red_flag_rejected_coin_analyses: int = 0

    def to_dict(
        self,
        active_count: int,
        tracked_count: int,
        post_migration_dex_tracked_count: int = 0,
    ) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        data.update(
            {
                "active_coin_pipelines": active_count,
                "currently_tracked_mints": tracked_count,
                "post_migration_dex_tracked_coins": post_migration_dex_tracked_count,
            }
        )
        return data


@dataclass(frozen=True)
class CoinContext:
    mint: str
    pair_addresses: tuple[str, ...]
    migration_event: dict[str, Any]
    coin_name: Optional[str] = None
    symbol: Optional[str] = None


@dataclass
class PostMigrationDexTrack:
    mint: str
    pair_addresses: tuple[str, ...] = ()
    started_at_epoch: float = field(default_factory=time.time)
    end_at_epoch: float = 0.0
    next_due_at_epoch: float = field(default_factory=time.time)
    samples: int = 0
    last_sample_at_utc: Optional[str] = None
    last_error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mint": self.mint,
            "pair_addresses": list(self.pair_addresses),
            "started_at_epoch": self.started_at_epoch,
            "end_at_epoch": self.end_at_epoch,
            "next_due_at_epoch": self.next_due_at_epoch,
            "samples": self.samples,
            "last_sample_at_utc": self.last_sample_at_utc,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PostMigrationDexTrack":
        return cls(
            mint=str(data["mint"]),
            pair_addresses=tuple(map(str, data.get("pair_addresses") or ())),
            started_at_epoch=float(data.get("started_at_epoch") or time.time()),
            end_at_epoch=float(data.get("end_at_epoch") or 0.0),
            next_due_at_epoch=float(data.get("next_due_at_epoch") or time.time()),
            samples=int(data.get("samples") or 0),
            last_sample_at_utc=data.get("last_sample_at_utc"),
            last_error=data.get("last_error"),
        )


@dataclass(frozen=True)
class SubprocessResult:
    returncode: int
    elapsed_seconds: float
    command: tuple[str, ...]


@dataclass(frozen=True)
class DexScreenerResult:
    features_path: Path
    features: dict[str, Any]
    website_url: Optional[str]
    telegram_url: Optional[str] = None
    x_url: Optional[str] = None


class PipelineError(RuntimeError):
    pass


class RedFlagRejected(PipelineError):
    def __init__(self, mint: str, decision: red_flag_filter.RedFlagDecision):
        self.mint = mint
        self.decision = decision
        reasons = ",".join(str(r.get("code") or r.get("rule")) for r in decision.reject_reasons)
        super().__init__(f"red flag rejected {mint}: {reasons}")


# -----------------------------
# JSONL event extraction
# -----------------------------


MINT_FIELDS = (
    "mint",
    "tokenMint",
    "token_mint",
    "mintAddress",
    "mint_address",
    "ca",
    "contract",
    "address",
)

PAIR_FIELDS = (
    "pair",
    "pairAddress",
    "pair_address",
    "pairId",
    "pair_id",
    "pool",
    "poolAddress",
    "pool_address",
    "poolId",
    "pool_id",
    "raydiumPool",
    "raydium_pool",
    "raydiumPair",
    "raydium_pair",
    "market",
    "marketAddress",
    "market_address",
    "amm",
    "ammId",
    "amm_id",
)

NAME_FIELDS = ("name", "tokenName", "token_name", "coinName", "coin_name")
SYMBOL_FIELDS = ("symbol", "ticker", "tokenSymbol", "token_symbol")


def event_dicts(row: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    yield row

    data = row.get("data")
    if isinstance(data, Mapping):
        yield data
        for nested_key in ("token", "baseToken", "pool", "pair", "metadata"):
            nested = data.get(nested_key)
            if isinstance(nested, Mapping):
                yield nested

    for nested_key in ("token", "baseToken", "pool", "pair", "metadata"):
        nested = row.get(nested_key)
        if isinstance(nested, Mapping):
            yield nested


def first_string(row: Mapping[str, Any], keys: Sequence[str]) -> Optional[str]:
    for obj in event_dicts(row):
        for key in keys:
            value = obj.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def string_values_from_keys(row: Mapping[str, Any], keys: Sequence[str]) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()

    for obj in event_dicts(row):
        for key in keys:
            value = obj.get(key)
            candidates: list[Any]
            if isinstance(value, (list, tuple)):
                candidates = list(value)
            else:
                candidates = [value]

            for candidate in candidates:
                if isinstance(candidate, str):
                    text = candidate.strip()
                    if text and text not in seen:
                        values.append(text)
                        seen.add(text)

    return tuple(values)


def detect_event_type(row: Mapping[str, Any]) -> str:
    explicit = row.get("event_type")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()

    data = row.get("data") if isinstance(row.get("data"), Mapping) else row
    assert isinstance(data, Mapping)

    if data.get("_decode_error") is True:
        return "decode_error"

    error_text = str(data.get("errors") or data.get("error") or "").strip().lower()
    message = str(data.get("message") or "").strip().lower()

    if error_text:
        if "minimum balance" in error_text:
            return "permission_error"
        return "error"

    if message:
        if "subscribed" in message:
            return "subscription_ack"
        return "control_message"

    raw_type = (
        data.get("txType")
        or data.get("type")
        or data.get("eventType")
        or data.get("method")
        or ""
    )
    tx_type = str(raw_type).strip().lower()

    if tx_type in {"create", "new_token", "newtoken", "new-token", "mint"}:
        return "new_token"

    if tx_type in {"migrate", "migration"} or "migrat" in tx_type:
        return "migration"

    return tx_type or "unknown"


def load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(
        json.dumps(make_json_safe(payload), indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    try:
        for attempt in range(5):
            try:
                tmp.replace(path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(make_json_safe(payload), ensure_ascii=False, separators=(",", ":")))
        f.write("\n")


# -----------------------------
# Subprocess execution
# -----------------------------


def redact_arg(arg: str) -> str:
    lowered = arg.lower()
    if "api-key=" in lowered:
        prefix, _, _rest = arg.partition("api-key=")
        return prefix + "api-key=***"
    if "apikey=" in lowered:
        prefix, _, _rest = arg.partition("apikey=")
        return prefix + "apikey=***"
    return arg


def display_command(command: Sequence[str]) -> list[str]:
    return [redact_arg(str(x)) for x in command]


def build_template_command(
    template: Optional[str],
    *,
    default_command: Sequence[str],
    replacements: Mapping[str, Any],
    list_replacements: Optional[Mapping[str, Sequence[str]]] = None,
) -> list[str]:
    """Build a command from an optional shell-style template.

    Template placeholders are written as {mint}, {script}, etc. A token that exactly
    matches a list placeholder such as {pair_args} expands to multiple argv items.
    The command is always returned as an argv list and is never run through a shell.
    """
    if not template:
        return [str(x) for x in default_command]

    list_replacements = list_replacements or {}
    tokens = shlex.split(template)
    command: list[str] = []

    for token in tokens:
        expanded_list = False
        for key, values in list_replacements.items():
            if token == "{" + key + "}":
                command.extend(str(v) for v in values)
                expanded_list = True
                break
        if expanded_list:
            continue

        rendered = token
        for key, value in replacements.items():
            rendered = rendered.replace("{" + key + "}", str(value))
        command.append(rendered)

    return command


async def log_stream(
    reader: Optional[asyncio.StreamReader],
    *,
    logger: logging.Logger,
    mint: str,
    stage: str,
    stream_name: str,
    stage_log_file: Optional[TextIO] = None,
) -> None:
    if reader is None:
        return

    while True:
        line = await reader.readline()
        if not line:
            return
        text = line.decode("utf-8", errors="replace").rstrip()
        if text:
            if stage_log_file is not None:
                stage_log_file.write(f"{datetime.now(timezone.utc).isoformat()} [{stream_name}] {text}\n")
                stage_log_file.flush()

            logger.info(
                "subprocess_output",
                extra={
                    "event": "subprocess_output",
                    "coin": mint,
                    "stage": stage,
                    "stream": stream_name,
                    "line": text[:4000],
                },
            )


def subprocess_creation_flags() -> int:
    """Isolate child processes from console Ctrl+C on Windows."""
    if os.name != "nt":
        return 0
    return subprocess.CREATE_NEW_PROCESS_GROUP


def process_is_alive(pid: Optional[int]) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


async def terminate_process(proc: asyncio.subprocess.Process, logger: logging.Logger) -> None:
    if proc.returncode is not None:
        return

    try:
        if os.name != "nt":
            os.killpg(proc.pid, signal.SIGTERM)
        else:
            proc.terminate()
    except ProcessLookupError:
        return
    except Exception as exc:
        logger.warning(
            "subprocess_terminate_error",
            extra={"event": "subprocess_terminate_error", "error": repr(exc)},
        )

    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(proc.wait(), timeout=10.0)
        return

    try:
        if os.name != "nt":
            os.killpg(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except ProcessLookupError:
        return

    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(proc.wait(), timeout=5.0)


async def run_subprocess(
    command: Sequence[str],
    *,
    logger: logging.Logger,
    mint: str,
    stage: str,
    timeout_seconds: int,
    cwd: Optional[Path] = None,
    dry_run: bool = False,
    stage_log_path: Optional[Path] = None,
) -> SubprocessResult:
    started = time.monotonic()
    safe_cmd = display_command(command)

    if stage_log_path is not None:
        stage_log_path.parent.mkdir(parents=True, exist_ok=True)
        with stage_log_path.open("a", encoding="utf-8") as f:
            f.write(
                f"{datetime.now(timezone.utc).isoformat()} [command] "
                + json.dumps(safe_cmd, ensure_ascii=False)
                + "\n"
            )

    logger.info(
        "subprocess_start",
        extra={"event": "subprocess_start", "coin": mint, "stage": stage, "command": safe_cmd},
    )

    if dry_run:
        elapsed = time.monotonic() - started
        logger.info(
            "subprocess_dry_run",
            extra={
                "event": "subprocess_dry_run",
                "coin": mint,
                "stage": stage,
                "elapsed_seconds": round(elapsed, 3),
                "command": safe_cmd,
            },
        )
        if stage_log_path is not None:
            with stage_log_path.open("a", encoding="utf-8") as f:
                f.write(f"{datetime.now(timezone.utc).isoformat()} [dry_run] skipped execution\n")
        return SubprocessResult(returncode=0, elapsed_seconds=elapsed, command=tuple(command))

    stage_log_file: Optional[TextIO] = None
    if stage_log_path is not None:
        stage_log_file = stage_log_path.open("a", encoding="utf-8")

    subprocess_kwargs: dict[str, Any] = {}
    creationflags = subprocess_creation_flags()
    if creationflags:
        subprocess_kwargs["creationflags"] = creationflags
    else:
        subprocess_kwargs["start_new_session"] = True

    proc = await asyncio.create_subprocess_exec(
        *[str(arg) for arg in command],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(cwd) if cwd else None,
        **subprocess_kwargs,
    )

    stdout_task = asyncio.create_task(
        log_stream(
            proc.stdout,
            logger=logger,
            mint=mint,
            stage=stage,
            stream_name="stdout",
            stage_log_file=stage_log_file,
        )  # type: ignore[arg-type]
    )
    stderr_task = asyncio.create_task(
        log_stream(
            proc.stderr,
            logger=logger,
            mint=mint,
            stage=stage,
            stream_name="stderr",
            stage_log_file=stage_log_file,
        )  # type: ignore[arg-type]
    )

    try:
        try:
            returncode = await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError as exc:
            await terminate_process(proc, logger)
            elapsed = time.monotonic() - started
            logger.error(
                "subprocess_timeout",
                extra={
                    "event": "subprocess_timeout",
                    "coin": mint,
                    "stage": stage,
                    "elapsed_seconds": round(elapsed, 3),
                    "timeout_seconds": timeout_seconds,
                    "command": safe_cmd,
                },
            )
            raise PipelineError(f"{stage} timed out after {timeout_seconds}s") from exc
        except asyncio.CancelledError:
            await terminate_process(proc, logger)
            raise
    finally:
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        if stage_log_file is not None:
            stage_log_file.close()

    elapsed = time.monotonic() - started
    logger.info(
        "subprocess_complete",
        extra={
            "event": "subprocess_complete",
            "coin": mint,
            "stage": stage,
            "returncode": returncode,
            "elapsed_seconds": round(elapsed, 3),
            "command": safe_cmd,
        },
    )

    if returncode != 0:
        raise PipelineError(f"{stage} failed with return code {returncode}")

    return SubprocessResult(returncode=returncode, elapsed_seconds=elapsed, command=tuple(command))


# -----------------------------
# Orchestrator
# -----------------------------


class MemeCoinPipelineOrchestrator:
    def __init__(self, config: OrchestratorConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.metrics = Metrics()

        self.shutdown_event = asyncio.Event()

        self.file_offset: int = 0
        self.file_inode: Optional[int] = None
        self.seen_mints: set[str] = set()
        self.completed_mints: set[str] = set()
        self.failed_mints: set[str] = set()
        self.skipped_mints: set[str] = set()
        self.red_flag_rejected_mints: set[str] = set()
        self.active_tasks: dict[str, asyncio.Task[None]] = {}
        self.current_stage: dict[str, str] = {}
        self.post_migration_dex_tracks: dict[str, PostMigrationDexTrack] = {}
        self._pumpportal_jsonl_ready = False

        self.load_state()

    @property
    def terminal_mints(self) -> set[str]:
        if self.config.retry_failed:
            return self.completed_mints | self.skipped_mints | self.red_flag_rejected_mints
        return self.completed_mints | self.failed_mints | self.skipped_mints | self.red_flag_rejected_mints

    def active_post_migration_dex_tracks(self) -> list[PostMigrationDexTrack]:
        now_epoch = time.time()
        expired = [
            mint
            for mint, track in self.post_migration_dex_tracks.items()
            if track.end_at_epoch <= now_epoch
        ]
        for mint in expired:
            self.post_migration_dex_tracks.pop(mint, None)
        return list(self.post_migration_dex_tracks.values())

    def emit_status_event(self, event_type: str, **fields: Any) -> None:
        if self.config.status_jsonl_path is None:
            return
        try:
            append_jsonl(
                self.config.status_jsonl_path,
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "event_type": event_type,
                    **fields,
                },
            )
        except Exception as exc:
            self.logger.warning(
                "status_event_write_failed",
                extra={"event": "status_event_write_failed", "error": repr(exc)},
            )

    def stage_log_path(self, mint: str, stage: str) -> Optional[Path]:
        if self.config.stage_log_root is None:
            return None
        safe_mint = "".join(ch if ch.isalnum() or ch in "._=-" else "_" for ch in mint)[:160]
        safe_stage = "".join(ch if ch.isalnum() or ch in "._=-" else "_" for ch in stage)[:160]
        return self.config.stage_log_root / safe_mint / f"{safe_stage}.log"

    def load_state(self) -> None:
        path = self.config.state_path
        if not path.exists():
            return

        try:
            state = load_json_file(path)
        except Exception as exc:
            self.logger.warning(
                "state_load_failed",
                extra={"event": "state_load_failed", "path": str(path), "error": repr(exc)},
            )
            return

        self.file_offset = int(state.get("file_offset", 0) or 0)
        self.file_inode = state.get("file_inode")
        self.seen_mints = set(map(str, state.get("seen_mints", [])))
        self.completed_mints = set(map(str, state.get("completed_mints", [])))
        self.failed_mints = set(map(str, state.get("failed_mints", [])))
        self.skipped_mints = set(map(str, state.get("skipped_mints", [])))
        self.red_flag_rejected_mints = set(map(str, state.get("red_flag_rejected_mints", [])))
        now_epoch = time.time()
        self.post_migration_dex_tracks = {}
        for item in state.get("post_migration_dex_tracks", []) or []:
            if not isinstance(item, Mapping) or not item.get("mint"):
                continue
            with contextlib.suppress(Exception):
                track = PostMigrationDexTrack.from_dict(item)
                if track.end_at_epoch > now_epoch:
                    self.post_migration_dex_tracks[track.mint] = track

        self.logger.info(
            "state_loaded",
            extra={
                "event": "state_loaded",
                "path": str(path),
                "file_offset": self.file_offset,
                "seen_mints": len(self.seen_mints),
                "completed_mints": len(self.completed_mints),
                "failed_mints": len(self.failed_mints),
                "skipped_mints": len(self.skipped_mints),
                "red_flag_rejected_mints": len(self.red_flag_rejected_mints),
                "post_migration_dex_tracks": len(self.post_migration_dex_tracks),
            },
        )

    def save_state(self) -> None:
        payload = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "pumpportal_jsonl_path": str(self.config.pumpportal_jsonl_path),
            "file_offset": self.file_offset,
            "file_inode": self.file_inode,
            "seen_mints": sorted(self.seen_mints),
            "completed_mints": sorted(self.completed_mints),
            "failed_mints": sorted(self.failed_mints),
            "skipped_mints": sorted(self.skipped_mints),
            "red_flag_rejected_mints": sorted(self.red_flag_rejected_mints),
            "post_migration_dex_tracks": [
                track.to_dict()
                for track in sorted(self.active_post_migration_dex_tracks(), key=lambda item: item.mint)
            ],
            "metrics": self.metrics.to_dict(
                active_count=len(self.active_tasks),
                tracked_count=len(self.seen_mints),
                post_migration_dex_tracked_count=len(self.active_post_migration_dex_tracks()),
            ),
            "current_stage": dict(self.current_stage),
        }
        save_json_atomic(self.config.state_path, payload)

    def validate_script_paths(self) -> None:
        script_paths = {
            "pumpportal_script_path": self.config.pumpportal_script_path,
            "capture_script_path": self.config.capture_script_path,
            "risk_report_script_path": self.config.risk_report_script_path,
            "dexscreener_script_path": self.config.dexscreener_script_path,
            "website_grader_script_path": self.config.website_grader_script_path,
            "telegram_info_script_path": self.config.telegram_info_script_path,
        }

        missing = {name: str(path) for name, path in script_paths.items() if not path.exists()}
        if self.config.red_flag_filter_enabled and not self.config.red_flag_config_path.exists():
            missing["red_flag_config_path"] = str(self.config.red_flag_config_path)
        if missing and not self.config.dry_run:
            raise FileNotFoundError(f"Missing script path(s): {missing}")
        if missing and self.config.dry_run:
            self.logger.warning(
                "dry_run_missing_script_paths_ignored",
                extra={"event": "dry_run_missing_script_paths_ignored", "missing": missing},
            )

    def validate_pumpportal_jsonl_setup(self) -> None:
        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)

        if self.config.run_pumpportal or self.config.dry_run:
            return

        if not path.exists():
            raise FileNotFoundError(
                f"PumpPortal JSONL not found: {path}. "
                "Omit --no-pumpportal to launch pumpportal_ws.py and create it, "
                "or pass --pumpportal-jsonl with an existing file."
            )

    async def wait_for_pumpportal_jsonl(
        self,
        proc: Optional[asyncio.subprocess.Process],
    ) -> None:
        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        deadline = time.monotonic() + self.config.pumpportal_jsonl_wait_seconds
        logged_wait = False

        while time.monotonic() < deadline:
            if path.exists():
                self._pumpportal_jsonl_ready = True
                self.logger.info(
                    "pumpportal_jsonl_ready",
                    extra={"event": "pumpportal_jsonl_ready", "path": str(path)},
                )
                return

            if proc is not None and proc.returncode is not None:
                raise RuntimeError(
                    f"PumpPortal writer exited before creating {path} "
                    f"(return code {proc.returncode}). "
                    "Check stage logs under data/raw/orchestrator/stage_logs/orchestrator/."
                )

            if not logged_wait:
                self.logger.info(
                    "pumpportal_jsonl_waiting",
                    extra={
                        "event": "pumpportal_jsonl_waiting",
                        "path": str(path),
                        "timeout_seconds": self.config.pumpportal_jsonl_wait_seconds,
                    },
                )
                logged_wait = True

            await asyncio.sleep(0.25)

        raise TimeoutError(
            f"Timed out after {self.config.pumpportal_jsonl_wait_seconds:.0f}s waiting for "
            f"PumpPortal JSONL at {path}. Ensure pumpportal_ws.py can connect and write."
        )

    def apply_start_at_end_if_needed(self) -> None:
        if not self.config.start_at_end:
            return
        if self.config.state_path.exists():
            return
        self.initialize_offset_to_end()

    def mark_pumpportal_jsonl_ready_if_present(self) -> bool:
        if self._pumpportal_jsonl_ready:
            return True
        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        if not path.exists():
            return False
        self._pumpportal_jsonl_ready = True
        self.logger.info(
            "pumpportal_jsonl_ready",
            extra={"event": "pumpportal_jsonl_ready", "path": str(path)},
        )
        return True

    async def start_pumpportal_process(
        self,
    ) -> tuple[asyncio.subprocess.Process, list[asyncio.Task[None]], Optional[TextIO]]:
        raw_jsonl = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        pumpportal_display = subprocess_pumpportal_display(self.config.pumpportal_display)
        default_command = [
            self.config.python_executable,
            str(self.config.pumpportal_script_path),
            "--duration",
            str(self.config.pumpportal_duration_seconds),
            "--display",
            pumpportal_display,
            "--raw-jsonl",
            str(raw_jsonl),
        ]
        command = build_template_command(
            self.config.pumpportal_command_template,
            default_command=default_command,
            replacements={
                "python": self.config.python_executable,
                "script": self.config.pumpportal_script_path,
                "duration_seconds": self.config.pumpportal_duration_seconds,
                "display": pumpportal_display,
                "raw_jsonl": raw_jsonl,
            },
        )
        command.extend(self.config.pumpportal_extra_args)

        stage = "pumpportal_ws"
        safe_cmd = display_command(command)
        stage_log_file: Optional[TextIO] = None
        stage_log_path = self.stage_log_path("orchestrator", stage)
        if stage_log_path is not None:
            stage_log_path.parent.mkdir(parents=True, exist_ok=True)
            stage_log_file = stage_log_path.open("a", encoding="utf-8")
            stage_log_file.write(
                f"{datetime.now(timezone.utc).isoformat()} [command] "
                + json.dumps(safe_cmd, ensure_ascii=False)
                + "\n"
            )
            stage_log_file.flush()

        self.logger.info(
            "pumpportal_process_start",
            extra={"event": "pumpportal_process_start", "stage": stage, "command": safe_cmd},
        )

        subprocess_kwargs: dict[str, Any] = {}
        creationflags = subprocess_creation_flags()
        if creationflags:
            subprocess_kwargs["creationflags"] = creationflags
        else:
            subprocess_kwargs["start_new_session"] = True

        proc = await asyncio.create_subprocess_exec(
            *[str(arg) for arg in command],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **subprocess_kwargs,
        )
        tasks = [
            asyncio.create_task(
                log_stream(
                    proc.stdout,  # type: ignore[arg-type]
                    logger=self.logger,
                    mint="orchestrator",
                    stage=stage,
                    stream_name="stdout",
                    stage_log_file=stage_log_file,
                )
            ),
            asyncio.create_task(
                log_stream(
                    proc.stderr,  # type: ignore[arg-type]
                    logger=self.logger,
                    mint="orchestrator",
                    stage=stage,
                    stream_name="stderr",
                    stage_log_file=stage_log_file,
                )
            ),
        ]
        return proc, tasks, stage_log_file

    async def stop_pumpportal_worker(
        self,
        proc: Optional[asyncio.subprocess.Process],
        log_tasks: list[asyncio.Task[None]],
        log_file: Optional[TextIO],
    ) -> None:
        if proc is not None and proc.returncode is None:
            await terminate_process(proc, self.logger)
        for task in log_tasks:
            task.cancel()
        if log_tasks:
            await asyncio.gather(*log_tasks, return_exceptions=True)
        if log_file is not None:
            with contextlib.suppress(Exception):
                log_file.close()

    async def ensure_pumpportal_jsonl_ready(
        self,
        proc: Optional[asyncio.subprocess.Process],
    ) -> None:
        if self._pumpportal_jsonl_ready:
            return

        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        if path.exists():
            self._pumpportal_jsonl_ready = True
            self.logger.info(
                "pumpportal_jsonl_ready",
                extra={"event": "pumpportal_jsonl_ready", "path": str(path)},
            )
            return

        await self.wait_for_pumpportal_jsonl(proc)

    def _pumpportal_jsonl_age_seconds(self) -> Optional[float]:
        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        if not path.exists():
            return None
        return max(0.0, time.time() - path.stat().st_mtime)

    async def supervise_pumpportal(self) -> None:
        """Keep pumpportal_ws.py running; restart after unexpected exits."""
        restarts = 0
        stale_threshold = max(0.0, self.config.pumpportal_jsonl_stale_restart_seconds)

        while not self.shutdown_event.is_set():
            proc, log_tasks, log_file = await self.start_pumpportal_process()
            exit_code: Optional[int] = None
            startup_error: Optional[BaseException] = None
            wait_task = asyncio.create_task(proc.wait())

            try:
                await self.ensure_pumpportal_jsonl_ready(proc)

                while not self.shutdown_event.is_set():
                    if wait_task.done():
                        exit_code = wait_task.result()
                        break

                    if stale_threshold > 0:
                        jsonl_age = self._pumpportal_jsonl_age_seconds()
                        if jsonl_age is not None and jsonl_age >= stale_threshold:
                            if not process_is_alive(proc.pid):
                                self.logger.warning(
                                    "pumpportal_process_dead_stale_jsonl",
                                    extra={
                                        "event": "pumpportal_process_dead_stale_jsonl",
                                        "jsonl_age_seconds": round(jsonl_age, 1),
                                        "pid": proc.pid,
                                    },
                                )
                                self.emit_status_event(
                                    "pumpportal_process_dead_stale_jsonl",
                                    jsonl_age_seconds=round(jsonl_age, 1),
                                    pid=proc.pid,
                                )
                                await terminate_process(proc, self.logger)
                                with contextlib.suppress(asyncio.TimeoutError):
                                    exit_code = await asyncio.wait_for(wait_task, timeout=10.0)
                                if exit_code is None:
                                    exit_code = proc.returncode if proc.returncode is not None else -1
                                break

                    await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                startup_error = exc
                if wait_task.done():
                    exit_code = wait_task.result()
                else:
                    exit_code = proc.returncode
            finally:
                if not wait_task.done():
                    wait_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await wait_task
                await self.stop_pumpportal_worker(proc, log_tasks, log_file)
                await asyncio.sleep(0.5)

            if self.shutdown_event.is_set():
                return

            if exit_code is None and startup_error is None:
                continue

            if not self.config.pumpportal_auto_restart:
                if startup_error is not None:
                    raise startup_error
                self.logger.error(
                    "pumpportal_process_exited_no_restart",
                    extra={
                        "event": "pumpportal_process_exited_no_restart",
                        "return_code": exit_code,
                    },
                )
                self.emit_status_event(
                    "pumpportal_process_exited_no_restart",
                    return_code=exit_code,
                )
                return

            restarts += 1
            delay = max(0.0, self.config.pumpportal_restart_delay_seconds)
            if exit_code == 3221225794:
                delay = max(delay, 10.0)
                self.logger.error(
                    "pumpportal_windows_crash",
                    extra={
                        "event": "pumpportal_windows_crash",
                        "return_code": exit_code,
                        "restart_count": restarts,
                        "hint": (
                            "PumpPortal subprocess crashed (Windows 0xC0000005). "
                            "Stop other orchestrator or pumpportal_ws.py instances, then retry. "
                            "You can also run pumpportal_ws.py in a separate terminal and start "
                            "the orchestrator with --no-pumpportal."
                        ),
                    },
                )
            if startup_error is not None:
                self.logger.warning(
                    "pumpportal_startup_failed",
                    extra={
                        "event": "pumpportal_startup_failed",
                        "error": repr(startup_error),
                        "return_code": exit_code,
                        "restart_count": restarts,
                        "restart_delay_seconds": delay,
                    },
                )
            else:
                self.logger.warning(
                    "pumpportal_process_exited",
                    extra={
                        "event": "pumpportal_process_exited",
                        "return_code": exit_code,
                        "restart_count": restarts,
                        "restart_delay_seconds": delay,
                    },
                )
                self.emit_status_event(
                    "pumpportal_process_exited",
                    return_code=exit_code,
                    restart_count=restarts,
                    restart_delay_seconds=delay,
                )

            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise

    async def run(self) -> None:
        self.validate_script_paths()
        self.validate_pumpportal_jsonl_setup()
        self.install_signal_handlers()

        metrics_task = asyncio.create_task(self.metrics_reporter())
        post_migration_dex_task = asyncio.create_task(self.post_migration_dex_scheduler())
        pumpportal_supervisor_task: Optional[asyncio.Task[None]] = None

        try:
            if self.config.run_pumpportal and not self.config.dry_run:
                pumpportal_supervisor_task = asyncio.create_task(self.supervise_pumpportal())
                ready_deadline = (
                    time.monotonic() + self.config.pumpportal_jsonl_wait_seconds
                )
                while (
                    not self.mark_pumpportal_jsonl_ready_if_present()
                    and time.monotonic() < ready_deadline
                    and not self.shutdown_event.is_set()
                ):
                    await asyncio.sleep(0.25)

                if (
                    not self._pumpportal_jsonl_ready
                    and not self.shutdown_event.is_set()
                ):
                    raise TimeoutError(
                        f"Timed out after {self.config.pumpportal_jsonl_wait_seconds:.0f}s "
                        f"waiting for PumpPortal JSONL at "
                        f"{resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)}."
                    )

            self.apply_start_at_end_if_needed()

            await self.scan_loop()

            if self.config.run_once:
                await self.wait_for_active_tasks()
        finally:
            self.shutdown_event.set()

            if pumpportal_supervisor_task is not None:
                pumpportal_supervisor_task.cancel()
                await asyncio.gather(pumpportal_supervisor_task, return_exceptions=True)

            metrics_task.cancel()
            post_migration_dex_task.cancel()
            await asyncio.gather(metrics_task, post_migration_dex_task, return_exceptions=True)

            if self.active_tasks and not self.config.run_once:
                for task in list(self.active_tasks.values()):
                    task.cancel()
                await asyncio.gather(*self.active_tasks.values(), return_exceptions=True)

            self.save_state()
            self.logger.info(
                "orchestrator_stopped",
                extra={
                    "event": "orchestrator_stopped",
                    **self.metrics.to_dict(
                        active_count=len(self.active_tasks),
                        tracked_count=len(self.seen_mints),
                        post_migration_dex_tracked_count=len(self.active_post_migration_dex_tracks()),
                    ),
                },
            )

    def install_signal_handlers(self) -> None:
        def request_shutdown(_signum: int | None = None, _frame: Any | None = None) -> None:
            self.logger.warning("shutdown_requested", extra={"event": "shutdown_requested"})
            self.shutdown_event.set()

        if os.name == "nt":
            with contextlib.suppress(ValueError, OSError):
                signal.signal(signal.SIGINT, request_shutdown)
                signal.signal(signal.SIGTERM, request_shutdown)
            return

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, request_shutdown)

    def initialize_offset_to_end(self) -> None:
        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        if not path.exists():
            return

        stat = path.stat()
        self.file_offset = stat.st_size
        self.file_inode = getattr(stat, "st_ino", None)
        self.save_state()

        self.logger.info(
            "initialized_offset_to_end",
            extra={
                "event": "initialized_offset_to_end",
                "path": str(path),
                "file_offset": self.file_offset,
            },
        )

    async def scan_loop(self) -> None:
        jsonl_path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)
        jsonl_size = jsonl_path.stat().st_size if jsonl_path.exists() else 0
        self.logger.info(
            "scan_started",
            extra={
                "event": "scan_started",
                "pumpportal_jsonl_path": str(self.config.pumpportal_jsonl_path),
                "max_concurrent_coins": self.config.max_concurrent_coins,
                "file_offset": self.file_offset,
                "file_size": jsonl_size,
            },
        )
        if jsonl_path.exists() and self.file_offset >= jsonl_size:
            self.logger.warning(
                "pumpportal_tail_at_eof",
                extra={
                    "event": "pumpportal_tail_at_eof",
                    "path": str(jsonl_path),
                    "file_offset": self.file_offset,
                    "file_size": jsonl_size,
                    "hint": "Only new JSONL bytes after this offset are processed; reset state file_offset or pass --start-at-end on a fresh state to follow live tail.",
                },
            )

        while not self.shutdown_event.is_set():
            processed_any = await self.read_available_lines()

            self.save_state()

            if self.config.run_once:
                break

            if not processed_any:
                await asyncio.sleep(self.config.scan_poll_seconds)

    async def read_available_lines(self) -> bool:
        path = resolve_pumpportal_jsonl_path(self.config.pumpportal_jsonl_path)

        if not path.exists():
            if not self._pumpportal_jsonl_ready:
                self.logger.debug(
                    "pumpportal_file_missing",
                    extra={"event": "pumpportal_file_missing", "path": str(path)},
                )
            return False

        stat = path.stat()
        inode = getattr(stat, "st_ino", None)

        if self.file_inode is not None and inode is not None and self.file_inode != inode:
            self.logger.warning(
                "pumpportal_file_rotated",
                extra={
                    "event": "pumpportal_file_rotated",
                    "old_inode": self.file_inode,
                    "new_inode": inode,
                },
            )
            self.file_offset = 0

        if self.file_offset > stat.st_size:
            self.logger.warning(
                "pumpportal_file_truncated",
                extra={
                    "event": "pumpportal_file_truncated",
                    "old_offset": self.file_offset,
                    "new_size": stat.st_size,
                },
            )
            self.file_offset = 0

        self.file_inode = inode

        processed_any = False

        with path.open("rb") as f:
            f.seek(self.file_offset)

            while not self.shutdown_event.is_set():
                line_start = f.tell()
                raw_line = f.readline()

                if not raw_line:
                    break

                if not raw_line.endswith(b"\n"):
                    f.seek(line_start)
                    break

                self.file_offset = f.tell()
                processed_any = True
                await self.handle_raw_line(raw_line.decode("utf-8", errors="replace").strip())

        return processed_any

    async def handle_raw_line(self, raw_line: str) -> None:
        if not raw_line:
            return

        self.metrics.rows_read += 1

        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            self.metrics.invalid_json_rows += 1
            self.logger.warning(
                "invalid_json_row",
                extra={
                    "event": "invalid_json_row",
                    "rows_read": self.metrics.rows_read,
                    "error": str(exc),
                },
            )
            return

        if not isinstance(row, Mapping):
            self.metrics.invalid_json_rows += 1
            self.logger.warning(
                "non_object_json_row",
                extra={"event": "non_object_json_row", "rows_read": self.metrics.rows_read},
            )
            return

        event_type = detect_event_type(row)
        mint = first_string(row, MINT_FIELDS)

        if event_type == "new_token":
            self.metrics.new_token_events += 1
            if mint:
                was_new = mint not in self.seen_mints
                self.seen_mints.add(mint)
                self.logger.info(
                    "mint_recorded",
                    extra={
                        "event": "mint_recorded",
                        "coin": mint,
                        "was_new": was_new,
                        "rows_read": self.metrics.rows_read,
                        "currently_tracked_mints": len(self.seen_mints),
                    },
                )
            return

        if event_type == "migration":
            self.metrics.migrated_coins_detected += 1
            await self.handle_migration_event(row, mint)
            return

    async def handle_migration_event(self, row: Mapping[str, Any], mint: Optional[str]) -> None:
        if not mint:
            self.metrics.ineligible_migrations += 1
            self.logger.warning(
                "migration_missing_mint",
                extra={
                    "event": "migration_missing_mint",
                    "migrated_coins_detected": self.metrics.migrated_coins_detected,
                    "rows_read": self.metrics.rows_read,
                },
            )
            return

        if mint not in self.seen_mints:
            self.metrics.ineligible_migrations += 1
            self.logger.info(
                "migration_rejected_without_prior_mint",
                extra={
                    "event": "migration_rejected_without_prior_mint",
                    "coin": mint,
                    "rows_read": self.metrics.rows_read,
                },
            )
            return

        if mint in self.active_tasks or mint in self.terminal_mints:
            self.metrics.duplicate_migrations += 1
            self.logger.info(
                "migration_duplicate_ignored",
                extra={
                    "event": "migration_duplicate_ignored",
                    "coin": mint,
                    "active": mint in self.active_tasks,
                    "skipped": mint in self.skipped_mints,
                    "completed": mint in self.completed_mints,
                    "failed": mint in self.failed_mints,
                    "red_flag_rejected": mint in self.red_flag_rejected_mints,
                    "retry_failed_enabled": self.config.retry_failed,
                },
            )
            return

        if mint in self.failed_mints and self.config.retry_failed:
            self.failed_mints.remove(mint)

        pairs = string_values_from_keys(row, PAIR_FIELDS)
        if self.config.require_pair_for_capture and not pairs:
            self.metrics.ineligible_migrations += 1
            self.logger.warning(
                "migration_rejected_without_pair",
                extra={"event": "migration_rejected_without_pair", "coin": mint},
            )
            return

        ctx = CoinContext(
            mint=mint,
            pair_addresses=pairs,
            migration_event=dict(row),
            coin_name=first_string(row, NAME_FIELDS),
            symbol=first_string(row, SYMBOL_FIELDS),
        )

        self.metrics.eligible_migrations += 1

        if len(self.active_tasks) >= self.config.max_concurrent_coins:
            self.skipped_mints.add(ctx.mint)
            self.current_stage[ctx.mint] = "skipped_capacity"
            self.metrics.skipped_due_to_concurrency += 1
            self.logger.info(
                "migration_skipped_due_to_concurrency",
                extra={
                    "event": "migration_skipped_due_to_concurrency",
                    "coin": mint,
                    "active_coin_pipelines": len(self.active_tasks),
                    "max_concurrent_coins": self.config.max_concurrent_coins,
                    "skipped_due_to_concurrency": self.metrics.skipped_due_to_concurrency,
                },
            )
            self.emit_status_event(
                "coin_skipped_capacity",
                mint=mint,
                active_coin_pipelines=len(self.active_tasks),
                max_concurrent_coins=self.config.max_concurrent_coins,
                skipped_due_to_concurrency=self.metrics.skipped_due_to_concurrency,
            )
            self.save_state()
            return

        self.metrics.coins_started += 1
        self.start_coin_task(ctx)

    def start_coin_task(self, ctx: CoinContext) -> None:
        task = asyncio.create_task(self.run_coin_pipeline_task(ctx))
        self.active_tasks[ctx.mint] = task

        self.logger.info(
            "coin_pipeline_started",
            extra={
                "event": "coin_pipeline_started",
                "coin": ctx.mint,
                "pair_addresses": ctx.pair_addresses,
                "active_coin_pipelines": len(self.active_tasks),
            },
        )
        self.emit_status_event(
            "coin_pipeline_started",
            mint=ctx.mint,
            pair_addresses=ctx.pair_addresses,
            active_coin_pipelines=len(self.active_tasks),
        )

    async def run_coin_pipeline_task(self, ctx: CoinContext) -> None:
        try:
            await self.run_coin_pipeline(ctx)
        except asyncio.CancelledError:
            self.current_stage[ctx.mint] = "cancelled"
            self.failed_mints.add(ctx.mint)
            self.metrics.failed_coin_analyses += 1
            self.logger.warning(
                "coin_pipeline_cancelled",
                extra={"event": "coin_pipeline_cancelled", "coin": ctx.mint},
            )
            self.emit_status_event("coin_pipeline_cancelled", mint=ctx.mint)
            raise
        except RedFlagRejected as exc:
            self.current_stage[ctx.mint] = "red_flag_rejected"
            self.red_flag_rejected_mints.add(ctx.mint)
            self.metrics.red_flag_rejected_coin_analyses += 1
            decision = exc.decision.to_dict()
            self.logger.info(
                "coin_pipeline_red_flag_rejected",
                extra={
                    "event": "coin_pipeline_red_flag_rejected",
                    "coin": ctx.mint,
                    "decision": decision,
                    "red_flag_rejected_coin_analyses": self.metrics.red_flag_rejected_coin_analyses,
                },
            )
            self.emit_status_event(
                "coin_pipeline_red_flag_rejected",
                mint=ctx.mint,
                decision=decision,
                red_flag_rejected_coin_analyses=self.metrics.red_flag_rejected_coin_analyses,
            )
        except Exception as exc:
            self.current_stage[ctx.mint] = "failed"
            self.failed_mints.add(ctx.mint)
            self.metrics.failed_coin_analyses += 1
            self.logger.error(
                "coin_pipeline_failed",
                extra={
                    "event": "coin_pipeline_failed",
                    "coin": ctx.mint,
                    "error": repr(exc),
                    "traceback": traceback.format_exc(),
                },
            )
            self.emit_status_event("coin_pipeline_failed", mint=ctx.mint, error=repr(exc))
        else:
            self.current_stage[ctx.mint] = "completed"
            self.completed_mints.add(ctx.mint)
            self.metrics.completed_coin_analyses += 1
            self.logger.info(
                "coin_pipeline_completed",
                extra={
                    "event": "coin_pipeline_completed",
                    "coin": ctx.mint,
                    "completed_coin_analyses": self.metrics.completed_coin_analyses,
                },
            )
            self.emit_status_event(
                "coin_pipeline_completed",
                mint=ctx.mint,
                completed_coin_analyses=self.metrics.completed_coin_analyses,
            )
        finally:
            self.active_tasks.pop(ctx.mint, None)
            self.save_state()

    def _set_pipeline_stage(self, mint: str, stage: str) -> None:
        """Keep solana_1h_capture as the dashboard stage while capture is running."""
        if self.current_stage.get(mint) == "solana_1h_capture":
            return
        self.current_stage[mint] = stage

    def _record_analytics_failure(self, ctx: CoinContext, exc: BaseException) -> None:
        self.logger.error(
            "coin_analytics_failed",
            extra={
                "event": "coin_analytics_failed",
                "coin": ctx.mint,
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            },
        )
        self.emit_status_event("coin_analytics_failed", mint=ctx.mint, error=repr(exc))

    async def run_coin_pipeline(self, ctx: CoinContext) -> None:
        capture_task = asyncio.create_task(self.run_capture(ctx))
        await asyncio.sleep(0)

        try:
            await self.run_risk_report(ctx)
            security_report = self.load_security_report(ctx.mint)
            decision = self.evaluate_red_flag_filter(ctx, security_report)
        except BaseException:
            if capture_task.done():
                await asyncio.gather(capture_task, return_exceptions=True)
            else:
                capture_task.cancel()
                await asyncio.gather(capture_task, return_exceptions=True)
            raise

        if decision.rejected:
            self.save_red_flag_audit_artifacts(ctx, security_report, decision)
            await self.cancel_capture_task(ctx, capture_task)
            self.delete_rejected_capture_data(ctx.mint)
            raise RedFlagRejected(ctx.mint, decision)

        self.start_post_migration_dex_tracking(ctx)
        analytics_task = asyncio.create_task(self.run_analytics_branch(ctx))

        capture_results = await asyncio.gather(capture_task, return_exceptions=True)
        capture_error = capture_results[0]
        analytics_results = await asyncio.gather(analytics_task, return_exceptions=True)
        analytics_error = analytics_results[0]
        if isinstance(capture_error, BaseException):
            raise capture_error
        if isinstance(analytics_error, BaseException):
            self._record_analytics_failure(ctx, analytics_error)

    async def run_analytics_branch(self, ctx: CoinContext) -> None:
        try:
            dex_result = await self.poll_dexscreener(ctx)
        except BaseException:
            raise

        enrichment_tasks: list[asyncio.Task[None]] = []
        if dex_result.website_url:
            enrichment_tasks.append(
                asyncio.create_task(self.run_website_grader(ctx, dex_result))
            )
        else:
            self.logger.info(
                "website_grader_skipped_no_website",
                extra={
                    "event": "website_grader_skipped_no_website",
                    "coin": ctx.mint,
                    "features_path": str(dex_result.features_path),
                },
            )

        if dex_result.telegram_url:
            enrichment_tasks.append(
                asyncio.create_task(self.run_telegram_info(ctx, dex_result.telegram_url))
            )
        else:
            self.logger.info(
                "telegram_info_skipped_no_telegram",
                extra={
                    "event": "telegram_info_skipped_no_telegram",
                    "coin": ctx.mint,
                    "features_path": str(dex_result.features_path),
                },
            )

        pending = [*enrichment_tasks]
        if pending:
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

    def start_post_migration_dex_tracking(self, ctx: CoinContext) -> None:
        if (
            self.config.max_post_migration_tracked_coins <= 0
            or self.config.post_migration_dex_tracking_hours <= 0
        ):
            return

        if ctx.mint in self.post_migration_dex_tracks:
            return

        active_tracks = self.active_post_migration_dex_tracks()
        if len(active_tracks) >= self.config.max_post_migration_tracked_coins:
            self.logger.warning(
                "post_migration_dex_tracking_skipped_capacity",
                extra={
                    "event": "post_migration_dex_tracking_skipped_capacity",
                    "coin": ctx.mint,
                    "active_tracks": len(active_tracks),
                    "max_tracks": self.config.max_post_migration_tracked_coins,
                },
            )
            self.emit_status_event(
                "post_migration_dex_tracking_skipped_capacity",
                mint=ctx.mint,
                active_tracks=len(active_tracks),
                max_tracks=self.config.max_post_migration_tracked_coins,
            )
            return

        now_epoch = time.time()
        track = PostMigrationDexTrack(
            mint=ctx.mint,
            pair_addresses=ctx.pair_addresses,
            started_at_epoch=now_epoch,
            end_at_epoch=now_epoch + self.config.post_migration_dex_tracking_hours * 3600.0,
            next_due_at_epoch=now_epoch,
        )
        self.post_migration_dex_tracks[ctx.mint] = track
        self.save_state()
        self.logger.info(
            "post_migration_dex_tracking_started",
            extra={
                "event": "post_migration_dex_tracking_started",
                "coin": ctx.mint,
                "active_tracks": len(self.active_post_migration_dex_tracks()),
                "max_tracks": self.config.max_post_migration_tracked_coins,
                "tracking_hours": self.config.post_migration_dex_tracking_hours,
                "out_root": str(self.config.post_migration_dex_out_root),
            },
        )
        self.emit_status_event(
            "post_migration_dex_tracking_started",
            mint=ctx.mint,
            active_tracks=len(self.active_post_migration_dex_tracks()),
            max_tracks=self.config.max_post_migration_tracked_coins,
            tracking_hours=self.config.post_migration_dex_tracking_hours,
        )

    def post_migration_dex_interval_seconds(self, active_count: int) -> float:
        safe_rpm = max(1.0, self.config.post_migration_dex_requests_per_minute)
        requests_per_round = max(1, self.config.post_migration_dex_requests_per_snapshot)
        return max(1.0, 60.0 * max(1, active_count) * requests_per_round / safe_rpm)

    def build_post_migration_dex_command(self, track: PostMigrationDexTrack) -> list[str]:
        command = [
            self.config.python_executable,
            str(self.config.dexscreener_script_path),
            "--token",
            track.mint,
            "--chain",
            self.config.dexscreener_chain,
            "--out",
            str(self.config.post_migration_dex_out_root),
            "--sleep",
            str(self.config.post_migration_dex_request_sleep_seconds),
            "--append-history",
            "--quiet",
            "--endpoint-profile",
            self.config.post_migration_dex_endpoint_profile,
        ]
        if self.config.post_migration_dex_timestamped_raw:
            command.append("--timestamped-raw")
        return command

    async def post_migration_dex_scheduler(self) -> None:
        stage = "post_migration_dexscreener"
        while not self.shutdown_event.is_set():
            active_tracks = self.active_post_migration_dex_tracks()
            if not active_tracks:
                await asyncio.sleep(1.0)
                continue

            now_epoch = time.time()
            due_tracks = [track for track in active_tracks if track.next_due_at_epoch <= now_epoch]
            if not due_tracks:
                next_due = min(track.next_due_at_epoch for track in active_tracks)
                await asyncio.sleep(min(5.0, max(0.5, next_due - now_epoch)))
                continue

            track = min(due_tracks, key=lambda item: item.next_due_at_epoch)
            command = self.build_post_migration_dex_command(track)
            try:
                await run_subprocess(
                    command,
                    logger=self.logger,
                    mint=track.mint,
                    stage=stage,
                    timeout_seconds=self.config.dexscreener_subprocess_timeout_seconds,
                    dry_run=self.config.dry_run,
                    stage_log_path=self.stage_log_path(track.mint, stage),
                )
                track.samples += 1
                track.last_sample_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                track.last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                track.last_error = repr(exc)
                self.logger.warning(
                    "post_migration_dex_snapshot_failed",
                    extra={
                        "event": "post_migration_dex_snapshot_failed",
                        "coin": track.mint,
                        "error": repr(exc),
                    },
                )

            active_count = len(self.active_post_migration_dex_tracks())
            interval_seconds = self.post_migration_dex_interval_seconds(active_count)
            track.next_due_at_epoch = time.time() + interval_seconds

            if track.end_at_epoch <= time.time():
                self.post_migration_dex_tracks.pop(track.mint, None)
                self.logger.info(
                    "post_migration_dex_tracking_completed",
                    extra={
                        "event": "post_migration_dex_tracking_completed",
                        "coin": track.mint,
                        "samples": track.samples,
                    },
                )
                self.emit_status_event(
                    "post_migration_dex_tracking_completed",
                    mint=track.mint,
                    samples=track.samples,
                )
            else:
                self.logger.info(
                    "post_migration_dex_snapshot",
                    extra={
                        "event": "post_migration_dex_snapshot",
                        "coin": track.mint,
                        "samples": track.samples,
                        "active_tracks": active_count,
                        "next_interval_seconds": round(interval_seconds, 3),
                        "out_root": str(self.config.post_migration_dex_out_root),
                    },
                )
                self.emit_status_event(
                    "post_migration_dex_snapshot",
                    mint=track.mint,
                    samples=track.samples,
                    active_tracks=active_count,
                    next_interval_seconds=round(interval_seconds, 3),
                )

            self.save_state()

    async def run_capture(self, ctx: CoinContext) -> None:
        stage = "solana_1h_capture"
        self.current_stage[ctx.mint] = stage

        default_command: list[str] = [
            self.config.python_executable,
            str(self.config.capture_script_path),
            "--mint",
            ctx.mint,
            "--duration-seconds",
            str(self.config.capture_duration_seconds),
            "--out",
            str(self.config.capture_out_root),
            "--network-sample-seconds",
            str(self.config.capture_network_sample_seconds),
        ]
        if self.config.capture_network_sample_fee_addresses:
            default_command.append("--network-sample-fee-addresses")

        network_sample_args = [
            "--network-sample-seconds",
            str(self.config.capture_network_sample_seconds),
        ]
        if self.config.capture_network_sample_fee_addresses:
            network_sample_args.append("--network-sample-fee-addresses")

        pair_args: list[str] = []
        valid_pair_addresses = tuple(pair for pair in ctx.pair_addresses if is_valid_solana_address(pair))
        invalid_pair_addresses = tuple(pair for pair in ctx.pair_addresses if pair not in valid_pair_addresses)
        if invalid_pair_addresses:
            self.logger.info(
                "capture_pair_addresses_filtered",
                extra={
                    "event": "capture_pair_addresses_filtered",
                    "coin": ctx.mint,
                    "invalid_pair_addresses": invalid_pair_addresses,
                },
            )

        for pair in valid_pair_addresses:
            default_command.extend(["--pair", pair])
            pair_args.extend(["--pair", pair])

        command = build_template_command(
            self.config.capture_command_template,
            default_command=default_command,
            replacements={
                "python": self.config.python_executable,
                "script": self.config.capture_script_path,
                "mint": ctx.mint,
                "duration_seconds": self.config.capture_duration_seconds,
                "capture_out_root": self.config.capture_out_root,
                "network_sample_seconds": self.config.capture_network_sample_seconds,
            },
            list_replacements={"pair_args": pair_args, "network_sample_args": network_sample_args},
        )
        command.extend(self.config.capture_extra_args)

        timeout = self.config.capture_duration_seconds + self.config.capture_timeout_grace_seconds
        await run_subprocess(
            command,
            logger=self.logger,
            mint=ctx.mint,
            stage=stage,
            timeout_seconds=timeout,
            dry_run=self.config.dry_run,
            stage_log_path=self.stage_log_path(ctx.mint, stage),
        )

    async def run_risk_report(self, ctx: CoinContext) -> None:
        stage = "solana_risk_report"
        self._set_pipeline_stage(ctx.mint, stage)

        risk_export_path = self.risk_report_export_path(ctx.mint)

        default_command = [
            self.config.python_executable,
            str(self.config.risk_report_script_path),
            "--mint",
            ctx.mint,
            "--analysis-root",
            str(self.config.risk_analysis_root),
            "--format",
            "json",
            "--out",
            str(risk_export_path),
            "--pretty",
            "--timeout",
            str(self.config.risk_http_timeout_seconds),
        ]
        command = build_template_command(
            self.config.risk_command_template,
            default_command=default_command,
            replacements={
                "python": self.config.python_executable,
                "script": self.config.risk_report_script_path,
                "mint": ctx.mint,
                "risk_analysis_root": self.config.risk_analysis_root,
                "risk_export_path": risk_export_path,
                "risk_http_timeout_seconds": self.config.risk_http_timeout_seconds,
            },
        )
        command.extend(self.config.risk_extra_args)

        await run_subprocess(
            command,
            logger=self.logger,
            mint=ctx.mint,
            stage=stage,
            timeout_seconds=self.config.risk_subprocess_timeout_seconds,
            dry_run=self.config.dry_run,
            stage_log_path=self.stage_log_path(ctx.mint, stage),
        )

    def risk_report_export_path(self, mint: str) -> Path:
        return self.config.risk_export_root / f"{mint}.json"

    def load_security_report(self, mint: str) -> dict[str, Any]:
        path = self.risk_report_export_path(mint)
        data = load_json_file(path)
        if not isinstance(data, dict):
            raise PipelineError(f"security report is not a JSON object: {path}")
        return data

    def evaluate_red_flag_filter(
        self,
        ctx: CoinContext,
        security_report: Mapping[str, Any],
        dex_features: Optional[Mapping[str, Any]] = None,
    ) -> red_flag_filter.RedFlagDecision:
        stage = "red_flag_filter"
        self._set_pipeline_stage(ctx.mint, stage)

        if not self.config.red_flag_filter_enabled:
            decision = red_flag_filter.RedFlagDecision(
                accepted=True,
                rejected=False,
                reject_reasons=[],
                passed_rules=[],
                skipped_rules=[{"rule": "red_flag_filter", "reason": "disabled"}],
                missing_required_data=[],
                evaluated_at_utc=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            )
            self.logger.info(
                "red_flag_filter_skipped",
                extra={"event": "red_flag_filter_skipped", "coin": ctx.mint, "decision": decision.to_dict()},
            )
            self.emit_status_event("red_flag_filter_skipped", mint=ctx.mint, decision=decision.to_dict())
            return decision

        cfg = red_flag_filter.load_red_flag_config(self.config.red_flag_config_path)
        decision = red_flag_filter.evaluate_red_flags(
            security_report=security_report,
            dex_features=dex_features,
            config=cfg,
        )
        event = "red_flag_filter_rejected" if decision.rejected else "red_flag_filter_accepted"
        self.logger.info(
            event,
            extra={"event": event, "coin": ctx.mint, "decision": decision.to_dict()},
        )
        self.emit_status_event(event, mint=ctx.mint, decision=decision.to_dict())
        return decision

    async def cancel_capture_task(self, ctx: CoinContext, capture_task: asyncio.Task[None]) -> None:
        if capture_task.done():
            await asyncio.gather(capture_task, return_exceptions=True)
            return
        capture_task.cancel()
        await asyncio.gather(capture_task, return_exceptions=True)
        self.logger.info(
            "capture_cancelled_for_red_flag",
            extra={"event": "capture_cancelled_for_red_flag", "coin": ctx.mint},
        )
        self.emit_status_event("capture_cancelled_for_red_flag", mint=ctx.mint)

    def red_flag_audit_dir(self, mint: str) -> Path:
        return self.config.red_flag_audit_root.expanduser().resolve() / mint

    def red_flag_human_security_report_path(self, mint: str) -> Path:
        return self.config.risk_analysis_root.expanduser().resolve() / mint / SECURITY_REPORT_FILENAME

    def red_flag_config_sha256(self) -> Optional[str]:
        path = self.config.red_flag_config_path.expanduser().resolve()
        if not path.exists():
            return None
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def save_red_flag_audit_artifacts(
        self,
        ctx: CoinContext,
        security_report: Mapping[str, Any],
        decision: red_flag_filter.RedFlagDecision,
    ) -> Optional[Path]:
        if not self.config.red_flag_save_audit_artifacts or self.config.dry_run:
            return None

        audit_dir = self.red_flag_audit_dir(ctx.mint)
        audit_dir.mkdir(parents=True, exist_ok=True)
        rejected_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        risk_export_path = self.risk_report_export_path(ctx.mint)
        human_report_path = self.red_flag_human_security_report_path(ctx.mint)

        decision_payload: dict[str, Any] = {
            "mint": ctx.mint,
            "rejected_at_utc": rejected_at_utc,
            "symbol": ctx.symbol,
            "coin_name": ctx.coin_name,
            "pair_addresses": list(ctx.pair_addresses),
            "red_flag_config_path": str(self.config.red_flag_config_path),
            "red_flag_config_sha256": self.red_flag_config_sha256(),
            "risk_analysis_root": str(self.config.risk_analysis_root),
            "risk_report_export_path": str(risk_export_path),
            "decision": decision.to_dict(),
        }
        save_json_atomic(audit_dir / "red_flag_decision.json", decision_payload)

        saved_files: list[str] = ["red_flag_decision.json"]
        if risk_export_path.exists():
            shutil.copy2(risk_export_path, audit_dir / "security_report.json")
            saved_files.append("security_report.json")
        else:
            save_json_atomic(audit_dir / "security_report.json", dict(security_report))
            saved_files.append("security_report.json")

        if human_report_path.exists():
            shutil.copy2(human_report_path, audit_dir / SECURITY_REPORT_FILENAME)
            saved_files.append(SECURITY_REPORT_FILENAME)

        reject_codes = [
            str(item.get("code") or item.get("rule") or "")
            for item in decision.reject_reasons
            if item.get("code") or item.get("rule")
        ]
        append_jsonl(
            self.config.red_flag_audit_root.expanduser().resolve() / RED_FLAG_REJECTIONS_INDEX,
            {
                "mint": ctx.mint,
                "rejected_at_utc": rejected_at_utc,
                "evaluated_at_utc": decision.evaluated_at_utc,
                "reject_codes": reject_codes,
                "audit_dir": str(audit_dir),
                "saved_files": saved_files,
            },
        )

        self.logger.info(
            "red_flag_audit_saved",
            extra={
                "event": "red_flag_audit_saved",
                "coin": ctx.mint,
                "audit_dir": str(audit_dir),
                "saved_files": saved_files,
                "reject_codes": reject_codes,
            },
        )
        self.emit_status_event(
            "red_flag_audit_saved",
            mint=ctx.mint,
            audit_dir=str(audit_dir),
            saved_files=saved_files,
            reject_codes=reject_codes,
        )
        return audit_dir

    def delete_rejected_capture_data(self, mint: str) -> Optional[Path]:
        if not self.config.red_flag_delete_rejected_capture_data:
            return None

        root = self.config.capture_out_root.expanduser().resolve()
        target = (self.config.capture_out_root / mint).expanduser().resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise PipelineError(f"Refusing to delete capture data outside capture root: {target}") from exc
        if target == root:
            raise PipelineError(f"Refusing to delete capture root itself: {target}")

        if not target.exists():
            return target
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        self.logger.info(
            "red_flag_rejected_capture_data_deleted",
            extra={"event": "red_flag_rejected_capture_data_deleted", "coin": mint, "path": str(target)},
        )
        self.emit_status_event("red_flag_rejected_capture_data_deleted", mint=mint, path=str(target))
        return target

    async def poll_dexscreener(self, ctx: CoinContext) -> DexScreenerResult:
        stage = "dexscreener_poll"
        self._set_pipeline_stage(ctx.mint, stage)

        if self.config.dry_run:
            features_path = self.dex_features_path(ctx.mint)
            features = {
                "pair_count": 1,
                "endpoint_status": {"token_pairs": {"status_code": 200}},
                "websites": (
                    [{"url": self.config.dry_run_dex_website}]
                    if self.config.dry_run_dex_website
                    else []
                ),
                "dry_run": True,
            }
            save_json_atomic(features_path, features)
            website_url = extract_website_url(features)
            self.logger.info(
                "dexscreener_dry_run_available",
                extra={
                    "event": "dexscreener_dry_run_available",
                    "coin": ctx.mint,
                    "stage": stage,
                    "website_url": website_url,
                    "features_path": str(features_path),
                },
            )
            self.emit_status_event(
                "dexscreener_dry_run_available",
                mint=ctx.mint,
                website_url=website_url,
                features_path=str(features_path),
            )
            return DexScreenerResult(
                features_path=features_path,
                features=features,
                website_url=website_url,
                telegram_url=extract_social_url(features, {"telegram", "tg"}),
                x_url=extract_social_url(features, {"twitter", "x"}),
            )

        wait_seconds = self.config.dexscreener_initial_wait_seconds
        last_error: Optional[str] = None

        for attempt in range(1, self.config.dexscreener_max_attempts + 1):
            if attempt > 1:
                jittered_wait = max(1.0, wait_seconds * random.uniform(0.85, 1.15))
                self.logger.info(
                    "dexscreener_retry_wait",
                    extra={
                        "event": "dexscreener_retry_wait",
                        "coin": ctx.mint,
                        "stage": stage,
                        "attempt": attempt,
                        "wait_seconds": round(jittered_wait, 3),
                    },
                )
                self.emit_status_event(
                    "dexscreener_retry_wait",
                    mint=ctx.mint,
                    attempt=attempt,
                    wait_seconds=round(jittered_wait, 3),
                )
                await asyncio.sleep(jittered_wait)

            default_command = [
                self.config.python_executable,
                str(self.config.dexscreener_script_path),
                "--token",
                ctx.mint,
                "--chain",
                self.config.dexscreener_chain,
                "--out",
                str(self.config.dexscreener_out_root),
                "--sleep",
                str(self.config.dexscreener_script_sleep_seconds),
            ]
            command = build_template_command(
                self.config.dexscreener_command_template,
                default_command=default_command,
                replacements={
                    "python": self.config.python_executable,
                    "script": self.config.dexscreener_script_path,
                    "mint": ctx.mint,
                    "chain": self.config.dexscreener_chain,
                    "dexscreener_out_root": self.config.dexscreener_out_root,
                    "dexscreener_script_sleep_seconds": self.config.dexscreener_script_sleep_seconds,
                },
            )
            command.extend(self.config.dexscreener_extra_args)

            self.logger.info(
                "dexscreener_attempt",
                extra={
                    "event": "dexscreener_attempt",
                    "coin": ctx.mint,
                    "stage": stage,
                    "attempt": attempt,
                    "max_attempts": self.config.dexscreener_max_attempts,
                },
            )

            try:
                await run_subprocess(
                    command,
                    logger=self.logger,
                    mint=ctx.mint,
                    stage=stage,
                    timeout_seconds=self.config.dexscreener_subprocess_timeout_seconds,
                    dry_run=self.config.dry_run,
                    stage_log_path=self.stage_log_path(ctx.mint, f"{stage}_attempt_{attempt}"),
                )
            except Exception as exc:
                last_error = repr(exc)
                features_path = self.dex_features_path(ctx.mint)
                features = self.read_dex_features(features_path)
                retry_after = self.dex_retry_after_seconds(features)
                if retry_after is not None:
                    wait_seconds = max(wait_seconds, retry_after)
                self.logger.warning(
                    "dexscreener_attempt_failed",
                    extra={
                        "event": "dexscreener_attempt_failed",
                        "coin": ctx.mint,
                        "stage": stage,
                        "attempt": attempt,
                        "error": last_error,
                        "retry_after_seconds": retry_after,
                    },
                )
            else:
                features_path = self.dex_features_path(ctx.mint)
                features = self.read_dex_features(features_path)

                available = self.dex_data_available(features)
                website_url = extract_website_url(features)
                telegram_url = extract_social_url(features, {"telegram", "tg"})
                x_url = extract_social_url(features, {"twitter", "x"})

                self.logger.info(
                    "dexscreener_attempt_result",
                    extra={
                        "event": "dexscreener_attempt_result",
                        "coin": ctx.mint,
                        "stage": stage,
                        "attempt": attempt,
                        "available": available,
                        "website_url": website_url,
                        "telegram_url": telegram_url,
                        "x_url": x_url,
                        "features_path": str(features_path),
                        "pair_count": features.get("pair_count"),
                        "endpoint_status": features.get("endpoint_status"),
                    },
                )

                if available:
                    return DexScreenerResult(
                        features_path=features_path,
                        features=features,
                        website_url=website_url,
                        telegram_url=telegram_url,
                        x_url=x_url,
                    )

                retry_after = self.dex_retry_after_seconds(features)
                if retry_after is not None:
                    wait_seconds = max(wait_seconds, retry_after)
                    self.logger.info(
                        "dexscreener_retry_after_observed",
                        extra={
                            "event": "dexscreener_retry_after_observed",
                            "coin": ctx.mint,
                            "stage": stage,
                            "attempt": attempt,
                            "retry_after_seconds": retry_after,
                        },
                    )

                last_error = "DexScreener returned no token-pair data yet"

            wait_seconds = min(
                self.config.dexscreener_max_wait_seconds,
                wait_seconds * self.config.dexscreener_backoff_multiplier,
            )

        raise PipelineError(
            f"DexScreener data unavailable for {ctx.mint} after "
            f"{self.config.dexscreener_max_attempts} attempts; last_error={last_error}"
        )

    def dex_features_path(self, mint: str) -> Path:
        return self.config.dexscreener_out_root / mint / "dexscreener" / "features.json"

    def read_dex_features(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = load_json_file(path)
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            self.logger.warning(
                "dexscreener_features_read_failed",
                extra={"event": "dexscreener_features_read_failed", "path": str(path), "error": repr(exc)},
            )
            return {}

    @staticmethod
    def dex_data_available(features: Mapping[str, Any]) -> bool:
        if not features:
            return False

        pair_count = features.get("pair_count")
        try:
            pair_count_int = int(pair_count or 0)
        except (TypeError, ValueError):
            pair_count_int = 0

        endpoint_status = features.get("endpoint_status")
        token_pairs_status = None
        if isinstance(endpoint_status, Mapping):
            token_pairs = endpoint_status.get("token_pairs")
            if isinstance(token_pairs, Mapping):
                token_pairs_status = token_pairs.get("status_code")

        return token_pairs_status == 200 and pair_count_int > 0

    @staticmethod
    def dex_retry_after_seconds(features: Mapping[str, Any]) -> Optional[float]:
        endpoint_status = features.get("endpoint_status")
        if not isinstance(endpoint_status, Mapping):
            return None

        values: list[float] = []
        for endpoint in endpoint_status.values():
            if not isinstance(endpoint, Mapping):
                continue
            status_code = endpoint.get("status_code")
            headers = endpoint.get("rate_headers") or endpoint.get("headers")
            if status_code != 429 or not isinstance(headers, Mapping):
                continue

            retry_after = headers.get("retry-after") or headers.get("Retry-After")
            try:
                if retry_after is not None:
                    values.append(float(retry_after))
            except (TypeError, ValueError):
                continue

        return max(values) if values else None

    async def run_website_grader(self, ctx: CoinContext, dex_result: DexScreenerResult) -> None:
        stage = "website_grader"
        self._set_pipeline_stage(ctx.mint, stage)
        website_url = dex_result.website_url
        if not website_url:
            raise PipelineError("website_grader called without a website URL")

        metadata = {
            "mint": ctx.mint,
            "website": website_url,
            "name": ctx.coin_name,
            "symbol": ctx.symbol,
            "telegram": dex_result.telegram_url,
            "twitter": dex_result.x_url,
            "x": dex_result.x_url,
            "expected_telegram": dex_result.telegram_url,
            "expected_x": dex_result.x_url,
        }

        metadata_json = json.dumps(metadata, separators=(",", ":"), ensure_ascii=False)
        default_command = [
            self.config.python_executable,
            str(self.config.website_grader_script_path),
            "--mint",
            ctx.mint,
            "--website",
            website_url,
            "--metadata-json",
            metadata_json,
            "--output-root",
            str(self.config.website_output_root),
            "--expected-telegram",
            dex_result.telegram_url or "",
            "--expected-x",
            dex_result.x_url or "",
            "--quiet",
        ]
        command = build_template_command(
            self.config.website_command_template,
            default_command=default_command,
            replacements={
                "python": self.config.python_executable,
                "script": self.config.website_grader_script_path,
                "mint": ctx.mint,
                "website_url": website_url,
                "metadata_json": metadata_json,
                "website_output_root": self.config.website_output_root,
                "telegram_url": dex_result.telegram_url or "",
                "x_url": dex_result.x_url or "",
            },
        )
        command.extend(self.config.website_extra_args)

        await run_subprocess(
            command,
            logger=self.logger,
            mint=ctx.mint,
            stage=stage,
            timeout_seconds=self.config.website_subprocess_timeout_seconds,
            dry_run=self.config.dry_run,
            stage_log_path=self.stage_log_path(ctx.mint, stage),
        )

    async def run_telegram_info(self, ctx: CoinContext, telegram_url: str) -> None:
        stage = "telegram_info"
        self._set_pipeline_stage(ctx.mint, stage)

        default_command = [
            self.config.python_executable,
            str(self.config.telegram_info_script_path),
            "--link",
            telegram_url,
            "--mint",
            ctx.mint,
        ]
        if self.config.telegram_join:
            default_command.append("--join")

        migration_time = first_string(
            ctx.migration_event,
            ("received_at_iso_utc", "timestamp_utc", "created_at_utc", "time_utc"),
        )
        if migration_time:
            default_command.extend(["--migration-time-utc", migration_time])

        command = build_template_command(
            self.config.telegram_command_template,
            default_command=default_command,
            replacements={
                "python": self.config.python_executable,
                "script": self.config.telegram_info_script_path,
                "mint": ctx.mint,
                "telegram_url": telegram_url,
                "migration_time_utc": migration_time or "",
            },
        )
        command.extend(self.config.telegram_extra_args)

        await run_subprocess(
            command,
            logger=self.logger,
            mint=ctx.mint,
            stage=stage,
            timeout_seconds=self.config.telegram_subprocess_timeout_seconds,
            dry_run=self.config.dry_run,
            stage_log_path=self.stage_log_path(ctx.mint, stage),
        )

    async def metrics_reporter(self) -> None:
        while not self.shutdown_event.is_set():
            metrics_payload = {
                **self.metrics.to_dict(
                    active_count=len(self.active_tasks),
                    tracked_count=len(self.seen_mints),
                    post_migration_dex_tracked_count=len(self.active_post_migration_dex_tracks()),
                ),
                "current_stage": dict(self.current_stage),
            }
            self.logger.info(
                "orchestrator_metrics",
                extra={"event": "orchestrator_metrics", **metrics_payload},
            )
            self.emit_status_event("metrics", **metrics_payload)
            await asyncio.sleep(self.config.metrics_log_seconds)

    async def wait_for_active_tasks(self) -> None:
        while self.active_tasks:
            await asyncio.gather(*list(self.active_tasks.values()), return_exceptions=True)


# -----------------------------
# Dex website extraction
# -----------------------------


def normalize_url(url: str) -> Optional[str]:
    text = url.strip()
    if not text:
        return None
    if text.startswith(("http://", "https://")):
        return text
    if "." in text and " " not in text:
        return "https://" + text
    return None


def is_probably_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def extract_website_url(features: Mapping[str, Any]) -> Optional[str]:
    websites = features.get("websites")
    if not isinstance(websites, list):
        return None

    for item in websites:
        raw_url: Optional[str] = None
        if isinstance(item, str):
            raw_url = item
        elif isinstance(item, Mapping):
            for key in ("url", "link", "href"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    raw_url = value
                    break

        if raw_url:
            normalized = normalize_url(raw_url)
            if normalized and is_probably_http_url(normalized):
                return normalized

    return None


def extract_social_url(features: Mapping[str, Any], platforms: set[str]) -> Optional[str]:
    socials = features.get("socials")
    if not isinstance(socials, list):
        return None

    normalized_platforms = {platform.lower() for platform in platforms}
    for item in socials:
        raw_url: Optional[str] = None
        platform_text = ""

        if isinstance(item, str):
            raw_url = item
        elif isinstance(item, Mapping):
            platform_text = str(
                item.get("type") or item.get("platform") or item.get("label") or ""
            ).strip().lower()
            for key in ("url", "link", "href"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    raw_url = value
                    break
        else:
            continue

        if not raw_url:
            continue

        url_l = raw_url.lower()
        platform_matches = platform_text in normalized_platforms or any(
            platform in platform_text for platform in normalized_platforms
        )
        url_matches = (
            ({"telegram", "tg"} & normalized_platforms and ("t.me/" in url_l or "telegram." in url_l))
            or ({"twitter", "x"} & normalized_platforms and ("x.com/" in url_l or "twitter.com/" in url_l))
        )

        if not platform_matches and not url_matches:
            continue

        if raw_url.startswith("@"):
            return raw_url

        normalized = normalize_url(raw_url)
        if normalized:
            return normalized

    return None


# -----------------------------
# CLI
# -----------------------------


def build_parser(config_defaults: Optional[Mapping[str, Any]] = None) -> argparse.ArgumentParser:
    if config_defaults is None:
        config_defaults = load_json_config_defaults(DEFAULT_ORCH_CONFIG_PATH)

    parser = argparse.ArgumentParser(
        description="Orchestrate PumpPortal migrated-coin acquisition pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--config", type=Path, default=DEFAULT_ORCH_CONFIG_PATH, help="JSON config file for stable orchestrator defaults.")
    parser.add_argument("--pumpportal-jsonl", type=Path, default=default_pumpportal_jsonl())

    parser.add_argument("--pumpportal-script", type=Path, default=Path("pumpportal_ws.py"))
    parser.add_argument("--capture-script", type=Path, default=Path("solana_coin_1h_capture.py"))
    parser.add_argument("--risk-report-script", type=Path, default=Path("solana_risk_report.py"))
    parser.add_argument("--dexscreener-script", type=Path, default=Path("dexscreener_api.py"))
    parser.add_argument("--website-grader-script", type=Path, default=Path("website_grader_v2.py"))
    parser.add_argument("--telegram-info-script", type=Path, default=Path("telegram_info.py"))

    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--state-path", type=Path, default=Path("data/raw/orchestrator/state.json"))

    parser.add_argument("--no-pumpportal", action="store_true", help="Do not launch pumpportal_ws.py; only tail --pumpportal-jsonl.")
    parser.add_argument("--pumpportal-duration-seconds", type=int, default=0, help="PumpPortal stream duration; 0 means run indefinitely.")
    parser.add_argument("--pumpportal-display", choices=("metrics", "all", "bar"), default="bar")
    parser.add_argument(
        "--pumpportal-jsonl-wait-seconds",
        type=float,
        default=120.0,
        help="Max seconds to wait for pumpportal_ws.py to create --pumpportal-jsonl after launch.",
    )
    parser.add_argument(
        "--no-pumpportal-auto-restart",
        action="store_true",
        help="Do not restart pumpportal_ws.py when the subprocess exits unexpectedly.",
    )
    parser.add_argument(
        "--pumpportal-restart-delay-seconds",
        type=float,
        default=5.0,
        help="Delay before relaunching pumpportal_ws.py after an unexpected exit.",
    )
    parser.add_argument(
        "--pumpportal-jsonl-stale-restart-seconds",
        type=float,
        default=180.0,
        help=(
            "If the migrations JSONL has not been modified for this many seconds and the "
            "PumpPortal PID is gone, force a supervisor restart (0 disables)."
        ),
    )

    parser.add_argument("--max-concurrent-coins", type=int, default=2)
    parser.add_argument("--scan-poll-seconds", type=float, default=2.0)
    parser.add_argument("--metrics-log-seconds", type=float, default=30.0)
    parser.add_argument("--start-at-end", action="store_true")
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Do not execute child scripts; useful for historical JSONL replay.")
    parser.add_argument("--dry-run-dex-website", default=None, help="Mock website URL exposed by DexScreener during --dry-run.")
    parser.add_argument("--retry-failed", action="store_true", help="Allow a previously failed mint to be retried if another eligible migration event is observed.")
    parser.add_argument("--status-jsonl-path", type=Path, default=Path("data/raw/orchestrator/status_events.jsonl"))
    parser.add_argument("--no-status-jsonl", action="store_true", help="Disable JSONL status event output.")
    parser.add_argument("--stage-log-root", type=Path, default=Path("data/raw/orchestrator/stage_logs"))
    parser.add_argument("--no-stage-log-files", action="store_true", help="Disable per-coin per-stage subprocess log files.")

    parser.add_argument(
        "--pumpportal-command-template",
        default=None,
        help=(
            "Optional shell-style argv template. Placeholders: "
            "{python} {script} {duration_seconds} {display} {raw_jsonl}."
        ),
    )
    parser.add_argument("--capture-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {duration_seconds} {capture_out_root} {pair_args} {network_sample_args}.")
    parser.add_argument("--risk-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {risk_analysis_root} {risk_export_path} {risk_http_timeout_seconds}.")
    parser.add_argument("--dexscreener-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {chain} {dexscreener_out_root} {dexscreener_script_sleep_seconds}.")
    parser.add_argument("--website-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {website_url} {metadata_json} {website_output_root} {telegram_url} {x_url}.")
    parser.add_argument("--telegram-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {telegram_url} {migration_time_utc}.")

    parser.add_argument("--capture-duration-seconds", type=int, default=3600)
    parser.add_argument("--capture-timeout-grace-seconds", type=int, default=900)
    parser.add_argument("--capture-out-root", type=Path, default=Path("data/raw/onchain"))
    parser.add_argument("--capture-network-sample-seconds", type=float, default=60.0, help="Sparse Helius congestion/fee sampling interval passed to solana_coin_1h_capture.py. Use 0 to disable.")
    parser.add_argument("--capture-network-sample-fee-addresses", action="store_true", help="Sample prioritization fees for watched addresses instead of global fee pressure.")
    parser.add_argument("--require-pair-for-capture", action="store_true")

    parser.add_argument("--risk-analysis-root", type=Path, default=Path("data/raw/analytics"))
    parser.add_argument("--risk-export-root", type=Path, default=Path("data/raw/orchestrator/risk_reports"))
    parser.add_argument("--risk-http-timeout-seconds", type=int, default=15)
    parser.add_argument("--risk-subprocess-timeout-seconds", type=int, default=240)
    parser.add_argument("--no-red-flag-filter", action="store_true", help="Disable the hard-stop red flag filter.")
    parser.add_argument("--red-flag-config-path", type=Path, default=Path("config/red_flags.json"))
    parser.add_argument("--keep-red-flag-rejected-capture-data", action="store_true", help="Keep temporary capture data for red-flag rejected coins.")
    parser.add_argument(
        "--red-flag-audit-root",
        type=Path,
        default=Path("data/raw/red_flags"),
        help="Per-mint folder root for red-flag rejection audit artifacts.",
    )
    parser.add_argument(
        "--no-red-flag-audit",
        action="store_true",
        help="Do not write red-flag decision and security report copies under --red-flag-audit-root.",
    )

    parser.add_argument("--dexscreener-out-root", type=Path, default=Path("data/raw/analytics"))
    parser.add_argument("--dexscreener-chain", default="solana")
    parser.add_argument("--dexscreener-initial-wait-seconds", type=float, default=60.0)
    parser.add_argument("--dexscreener-max-wait-seconds", type=float, default=300.0)
    parser.add_argument("--dexscreener-backoff-multiplier", type=float, default=1.35)
    parser.add_argument("--dexscreener-max-attempts", type=int, default=30)
    parser.add_argument("--dexscreener-script-sleep-seconds", type=float, default=1.1)
    parser.add_argument("--dexscreener-subprocess-timeout-seconds", type=int, default=180)
    parser.add_argument("--max-post-migration-tracked-coins", type=int, default=6, help="Maximum coins tracked by the cheap post-Helius DexScreener label sampler. Use 0 to disable.")
    parser.add_argument("--post-migration-dex-tracking-hours", type=float, default=24.0, help="How long to keep sampling DexScreener after Helius capture stops.")
    parser.add_argument("--post-migration-dex-requests-per-minute", type=float, default=50.0, help="Safe global DexScreener request budget for post-Helius label tracking.")
    parser.add_argument("--post-migration-dex-requests-per-snapshot", type=int, default=1, help="Requests consumed by each post-migration Dex snapshot. Market profile uses 1.")
    parser.add_argument("--post-migration-dex-endpoint-profile", choices=("market", "full"), default="market", help="market uses only token-pairs for cheap labels; full uses every enrichment endpoint.")
    parser.add_argument("--post-migration-dex-request-sleep-seconds", type=float, default=0.0, help="Per-request sleep inside dexscreener_api.py for post-migration snapshots. Keep 0 because orchestrator rate-limits globally.")
    parser.add_argument("--post-migration-dex-out-root", type=Path, default=Path("data/raw/onchain"))
    parser.add_argument("--no-post-migration-dex-raw-history", action="store_true", help="Do not save timestamped raw DexScreener endpoint responses for the post-migration sampler.")

    parser.add_argument("--website-output-root", type=Path, default=Path("data/raw/analytics"))
    parser.add_argument("--website-subprocess-timeout-seconds", type=int, default=900)
    parser.add_argument("--telegram-subprocess-timeout-seconds", type=int, default=900)
    parser.add_argument("--no-telegram-join", action="store_true", help="Do not pass --join to telegram_info.py.")

    parser.add_argument("--pumpportal-extra-arg", action="append", default=[])
    parser.add_argument("--capture-extra-arg", action="append", default=[])
    parser.add_argument("--risk-extra-arg", action="append", default=[])
    parser.add_argument("--dexscreener-extra-arg", action="append", default=[])
    parser.add_argument("--website-extra-arg", action="append", default=[])
    parser.add_argument("--telegram-extra-arg", action="append", default=[])

    parser.add_argument("--console-format", default="pretty", choices=["pretty", "json"])
    parser.add_argument(
        "--console-display",
        default="dashboard",
        choices=["dashboard", "verbose"],
        help="pretty console only: dashboard keeps 2 in-place lines; verbose prints every event.",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    path_keys = {
        "config",
        "pumpportal_jsonl",
        "pumpportal_script",
        "capture_script",
        "risk_report_script",
        "dexscreener_script",
        "website_grader_script",
        "telegram_info_script",
        "state_path",
        "status_jsonl_path",
        "stage_log_root",
        "capture_out_root",
        "risk_analysis_root",
        "risk_export_root",
        "red_flag_config_path",
        "red_flag_audit_root",
        "dexscreener_out_root",
        "post_migration_dex_out_root",
        "website_output_root",
    }
    parser.set_defaults(**coerce_argparse_defaults(config_defaults, path_keys))
    return parser


def config_from_args(args: argparse.Namespace) -> OrchestratorConfig:
    post_dex_requests_per_snapshot = args.post_migration_dex_requests_per_snapshot
    if args.post_migration_dex_endpoint_profile == "full" and post_dex_requests_per_snapshot == 1:
        post_dex_requests_per_snapshot = 8

    return OrchestratorConfig(
        pumpportal_jsonl_path=args.pumpportal_jsonl,
        pumpportal_script_path=args.pumpportal_script,
        capture_script_path=args.capture_script,
        risk_report_script_path=args.risk_report_script,
        dexscreener_script_path=args.dexscreener_script,
        website_grader_script_path=args.website_grader_script,
        telegram_info_script_path=args.telegram_info_script,
        python_executable=args.python_executable,
        state_path=args.state_path,
        run_pumpportal=not args.no_pumpportal,
        pumpportal_duration_seconds=args.pumpportal_duration_seconds,
        pumpportal_display=args.pumpportal_display,
        pumpportal_jsonl_wait_seconds=args.pumpportal_jsonl_wait_seconds,
        pumpportal_auto_restart=not args.no_pumpportal_auto_restart,
        pumpportal_restart_delay_seconds=args.pumpportal_restart_delay_seconds,
        pumpportal_jsonl_stale_restart_seconds=args.pumpportal_jsonl_stale_restart_seconds,
        max_concurrent_coins=args.max_concurrent_coins,
        scan_poll_seconds=args.scan_poll_seconds,
        metrics_log_seconds=args.metrics_log_seconds,
        start_at_end=args.start_at_end,
        run_once=args.run_once,
        dry_run=args.dry_run,
        dry_run_dex_website=args.dry_run_dex_website,
        retry_failed=args.retry_failed,
        status_jsonl_path=None if args.no_status_jsonl else args.status_jsonl_path,
        stage_log_root=None if args.no_stage_log_files else args.stage_log_root,
        pumpportal_command_template=args.pumpportal_command_template,
        capture_command_template=args.capture_command_template,
        risk_command_template=args.risk_command_template,
        dexscreener_command_template=args.dexscreener_command_template,
        website_command_template=args.website_command_template,
        telegram_command_template=args.telegram_command_template,
        capture_duration_seconds=args.capture_duration_seconds,
        capture_timeout_grace_seconds=args.capture_timeout_grace_seconds,
        capture_out_root=args.capture_out_root,
        capture_network_sample_seconds=args.capture_network_sample_seconds,
        capture_network_sample_fee_addresses=args.capture_network_sample_fee_addresses,
        require_pair_for_capture=args.require_pair_for_capture,
        risk_analysis_root=args.risk_analysis_root,
        risk_export_root=args.risk_export_root,
        risk_http_timeout_seconds=args.risk_http_timeout_seconds,
        risk_subprocess_timeout_seconds=args.risk_subprocess_timeout_seconds,
        red_flag_filter_enabled=(
            bool(getattr(args, "red_flag_filter_enabled", True)) and not args.no_red_flag_filter
        ),
        red_flag_config_path=args.red_flag_config_path,
        red_flag_delete_rejected_capture_data=(
            bool(getattr(args, "red_flag_delete_rejected_capture_data", True))
            and not args.keep_red_flag_rejected_capture_data
        ),
        red_flag_audit_root=args.red_flag_audit_root,
        red_flag_save_audit_artifacts=(
            bool(getattr(args, "red_flag_save_audit_artifacts", True))
            and not args.no_red_flag_audit
        ),
        dexscreener_out_root=args.dexscreener_out_root,
        dexscreener_chain=args.dexscreener_chain,
        dexscreener_initial_wait_seconds=args.dexscreener_initial_wait_seconds,
        dexscreener_max_wait_seconds=args.dexscreener_max_wait_seconds,
        dexscreener_backoff_multiplier=args.dexscreener_backoff_multiplier,
        dexscreener_max_attempts=args.dexscreener_max_attempts,
        dexscreener_script_sleep_seconds=args.dexscreener_script_sleep_seconds,
        dexscreener_subprocess_timeout_seconds=args.dexscreener_subprocess_timeout_seconds,
        max_post_migration_tracked_coins=args.max_post_migration_tracked_coins,
        post_migration_dex_tracking_hours=args.post_migration_dex_tracking_hours,
        post_migration_dex_requests_per_minute=args.post_migration_dex_requests_per_minute,
        post_migration_dex_requests_per_snapshot=post_dex_requests_per_snapshot,
        post_migration_dex_endpoint_profile=args.post_migration_dex_endpoint_profile,
        post_migration_dex_request_sleep_seconds=args.post_migration_dex_request_sleep_seconds,
        post_migration_dex_out_root=args.post_migration_dex_out_root,
        post_migration_dex_timestamped_raw=not args.no_post_migration_dex_raw_history,
        website_output_root=args.website_output_root,
        website_subprocess_timeout_seconds=args.website_subprocess_timeout_seconds,
        telegram_subprocess_timeout_seconds=args.telegram_subprocess_timeout_seconds,
        telegram_join=not args.no_telegram_join,
        pumpportal_extra_args=tuple(args.pumpportal_extra_arg),
        capture_extra_args=tuple(args.capture_extra_arg),
        risk_extra_args=tuple(args.risk_extra_arg),
        dexscreener_extra_args=tuple(args.dexscreener_extra_arg),
        website_extra_args=tuple(args.website_extra_arg),
        telegram_extra_args=tuple(args.telegram_extra_arg),
    )


async def async_main(argv: Optional[Sequence[str]] = None) -> int:
    config_parser = argparse.ArgumentParser(add_help=False)
    config_parser.add_argument("--config", type=Path, default=DEFAULT_ORCH_CONFIG_PATH)
    config_args, _ = config_parser.parse_known_args(argv)
    parser = build_parser(load_json_config_defaults(config_args.config))
    args = parser.parse_args(argv)
    logger = setup_logging(
        args.log_level,
        console_format=args.console_format,
        console_display=args.console_display,
    )

    try:
        config = config_from_args(args)
        orchestrator = MemeCoinPipelineOrchestrator(config=config, logger=logger)
        await orchestrator.run()
        return 0
    except Exception as exc:
        if not getattr(args, "no_status_jsonl", False):
            with contextlib.suppress(Exception):
                append_jsonl(
                    args.status_jsonl_path,
                    {
                        "ts_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "event_type": "orchestrator_fatal_error",
                        "error": repr(exc),
                        "traceback": traceback.format_exc(),
                    },
                )
        logger.error(
            "orchestrator_fatal_error",
            extra={
                "event": "orchestrator_fatal_error",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            },
            exc_info=True,
        )
        return 1


def main() -> int:
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        print("Shutdown requested.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
