#!/usr/bin/env python3
"""
meme_coin_pipeline_orchestrator.py

Production orchestrator for a PumpPortal -> migrated meme coin acquisition pipeline.

It tails the PumpPortal JSONL file, accepts only migrations whose mint was seen in a
previous new-token/mint event, and runs the downstream scripts for at most N coins
concurrently.

Default stage order per accepted coin:
  1. Solana 1h capture
  2. Solana risk report
  3. DexScreener polling until token-pair data is available
  4. Website grader if DexScreener exposes a website link

The orchestrator never uses shell=True. All script calls are executed as argv lists.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import json
import logging
import os
import random
import signal
import shlex
import sys
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, TextIO
from urllib.parse import urlparse


LOGGER_NAME = "meme_coin_orchestrator"


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


def setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
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


def default_pumpportal_jsonl() -> Path:
    day = datetime.now(timezone.utc).date().isoformat()
    return Path("data") / "raw" / "migrations" / f"{day}.jsonl"


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
    pumpportal_display: str = "bar"
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

    risk_analysis_root: Path = Path("data/raw/analytics")
    risk_export_root: Path = Path("data/raw/orchestrator/risk_reports")
    risk_http_timeout_seconds: int = 15
    risk_subprocess_timeout_seconds: int = 240

    dexscreener_out_root: Path = Path("data/raw/analytics")
    dexscreener_chain: str = "solana"
    dexscreener_initial_wait_seconds: float = 60.0
    dexscreener_max_wait_seconds: float = 300.0
    dexscreener_backoff_multiplier: float = 1.35
    dexscreener_max_attempts: int = 30
    dexscreener_script_sleep_seconds: float = 1.1
    dexscreener_subprocess_timeout_seconds: int = 180

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

    def to_dict(self, active_count: int, tracked_count: int) -> dict[str, Any]:
        data = dataclasses.asdict(self)
        data.update(
            {
                "active_coin_pipelines": active_count,
                "currently_tracked_mints": tracked_count,
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
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(make_json_safe(payload), indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    tmp.replace(path)


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
    reader: asyncio.StreamReader,
    *,
    logger: logging.Logger,
    mint: str,
    stage: str,
    stream_name: str,
    stage_log_file: Optional[TextIO] = None,
) -> None:
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

    proc = await asyncio.create_subprocess_exec(
        *[str(arg) for arg in command],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(cwd) if cwd else None,
        start_new_session=(os.name != "nt"),
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
        returncode = await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        await terminate_process(proc, logger)
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        if stage_log_file is not None:
            stage_log_file.close()
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
        self.active_tasks: dict[str, asyncio.Task[None]] = {}
        self.pending_contexts: deque[CoinContext] = deque()
        self.pending_mints: set[str] = set()
        self.current_stage: dict[str, str] = {}

        self.load_state()

    @property
    def terminal_mints(self) -> set[str]:
        if self.config.retry_failed:
            return set(self.completed_mints)
        return self.completed_mints | self.failed_mints

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

        self.logger.info(
            "state_loaded",
            extra={
                "event": "state_loaded",
                "path": str(path),
                "file_offset": self.file_offset,
                "seen_mints": len(self.seen_mints),
                "completed_mints": len(self.completed_mints),
                "failed_mints": len(self.failed_mints),
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
            "pending_mints": sorted(self.pending_mints),
            "metrics": self.metrics.to_dict(
                active_count=len(self.active_tasks),
                tracked_count=len(self.seen_mints),
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
        if missing and not self.config.dry_run:
            raise FileNotFoundError(f"Missing script path(s): {missing}")
        if missing and self.config.dry_run:
            self.logger.warning(
                "dry_run_missing_script_paths_ignored",
                extra={"event": "dry_run_missing_script_paths_ignored", "missing": missing},
            )

    async def start_pumpportal_process(
        self,
    ) -> tuple[asyncio.subprocess.Process, list[asyncio.Task[None]], Optional[TextIO]]:
        default_command = [
            self.config.python_executable,
            str(self.config.pumpportal_script_path),
            "--duration",
            str(self.config.pumpportal_duration_seconds),
            "--display",
            self.config.pumpportal_display,
        ]
        command = build_template_command(
            self.config.pumpportal_command_template,
            default_command=default_command,
            replacements={
                "python": self.config.python_executable,
                "script": self.config.pumpportal_script_path,
                "duration_seconds": self.config.pumpportal_duration_seconds,
                "display": self.config.pumpportal_display,
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

        proc = await asyncio.create_subprocess_exec(
            *[str(arg) for arg in command],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=(os.name != "nt"),
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

    async def run(self) -> None:
        self.validate_script_paths()
        self.install_signal_handlers()

        if self.config.start_at_end and not self.config.state_path.exists():
            self.initialize_offset_to_end()

        metrics_task = asyncio.create_task(self.metrics_reporter())
        pumpportal_proc: Optional[asyncio.subprocess.Process] = None
        pumpportal_log_tasks: list[asyncio.Task[None]] = []
        pumpportal_log_file: Optional[TextIO] = None

        try:
            if self.config.run_pumpportal and not self.config.dry_run:
                pumpportal_proc, pumpportal_log_tasks, pumpportal_log_file = (
                    await self.start_pumpportal_process()
                )

            await self.scan_loop()

            if self.config.run_once:
                await self.wait_for_active_tasks()
        finally:
            self.shutdown_event.set()
            if pumpportal_proc is not None:
                await terminate_process(pumpportal_proc, self.logger)
                await asyncio.gather(*pumpportal_log_tasks, return_exceptions=True)
                if pumpportal_log_file is not None:
                    pumpportal_log_file.close()

            metrics_task.cancel()
            await asyncio.gather(metrics_task, return_exceptions=True)

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
                    ),
                },
            )

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def request_shutdown() -> None:
            self.logger.warning("shutdown_requested", extra={"event": "shutdown_requested"})
            self.shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, request_shutdown)

    def initialize_offset_to_end(self) -> None:
        path = self.config.pumpportal_jsonl_path
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
        self.logger.info(
            "scan_started",
            extra={
                "event": "scan_started",
                "pumpportal_jsonl_path": str(self.config.pumpportal_jsonl_path),
                "max_concurrent_coins": self.config.max_concurrent_coins,
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
        path = self.config.pumpportal_jsonl_path

        if not path.exists():
            self.logger.info(
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

        if mint in self.active_tasks or mint in self.pending_mints or mint in self.terminal_mints:
            self.metrics.duplicate_migrations += 1
            self.logger.info(
                "migration_duplicate_ignored",
                extra={
                    "event": "migration_duplicate_ignored",
                    "coin": mint,
                    "active": mint in self.active_tasks,
                    "pending": mint in self.pending_mints,
                    "completed": mint in self.completed_mints,
                    "failed": mint in self.failed_mints,
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
        self.metrics.coins_started += 1

        if len(self.active_tasks) >= self.config.max_concurrent_coins:
            self.pending_contexts.append(ctx)
            self.pending_mints.add(ctx.mint)
            self.logger.info(
                "migration_queued_due_to_concurrency",
                extra={
                    "event": "migration_queued_due_to_concurrency",
                    "coin": mint,
                    "active_coin_pipelines": len(self.active_tasks),
                    "pending_coin_pipelines": len(self.pending_contexts),
                    "max_concurrent_coins": self.config.max_concurrent_coins,
                },
            )
            self.emit_status_event(
                "coin_queued_capacity",
                mint=mint,
                active_coin_pipelines=len(self.active_tasks),
                pending_coin_pipelines=len(self.pending_contexts),
                max_concurrent_coins=self.config.max_concurrent_coins,
            )
            return

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

    def start_pending_tasks(self) -> None:
        while self.pending_contexts and len(self.active_tasks) < self.config.max_concurrent_coins:
            ctx = self.pending_contexts.popleft()
            self.pending_mints.discard(ctx.mint)
            if ctx.mint in self.active_tasks or ctx.mint in self.terminal_mints:
                continue
            self.start_coin_task(ctx)

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
            self.start_pending_tasks()
            self.save_state()

    async def run_coin_pipeline(self, ctx: CoinContext) -> None:
        await self.run_capture(ctx)
        await self.run_risk_report(ctx)
        dex_result = await self.poll_dexscreener(ctx)

        enrichment_tasks: list[asyncio.Task[None]] = []
        if dex_result.website_url:
            enrichment_tasks.append(
                asyncio.create_task(self.run_website_grader(ctx, dex_result))
            )
        else:
            self.current_stage[ctx.mint] = "website_grader_skipped_no_website"
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

        if enrichment_tasks:
            await asyncio.gather(*enrichment_tasks)

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
        ]

        pair_args: list[str] = []
        for pair in ctx.pair_addresses:
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
            },
            list_replacements={"pair_args": pair_args},
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
        self.current_stage[ctx.mint] = stage

        risk_export_path = self.config.risk_export_root / f"{ctx.mint}.json"

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

    async def poll_dexscreener(self, ctx: CoinContext) -> DexScreenerResult:
        stage = "dexscreener_poll"
        self.current_stage[ctx.mint] = stage

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
        self.current_stage[ctx.mint] = stage
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
        self.current_stage[ctx.mint] = stage

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Orchestrate PumpPortal migrated-coin acquisition pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

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

    parser.add_argument("--pumpportal-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {duration_seconds} {display}.")
    parser.add_argument("--capture-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {duration_seconds} {capture_out_root} {pair_args}.")
    parser.add_argument("--risk-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {risk_analysis_root} {risk_export_path} {risk_http_timeout_seconds}.")
    parser.add_argument("--dexscreener-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {chain} {dexscreener_out_root} {dexscreener_script_sleep_seconds}.")
    parser.add_argument("--website-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {website_url} {metadata_json} {website_output_root} {telegram_url} {x_url}.")
    parser.add_argument("--telegram-command-template", default=None, help="Optional shell-style argv template. Placeholders: {python} {script} {mint} {telegram_url} {migration_time_utc}.")

    parser.add_argument("--capture-duration-seconds", type=int, default=3600)
    parser.add_argument("--capture-timeout-grace-seconds", type=int, default=900)
    parser.add_argument("--capture-out-root", type=Path, default=Path("data/raw/onchain"))
    parser.add_argument("--require-pair-for-capture", action="store_true")

    parser.add_argument("--risk-analysis-root", type=Path, default=Path("data/raw/analytics"))
    parser.add_argument("--risk-export-root", type=Path, default=Path("data/raw/orchestrator/risk_reports"))
    parser.add_argument("--risk-http-timeout-seconds", type=int, default=15)
    parser.add_argument("--risk-subprocess-timeout-seconds", type=int, default=240)

    parser.add_argument("--dexscreener-out-root", type=Path, default=Path("data/raw/analytics"))
    parser.add_argument("--dexscreener-chain", default="solana")
    parser.add_argument("--dexscreener-initial-wait-seconds", type=float, default=60.0)
    parser.add_argument("--dexscreener-max-wait-seconds", type=float, default=300.0)
    parser.add_argument("--dexscreener-backoff-multiplier", type=float, default=1.35)
    parser.add_argument("--dexscreener-max-attempts", type=int, default=30)
    parser.add_argument("--dexscreener-script-sleep-seconds", type=float, default=1.1)
    parser.add_argument("--dexscreener-subprocess-timeout-seconds", type=int, default=180)

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

    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return parser


def config_from_args(args: argparse.Namespace) -> OrchestratorConfig:
    if args.max_concurrent_coins != 2:
        raise ValueError("Requirement is a hard cap of 2 concurrent coins; keep --max-concurrent-coins=2.")

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
        require_pair_for_capture=args.require_pair_for_capture,
        risk_analysis_root=args.risk_analysis_root,
        risk_export_root=args.risk_export_root,
        risk_http_timeout_seconds=args.risk_http_timeout_seconds,
        risk_subprocess_timeout_seconds=args.risk_subprocess_timeout_seconds,
        dexscreener_out_root=args.dexscreener_out_root,
        dexscreener_chain=args.dexscreener_chain,
        dexscreener_initial_wait_seconds=args.dexscreener_initial_wait_seconds,
        dexscreener_max_wait_seconds=args.dexscreener_max_wait_seconds,
        dexscreener_backoff_multiplier=args.dexscreener_backoff_multiplier,
        dexscreener_max_attempts=args.dexscreener_max_attempts,
        dexscreener_script_sleep_seconds=args.dexscreener_script_sleep_seconds,
        dexscreener_subprocess_timeout_seconds=args.dexscreener_subprocess_timeout_seconds,
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
    parser = build_parser()
    args = parser.parse_args(argv)
    logger = setup_logging(args.log_level)

    try:
        config = config_from_args(args)
        orchestrator = MemeCoinPipelineOrchestrator(config=config, logger=logger)
        await orchestrator.run()
        return 0
    except Exception as exc:
        logger.error(
            "orchestrator_fatal_error",
            extra={
                "event": "orchestrator_fatal_error",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            },
        )
        return 1


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
