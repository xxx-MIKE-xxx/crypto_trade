#!/usr/bin/env python3
"""
test_pumpportal_ws_env.py

Reads PumpPortal API key from local .env in the same folder.

Create .env next to this script:
  PUMPPORTAL_API_KEY=your_key_here

Run:
  pip install websockets
  python test_pumpportal_ws_env.py

Default:
  Shows aggregate metrics only.

Other modes:
  python test_pumpportal_ws_env.py --display all
  python test_pumpportal_ws_env.py --display bar

Run indefinitely:
  python test_pumpportal_ws_env.py --duration 0 --display bar
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets


ENV_FILE = ".env"
DEFAULT_DURATION_SECONDS = 120
RAW_SUBDIR = Path("data") / "raw" / "migrations"


@dataclass
class StreamMetrics:
    messages_total: int = 0

    new_token_events: int = 0
    migration_events: int = 0

    unique_new_token_mints: set[str] = field(default_factory=set)
    unique_migration_mints: set[str] = field(default_factory=set)

    subscription_ack_messages: int = 0
    control_messages: int = 0
    permission_errors: int = 0
    errors: int = 0
    unknown_messages: int = 0
    decode_errors: int = 0

    started_at: float = field(default_factory=time.time)

    def update(self, raw_row: dict[str, Any]) -> None:
        self.messages_total += 1

        event_type = str(raw_row.get("event_type") or "unknown")
        mint = raw_row.get("mint")

        if event_type == "new_token":
            self.new_token_events += 1
            if isinstance(mint, str) and mint:
                self.unique_new_token_mints.add(mint)
            return

        if event_type == "migration":
            self.migration_events += 1
            if isinstance(mint, str) and mint:
                self.unique_migration_mints.add(mint)
            return

        if event_type == "subscription_ack":
            self.subscription_ack_messages += 1
            return

        if event_type == "control_message":
            self.control_messages += 1
            return

        if event_type == "permission_error":
            self.permission_errors += 1
            return

        if event_type == "error":
            self.errors += 1
            return

        if event_type == "decode_error":
            self.decode_errors += 1
            return

        if event_type == "unknown":
            self.unknown_messages += 1
            return

    @property
    def unique_new_token_count(self) -> int:
        return len(self.unique_new_token_mints)

    @property
    def unique_migration_count(self) -> int:
        return len(self.unique_migration_mints)

    @property
    def observed_migration_rate(self) -> float:
        if self.unique_new_token_count == 0:
            return 0.0
        return self.unique_migration_count / self.unique_new_token_count

    def as_dict(self) -> dict[str, int | float]:
        elapsed = max(time.time() - self.started_at, 0.001)

        return {
            "messages_total": self.messages_total,
            "new_token_events": self.new_token_events,
            "migration_events": self.migration_events,
            "unique_new_token_mints": self.unique_new_token_count,
            "unique_migration_mints": self.unique_migration_count,
            "observed_migration_rate_pct": self.observed_migration_rate * 100.0,
            "subscription_ack_messages": self.subscription_ack_messages,
            "control_messages": self.control_messages,
            "permission_errors": self.permission_errors,
            "errors": self.errors,
            "unknown_messages": self.unknown_messages,
            "decode_errors": self.decode_errors,
            "messages_per_second": self.messages_total / elapsed,
        }


def load_local_env(filename: str = ENV_FILE) -> dict[str, str]:
    path = Path(__file__).resolve().parent / filename
    values: dict[str, str] = {}

    if not path.exists():
        print(f"WARNING: {filename} not found next to script; connecting without API key.")
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def utc_iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def default_raw_jsonl_path(repo_root: Path | None = None) -> Path:
    root = repo_root or Path(__file__).resolve().parent
    raw_dir = root / RAW_SUBDIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    current_day = datetime.now(timezone.utc).date().isoformat()
    return raw_dir / f"{current_day}.jsonl"


def build_raw_file_path(repo_root: Path) -> Path:
    """Backward-compatible alias for default_raw_jsonl_path."""
    return default_raw_jsonl_path(repo_root)


def extract_mint(data: dict[str, Any]) -> str | None:
    for key in ("mint", "tokenMint", "token_mint", "mintAddress", "ca"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def extract_signature(data: dict[str, Any]) -> str | None:
    for key in ("signature", "txSignature", "transactionSignature", "sig"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def detect_event_type(data: dict[str, Any]) -> str:
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

    if tx_type in {"create", "new_token", "newtoken"}:
        return "new_token"

    if tx_type in {"migrate", "migration"}:
        return "migration"

    if "migrat" in tx_type:
        return "migration"

    if tx_type:
        return tx_type

    return "unknown"


def normalize_message(msg: str) -> dict[str, Any]:
    try:
        data = json.loads(msg)
    except json.JSONDecodeError:
        return {
            "_decode_error": True,
            "raw_message": msg,
        }

    if not isinstance(data, dict):
        return {
            "_non_object_message": True,
            "raw_message": data,
        }

    return data


def build_raw_row(data: dict[str, Any], received_at_ms: int) -> dict[str, Any]:
    return {
        "received_at_ms": received_at_ms,
        "received_at_iso_utc": utc_iso_from_ms(received_at_ms),
        "source": "pumpportal",
        "event_type": detect_event_type(data),
        "mint": extract_mint(data),
        "signature": extract_signature(data),
        "data": data,
    }


def print_metrics(metrics: StreamMetrics, raw_path: Path) -> None:
    snapshot = metrics.as_dict()

    print()
    print("=" * 72)
    print("PumpPortal stream metrics")
    print("=" * 72)
    print(f"raw_file:                    {raw_path}")
    print(f"messages_total:              {snapshot['messages_total']}")
    print(f"new_token_events:            {snapshot['new_token_events']}")
    print(f"migration_events:            {snapshot['migration_events']}")
    print(f"unique_new_token_mints:      {snapshot['unique_new_token_mints']}")
    print(f"unique_migration_mints:      {snapshot['unique_migration_mints']}")
    print(f"observed_migration_rate_pct: {snapshot['observed_migration_rate_pct']:.4f}")
    print(f"subscription_ack_messages:   {snapshot['subscription_ack_messages']}")
    print(f"control_messages:            {snapshot['control_messages']}")
    print(f"permission_errors:           {snapshot['permission_errors']}")
    print(f"errors:                      {snapshot['errors']}")
    print(f"unknown_messages:            {snapshot['unknown_messages']}")
    print(f"decode_errors:               {snapshot['decode_errors']}")
    print(f"messages_per_second:         {snapshot['messages_per_second']:.4f}")


def format_status_bar(
    metrics: StreamMetrics,
    elapsed: float,
    duration_seconds: int,
) -> str:
    columns = shutil.get_terminal_size((100, 20)).columns

    if duration_seconds > 0:
        progress = min(elapsed / duration_seconds, 1.0)
        bar_width = 24
        filled = int(progress * bar_width)
        bar = "[" + "#" * filled + "-" * (bar_width - filled) + "]"
        time_part = f"{elapsed:6.1f}s/{duration_seconds}s"
    else:
        spinner = "|/-\\"[metrics.messages_total % 4]
        bar = f"[{spinner}]"
        time_part = f"{elapsed:6.1f}s"

    text = (
        f"{bar} {time_part} "
        f"msgs={metrics.messages_total} "
        f"mints={metrics.new_token_events} "
        f"uniq_mints={metrics.unique_new_token_count} "
        f"migrations={metrics.migration_events} "
        f"uniq_migr={metrics.unique_migration_count} "
        f"perm_err={metrics.permission_errors} "
        f"unknown={metrics.unknown_messages}"
    )

    if len(text) >= columns:
        text = text[: columns - 1]

    return text


def print_bar(metrics: StreamMetrics, duration_seconds: int) -> None:
    elapsed = time.time() - metrics.started_at
    line = format_status_bar(
        metrics=metrics,
        elapsed=elapsed,
        duration_seconds=duration_seconds,
    )

    sys.stdout.write("\r\x1b[2K" + line)
    sys.stdout.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        help="How long to stream in seconds. Use 0 to run indefinitely.",
    )

    parser.add_argument(
        "--display",
        choices=("metrics", "all", "bar"),
        default="metrics",
        help=(
            "metrics: aggregate metrics only, "
            "all: print every raw event, "
            "bar: live updating one-line metric bar"
        ),
    )

    parser.add_argument(
        "--metrics-every",
        type=float,
        default=5.0,
        help="How often to print aggregate metrics in metrics mode.",
    )

    parser.add_argument(
        "--raw-jsonl",
        type=Path,
        default=None,
        help=(
            "JSONL output path. Parent directories are created if needed. "
            "Default: data/raw/migrations/<UTC-date>.jsonl under the script directory."
        ),
    )

    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent

    env = load_local_env()
    api_key = env.get("PUMPPORTAL_API_KEY", "")

    url = "wss://pumpportal.fun/api/data"
    if api_key:
        url += f"?api-key={api_key}"

    if args.raw_jsonl is not None:
        raw_path = args.raw_jsonl.expanduser().resolve()
        raw_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        raw_path = default_raw_jsonl_path(repo_root)
    metrics = StreamMetrics()

    print("listening to PumpPortal WebSocket")
    print(f"writing raw JSONL to: {raw_path}")
    print(f"display mode: {args.display}")

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"method": "subscribeMigration"}))
        await ws.send(json.dumps({"method": "subscribeNewToken"}))

        start = time.time()
        last_metrics_print = start

        with raw_path.open("a", encoding="utf-8") as out:
            while args.duration <= 0 or time.time() - start < args.duration:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    if args.display == "bar":
                        print_bar(metrics, args.duration)
                    else:
                        print("no message in 30s; still connected")
                    continue

                received_at_ms = now_ms()
                data = normalize_message(msg)
                raw_row = build_raw_row(data=data, received_at_ms=received_at_ms)

                out.write(
                    json.dumps(
                        raw_row,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
                out.write("\n")
                out.flush()

                metrics.update(raw_row)

                if args.display == "all":
                    print(json.dumps(raw_row, ensure_ascii=False))

                elif args.display == "bar":
                    print_bar(metrics, args.duration)

                else:
                    now = time.time()
                    if now - last_metrics_print >= args.metrics_every:
                        print_metrics(metrics, raw_path)
                        last_metrics_print = now

    if args.display == "bar":
        print()

    print_metrics(metrics, raw_path)
    print("done")


if __name__ == "__main__":
    asyncio.run(main())