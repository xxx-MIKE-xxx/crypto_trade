"""Pipeline configuration assembled from CLI args and environment variables."""

from __future__ import annotations

import argparse
import dataclasses
import os
from pathlib import Path


@dataclasses.dataclass(frozen=True)
class PipelineConfig:
    repo_root: Path
    data_root: Path
    pumpportal_ws_url: str
    pumpportal_api_key: str | None
    chain_id: str
    capture_seconds: int
    dex_poll_seconds: float
    dex_timeout_seconds: float
    max_concurrent_tokens: int
    parquet_batch_size: int
    command_timeout_seconds: float
    heartbeat_seconds: float
    dry_run: bool

    @classmethod
    def from_env(cls, args: argparse.Namespace) -> "PipelineConfig":
        repo_root = Path(args.repo_root or os.getenv("PIPELINE_REPO_ROOT", ".")).resolve()
        data_root = Path(
            args.data_root or os.getenv("PIPELINE_DATA_ROOT", str(repo_root / "data"))
        ).resolve()
        api_key = os.getenv("PUMPPORTAL_API_KEY") or None
        base_ws = os.getenv("PUMPPORTAL_WS_URL", "wss://pumpportal.fun/api/data")
        return cls(
            repo_root=repo_root,
            data_root=data_root,
            pumpportal_ws_url=base_ws,
            pumpportal_api_key=api_key,
            chain_id=os.getenv("PIPELINE_CHAIN_ID", "solana"),
            capture_seconds=int(
                os.getenv("PIPELINE_CAPTURE_SECONDS", str(args.capture_seconds))
            ),
            dex_poll_seconds=float(
                os.getenv("PIPELINE_DEX_POLL_SECONDS", str(args.dex_poll_seconds))
            ),
            dex_timeout_seconds=float(
                os.getenv("PIPELINE_DEX_TIMEOUT_SECONDS", str(args.dex_timeout_seconds))
            ),
            max_concurrent_tokens=int(
                os.getenv("PIPELINE_MAX_TOKENS", str(args.max_concurrent_tokens))
            ),
            parquet_batch_size=int(
                os.getenv("PIPELINE_PARQUET_BATCH_SIZE", str(args.parquet_batch_size))
            ),
            command_timeout_seconds=float(
                os.getenv("PIPELINE_COMMAND_TIMEOUT_SECONDS", str(args.command_timeout_seconds))
            ),
            heartbeat_seconds=float(
                os.getenv("PIPELINE_HEARTBEAT_SECONDS", str(args.heartbeat_seconds))
            ),
            dry_run=bool(args.dry_run),
        )

    @property
    def pumpportal_url(self) -> str:
        if self.pumpportal_api_key and "api-key=" not in self.pumpportal_ws_url:
            sep = "&" if "?" in self.pumpportal_ws_url else "?"
            return f"{self.pumpportal_ws_url}{sep}api-key={self.pumpportal_api_key}"
        return self.pumpportal_ws_url

    @property
    def state_db_path(self) -> Path:
        return self.data_root / "pipeline_state.sqlite3"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run PumpPortal -> Helius -> DexScreener data acquisition pipeline.",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repo root. Defaults to PIPELINE_REPO_ROOT or current directory.",
    )
    parser.add_argument(
        "--data-root",
        default=None,
        help="Data root. Defaults to PIPELINE_DATA_ROOT or <repo>/data.",
    )
    parser.add_argument("--capture-seconds", type=int, default=3600)
    parser.add_argument("--dex-poll-seconds", type=float, default=10.0)
    parser.add_argument("--dex-timeout-seconds", type=float, default=3600.0)
    parser.add_argument("--max-concurrent-tokens", type=int, default=4)
    parser.add_argument("--parquet-batch-size", type=int, default=500)
    parser.add_argument("--command-timeout-seconds", type=float, default=600.0)
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=15.0,
        help="Print websocket heartbeat if no PumpPortal messages arrive in this many seconds.",
    )
    parser.add_argument(
        "--simulate-migration",
        default=None,
        help="Bypass PumpPortal and run the downstream worker for this mint. "
        "Useful for smoke-testing child scripts.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not run child commands or write remote-side effects; still connects unless you stop it.",
    )
    return parser
