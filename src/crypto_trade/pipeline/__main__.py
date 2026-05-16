"""CLI entrypoint for the data acquisition pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import signal

from crypto_trade.core.logging import configure_logging
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.ingest.bronze import EventSink
from crypto_trade.pipeline.config import PipelineConfig, build_arg_parser
from crypto_trade.pipeline.mint import looks_like_solana_address
from crypto_trade.pipeline.orchestrator import migrated_token_worker
from crypto_trade.pipeline.pumpportal_loop import pumpportal_loop
from crypto_trade.pipeline.state import StateStore

try:
    import pyarrow  # noqa: F401
    _HAVE_PYARROW = True
except ImportError:  # pragma: no cover
    _HAVE_PYARROW = False


async def async_main() -> int:
    args = build_arg_parser().parse_args()
    cfg = PipelineConfig.from_env(args)
    cfg.data_root.mkdir(parents=True, exist_ok=True)

    if not (cfg.repo_root / "scripts").exists():
        raise SystemExit(f"Could not find scripts/ under repo root: {cfg.repo_root}")

    state = StateStore(cfg.state_db_path)
    sink = EventSink(cfg.data_root, batch_size=cfg.parquet_batch_size)
    stop = asyncio.Event()

    def request_stop() -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, request_stop)

    print(f"[pipeline] repo_root={cfg.repo_root}", flush=True)
    print(f"[pipeline] data_root={cfg.data_root}", flush=True)
    print(f"[pipeline] state_db={cfg.state_db_path}", flush=True)
    if not _HAVE_PYARROW:
        print(
            "[pipeline] WARNING: pyarrow not installed; falling back to JSONL. "
            "Install pyarrow for Parquet.",
            flush=True,
        )

    if args.simulate_migration:
        mint = str(args.simulate_migration).strip()
        if not looks_like_solana_address(mint):
            raise SystemExit(f"Invalid Solana mint for --simulate-migration: {mint}")
        print(f"[pipeline] simulate_migration mint={mint}", flush=True)
        try:
            await migrated_token_worker(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                migration_event={
                    "source": "manual_simulation",
                    "mint": mint,
                    "ts": utc_now_iso_ms_z(),
                },
                semaphore=asyncio.Semaphore(1),
            )
        finally:
            await sink.flush()
            state.close()
        return 0

    try:
        await pumpportal_loop(cfg, state, sink, stop)
    finally:
        await sink.flush()
        state.close()

    return 0


def main() -> None:
    configure_logging()
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
