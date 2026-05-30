from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Mapping

from crypto_trade.core.io import append_jsonl, iter_jsonl
from crypto_trade.core.time import utc_now, utc_now_iso_ms_z
from crypto_trade.ingest.bronze import EventSink
from crypto_trade.ingest.pumpportal import shared_listen
from crypto_trade.pipeline.config import PipelineConfig
from crypto_trade.pipeline.mint import classify_pumpportal_event, extract_mint
from crypto_trade.pipeline.orchestrator import migrated_token_worker
from crypto_trade.pipeline.state import StateStore

logger = logging.getLogger(__name__)


def pumpportal_file(cfg: PipelineConfig) -> Path:
    now = utc_now()
    return cfg.data_root / "raw" / "pumpportal" / f"{now:%Y-%m-%d}.jsonl"


def migrations_file(cfg: PipelineConfig) -> Path:
    return pumpportal_file(cfg)


def orchestrator_file(cfg: PipelineConfig) -> Path:
    now = utc_now()
    return cfg.data_root / "raw" / "orchestrator" / f"{now:%Y-%m-%d}.jsonl"


def write_orchestrator_event(
    cfg: PipelineConfig,
    event_type: str,
    payload: dict[str, Any],
    *,
    mint: str | None = None,
    level: str = "info",
) -> None:
    append_jsonl(
        orchestrator_file(cfg),
        {
            "time": utc_now_iso_ms_z(),
            "level": level,
            "event_type": event_type,
            "mint": mint,
            **payload,
        },
    )


def load_seen_mints_from_jsonl(path: Path) -> set[str]:
    seen: set[str] = set()
    if not path.exists():
        return seen
    for event in iter_jsonl(path):
        event_type = classify_pumpportal_event(event)
        if event_type in {"mint", "new_token"}:
            mint = extract_mint(event)
            if mint:
                seen.add(mint)
    return seen


def mint_exists_in_migration_file(path: Path, mint: str) -> bool:
    if not path.exists():
        return False
    for event in iter_jsonl(path):
        event_type = classify_pumpportal_event(event)
        if event_type in {"mint", "new_token"} and extract_mint(event) == mint:
            return True
    return False


async def write_sink_event(
    sink: EventSink,
    *,
    source: str,
    event_type: str,
    payload: Mapping[str, Any],
    mint: str | None = None,
    level: str = "info",
) -> None:
    await sink.write(
        source=source,
        event_type=event_type,
        token_mint=mint,
        payload=dict(payload),
        level=level,
    )


def cleanup_finished_tasks(tasks: set[asyncio.Task[Any]]) -> None:
    finished = {task for task in tasks if task.done()}
    for task in finished:
        tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.exception("Token worker crashed: %s", exc)


async def pumpportal_loop(
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    stop: asyncio.Event,
) -> None:
    event_path = pumpportal_file(cfg)
    event_path.parent.mkdir(parents=True, exist_ok=True)

    initial_offset = event_path.stat().st_size if event_path.exists() else 0
    seen_mints = load_seen_mints_from_jsonl(event_path)
    semaphore = asyncio.Semaphore(cfg.max_concurrent_tokens)
    worker_tasks: set[asyncio.Task[Any]] = set()

    write_orchestrator_event(
        cfg,
        "pumpportal_loop_started",
        {
            "pumpportal_file": str(event_path),
            "seen_mints_loaded": len(seen_mints),
            "initial_offset": initial_offset,
            "max_concurrent_tokens": cfg.max_concurrent_tokens,
        },
    )

    await write_sink_event(
        sink,
        source="pipeline",
        event_type="pumpportal_loop_started",
        payload={
            "pumpportal_file": str(event_path),
            "seen_mints_loaded": len(seen_mints),
            "initial_offset": initial_offset,
            "max_concurrent_tokens": cfg.max_concurrent_tokens,
        },
    )

    last_event_time = asyncio.get_running_loop().time()

    try:
        async for event, _offset in shared_listen(
            path=event_path,
            owner="main_pipeline",
            url=cfg.pumpportal_url,
            initial_offset=initial_offset,
            poll_seconds=1,
        ):
            if stop.is_set():
                break

            cleanup_finished_tasks(worker_tasks)
            now = asyncio.get_running_loop().time()
            if now - last_event_time >= cfg.heartbeat_seconds:
                write_orchestrator_event(
                    cfg,
                    "pumpportal_heartbeat",
                    {"active_workers": len(worker_tasks), "seen_mints": len(seen_mints)},
                )
                last_event_time = now

            event_type = classify_pumpportal_event(event)
            mint = extract_mint(event)

            if mint:
                is_new_event = state.record_seen_event(
                    source="pumpportal",
                    event_type=event_type,
                    mint=mint,
                    event_ts=str(event.get("time") or utc_now_iso_ms_z()),
                    payload=event,
                )
            else:
                is_new_event = True

            await write_sink_event(
                sink,
                source="pumpportal",
                event_type=event_type,
                mint=mint,
                payload=event,
            )

            if not mint:
                write_orchestrator_event(
                    cfg,
                    "pumpportal_event_missing_mint",
                    {"event_type": event_type},
                    level="warning",
                )
                continue

            if event_type in {"mint", "new_token"}:
                seen_mints.add(mint)
                state.upsert_new_token(mint, cfg.data_root / "raw" / "migrations" / mint)
                write_orchestrator_event(cfg, "mint_seen", {"event_type": event_type}, mint=mint)
                continue

            if event_type != "migration":
                continue

            if not is_new_event:
                write_orchestrator_event(
                    cfg,
                    "migration_duplicate_skipped",
                    {"event_type": event_type},
                    mint=mint,
                )
                continue

            if not (mint in seen_mints or mint_exists_in_migration_file(event_path, mint)):
                write_orchestrator_event(
                    cfg,
                    "migration_skipped_missing_mint_event",
                    {"reason": "matching mint/create event was not captured"},
                    mint=mint,
                    level="warning",
                )
                await write_sink_event(
                    sink,
                    source="pipeline",
                    event_type="migration_skipped_missing_mint_event",
                    mint=mint,
                    payload={"reason": "matching mint/create event was not captured"},
                    level="warning",
                )
                continue

            write_orchestrator_event(cfg, "migration_accepted_worker_started", {}, mint=mint)
            task = asyncio.create_task(
                migrated_token_worker(
                    cfg=cfg,
                    state=state,
                    sink=sink,
                    mint=mint,
                    migration_event=event,
                    semaphore=semaphore,
                )
            )
            worker_tasks.add(task)

    finally:
        write_orchestrator_event(
            cfg,
            "pumpportal_loop_stopping",
            {"active_workers": len(worker_tasks)},
        )
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        await sink.flush()
        write_orchestrator_event(cfg, "pumpportal_loop_stopped", {"active_workers": 0})
