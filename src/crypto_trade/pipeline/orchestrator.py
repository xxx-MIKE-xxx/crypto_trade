from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, Callable, Mapping

from crypto_trade.core.io import append_jsonl
from crypto_trade.core.paths import CONFIG_DIR
from crypto_trade.core.time import now_ms, utc_now_iso_ms_z
from crypto_trade.core.yaml import load_yaml
from crypto_trade.ingest import dexscreener, onchain, red_flags, security_api, telegram_info, twitter
from crypto_trade.ingest import website_grader
from crypto_trade.ingest.bronze import EventSink
from crypto_trade.pipeline.config import PipelineConfig
from crypto_trade.pipeline.mint import looks_like_solana_address
from crypto_trade.pipeline.state import StateStore

ORCHESTRATOR_CONFIG_PATH = CONFIG_DIR / "orchestrator.yaml"

PAIR_KEYS = {
    "pair",
    "pairaddress",
    "pool",
    "pooladdress",
    "raydiumpool",
    "raydiumpooladdress",
    "amm",
    "ammid",
    "market",
    "marketid",
}


def load_orchestrator_config() -> dict[str, Any]:
    return load_yaml(ORCHESTRATOR_CONFIG_PATH)


def analytics_dir(cfg: PipelineConfig, mint: str) -> Path:
    return cfg.data_root / "raw" / "analytics" / mint


def onchain_dir(cfg: PipelineConfig, mint: str) -> Path:
    return cfg.data_root / "raw" / "onchain" / mint


def orchestrator_log_path(cfg: PipelineConfig) -> Path:
    return cfg.data_root / "raw" / "orchestrator" / f"{utc_now_iso_ms_z()[:10]}.jsonl"


async def log_event(
    cfg: PipelineConfig,
    sink: EventSink,
    *,
    event_type: str,
    mint: str,
    payload: dict[str, Any] | None = None,
    level: str = "info",
) -> None:
    row = {
        "time": utc_now_iso_ms_z(),
        "level": level,
        "event_type": event_type,
        "mint": mint,
        **(payload or {}),
    }

    append_jsonl(orchestrator_log_path(cfg), row)

    await sink.write(
        source="orchestrator",
        event_type=event_type,
        token_mint=mint,
        payload=row,
        level=level,
    )


def find_pair_address(obj: Any) -> str | None:
    if isinstance(obj, Mapping):
        for key, value in obj.items():
            normalized = str(key).replace("_", "").replace("-", "").lower()

            if normalized in PAIR_KEYS and looks_like_solana_address(value):
                return str(value)

            nested = find_pair_address(value)
            if nested:
                return nested

    if isinstance(obj, list):
        for item in obj:
            nested = find_pair_address(item)
            if nested:
                return nested

    return None


def dex_pair(security_report: dict[str, Any]) -> dict[str, Any]:
    pairs = security_report["dexscreener"]["data"] or []
    return pairs[0] if pairs else {}


def first_website(pair: dict[str, Any]) -> str | None:
    for website in (pair.get("info") or {}).get("websites") or []:
        if isinstance(website, dict) and website.get("url"):
            return str(website["url"])

    return None


def first_social(pair: dict[str, Any], name: str) -> str | None:
    for social in (pair.get("info") or {}).get("socials") or []:
        if not isinstance(social, dict):
            continue

        social_type = str(social.get("type") or "").lower()
        url = social.get("url")

        if url and social_type == name:
            return str(url)

        if url and name == "twitter" and social_type == "x":
            return str(url)

    return None


def metadata_from_security_report(
    mint: str,
    migration_event: Mapping[str, Any],
    security_report: dict[str, Any],
) -> dict[str, Any]:
    pair = dex_pair(security_report) if security_report else {}
    base_token = pair.get("baseToken") or {}

    return {
        "name": base_token.get("name") or migration_event.get("name") or mint,
        "symbol": base_token.get("symbol") or migration_event.get("symbol") or mint[:6],
        "website": first_website(pair) or migration_event.get("website"),
        "twitter": first_social(pair, "twitter") or migration_event.get("twitter"),
        "telegram": first_social(pair, "telegram") or migration_event.get("telegram"),
        "pair_address": find_pair_address(migration_event) or pair.get("pairAddress"),
    }


async def run_stage(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    stage: str,
    fn: Callable[..., Any],
    required: bool = False,
    args: tuple[Any, ...] = (),
    kwargs: dict[str, Any] | None = None,
) -> Any:
    kwargs = kwargs or {}
    started = utc_now_iso_ms_z()
    job_id = state.start_job(mint, stage, ["direct", stage])

    await log_event(
        cfg,
        sink,
        event_type=f"{stage}_started",
        mint=mint,
        payload={"started": started},
    )

    try:
        result = fn(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result

        state.finish_job(job_id, status="ok", return_code=0)

        await log_event(
            cfg,
            sink,
            event_type=f"{stage}_finished",
            mint=mint,
            payload={"started": started, "finished": utc_now_iso_ms_z()},
        )

        return result

    except asyncio.CancelledError:
        state.finish_job(job_id, status="cancelled", error="cancelled")

        await log_event(
            cfg,
            sink,
            event_type=f"{stage}_cancelled",
            mint=mint,
            payload={"started": started, "finished": utc_now_iso_ms_z()},
            level="warning",
        )

        raise

    except Exception as exc:
        state.finish_job(job_id, status="error", error=repr(exc))

        await log_event(
            cfg,
            sink,
            event_type=f"{stage}_failed",
            mint=mint,
            payload={"error": repr(exc)},
            level="error",
        )

        if required:
            raise

        return None


def remaining_capture_seconds(start_ms: int, total_seconds: int) -> int:
    elapsed_seconds = max(0, (now_ms() - start_ms) // 1000)
    return max(0, total_seconds - elapsed_seconds)


async def cancel_task(task: asyncio.Task[Any]) -> None:
    if not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def start_onchain_capture(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    stage: str,
    onchain_cfg: dict[str, Any],
    save_dir: Path,
    pair_address: str | None,
    capture_time: int,
    window_start_ms: int,
    backfill_on_cancel: bool,
) -> asyncio.Task[Any]:
    return asyncio.create_task(
        run_stage(
            cfg=cfg,
            state=state,
            sink=sink,
            mint=mint,
            stage=stage,
            fn=onchain.main,
            required=True,
            kwargs={
                "mint": mint,
                "capture_time": capture_time,
                "rpc_interval": int(onchain_cfg["rpc_interval"]),
                "watch_accounts": [],
                "pair_address": pair_address,
                "infer_vaults_limit": int(onchain_cfg["infer_vaults_limit"]),
                "simulate_tx_base64": onchain_cfg.get("simulate_tx_base64"),
                "performance_sample_limit": int(onchain_cfg["performance_sample_limit"]),
                "max_signatures_per_address": int(onchain_cfg["max_signatures_per_address"]),
                "max_transactions_total": int(onchain_cfg["max_transactions_total"]),
                "save_dir": save_dir,
                "window_start_ms": window_start_ms,
                "backfill_on_cancel": backfill_on_cancel,
            },
        )
    )


def save_website_report(
    *,
    mint: str,
    meta: dict[str, Any],
    save_dir: Path,
) -> Path | None:
    website_url = meta.get("website")
    if not website_url:
        return None

    report = website_grader.run_report(
        coin_name=str(meta.get("name") or mint),
        coin_symbol=str(meta.get("symbol") or mint[:6]),
        coin_mint=mint,
        website_url=str(website_url),
        x_account=meta.get("twitter"),
        telegram_link=meta.get("telegram"),
    )

    return website_grader.save_report(report, save_dir / "website_report.json")


async def run_optional_analytics(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    meta: dict[str, Any],
    orch_cfg: dict[str, Any],
) -> None:
    analytics_cfg = orch_cfg["analytics"]
    save_dir = analytics_dir(cfg, mint)
    tasks = []

    if analytics_cfg["website_enabled"] and meta.get("website"):
        tasks.append(
            run_stage(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="website_grader",
                fn=asyncio.to_thread,
                args=(save_website_report,),
                kwargs={
                    "mint": mint,
                    "meta": meta,
                    "save_dir": save_dir,
                },
            )
        )

    if analytics_cfg["twitter_enabled"] and meta.get("twitter"):
        tasks.append(
            run_stage(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="twitter",
                fn=twitter.main,
                kwargs={
                    "link": str(meta["twitter"]),
                    "save_dir": save_dir,
                    "posts_limit": analytics_cfg["twitter_posts_limit"],
                },
            )
        )

    if analytics_cfg["telegram_enabled"] and meta.get("telegram"):
        tasks.append(
            run_stage(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="telegram",
                fn=telegram_info.main,
                kwargs={
                    "mint": mint,
                    "invite_link": str(meta["telegram"]),
                    "save_dir": save_dir,
                },
            )
        )

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def migrated_token_worker(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    migration_event: Mapping[str, Any],
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        orch_cfg = load_orchestrator_config()
        onchain_cfg = orch_cfg["onchain"]
        security_cfg = orch_cfg["security"]
        dexscreener_cfg = orch_cfg["dexscreener"]
        drop_cfg = orch_cfg["drop"]

        a_dir = analytics_dir(cfg, mint)
        o_dir = onchain_dir(cfg, mint)

        a_dir.mkdir(parents=True, exist_ok=True)
        o_dir.mkdir(parents=True, exist_ok=True)

        state.mark_migrated(mint, a_dir, migration_event)
        state.mark_status(mint, "capturing")

        capture_start_ms = now_ms()
        capture_seconds = int(onchain_cfg["capture_time"])
        backfill_on_cancel = bool(drop_cfg["backfill_on_cancel"])
        pair_address = find_pair_address(migration_event)
        active_pair_address = None

        await log_event(
            cfg,
            sink,
            event_type="worker_started",
            mint=mint,
            payload={
                "analytics_dir": str(a_dir),
                "onchain_dir": str(o_dir),
                "pair_address": pair_address,
                "capture_start_ms": capture_start_ms,
            },
        )

        active_capture_task = start_onchain_capture(
            cfg=cfg,
            state=state,
            sink=sink,
            mint=mint,
            stage="helius_basic_capture",
            onchain_cfg=onchain_cfg,
            save_dir=o_dir,
            pair_address=None,
            capture_time=capture_seconds,
            window_start_ms=capture_start_ms,
            backfill_on_cancel=False,
        )

        async def switch_to_vault_capture(
            discovered_pair_address: str,
            event_type: str,
        ) -> None:
            nonlocal active_capture_task, active_pair_address

            if discovered_pair_address == active_pair_address:
                return

            await cancel_task(active_capture_task)

            active_pair_address = discovered_pair_address
            remaining = remaining_capture_seconds(capture_start_ms, capture_seconds)

            await log_event(
                cfg,
                sink,
                event_type=event_type,
                mint=mint,
                payload={
                    "pair_address": discovered_pair_address,
                    "remaining_capture_seconds": remaining,
                    "window_start_ms": capture_start_ms,
                },
                level="warning" if event_type == "pair_address_found_late" else "info",
            )

            active_capture_task = start_onchain_capture(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="helius_vault_capture",
                onchain_cfg=onchain_cfg,
                save_dir=o_dir,
                pair_address=discovered_pair_address,
                capture_time=remaining,
                window_start_ms=capture_start_ms,
                backfill_on_cancel=backfill_on_cancel,
            )

        if pair_address:
            await switch_to_vault_capture(pair_address, "pair_address_found_initially")

        try:
            security_report = {}

            if security_cfg["enabled"]:
                state.mark_status(mint, "security_checking")

                security_report = await run_stage(
                    cfg=cfg,
                    state=state,
                    sink=sink,
                    mint=mint,
                    stage="security_report",
                    fn=security_api.main,
                    required=True,
                    kwargs={
                        "mint": mint,
                        "save_dir": a_dir,
                    },
                )

                red_flags_result = await run_stage(
                    cfg=cfg,
                    state=state,
                    sink=sink,
                    mint=mint,
                    stage="red_flags",
                    fn=red_flags.main,
                    required=True,
                    kwargs={
                        "security_report_path": a_dir / "security_report.json",
                        "mint": mint,
                        "save_dir": a_dir,
                        "config_path": Path(security_cfg["red_flags_config"]),
                    },
                )

                if red_flags_result["failed"]:
                    state.mark_status(mint, "dropped_red_flags")

                    await log_event(
                        cfg,
                        sink,
                        event_type="coin_dropped_red_flags",
                        mint=mint,
                        payload={
                            "failed_rules": red_flags_result["failed_rules"],
                            "keep_partial_onchain": drop_cfg["keep_partial_onchain"],
                            "backfill_on_cancel": backfill_on_cancel,
                        },
                        level="warning",
                    )

                    if security_cfg["cancel_on_red_flags"]:
                        await cancel_task(active_capture_task)

                    return

            meta = metadata_from_security_report(mint, migration_event, security_report)
            discovered_pair_address = pair_address or meta.get("pair_address")

            if discovered_pair_address:
                await switch_to_vault_capture(
                    str(discovered_pair_address),
                    "pair_address_found_late" if not pair_address else "pair_address_confirmed",
                )

            state.mark_status(mint, "analytics_running")

            await run_optional_analytics(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                meta=meta,
                orch_cfg=orch_cfg,
            )

            state.mark_status(mint, "waiting_onchain_capture")
            await active_capture_task

            if dexscreener_cfg["enabled"]:
                state.mark_status(mint, "dexscreener_24h")

                await run_stage(
                    cfg=cfg,
                    state=state,
                    sink=sink,
                    mint=mint,
                    stage="dexscreener_24h",
                    fn=dexscreener.stream_dexscreener_24h,
                    kwargs={
                        "mint": mint,
                        "interval": int(dexscreener_cfg["interval"]),
                        "length": int(dexscreener_cfg["length"]),
                        "save_dir": o_dir,
                    },
                )

            state.mark_status(mint, "reports_done")

            await log_event(
                cfg,
                sink,
                event_type="worker_finished",
                mint=mint,
                payload={"status": "reports_done"},
            )

        except Exception as exc:
            state.mark_status(mint, "failed")
            await cancel_task(active_capture_task)

            await log_event(
                cfg,
                sink,
                event_type="worker_failed",
                mint=mint,
                payload={"error": repr(exc)},
                level="error",
            )

            raise