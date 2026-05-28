from __future__ import annotations

import asyncio
import inspect
import logging
from pathlib import Path
from typing import Any, Callable, Mapping

from crypto_trade.core.io import append_jsonl
from crypto_trade.core.paths import CONFIG_DIR
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.core.yaml import load_yaml
from crypto_trade.ingest import dexscreener, onchain, red_flags, security_api, telegram_info, twitter
from crypto_trade.ingest import website_grader
from crypto_trade.ingest.bronze import EventSink
from crypto_trade.pipeline.config import PipelineConfig
from crypto_trade.pipeline.mint import looks_like_solana_address
from crypto_trade.pipeline.state import StateStore

logger = logging.getLogger(__name__)

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

DEFAULT_CONFIG: dict[str, Any] = {
    "onchain": {
        "capture_time": 3600,
        "rpc_interval": 10,
        "infer_vaults_limit": 50,
        "performance_sample_limit": 60,
        "max_signatures_per_address": 1000,
        "max_transactions_total": 3000,
        "simulate_tx_base64": None,
    },
    "security": {
        "enabled": True,
        "cancel_on_red_flags": True,
        "red_flags_config": "config/red_flags.yaml",
    },
    "analytics": {
        "website_enabled": True,
        "twitter_enabled": True,
        "telegram_enabled": True,
        "twitter_posts_limit": 20,
    },
    "dexscreener": {
        "enabled": True,
        "interval": 60,
        "length": 24 * 60 * 60,
    },
    "drop": {
        "keep_partial_onchain": True,
    },
}


def merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = merge_dict(out[key], value)
        else:
            out[key] = value

    return out


def load_orchestrator_config(path: Path = ORCHESTRATOR_CONFIG_PATH) -> dict[str, Any]:
    if not path.exists():
        return DEFAULT_CONFIG

    loaded = load_yaml(path) or {}
    if not isinstance(loaded, dict):
        return DEFAULT_CONFIG

    return merge_dict(DEFAULT_CONFIG, loaded)


def raw_dir(cfg: PipelineConfig) -> Path:
    return cfg.data_root / "raw"


def analytics_dir(cfg: PipelineConfig, mint: str) -> Path:
    return raw_dir(cfg) / "analytics" / mint


def onchain_dir(cfg: PipelineConfig, mint: str) -> Path:
    return raw_dir(cfg) / "onchain" / mint


def orchestrator_log_path(cfg: PipelineConfig) -> Path:
    return raw_dir(cfg) / "orchestrator" / f"{utc_now_iso_ms_z()[:10]}.jsonl"


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


def source_data(report: dict[str, Any], name: str) -> Any:
    source = report.get(name)

    if isinstance(source, dict):
        return source.get("data")

    return getattr(source, "data", None)


def dex_pairs(security_report: dict[str, Any]) -> list[dict[str, Any]]:
    data = source_data(security_report, "dexscreener")

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    if isinstance(data, dict):
        pairs = data.get("pairs") or data.get("data") or []
        return [x for x in pairs if isinstance(x, dict)]

    return []


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


def first_url_from_pair(pair: dict[str, Any], kind: str) -> str | None:
    info = pair.get("info") or {}

    if kind == "website":
        for item in info.get("websites") or []:
            if isinstance(item, dict) and item.get("url"):
                return str(item["url"])

    for item in info.get("socials") or []:
        if not isinstance(item, dict):
            continue

        social_type = str(item.get("type") or "").lower()
        url = item.get("url")

        if not url:
            continue

        if kind == "twitter" and social_type in {"twitter", "x"}:
            return str(url)

        if kind == "telegram" and social_type == "telegram":
            return str(url)

    return None


def metadata_from_security_report(
    mint: str,
    migration_event: Mapping[str, Any],
    security_report: dict[str, Any],
) -> dict[str, Any]:
    pair = dex_pairs(security_report)[0] if dex_pairs(security_report) else {}
    base_token = pair.get("baseToken") or {}

    return {
        "name": base_token.get("name") or migration_event.get("name") or mint,
        "symbol": base_token.get("symbol") or migration_event.get("symbol") or mint[:6],
        "website": first_url_from_pair(pair, "website") or migration_event.get("website"),
        "twitter": first_url_from_pair(pair, "twitter") or migration_event.get("twitter"),
        "telegram": first_url_from_pair(pair, "telegram") or migration_event.get("telegram"),
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
    job_id = state.start_job(mint, stage, ["direct", stage])
    started = utc_now_iso_ms_z()

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

    if analytics_cfg.get("website_enabled") and meta.get("website"):
        tasks.append(
            run_stage(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="website_grader",
                fn=asyncio.to_thread,
                kwargs={
                    "func": save_website_report,
                    "mint": mint,
                    "meta": meta,
                    "save_dir": save_dir,
                },
            )
        )

    if analytics_cfg.get("twitter_enabled") and meta.get("twitter"):
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
                    "posts_limit": analytics_cfg.get("twitter_posts_limit"),
                },
            )
        )

    if analytics_cfg.get("telegram_enabled") and meta.get("telegram"):
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
        a_dir = analytics_dir(cfg, mint)
        o_dir = onchain_dir(cfg, mint)

        a_dir.mkdir(parents=True, exist_ok=True)
        o_dir.mkdir(parents=True, exist_ok=True)

        state.mark_migrated(mint, a_dir, migration_event)
        state.mark_status(mint, "capturing")

        pair_address = find_pair_address(migration_event)

        await log_event(
            cfg,
            sink,
            event_type="worker_started",
            mint=mint,
            payload={
                "analytics_dir": str(a_dir),
                "onchain_dir": str(o_dir),
                "pair_address": pair_address,
            },
        )

        onchain_cfg = orch_cfg["onchain"]

        capture_task = asyncio.create_task(
            run_stage(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="helius_capture",
                fn=onchain.main,
                required=True,
                kwargs={
                    "mint": mint,
                    "capture_time": int(onchain_cfg["capture_time"]),
                    "rpc_interval": int(onchain_cfg["rpc_interval"]),
                    "watch_accounts": [],
                    "pair_address": pair_address,
                    "infer_vaults_limit": int(onchain_cfg["infer_vaults_limit"]),
                    "simulate_tx_base64": onchain_cfg.get("simulate_tx_base64"),
                    "performance_sample_limit": int(onchain_cfg["performance_sample_limit"]),
                    "max_signatures_per_address": int(onchain_cfg["max_signatures_per_address"]),
                    "max_transactions_total": int(onchain_cfg["max_transactions_total"]),
                    "save_dir": o_dir,
                },
            )
        )

        try:
            security_report = {}

            if orch_cfg["security"].get("enabled", True):
                state.mark_status(mint, "security_checking")

                security_report = await run_stage(
                    cfg=cfg,
                    state=state,
                    sink=sink,
                    mint=mint,
                    stage="security_report",
                    fn=security_api.main,
                    required=True,
                    kwargs={"mint": mint, "save_dir": a_dir},
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
                        "config_path": Path(orch_cfg["security"]["red_flags_config"]),
                    },
                )

                if red_flags_result.get("failed"):
                    state.mark_status(mint, "dropped_red_flags")

                    await log_event(
                        cfg,
                        sink,
                        event_type="coin_dropped_red_flags",
                        mint=mint,
                        payload={
                            "failed_rules": red_flags_result.get("failed_rules", []),
                            "keep_partial_onchain": orch_cfg["drop"].get("keep_partial_onchain", True),
                        },
                        level="warning",
                    )

                    if orch_cfg["security"].get("cancel_on_red_flags", True):
                        capture_task.cancel()
                        await asyncio.gather(capture_task, return_exceptions=True)

                    return

            meta = metadata_from_security_report(mint, migration_event, security_report)

            if not pair_address and meta.get("pair_address"):
                await log_event(
                    cfg,
                    sink,
                    event_type="pair_address_found_late",
                    mint=mint,
                    payload={"pair_address": meta["pair_address"]},
                    level="warning",
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
            await capture_task

            if orch_cfg["dexscreener"].get("enabled", True):
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
                        "interval": int(orch_cfg["dexscreener"]["interval"]),
                        "length": int(orch_cfg["dexscreener"]["length"]),
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

            if not capture_task.done():
                capture_task.cancel()
                await asyncio.gather(capture_task, return_exceptions=True)

            await log_event(
                cfg,
                sink,
                event_type="worker_failed",
                mint=mint,
                payload={"error": repr(exc)},
                level="error",
            )

            raise