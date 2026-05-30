from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.io import append_jsonl
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import CONFIG_DIR, PROJECT_ROOT, RAW_DIR
from crypto_trade.core.telegram import TELEGRAM
from crypto_trade.core.time import now_ms, now_ts, utc_now_iso_ms_z
from crypto_trade.core.yaml import load_yaml
from crypto_trade.ingest import dexscreener, security_api, twitter, website_grader
from crypto_trade.ingest.pumpportal import listen, shared_listen
from crypto_trade.pipeline.premigration_state import PreMigrationState

CONFIG_PATH = CONFIG_DIR / "premigration.yaml"
RETRYABLE_ERRORS = {"TimeoutError", "ReadTimeout", "ConnectTimeout", "PoolTimeout", "RateLimitError"}
DEXSCREENER_HEARTBEAT_NAME = "dexscreener_loop_heartbeat"
DEXSCREENER_HEARTBEAT_PATH = Path("dexscreener_loop")
TWITTER_RESERVED_PATHS = {
    "i", "home", "explore", "search", "notifications", "messages", "settings",
    "login", "signup", "intent", "share", "privacy", "tos",
}

logger = logging.getLogger(__name__)


def resolve_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def date_key() -> str:
    return utc_now_iso_ms_z()[:10]


def raw_event_path(root: Path) -> Path:
    return root / "pumpportal" / f"{date_key()}.jsonl"


def shared_pumpportal_path(cfg: dict[str, Any]) -> Path:
    return resolve_path(cfg["pumpportal"].get("shared_dir", "data/raw/pumpportal")) / f"{date_key()}.jsonl"


def dex_path(root: Path) -> Path:
    return root / "dexscreener" / f"{date_key()}.jsonl"


def enrichment_path(root: Path, name: str) -> Path:
    return root / name / f"{date_key()}.jsonl"


def pair_mints(pair: dict[str, Any]) -> set[str]:
    return {
        str((pair.get("baseToken") or {}).get("address") or ""),
        str((pair.get("quoteToken") or {}).get("address") or ""),
    }


def pairs_for_mint(data: Any, mint: str) -> list[dict[str, Any]]:
    if not isinstance(data, list):
        return []
    return [pair for pair in data if isinstance(pair, dict) and mint in pair_mints(pair)]


def best_pair(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    return max(pairs, key=lambda p: float(p.get("marketCap") or p.get("fdv") or 0), default={})


def max_market_cap_usd(pairs: list[dict[str, Any]]) -> float:
    return max((float((p.get("marketCap") or p.get("fdv") or 0) or 0) for p in pairs), default=0.0)


def window_volume(pair: dict[str, Any], window: str) -> float:
    return float((pair.get("volume") or {}).get(window) or 0)


def window_txns(pair: dict[str, Any], window: str) -> float:
    txns = (pair.get("txns") or {}).get(window) or {}
    return float(txns.get("buys") or 0) + float(txns.get("sells") or 0)


def activity_score(pairs: list[dict[str, Any]]) -> float:
    if not pairs:
        return 0.0
    vol5 = max(window_volume(p, "m5") for p in pairs)
    tx5 = max(window_txns(p, "m5") for p in pairs)
    liq = max(float((p.get("liquidity") or {}).get("usd") or 0) for p in pairs)
    score = (
        0.45 * math.log1p(vol5) / math.log1p(10_000)
        + 0.35 * math.log1p(tx5) / math.log1p(200)
        + 0.20 * math.log1p(liq) / math.log1p(100_000)
    )
    return max(0.0, min(score, 1.0))


def priority_score(pairs: list[dict[str, Any]], market_cap: float, selection_cfg: dict[str, Any]) -> float:
    trigger = float(selection_cfg["migration_market_cap_usd"]) * float(selection_cfg["trigger_fraction"])
    progress = max(0.0, min(market_cap / trigger, 1.0)) if trigger > 0 else 0.0
    return max(0.0, min(0.75 * activity_score(pairs) + 0.25 * progress, 1.0))


def is_inactive(pairs: list[dict[str, Any]], dead_cfg: dict[str, Any]) -> bool:
    if not pairs:
        return False
    max_volume = float(dead_cfg["max_volume_m5_usd"])
    max_txns = float(dead_cfg["max_txns_m5"])
    return all(window_volume(pair, "m5") <= max_volume and window_txns(pair, "m5") <= max_txns for pair in pairs)


def next_interval_ms(score: float, min_s: int, max_s: int) -> int:
    return int((max_s - (max_s - min_s) * score) * 1000)


def next_poll_ms(*, has_pairs: bool, age_ms: int, score: float, polling_cfg: dict[str, Any]) -> int:
    fresh_ms = int(polling_cfg["fresh_empty_minutes"]) * 60_000
    if not has_pairs and age_ms < fresh_ms:
        return now_ms() + int(polling_cfg["fresh_empty_seconds"]) * 1000
    return now_ms() + next_interval_ms(score, int(polling_cfg["min_seconds"]), int(polling_cfg["max_seconds"]))


def deterministic_unit(value: str) -> float:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:8], 16) / 0xFFFFFFFF


def market_cap_sample_rate(market_cap: float, selection_cfg: dict[str, Any]) -> tuple[float, int | None]:
    sample_cfg = selection_cfg.get("market_cap_random_sample") or {}
    if not sample_cfg.get("enabled") or market_cap <= 0:
        return 0.0, None
    trigger = float(selection_cfg["migration_market_cap_usd"]) * float(selection_cfg["trigger_fraction"])
    interval = trigger * float(sample_cfg.get("interval_fraction_of_trigger", 0.1))
    if trigger <= 0 or interval <= 0:
        return 0.0, None
    bin_count = max(1, math.ceil(trigger / interval))
    bin_index = max(0, min(int(market_cap // interval), bin_count - 1))
    lower_fraction = float(sample_cfg.get("lower_interval_fraction", 0.7))
    weights = [lower_fraction ** (bin_count - 1 - i) for i in range(bin_count)]
    rate = float(sample_cfg.get("limit_rate", 0.0)) * weights[bin_index] / sum(weights)
    return rate, bin_index


def normalize_social_url(url: str | None, kind: str) -> str | None:
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    host = parsed.netloc.lower().removeprefix("www.")
    path = parsed.path.strip("/")
    if kind == "twitter":
        if host not in {"x.com", "twitter.com"}:
            return None
        username = path.split("/", 1)[0].lstrip("@")
        if not username or username.lower() in TWITTER_RESERVED_PATHS:
            return None
        return f"https://x.com/{username}"
    if kind == "telegram":
        if host not in {"t.me", "telegram.me"}:
            return None
        group = path.split("/", 1)[0]
        return f"https://t.me/{group}" if group else None
    return url


def social_url(pair: dict[str, Any], name: str) -> str | None:
    for social in (pair.get("info") or {}).get("socials") or []:
        if not isinstance(social, dict):
            continue
        platform = str(social.get("type") or social.get("platform") or "").lower()
        url = social.get("url")
        if url and (platform == name or (name == "twitter" and platform == "x")):
            return normalize_social_url(str(url), name)
    return None


def website_url(pair: dict[str, Any]) -> str | None:
    for website in (pair.get("info") or {}).get("websites") or []:
        if isinstance(website, dict) and website.get("url"):
            return str(website["url"])
    return None


def token_meta(mint: str, pair: dict[str, Any]) -> dict[str, str]:
    base = pair.get("baseToken") or {}
    return {"name": str(base.get("name") or mint), "symbol": str(base.get("symbol") or mint[:6])}


def telegram_channel_name(link: str) -> str:
    value = link.strip()
    if value.startswith("@"):
        return value[1:]
    parsed = urlparse(value if "://" in value else f"https://{value}")
    return parsed.path.strip("/").split("/", 1)[0]


async def download_telegram_lite_data(invite_link: str) -> dict[str, Any]:
    channel_name = telegram_channel_name(invite_link)
    session_path = PROJECT_ROOT / "app_data" / "meme_metrics_session"
    async with TELEGRAM(int(get_env("TG_API_ID")), get_env("TG_API_HASH"), session_path=session_path) as tg:
        metrics = await tg.collect_lite_info(channel_name)
    return {
        "time": utc_now_iso_ms_z(),
        "source": "telethon",
        "mode": "lite",
        "input": {"invite_link": invite_link, "channel_name": channel_name},
        "metrics": metrics,
    }


class RateLimiter:
    def __init__(self, per_minute: int) -> None:
        self.delay = 60 / max(1, per_minute)
        self.lock = asyncio.Lock()
        self.next_at = 0.0

    async def wait(self) -> None:
        async with self.lock:
            loop = asyncio.get_running_loop()
            sleep_for = self.next_at - loop.time()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self.next_at = loop.time() + self.delay


def should_enrich(mint: str, market_cap: float, selection_cfg: dict[str, Any]) -> tuple[bool, str]:
    trigger = float(selection_cfg["migration_market_cap_usd"]) * float(selection_cfg["trigger_fraction"])
    if market_cap >= trigger:
        return True, "market_cap_threshold"
    sample_rate, bin_index = market_cap_sample_rate(market_cap, selection_cfg)
    if deterministic_unit(mint) < sample_rate:
        return True, f"market_cap_control_sample_bin_{bin_index}_rate_{sample_rate:.6f}"
    return False, "not_selected"


def enrichment_done_result(result: Any) -> bool:
    if not isinstance(result, dict) or result.get("ok") is not False:
        return True
    return result.get("error_type") not in RETRYABLE_ERRORS


async def save_enrichment_row(root: Path, name: str, mint: str, trigger_reason: str, result: Any) -> None:
    append_jsonl(
        enrichment_path(root, name),
        {
            "row_type": name,
            "timestamp": now_ts(),
            "local_received_at_ms": now_ms(),
            "mint": mint,
            "trigger_reason": trigger_reason,
            "data": result,
        },
    )


def error_result(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error_type": type(exc).__name__, "error_message": str(exc)}


async def collect_enrichment(
    *,
    state: PreMigrationState,
    root: Path,
    mint: str,
    name: str,
    trigger_reason: str,
    limiter: RateLimiter,
    coro_factory: Any,
) -> None:
    if not state.enrichment_due(mint, name):
        return
    state.mark_enrichment_selected(mint, trigger_reason)
    await limiter.wait()
    try:
        result = await coro_factory()
    except Exception as exc:
        result = error_result(exc)
    await save_enrichment_row(root, name, mint, trigger_reason, result)
    if enrichment_done_result(result):
        state.mark_enrichment_done(mint, name)


async def run_enrichments(
    *,
    cfg: dict[str, Any],
    state: PreMigrationState,
    root: Path,
    mint: str,
    pair: dict[str, Any],
    market_cap: float,
    limiters: dict[str, RateLimiter],
) -> None:
    selected, reason = should_enrich(mint, market_cap, cfg["selection"])
    if not selected:
        return
    meta = token_meta(mint, pair)
    site = website_url(pair)
    x_link = social_url(pair, "twitter")
    tg_link = social_url(pair, "telegram")
    enrich_cfg = cfg["enrichment"]

    if enrich_cfg["security"].get("enabled"):
        await collect_enrichment(
            state=state,
            root=root,
            mint=mint,
            name="security",
            trigger_reason=reason,
            limiter=limiters["security"],
            coro_factory=lambda: security_api.collect_security_report(mint),
        )
    if site and enrich_cfg["website"].get("enabled"):
        await collect_enrichment(
            state=state,
            root=root,
            mint=mint,
            name="website",
            trigger_reason=reason,
            limiter=limiters["website"],
            coro_factory=lambda: asyncio.to_thread(
                website_grader.run_report,
                coin_name=meta["name"],
                coin_symbol=meta["symbol"],
                coin_mint=mint,
                website_url=site,
                x_account=x_link,
                telegram_link=tg_link,
            ),
        )
    if x_link and enrich_cfg["twitter_lite"].get("enabled"):
        await collect_enrichment(
            state=state,
            root=root,
            mint=mint,
            name="twitter_lite",
            trigger_reason=reason,
            limiter=limiters["twitter_lite"],
            coro_factory=lambda: twitter.download_twitter_lite_data(x_link),
        )
    if tg_link and enrich_cfg["telegram_lite"].get("enabled"):
        await collect_enrichment(
            state=state,
            root=root,
            mint=mint,
            name="telegram_lite",
            trigger_reason=reason,
            limiter=limiters["telegram_lite"],
            coro_factory=lambda: download_telegram_lite_data(tg_link),
        )


def process_pumpportal_event(state: PreMigrationState, event: dict[str, Any]) -> None:
    if not state.record_event(event):
        return
    mint = event.get("mint")
    if not mint:
        return
    if event.get("type") == "migration":
        state.mark_migrated(mint)
    else:
        state.upsert_mint(mint)


async def pumpportal_direct_loop(state: PreMigrationState, root: Path, url: str | None) -> None:
    async for event in listen(mints=True, migrations=True, url=url):
        append_jsonl(raw_event_path(root), {"row_type": "pumpportal_event", **event})
        process_pumpportal_event(state, event)


async def pumpportal_file_loop(cfg: dict[str, Any], state: PreMigrationState) -> None:
    path = shared_pumpportal_path(cfg)
    poll_seconds = float(cfg["pumpportal"].get("poll_seconds", 1))
    cursor_name = "shared_pumpportal"
    offset = state.get_cursor(cursor_name, path)

    async for event, new_offset in shared_listen(
        path=path,
        owner="premigration_tracker",
        url=cfg["pumpportal"].get("url"),
        initial_offset=offset,
        poll_seconds=poll_seconds,
    ):
        process_pumpportal_event(state, event)
        if new_offset is not None:
            state.set_cursor(cursor_name, path, new_offset)
        elif path.exists():
            state.set_cursor(cursor_name, path, path.stat().st_size)


async def pumpportal_loop(cfg: dict[str, Any], state: PreMigrationState, root: Path) -> None:
    pump_cfg = cfg.get("pumpportal") or {}
    if pump_cfg.get("mode", "direct") == "file":
        await pumpportal_file_loop(cfg, state)
    else:
        await pumpportal_direct_loop(state, root, pump_cfg.get("url"))


async def dashboard_loop(cfg: dict[str, Any], state: PreMigrationState) -> None:
    dashboard_cfg = cfg.get("dashboard") or {}
    if not dashboard_cfg.get("enabled", True):
        return
    trigger = float(cfg["selection"]["migration_market_cap_usd"]) * float(cfg["selection"]["trigger_fraction"])
    interval = int(dashboard_cfg.get("interval_seconds", 30))
    while True:
        stats = state.dashboard_stats(trigger)
        logger.info(
            "PREMIGRATION DASHBOARD | mints=%s active=%s migrated=%s dead=%s expired=%s "
            "tracking_level=%s due=%s analytics: threshold=%s random=%s security=%s x=%s telegram=%s website=%s",
            stats["mints_seen"], stats["active"], stats["migrated"], stats["dead"], stats["expired"],
            stats["tracking_level_reached"], stats["due_now"], stats["threshold_analytics_selected"],
            stats["random_analytics_sampled"], stats["security_reports"], stats["twitter_lite_reports"],
            stats["telegram_lite_reports"], stats["website_reports"],
        )
        await asyncio.sleep(interval)


def post_row(batch_row: dict[str, Any], mint: str, pairs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "timestamp": batch_row["timestamp"],
        "local_received_at_ms": batch_row["local_received_at_ms"],
        "source": "dexscreener",
        "method": "tokens-v1",
        "mint": mint,
        "http_status": batch_row["http_status"],
        "elapsed_ms": batch_row["elapsed_ms"],
        "rate_limit": batch_row["rate_limit"],
        "error_type": batch_row["error_type"],
        "error_message": batch_row["error_message"],
        "data": pairs,
    }


async def dexscreener_loop(cfg: dict[str, Any], state: PreMigrationState, root: Path) -> None:
    dex_cfg = cfg["dexscreener"]
    batch_size = int(dex_cfg.get("batch_size", 30))
    request_delay = 60 / int(dex_cfg["max_requests_per_minute"])
    coalesce_s = float(dex_cfg.get("coalesce_ms", 0)) / 1000
    limiters = {
        name: RateLimiter(int(cfg["enrichment_limits"][f"{name}_per_minute"]))
        for name in ["security", "twitter_lite", "telegram_lite", "website"]
    }

    while True:
        state.set_cursor(DEXSCREENER_HEARTBEAT_NAME, DEXSCREENER_HEARTBEAT_PATH, now_ms())
        targets = state.due_dex_targets(batch_size)
        post_mints = list(dict.fromkeys(target["mint"] for target in targets))
        pre_mints = state.due_mints(batch_size - len(post_mints), exclude=set(post_mints))

        if coalesce_s and len(post_mints) + len(pre_mints) < batch_size:
            await asyncio.sleep(coalesce_s)
            state.set_cursor(DEXSCREENER_HEARTBEAT_NAME, DEXSCREENER_HEARTBEAT_PATH, now_ms())
            targets = state.due_dex_targets(batch_size)
            post_mints = list(dict.fromkeys(target["mint"] for target in targets))
            pre_mints = state.due_mints(batch_size - len(post_mints), exclude=set(post_mints))

        mints = post_mints + pre_mints
        if not mints:
            await asyncio.sleep(int(cfg["polling"]["idle_sleep_seconds"]))
            continue

        response = await dexscreener.transactions_multiple_tokens(*mints)
        batch_row = {
            "row_type": "batch_snapshot",
            "timestamp": now_ts(),
            "local_received_at_ms": now_ms(),
            "source": "dexscreener",
            "method": "tokens-v1",
            "mints": mints,
            "http_status": response.http_status,
            "elapsed_ms": response.elapsed_ms,
            "rate_limit": response.rate_limit,
            "error_type": response.error_type,
            "error_message": response.error_message,
            "data": response.data,
        }
        append_jsonl(dex_path(root), batch_row)

        target_by_mint: dict[str, list[dict[str, Any]]] = {}
        for target in targets:
            target_by_mint.setdefault(target["mint"], []).append(target)

        for mint in mints:
            pairs = pairs_for_mint(response.data, mint)
            for target in target_by_mint.get(mint, []):
                append_jsonl(Path(target["output_path"]), post_row(batch_row, mint, pairs))
                state.mark_dex_target_polled(mint, target["target_type"])

            if mint not in pre_mints:
                continue

            pair = best_pair(pairs)
            market_cap = max_market_cap_usd(pairs)
            score = priority_score(pairs, market_cap, cfg["selection"])
            age_ms = state.mint_age_ms(mint)
            inactive = is_inactive(pairs, cfg["dead_detection"])
            state_result = state.update_after_dex_poll(
                mint,
                has_pairs=bool(pairs),
                inactive=inactive,
                score=score,
                market_cap_usd=market_cap,
                next_poll_ms=next_poll_ms(
                    has_pairs=bool(pairs),
                    age_ms=age_ms,
                    score=score,
                    polling_cfg=cfg["polling"],
                ),
                no_pair_dead_ms=int(cfg["dead_detection"]["no_pair_after_minutes"]) * 60_000,
                inactive_dead_ms=int(cfg["dead_detection"]["inactive_after_minutes"]) * 60_000,
                inactive_confirmations=int(cfg["dead_detection"]["inactive_confirmations"]),
                max_track_ms=int(cfg["polling"]["max_track_hours"]) * 60 * 60_000,
            ) or {}
            append_jsonl(
                dex_path(root),
                {
                    "row_type": "mint_snapshot",
                    "timestamp": batch_row["timestamp"],
                    "local_received_at_ms": batch_row["local_received_at_ms"],
                    "source": "dexscreener",
                    "method": "tokens-v1",
                    "mint": mint,
                    "http_status": response.http_status,
                    "error_type": response.error_type,
                    "error_message": response.error_message,
                    "priority_score": score,
                    "market_cap_usd": market_cap,
                    "inactive": inactive,
                    "status_after_poll": state_result.get("status"),
                    "dead_reason": state_result.get("dead_reason"),
                    "inactive_polls": state_result.get("inactive_polls"),
                    "data": pairs,
                },
            )
            await run_enrichments(cfg=cfg, state=state, root=root, mint=mint, pair=pair, market_cap=market_cap, limiters=limiters)

        await asyncio.sleep(request_delay)


async def main(config_path: Path) -> None:
    configure_logging()
    load_env()
    cfg = load_yaml(config_path)
    root = resolve_path(cfg.get("storage", {}).get("root", RAW_DIR / "premigration"))
    state = PreMigrationState(root / "state.sqlite3")
    await asyncio.gather(
        pumpportal_loop(cfg, state, root),
        dexscreener_loop(cfg, state, root),
        dashboard_loop(cfg, state),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    asyncio.run(main(parser.parse_args().config))
