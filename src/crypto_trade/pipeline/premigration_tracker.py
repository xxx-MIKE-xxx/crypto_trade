from __future__ import annotations

import argparse
import asyncio
import math
from pathlib import Path
from typing import Any

from crypto_trade.core.env import load_env
from crypto_trade.core.io import append_jsonl, chunked
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import CONFIG_DIR, PROJECT_ROOT, RAW_DIR
from crypto_trade.core.time import now_ms, now_ts, utc_now_iso_ms_z
from crypto_trade.core.yaml import load_yaml
from crypto_trade.ingest import dexscreener, security_api
from crypto_trade.ingest.pumpportal import listen
from crypto_trade.pipeline.premigration_state import PreMigrationState

CONFIG_PATH = CONFIG_DIR / "premigration.yaml"
BATCH_SIZE = 30


def resolve_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def date_key() -> str:
    return utc_now_iso_ms_z()[:10]


def raw_event_path(root: Path) -> Path:
    return root / "pumpportal" / f"{date_key()}.jsonl"


def dex_path(root: Path) -> Path:
    return root / "dexscreener" / f"{date_key()}.jsonl"


def security_dir(root: Path, mint: str) -> Path:
    return root / "security" / mint


def pair_mints(pair: dict[str, Any]) -> set[str]:
    return {
        str((pair.get("baseToken") or {}).get("address") or ""),
        str((pair.get("quoteToken") or {}).get("address") or ""),
    }


def pairs_for_mint(data: Any, mint: str) -> list[dict[str, Any]]:
    if not isinstance(data, list):
        return []
    return [pair for pair in data if isinstance(pair, dict) and mint in pair_mints(pair)]


def pair_score(pairs: list[dict[str, Any]]) -> float:
    if not pairs:
        return 0.0

    vol5 = max(float((p.get("volume") or {}).get("m5") or 0) for p in pairs)
    liq = max(float((p.get("liquidity") or {}).get("usd") or 0) for p in pairs)
    tx5 = max(
        float(((p.get("txns") or {}).get("m5") or {}).get("buys") or 0)
        + float(((p.get("txns") or {}).get("m5") or {}).get("sells") or 0)
        for p in pairs
    )

    score = (
        0.45 * math.log1p(vol5) / math.log1p(10_000)
        + 0.35 * math.log1p(tx5) / math.log1p(200)
        + 0.20 * math.log1p(liq) / math.log1p(100_000)
    )
    return max(0.0, min(score, 1.0))


def next_interval_ms(score: float, min_s: int, max_s: int) -> int:
    return int((max_s - (max_s - min_s) * score) * 1000)


def max_market_cap_usd(pairs: list[dict[str, Any]]) -> float:
    values = [p.get("marketCap") or p.get("fdv") or 0 for p in pairs]
    return max((float(v or 0) for v in values), default=0.0)


def next_poll_ms(
    *,
    has_pairs: bool,
    age_ms: int,
    score: float,
    polling_cfg: dict[str, Any],
) -> int:
    fresh_ms = int(polling_cfg["fresh_empty_minutes"]) * 60_000
    if not has_pairs and age_ms < fresh_ms:
        return now_ms() + int(polling_cfg["fresh_empty_seconds"]) * 1000

    return now_ms() + next_interval_ms(
        score,
        int(polling_cfg["min_seconds"]),
        int(polling_cfg["max_seconds"]),
    )


async def maybe_collect_security(
    *,
    state: PreMigrationState,
    root: Path,
    mint: str,
    pairs: list[dict[str, Any]],
    security_cfg: dict[str, Any],
) -> None:
    if not security_cfg.get("enabled") or not state.security_due(mint):
        return

    trigger = float(security_cfg["migration_market_cap_usd"]) * float(security_cfg["trigger_fraction"])
    if max_market_cap_usd(pairs) < trigger:
        return

    try:
        await security_api.main(mint=mint, save_dir=security_dir(root, mint))
    finally:
        state.mark_security_reported(mint)


async def pumpportal_loop(state: PreMigrationState, root: Path, url: str | None) -> None:
    async for event in listen(mints=True, migrations=True, url=url):
        append_jsonl(raw_event_path(root), event)
        if not state.record_event(event):
            continue

        mint = event.get("mint")
        if not mint:
            continue

        if event.get("type") == "migration":
            state.mark_migrated(mint)
        else:
            state.upsert_mint(mint)


async def dexscreener_loop(cfg: dict[str, Any], state: PreMigrationState, root: Path) -> None:
    dex_cfg = cfg["dexscreener"]
    polling_cfg = cfg["polling"]
    dead_cfg = cfg["dead_detection"]
    security_cfg = cfg["security"]
    request_delay = 60 / int(dex_cfg["max_requests_per_minute"])

    while True:
        mints = state.due_mints(BATCH_SIZE)
        if not mints:
            await asyncio.sleep(int(polling_cfg["idle_sleep_seconds"]))
            continue

        response = await dexscreener.transactions_multiple_tokens(*mints)
        row = {
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
        append_jsonl(dex_path(root), row)

        for mint in mints:
            pairs = pairs_for_mint(response.data, mint)
            score = pair_score(pairs)
            state.update_after_dex_poll(
                mint,
                has_pairs=bool(pairs),
                score=score,
                next_poll_ms=next_poll_ms(
                    has_pairs=bool(pairs),
                    age_ms=state.mint_age_ms(mint),
                    score=score,
                    polling_cfg=polling_cfg,
                ),
                no_pair_dead_ms=int(dead_cfg["no_pair_after_minutes"]) * 60_000,
                max_track_ms=int(polling_cfg["max_track_hours"]) * 60 * 60_000,
            )
            append_jsonl(
                dex_path(root),
                {
                    **row,
                    "mint": mint,
                    "data": pairs,
                    "priority_score": score,
                    "market_cap_usd": max_market_cap_usd(pairs),
                },
            )
            await maybe_collect_security(
                state=state,
                root=root,
                mint=mint,
                pairs=pairs,
                security_cfg=security_cfg,
            )

        await asyncio.sleep(request_delay)


async def main(config_path: Path) -> None:
    configure_logging()
    load_env()

    cfg = load_yaml(config_path)
    root = resolve_path(cfg.get("storage", {}).get("root", RAW_DIR / "premigration"))
    state = PreMigrationState(root / "state.sqlite3")

    await asyncio.gather(
        pumpportal_loop(state, root, cfg.get("pumpportal", {}).get("url")),
        dexscreener_loop(cfg, state, root),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    asyncio.run(main(parser.parse_args().config))
