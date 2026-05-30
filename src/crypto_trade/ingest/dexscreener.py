from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from crypto_trade.core.http import request_json
from crypto_trade.core.io import append_jsonl, save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ANALYTICS_DIR, ONCHAIN_DIR
from crypto_trade.core.time import now_ms, now_ts

load_dotenv()

logger = logging.getLogger(__name__)

DEXSCREENER_BASE = "https://api.dexscreener.com"
CHAIN_ID = "solana"
DEXSCREENER_24H_FILENAME = "dexscreener_24h.jsonl"
DEXSCREENER_ENRICHMENT_FILENAME = "dexscreener_enrichment.json"
DEXSCREENER_ENRICHMENT_MIN_INTERVAL_SECONDS = 1.05

_enrichment_lock = asyncio.Lock()
_next_enrichment_request_at = 0.0


def dexscreener_24h_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / DEXSCREENER_24H_FILENAME
    return ONCHAIN_DIR / mint / DEXSCREENER_24H_FILENAME


def enrichment_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / DEXSCREENER_ENRICHMENT_FILENAME
    return ANALYTICS_DIR / mint / DEXSCREENER_ENRICHMENT_FILENAME


async def wait_enrichment_rate_limit() -> None:
    global _next_enrichment_request_at

    async with _enrichment_lock:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now < _next_enrichment_request_at:
            await asyncio.sleep(_next_enrichment_request_at - now)
        _next_enrichment_request_at = asyncio.get_running_loop().time() + DEXSCREENER_ENRICHMENT_MIN_INTERVAL_SECONDS


async def request_enrichment_json(url: str) -> Any:
    await wait_enrichment_rate_limit()
    return await request_json("GET", url)


def response_to_dict(response: Any) -> dict[str, Any]:
    if isinstance(response, Exception):
        return {
            "data": None,
            "http_status": None,
            "error_type": type(response).__name__,
            "error_message": str(response),
            "elapsed_ms": None,
            "rate_limit": {},
        }

    return {
        "data": getattr(response, "data", None),
        "http_status": getattr(response, "http_status", None),
        "error_type": getattr(response, "error_type", None),
        "error_message": getattr(response, "error_message", None),
        "elapsed_ms": getattr(response, "elapsed_ms", None),
        "rate_limit": getattr(response, "rate_limit", {}),
    }


def body_list(response_dict: dict[str, Any]) -> list[Any]:
    data = response_dict.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        inner = data.get("data")
        if isinstance(inner, list):
            return inner
        return [data]
    return []


def lower(value: Any) -> str:
    return str(value or "").lower()


def token_matches(item: Any, mint: str, chain_id: str = CHAIN_ID) -> bool:
    if not isinstance(item, dict):
        return False

    mint_l = mint.lower()
    chain_l = chain_id.lower()
    candidates = [
        item.get("tokenAddress"),
        item.get("address"),
        item.get("baseToken", {}).get("address") if isinstance(item.get("baseToken"), dict) else None,
        item.get("quoteToken", {}).get("address") if isinstance(item.get("quoteToken"), dict) else None,
    ]
    chain_values = [item.get("chainId"), item.get("chain")]

    token_ok = any(str(candidate).lower() == mint_l for candidate in candidates if candidate)
    chain_ok = not any(chain_values) or any(str(value).lower() == chain_l for value in chain_values if value)
    return token_ok and chain_ok


def extract_pairs(response_dict: dict[str, Any], mint: str, chain_id: str = CHAIN_ID) -> list[dict[str, Any]]:
    pairs = []
    for item in body_list(response_dict):
        if isinstance(item, dict) and token_matches(item, mint, chain_id):
            pairs.append(item)
    return pairs


def extract_links_from_pairs(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    websites = []
    socials = []
    for pair in pairs:
        info = pair.get("info") or {}
        if isinstance(info.get("websites"), list):
            websites.extend(info["websites"])
        if isinstance(info.get("socials"), list):
            socials.extend(info["socials"])

    platforms = []
    for social in socials:
        if isinstance(social, dict):
            platforms.append(lower(social.get("type") or social.get("platform") or social.get("label")))

    return {
        "websites": websites,
        "socials": socials,
        "has_website": bool(websites),
        "has_telegram": any("telegram" in platform or platform == "tg" for platform in platforms),
        "has_x": any(platform in {"twitter", "x"} or "twitter" in platform for platform in platforms),
    }


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def extract_market_features(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    def liquidity_usd(pair: dict[str, Any]) -> float:
        liquidity = pair.get("liquidity") if isinstance(pair.get("liquidity"), dict) else {}
        return as_float(liquidity.get("usd")) or 0.0

    primary_pair = max(pairs, key=liquidity_usd) if pairs else {}
    liquidity = primary_pair.get("liquidity") if isinstance(primary_pair.get("liquidity"), dict) else {}
    volume = primary_pair.get("volume") if isinstance(primary_pair.get("volume"), dict) else {}
    txns = primary_pair.get("txns") if isinstance(primary_pair.get("txns"), dict) else {}
    price_change = primary_pair.get("priceChange") if isinstance(primary_pair.get("priceChange"), dict) else {}

    def tx_count(window: str, side: str) -> int | None:
        bucket = txns.get(window) if isinstance(txns.get(window), dict) else {}
        try:
            value = bucket.get(side)
            return int(value) if value is not None else None
        except Exception:
            return None

    return {
        "pair_count": len(pairs),
        "primary_pair_address": primary_pair.get("pairAddress"),
        "primary_dex_id": primary_pair.get("dexId"),
        "primary_pair_url": primary_pair.get("url"),
        "price_native": as_float(primary_pair.get("priceNative")),
        "price_usd": as_float(primary_pair.get("priceUsd")),
        "liquidity_usd": as_float(liquidity.get("usd")),
        "liquidity_base": as_float(liquidity.get("base")),
        "liquidity_quote": as_float(liquidity.get("quote")),
        "volume_m5": as_float(volume.get("m5")),
        "volume_h1": as_float(volume.get("h1")),
        "volume_h6": as_float(volume.get("h6")),
        "volume_h24": as_float(volume.get("h24")),
        "price_change_m5": as_float(price_change.get("m5")),
        "price_change_h1": as_float(price_change.get("h1")),
        "price_change_h6": as_float(price_change.get("h6")),
        "price_change_h24": as_float(price_change.get("h24")),
        "txns_m5_buys": tx_count("m5", "buys"),
        "txns_m5_sells": tx_count("m5", "sells"),
        "txns_h1_buys": tx_count("h1", "buys"),
        "txns_h1_sells": tx_count("h1", "sells"),
        "txns_h6_buys": tx_count("h6", "buys"),
        "txns_h6_sells": tx_count("h6", "sells"),
        "txns_h24_buys": tx_count("h24", "buys"),
        "txns_h24_sells": tx_count("h24", "sells"),
        "fdv": as_float(primary_pair.get("fdv")),
        "market_cap": as_float(primary_pair.get("marketCap")),
        "pair_created_at": primary_pair.get("pairCreatedAt"),
    }


def extract_orders_features(response_dict: dict[str, Any]) -> dict[str, Any]:
    orders = body_list(response_dict)
    latest_payment_ts = None
    compact = []
    has_profile = False
    has_active_ad = False
    community_takeover = False

    for order in orders:
        if not isinstance(order, dict):
            continue
        order_type = lower(order.get("type") or order.get("orderType") or order.get("kind"))
        status = lower(order.get("status"))
        payment_ts = order.get("paymentTimestamp") or order.get("paymentTime") or order.get("createdAt")
        approved = status in {"approved", "active", "completed", "success", "paid"}

        compact.append({"type": order_type, "status": status, "paymentTimestamp": payment_ts})
        if payment_ts is not None:
            try:
                latest_payment_ts = max(int(payment_ts), latest_payment_ts or 0)
            except Exception:
                pass
        if "profile" in order_type and approved:
            has_profile = True
        if ("ad" in order_type or "trending" in order_type) and approved:
            has_active_ad = True
        if ("community" in order_type or "takeover" in order_type or "cto" in order_type) and (approved or not status):
            community_takeover = True

    latest_payment_age_seconds = None
    if latest_payment_ts:
        ts_ms = latest_payment_ts if latest_payment_ts > 10_000_000_000 else latest_payment_ts * 1000
        latest_payment_age_seconds = max(0, int((now_ms() - ts_ms) / 1000))

    return {
        "paid_order_count": len([order for order in orders if isinstance(order, dict)]),
        "latest_payment_age_seconds": latest_payment_age_seconds,
        "has_profile_from_orders": has_profile,
        "has_active_ad_from_orders": has_active_ad,
        "community_takeover_from_orders": community_takeover,
        "orders_compact": compact,
    }


def extract_boost_features(
    latest_response: dict[str, Any],
    top_response: dict[str, Any],
    pairs: list[dict[str, Any]],
    mint: str,
) -> dict[str, Any]:
    matched = [item for response in (latest_response, top_response) for item in body_list(response) if token_matches(item, mint)]
    amounts = [as_float(item.get("amount")) for item in matched if isinstance(item, dict)]
    total_amounts = [as_float(item.get("totalAmount")) for item in matched if isinstance(item, dict)]
    amounts = [value for value in amounts if value is not None]
    total_amounts = [value for value in total_amounts if value is not None]

    pair_boost_active = []
    for pair in pairs:
        boosts = pair.get("boosts") or {}
        if isinstance(boosts, dict) and boosts.get("active") is not None:
            value = as_float(boosts.get("active"))
            if value is not None:
                pair_boost_active.append(value)

    return {
        "boost_amount_now": max(amounts) if amounts else None,
        "boost_total_amount": max(total_amounts) if total_amounts else None,
        "boosts_matched_count": len(matched),
        "pair_boosts_active_max": max(pair_boost_active) if pair_boost_active else None,
        "boosts_compact": [
            {
                "chainId": item.get("chainId"),
                "tokenAddress": item.get("tokenAddress"),
                "amount": item.get("amount"),
                "totalAmount": item.get("totalAmount"),
                "url": item.get("url"),
            }
            for item in matched
            if isinstance(item, dict)
        ],
    }


def extract_ads_features(response_dict: dict[str, Any], mint: str) -> dict[str, Any]:
    ads = [item for item in body_list(response_dict) if token_matches(item, mint)]
    impressions = []
    durations = []
    statuses = []
    compact = []

    for ad in ads:
        if not isinstance(ad, dict):
            continue
        statuses.append(lower(ad.get("status")))
        impression = as_float(ad.get("impressions"))
        duration = as_float(ad.get("durationHours"))
        if impression is not None:
            impressions.append(impression)
        if duration is not None:
            durations.append(duration)
        compact.append(
            {
                "chainId": ad.get("chainId"),
                "tokenAddress": ad.get("tokenAddress"),
                "type": ad.get("type"),
                "status": ad.get("status"),
                "impressions": ad.get("impressions"),
                "durationHours": ad.get("durationHours"),
                "paymentTimestamp": ad.get("paymentTimestamp"),
                "url": ad.get("url"),
            }
        )

    return {
        "has_active_ad_from_ads_feed": any(status in {"approved", "active", "running", "paid", "completed"} for status in statuses) or bool(ads),
        "ad_impressions": max(impressions) if impressions else None,
        "ad_duration_hours": max(durations) if durations else None,
        "ads_matched_count": len(ads),
        "ads_compact": compact,
    }


def extract_community_takeover_features(response_dict: dict[str, Any], mint: str) -> dict[str, Any]:
    matches = [item for item in body_list(response_dict) if token_matches(item, mint)]
    return {
        "community_takeover_from_feed": bool(matches),
        "community_takeover_matches_count": len(matches),
        "community_takeover_compact": [
            {
                "chainId": item.get("chainId"),
                "tokenAddress": item.get("tokenAddress"),
                "url": item.get("url"),
                "description": item.get("description"),
                "claimDate": item.get("claimDate"),
            }
            for item in matches
            if isinstance(item, dict)
        ],
    }


def extract_profile_features(response_dict: dict[str, Any], mint: str) -> dict[str, Any]:
    matches = [item for item in body_list(response_dict) if token_matches(item, mint)]
    return {
        "token_profile_latest_feed_matches_count": len(matches),
        "token_profiles_compact": [
            {
                "chainId": item.get("chainId"),
                "tokenAddress": item.get("tokenAddress"),
                "url": item.get("url"),
                "description": item.get("description"),
                "links": item.get("links"),
            }
            for item in matches
            if isinstance(item, dict)
        ],
    }


async def stream_dexscreener_24h(
    mint: str,
    interval: int = 60,
    length: int = 24 * 60 * 60,
    save_dir: Path | None = None,
) -> Path:
    path = dexscreener_24h_path(mint, save_dir)
    start_ms = now_ms()
    length_ms = length * 1000

    while now_ms() - start_ms <= length_ms:
        try:
            response = await trading_info_multi_pool(mint)
            append_jsonl(
                path,
                {
                    "timestamp": now_ts(),
                    "local_received_at_ms": now_ms(),
                    "source": "dexscreener",
                    "method": "token-pairs",
                    "mint": mint,
                    "http_status": response.http_status,
                    "elapsed_ms": response.elapsed_ms,
                    "rate_limit": response.rate_limit,
                    "error_type": response.error_type,
                    "error_message": response.error_message,
                    "data": response.data,
                },
            )
        except Exception as exc:
            logger.exception("DexScreener 24h polling failed for %s: %s", mint, exc)
            append_jsonl(
                path,
                {
                    "timestamp": now_ts(),
                    "local_received_at_ms": now_ms(),
                    "source": "dexscreener",
                    "method": "token-pairs",
                    "mint": mint,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "data": None,
                },
            )

        await asyncio.sleep(interval)

    return path


async def general_info(mint: str):
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/{CHAIN_ID}/{mint}"
    response = await request_json("GET", url)
    if response.error_type:
        logger.warning("Failed to download dexscreener general token information: %s", response)
    else:
        logger.info("Downloaded dexscreener general token information")
    return response


async def check_paid_orders(mint: str):
    url = f"{DEXSCREENER_BASE}/orders/v1/{CHAIN_ID}/{mint}"
    response = await request_enrichment_json(url)
    if response.error_type:
        logger.warning("Failed to download dexscreener token paid orders info: %s", response)
    else:
        logger.info("Downloaded dexscreener token paid orders info")
    return response


async def token_boosts_latest():
    return await request_enrichment_json(f"{DEXSCREENER_BASE}/token-boosts/latest/v1")


async def token_boosts_top():
    return await request_enrichment_json(f"{DEXSCREENER_BASE}/token-boosts/top/v1")


async def ads_latest():
    return await request_enrichment_json(f"{DEXSCREENER_BASE}/ads/latest/v1")


async def community_takeovers_latest():
    return await request_enrichment_json(f"{DEXSCREENER_BASE}/community-takeovers/latest/v1")


async def token_profiles_latest():
    return await request_enrichment_json(f"{DEXSCREENER_BASE}/token-profiles/latest/v1")


async def trading_info_multi_pool(mint: str):
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/{CHAIN_ID}/{mint}"
    response = await request_json("GET", url)
    if response.error_type:
        logger.warning("Failed to download dexcreener coin trading info for %s", response)
    else:
        logger.info("Downloaded dexscreener coin trading info")
    return response


async def transactions_multiple_tokens(*mints: str):
    joined = ",".join(str(mint) for mint in mints)
    url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{joined}"
    response = await request_json("GET", url)
    if response.error_type:
        logger.warning("Failed to download dexscreener multi-coin info: %s", response)
    else:
        logger.info("Downloaded dexscreener multi-coin info")
    return response


async def stream_trading_info_one_coin(save_path: Path, interval: int, length: int, mint: str):
    length_ms = length * 1000
    starting_time = now_ms()
    while now_ms() - starting_time <= length_ms:
        try:
            trading_info = await trading_info_multi_pool(mint)
            if not trading_info.error_type:
                append_jsonl(save_path, {"timestamp": now_ts(), "trading_info": trading_info.data})
        except Exception as exc:
            logger.exception("Polling failed: %s", exc)
        await asyncio.sleep(interval)


async def stream_trading_info_multi_coin(save_path: Path, interval: int, length: int, *mints: str):
    length_ms = length * 1000
    starting_time = now_ms()
    while now_ms() - starting_time <= length_ms:
        try:
            trading_info = await transactions_multiple_tokens(*mints)
            if not trading_info.error_type:
                append_jsonl(
                    save_path,
                    {"timestamp": now_ts(), "mints": list(mints), "trading_info": trading_info.data},
                )
        except Exception as exc:
            logger.exception("Polling failed: %s", exc)
        await asyncio.sleep(interval)


async def collect_enrichment(mint: str, save_dir: Path | None = None) -> dict[str, Any]:
    configure_logging()

    endpoint_calls = {
        "token_pairs": general_info(mint),
        "tokens": transactions_multiple_tokens(mint),
        "orders": check_paid_orders(mint),
        "token_boosts_latest": token_boosts_latest(),
        "token_boosts_top": token_boosts_top(),
        "ads_latest": ads_latest(),
        "community_takeovers_latest": community_takeovers_latest(),
        "token_profiles_latest": token_profiles_latest(),
    }

    results = await asyncio.gather(*endpoint_calls.values(), return_exceptions=True)
    raw = {name: response_to_dict(response) for name, response in zip(endpoint_calls.keys(), results)}

    pairs = extract_pairs(raw["token_pairs"], mint, CHAIN_ID)
    links = extract_links_from_pairs(pairs)
    orders = extract_orders_features(raw["orders"])
    boosts = extract_boost_features(raw["token_boosts_latest"], raw["token_boosts_top"], pairs, mint)
    ads = extract_ads_features(raw["ads_latest"], mint)
    takeovers = extract_community_takeover_features(raw["community_takeovers_latest"], mint)
    profiles = extract_profile_features(raw["token_profiles_latest"], mint)

    features = {
        "mint": mint,
        "chain": CHAIN_ID,
        "generated_at": now_ts(),
        "endpoint_status": {
            name: {
                "http_status": response.get("http_status"),
                "error_type": response.get("error_type"),
                "error_message": response.get("error_message"),
                "rate_limit": response.get("rate_limit", {}),
            }
            for name, response in raw.items()
        },
        "has_profile": bool(orders["has_profile_from_orders"] or profiles["token_profile_latest_feed_matches_count"]),
        "has_website": links["has_website"],
        "has_telegram": links["has_telegram"],
        "has_x": links["has_x"],
        "boost_total_amount": boosts["boost_total_amount"],
        "boost_amount_now": boosts["boost_amount_now"],
        "has_active_ad": bool(ads["has_active_ad_from_ads_feed"] or orders["has_active_ad_from_orders"]),
        "ad_impressions": ads["ad_impressions"],
        "ad_duration_hours": ads["ad_duration_hours"],
        "paid_order_count": orders["paid_order_count"],
        "latest_payment_age_seconds": orders["latest_payment_age_seconds"],
        "community_takeover_flag": bool(orders["community_takeover_from_orders"] or takeovers["community_takeover_from_feed"]),
        **extract_market_features(pairs),
        "websites": links["websites"],
        "socials": links["socials"],
        "orders_compact": orders["orders_compact"],
        "boosts_compact": boosts["boosts_compact"],
        "ads_compact": ads["ads_compact"],
        "community_takeover_compact": takeovers["community_takeover_compact"],
        "token_profiles_compact": profiles["token_profiles_compact"],
        "token_profile_latest_feed_matches_count": profiles["token_profile_latest_feed_matches_count"],
        "pair_boosts_active_max": boosts["pair_boosts_active_max"],
    }

    report = {
        "timestamp": now_ts(),
        "source": "dexscreener",
        "method": "paid_attention_enrichment",
        "mint": mint,
        "chain": CHAIN_ID,
        "features": features,
        "raw": raw,
    }
    save_json(enrichment_path(mint, save_dir), report)
    logger.info("Saved DexScreener enrichment to %s", enrichment_path(mint, save_dir))
    return report


async def main(mint: str, save_dir: Path | None = None, enrichment: bool = False) -> dict[str, Any]:
    configure_logging()
    if enrichment:
        return await collect_enrichment(mint, save_dir=save_dir)

    names = ["general_info", "paid_orders"]
    data = await asyncio.gather(general_info(mint), check_paid_orders(mint))
    output = dict(zip(names, data))
    output["time"] = now_ts()
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mint", help="Solana token mint address")
    parser.add_argument("--stream-24h", action="store_true")
    parser.add_argument("--enrichment", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--length", type=int, default=24 * 60 * 60)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    configure_logging()

    if args.stream_24h:
        path = asyncio.run(
            stream_dexscreener_24h(
                mint=args.mint,
                interval=args.interval,
                length=args.length,
                save_dir=args.out_dir,
            )
        )
        print(json.dumps({"saved_to": str(path)}, indent=2))
    elif args.enrichment:
        data = asyncio.run(collect_enrichment(args.mint, save_dir=args.out_dir))
        print(json.dumps({"saved_to": str(enrichment_path(args.mint, args.out_dir)), "features": data["features"]}, indent=2))
    else:
        data = asyncio.run(main(args.mint))
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
