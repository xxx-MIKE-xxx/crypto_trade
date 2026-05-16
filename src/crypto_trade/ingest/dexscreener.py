#!/usr/bin/env python3
"""
test_dexscreener_enrichment_api.py

Tests DexScreener public API endpoints for enrichment features:

  has_profile
  has_website
  has_telegram
  has_x
  boost_total_amount
  boost_amount_now
  has_active_ad
  ad_impressions
  ad_duration_hours
  paid_order_count
  latest_payment_age_seconds
  community_takeover_flag

No API key/account required.

Install:
  pip install requests

Run:
  python test_dexscreener_enrichment_api.py --token 33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump --out ./dexscreener_test --debug

Outputs:
  raw/*.json
  features.json
  summary.txt
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, List

from crypto_trade.core.http import HttpResponse, get_json, make_session
from crypto_trade.core.io import save_json
from crypto_trade.core.logging import configure_logging
from crypto_trade.core.time import now_ms, utc_now

BASE = "https://api.dexscreener.com"
CHAIN = "solana"
DEFAULT_TOKEN = "33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump"

_SESSION = make_session(user_agent="dexscreener-enrichment-test/1.0")


def request_json(url: str, sleep_s: float, debug: bool) -> Dict[str, Any]:
    """Backwards-compatible wrapper around :func:`core.http.get_json`.

    Downstream extractors depend on the ``body``/``headers``/``status_code``
    keys, so we render the :class:`HttpResponse` into the historical dict shape.
    """
    time.sleep(sleep_s)
    resp: HttpResponse = get_json(url, session=_SESSION, timeout=30, name="dexscreener")

    body: Any = resp.body
    if body is None and resp.text is not None:
        body = {"_raw_text": resp.text}

    out = {
        "url": resp.url,
        "status_code": resp.status_code,
        "elapsed_seconds": resp.elapsed_seconds,
        "headers": resp.headers,
        "body": body,
    }

    if debug:
        print(f"GET {url}")
        print(f"  -> {resp.status_code}, {resp.elapsed_seconds:.2f}s")

    return out


def as_list(x: Any) -> List[Any]:
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        data = x.get("data")
        if isinstance(data, list):
            return data
        return [x]
    return []


def body_list(resp: Dict[str, Any]) -> List[Any]:
    return as_list(resp.get("body"))


def lower(s: Any) -> str:
    return str(s or "").lower()


def item_token_matches(item: Any, token: str, chain: str = CHAIN) -> bool:
    if not isinstance(item, dict):
        return False

    token_l = token.lower()
    chain_l = chain.lower()

    candidates = [
        item.get("tokenAddress"),
        item.get("address"),
        item.get("baseToken", {}).get("address") if isinstance(item.get("baseToken"), dict) else None,
    ]
    chain_ids = [item.get("chainId"), item.get("chain")]

    token_ok = any(str(x).lower() == token_l for x in candidates if x)
    chain_ok = not any(chain_ids) or any(str(x).lower() == chain_l for x in chain_ids if x)

    return token_ok and chain_ok


def extract_pairs(resp: Dict[str, Any], token: str) -> List[Dict[str, Any]]:
    pairs = body_list(resp)
    out = []
    for p in pairs:
        if not isinstance(p, dict):
            continue
        base = lower((p.get("baseToken") or {}).get("address") if isinstance(p.get("baseToken"), dict) else None)
        quote = lower((p.get("quoteToken") or {}).get("address") if isinstance(p.get("quoteToken"), dict) else None)
        if lower(p.get("chainId")) == CHAIN and (base == lower(token) or quote == lower(token)):
            out.append(p)
    return out


def extract_links_from_pairs(pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    websites = []
    socials = []
    for p in pairs:
        info = p.get("info") or {}
        if isinstance(info.get("websites"), list):
            websites.extend(info["websites"])
        if isinstance(info.get("socials"), list):
            socials.extend(info["socials"])

    def social_platforms() -> List[str]:
        out = []
        for s in socials:
            if isinstance(s, dict):
                out.append(lower(s.get("type") or s.get("platform") or s.get("label")))
        return out

    platforms = social_platforms()
    return {
        "websites": websites,
        "socials": socials,
        "has_website": len(websites) > 0,
        "has_telegram": any("telegram" in p or p == "tg" for p in platforms),
        "has_x": any(p in ("twitter", "x") or "twitter" in p for p in platforms),
    }


def extract_orders_features(resp: Dict[str, Any]) -> Dict[str, Any]:
    orders = body_list(resp)

    paid_order_count = 0
    latest_payment_ts = None
    has_profile = False
    has_active_ad_order = False
    community_takeover_flag = False

    normalized_orders = []
    for o in orders:
        if not isinstance(o, dict):
            continue

        paid_order_count += 1
        typ = lower(o.get("type") or o.get("orderType") or o.get("kind"))
        status = lower(o.get("status"))
        payment_ts = o.get("paymentTimestamp") or o.get("paymentTime") or o.get("createdAt")

        normalized_orders.append({
            "type": typ,
            "status": status,
            "paymentTimestamp": payment_ts,
        })

        if payment_ts is not None:
            try:
                latest_payment_ts = max(int(payment_ts), latest_payment_ts or 0)
            except Exception:
                pass

        approved_or_active = status in ("approved", "active", "completed", "success", "paid")
        if "profile" in typ and approved_or_active:
            has_profile = True
        if ("ad" in typ or "trending" in typ) and approved_or_active:
            has_active_ad_order = True
        if "community" in typ or "takeover" in typ or "cto" in typ:
            if approved_or_active or not status:
                community_takeover_flag = True

    latest_payment_age_seconds = None
    if latest_payment_ts:
        # DexScreener timestamps are usually ms; tolerate seconds too.
        ts_ms = latest_payment_ts if latest_payment_ts > 10_000_000_000 else latest_payment_ts * 1000
        latest_payment_age_seconds = max(0, int((now_ms() - ts_ms) / 1000))

    return {
        "paid_order_count": paid_order_count,
        "latest_payment_age_seconds": latest_payment_age_seconds,
        "has_profile_from_orders": has_profile,
        "has_active_ad_from_orders": has_active_ad_order,
        "community_takeover_from_orders": community_takeover_flag,
        "orders_compact": normalized_orders,
    }


def extract_boost_features(latest_resp: Dict[str, Any], top_resp: Dict[str, Any], pairs: List[Dict[str, Any]], token: str) -> Dict[str, Any]:
    matched_boosts = []

    for resp in (latest_resp, top_resp):
        for item in body_list(resp):
            if item_token_matches(item, token):
                matched_boosts.append(item)

    amounts = []
    total_amounts = []
    for b in matched_boosts:
        if not isinstance(b, dict):
            continue
        for key, dest in [("amount", amounts), ("totalAmount", total_amounts)]:
            val = b.get(key)
            if val is not None:
                try:
                    dest.append(float(val))
                except Exception:
                    pass

    # Pair endpoint sometimes has boosts.active.
    pair_boost_active_values = []
    for p in pairs:
        boosts = p.get("boosts") or {}
        if isinstance(boosts, dict) and boosts.get("active") is not None:
            try:
                pair_boost_active_values.append(float(boosts.get("active")))
            except Exception:
                pass

    return {
        "boost_amount_now": max(amounts) if amounts else None,
        "boost_total_amount": max(total_amounts) if total_amounts else None,
        "boosts_matched_count": len(matched_boosts),
        "pair_boosts_active_max": max(pair_boost_active_values) if pair_boost_active_values else None,
        "boosts_compact": [
            {
                "chainId": b.get("chainId"),
                "tokenAddress": b.get("tokenAddress"),
                "amount": b.get("amount"),
                "totalAmount": b.get("totalAmount"),
                "url": b.get("url"),
            }
            for b in matched_boosts
            if isinstance(b, dict)
        ],
    }


def extract_ads_features(resp: Dict[str, Any], token: str) -> Dict[str, Any]:
    ads = []
    for item in body_list(resp):
        if item_token_matches(item, token):
            ads.append(item)

    impressions = []
    durations = []
    statuses = []
    compact = []

    for a in ads:
        if not isinstance(a, dict):
            continue
        status = lower(a.get("status"))
        statuses.append(status)

        for key, dest in [("impressions", impressions), ("durationHours", durations)]:
            val = a.get(key)
            if val is not None:
                try:
                    dest.append(float(val))
                except Exception:
                    pass

        compact.append({
            "chainId": a.get("chainId"),
            "tokenAddress": a.get("tokenAddress"),
            "type": a.get("type"),
            "status": a.get("status"),
            "impressions": a.get("impressions"),
            "durationHours": a.get("durationHours"),
            "paymentTimestamp": a.get("paymentTimestamp"),
            "url": a.get("url"),
        })

    has_active_ad = any(s in ("approved", "active", "running", "paid", "completed") for s in statuses) or bool(ads)

    return {
        "has_active_ad_from_ads_feed": has_active_ad,
        "ad_impressions": max(impressions) if impressions else None,
        "ad_duration_hours": max(durations) if durations else None,
        "ads_matched_count": len(ads),
        "ads_compact": compact,
    }


def extract_community_takeover_features(resp: Dict[str, Any], token: str) -> Dict[str, Any]:
    matches = [x for x in body_list(resp) if item_token_matches(x, token)]
    return {
        "community_takeover_from_feed": len(matches) > 0,
        "community_takeover_matches_count": len(matches),
        "community_takeover_compact": [
            {
                "chainId": x.get("chainId"),
                "tokenAddress": x.get("tokenAddress"),
                "url": x.get("url"),
                "description": x.get("description"),
            }
            for x in matches
            if isinstance(x, dict)
        ],
    }


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    parser.add_argument("--chain", default=CHAIN)
    parser.add_argument("--out", default="./dexscreener_enrichment_test")
    parser.add_argument("--sleep", type=float, default=1.1, help="Safe delay; 60 rpm endpoints need >=1.0 sec.")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    token = args.token
    chain = args.chain
    out_dir = Path(args.out) / token
    raw_dir = out_dir / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    endpoints = {
        "token_pairs": f"{BASE}/token-pairs/v1/{chain}/{token}",
        "tokens": f"{BASE}/tokens/v1/{chain}/{token}",
        "orders": f"{BASE}/orders/v1/{chain}/{token}",
        "token_boosts_latest": f"{BASE}/token-boosts/latest/v1",
        "token_boosts_top": f"{BASE}/token-boosts/top/v1",
        "ads_latest": f"{BASE}/ads/latest/v1",
        "community_takeovers_latest": f"{BASE}/community-takeovers/latest/v1",
        "token_profiles_latest": f"{BASE}/token-profiles/latest/v1",
    }

    raw: Dict[str, Dict[str, Any]] = {}
    for name, url in endpoints.items():
        print(f"[fetch] {name}")
        resp = request_json(url, sleep_s=args.sleep, debug=args.debug)
        raw[name] = resp
        save_json(raw_dir / f"{name}.json", resp)

    pairs = extract_pairs(raw["token_pairs"], token)
    links = extract_links_from_pairs(pairs)
    orders_features = extract_orders_features(raw["orders"])
    boosts_features = extract_boost_features(raw["token_boosts_latest"], raw["token_boosts_top"], pairs, token)
    ads_features = extract_ads_features(raw["ads_latest"], token)
    cto_features = extract_community_takeover_features(raw["community_takeovers_latest"], token)

    # Extra: global token profiles feed can prove profile existence only if token is currently in latest feed.
    token_profile_feed_matches = [x for x in body_list(raw["token_profiles_latest"]) if item_token_matches(x, token)]

    features = {
        "token": token,
        "chain": chain,
        "generated_at": utc_now().isoformat(),
        "endpoint_status": {
            name: {
                "status_code": resp.get("status_code"),
                "url": resp.get("url"),
                "rate_headers": resp.get("headers", {}),
            }
            for name, resp in raw.items()
        },

        # Requested features:
        "has_profile": bool(orders_features["has_profile_from_orders"] or token_profile_feed_matches),
        "has_website": links["has_website"],
        "has_telegram": links["has_telegram"],
        "has_x": links["has_x"],
        "boost_total_amount": boosts_features["boost_total_amount"],
        "boost_amount_now": boosts_features["boost_amount_now"],
        "has_active_ad": bool(ads_features["has_active_ad_from_ads_feed"] or orders_features["has_active_ad_from_orders"]),
        "ad_impressions": ads_features["ad_impressions"],
        "ad_duration_hours": ads_features["ad_duration_hours"],
        "paid_order_count": orders_features["paid_order_count"],
        "latest_payment_age_seconds": orders_features["latest_payment_age_seconds"],
        "community_takeover_flag": bool(
            orders_features["community_takeover_from_orders"] or cto_features["community_takeover_from_feed"]
        ),

        # Useful debug/support fields:
        "pair_count": len(pairs),
        "websites": links["websites"],
        "socials": links["socials"],
        "orders_compact": orders_features["orders_compact"],
        "boosts_compact": boosts_features["boosts_compact"],
        "ads_compact": ads_features["ads_compact"],
        "community_takeover_compact": cto_features["community_takeover_compact"],
        "token_profile_latest_feed_matches_count": len(token_profile_feed_matches),
        "pair_boosts_active_max": boosts_features["pair_boosts_active_max"],
    }

    save_json(out_dir / "features.json", features)

    summary_lines = [
        f"Token: {token}",
        f"Chain: {chain}",
        "",
        "Requested features:",
        f"  has_profile: {features['has_profile']}",
        f"  has_website: {features['has_website']}",
        f"  has_telegram: {features['has_telegram']}",
        f"  has_x: {features['has_x']}",
        f"  boost_total_amount: {features['boost_total_amount']}",
        f"  boost_amount_now: {features['boost_amount_now']}",
        f"  has_active_ad: {features['has_active_ad']}",
        f"  ad_impressions: {features['ad_impressions']}",
        f"  ad_duration_hours: {features['ad_duration_hours']}",
        f"  paid_order_count: {features['paid_order_count']}",
        f"  latest_payment_age_seconds: {features['latest_payment_age_seconds']}",
        f"  community_takeover_flag: {features['community_takeover_flag']}",
        "",
        f"Saved raw responses in: {raw_dir}",
        f"Saved features in: {out_dir / 'features.json'}",
    ]
    (out_dir / "summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")

    print("\n".join(summary_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
