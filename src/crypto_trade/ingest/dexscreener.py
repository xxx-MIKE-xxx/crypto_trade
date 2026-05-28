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
  python test_dexscreener_enrichment_api.py 33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump
  

Outputs:
  raw/*.json
  features.json
  summary.txt
"""

from __future__ import annotations

import argparse
import json
from dotenv import load_dotenv
import asyncio
import logging
from pathlib import Path
from crypto_trade.core.paths import ONCHAIN_DIR
from crypto_trade.core.http import request_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.time import now_ts, now_ms
from crypto_trade.core.io import append_jsonl
from dataclasses import asdict, is_dataclass

load_dotenv()

logger = logging.getLogger(__name__)

DEXSCREENER_BASE = "https://api.dexscreener.com"

CHAIN_ID = "solana"

DEXSCREENER_24H_FILENAME = "dexscreener_24h.jsonl"


def dexscreener_24h_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / DEXSCREENER_24H_FILENAME

    return ONCHAIN_DIR / mint / DEXSCREENER_24H_FILENAME


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

def json_default(obj):
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    return str(obj)

async def general_info(mint):
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/{CHAIN_ID}/{mint}"
    response = await request_json(
        "GET",
        url
    )
    if response.error_type:
        logger.warning("Failed to download dexscreener general token information: %s", response)
    else:
        logger.info("Downloaded dexscreener general token information")

    return response

    
async def check_paid_orders(mint):
    url = f"{DEXSCREENER_BASE}/orders/v1/{CHAIN_ID}/{mint}"
    response = await request_json(
        "GET",
        url
    )
    if response.error_type:
        logger.warning("Failed to download dexscreener token paid orders info: %s", response)
    else:
        logger.info("Downloaded dexscreener token paid orders info")
    
    return response


async def trading_info_multi_pool(mint):
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/{CHAIN_ID}/{mint}"
    response = await request_json(
        "GET",
        url
    )
    if response.error_type:
        logger.warning("Failed to download dexcreener coin trading info for %s", response)
    else:
        logger.info("Downloaded dexscreener coin trading info")
    return response

async def transactions_multiple_tokens(*mints):
    a = [str(a) for a in [*mints]]
    a = (",".join(a))
    url = f"{DEXSCREENER_BASE}/tokens/v1/solana/{a}"
    response = await request_json(
        "GET",
        url
    )
    if response.error_type:
        logger.warning("Failed to download dexscreener multi-coin info: %s", response)
    else:
        logger.info("Downloaded dexscreener multi-coin info")
    
    return response

 

async def stream_trading_info_one_coin(save_path, interval, length, mint):
    length_ms = length * 1000
    starting_time = now_ms()
    while now_ms() - starting_time <= length_ms:
        try:
            trading_info = await trading_info_multi_pool(mint)
            if not trading_info.error_type:
                snapshot = {
                    "timestamp": now_ts(),
                    "trading_info": trading_info.data
                }
                append_jsonl(save_path, snapshot)
        except Exception as e:
            logger.exception("Polling failed: %s", e)
        await asyncio.sleep(interval)

        
async def stream_trading_info_multi_coin(save_path, interval, length, *mints):
    length_ms = length * 1000
    starting_time = now_ms()
    while now_ms() - starting_time <= length_ms:
        try:
            trading_info = await transactions_multiple_tokens(*mints)

            if not trading_info.error_type:
                snapshot = {
                    "timestamp": now_ts(),
                    "mints": list(mints),
                    "trading_info": trading_info.data
                }

                append_jsonl(save_path, snapshot)

        except Exception as e:
            logger.exception("Polling failed: %s", e)

        await asyncio.sleep(interval)

        
        

async def main(mint):
    configure_logging()
    names = ["general_info", "paid_orders"]
    data = await asyncio.gather(
        general_info(mint),
        check_paid_orders(mint)
    )
    output = dict(zip(names, data))
    output["time"] = now_ts()
    return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mint", help="Solana token mint address")
    parser.add_argument("--stream-24h", action="store_true")
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
    else:
        data = asyncio.run(main(args.mint))
        print(json.dumps(data, indent=2, ensure_ascii=False, default=json_default))
