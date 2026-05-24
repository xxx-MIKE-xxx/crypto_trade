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
from crypto_trade.core.http import request_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.time import now_ts

load_dotenv()

logger = logging.getLogger(__name__)

DEXSCREENER_BASE = "https://api.dexscreener.com"

CHAIN_ID = "solana"

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
    parser.add_argument("mint", help="solana token mint address")
    args = parser.parse_args()
    data = asyncio.run(main(args.mint))
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
