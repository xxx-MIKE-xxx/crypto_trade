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
from dotenv import load_dotenv
import os

'''
from crypto_trade.core.http import HttpResponse, get_json, make_session
from crypto_trade.core.io import save_json
from crypto_trade.core.logging import configure_logging
from crypto_trade.core.time import now_ms, utc_now

'''

BASE_PATH = Path(__file__).resolve().parents

load_dotenv()

dex_screener_api_key = os.getenv("HELIUS_API_KEY")

print(dex_screener_api_key)