import json
from pathlib import Path

import pytest

import dexscreener_api


MINT_ADDRESS = "33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump"


def test_module_imports():
    assert dexscreener_api is not None


def test_save_json_creates_parent_directories_and_writes_valid_json(tmp_path):
    output_path = (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / MINT_ADDRESS
        / "dexscreener"
        / "features.json"
    )

    payload = {
        "token": MINT_ADDRESS,
        "chain": "solana",
        "has_profile": True,
        "has_website": True,
        "has_telegram": False,
        "has_x": True,
        "boost_total_amount": 100.0,
        "boost_amount_now": 10.0,
        "has_active_ad": False,
        "ad_impressions": None,
        "ad_duration_hours": None,
        "paid_order_count": 2,
        "latest_payment_age_seconds": 100,
        "community_takeover_flag": False,
    }

    dexscreener_api.save_json(output_path, payload)

    assert output_path.exists()
    assert output_path.is_file()

    saved = json.loads(output_path.read_text(encoding="utf-8"))

    assert saved == payload


def test_expected_dexscreener_output_paths_are_under_mint_address(tmp_path):
    base_out_dir = tmp_path / "data" / "raw" / "analytics"
    out_dir = dexscreener_api.build_output_dir(base_out_dir, MINT_ADDRESS)
    raw_dir = out_dir / "raw"

    features_path = out_dir / "features.json"
    summary_path = out_dir / "summary.txt"
    raw_token_pairs_path = raw_dir / "token_pairs.json"

    assert features_path == (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / MINT_ADDRESS
        / "dexscreener"
        / "features.json"
    )

    assert summary_path == (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / MINT_ADDRESS
        / "dexscreener"
        / "summary.txt"
    )

    assert raw_token_pairs_path == (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / MINT_ADDRESS
        / "dexscreener"
        / "raw"
        / "token_pairs.json"
    )


@pytest.mark.parametrize(
    "filename",
    [
        "token_pairs.json",
        "tokens.json",
        "orders.json",
        "token_boosts_latest.json",
        "token_boosts_top.json",
        "ads_latest.json",
        "community_takeovers_latest.json",
        "token_profiles_latest.json",
    ],
)
def test_expected_raw_file_names(filename, tmp_path):
    raw_dir = (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / MINT_ADDRESS
        / "dexscreener"
        / "raw"
    )

    path = raw_dir / filename

    assert path.name == filename
    assert path.parent.name == "raw"
    assert path.parent.parent.name == "dexscreener"
    assert path.parent.parent.parent.name == MINT_ADDRESS


def test_features_json_format(tmp_path):
    features_path = (
        tmp_path
        / "data"
        / "raw"
        / "analytics"
        / MINT_ADDRESS
        / "dexscreener"
        / "features.json"
    )

    payload = {
        "token": MINT_ADDRESS,
        "chain": "solana",
        "generated_at": "2026-05-19T14:00:00+00:00",
        "endpoint_status": {
            "orders": {
                "status_code": 200,
                "url": "https://api.dexscreener.com/orders/v1/solana/test",
                "rate_headers": {},
            }
        },
        "has_profile": False,
        "has_website": True,
        "has_telegram": True,
        "has_x": False,
        "boost_total_amount": None,
        "boost_amount_now": None,
        "has_active_ad": False,
        "ad_impressions": None,
        "ad_duration_hours": None,
        "paid_order_count": 0,
        "latest_payment_age_seconds": None,
        "community_takeover_flag": False,
    }

    dexscreener_api.save_json(features_path, payload)

    saved = json.loads(features_path.read_text(encoding="utf-8"))

    required_keys = {
        "token",
        "chain",
        "generated_at",
        "endpoint_status",
        "has_profile",
        "has_website",
        "has_telegram",
        "has_x",
        "boost_total_amount",
        "boost_amount_now",
        "has_active_ad",
        "ad_impressions",
        "ad_duration_hours",
        "paid_order_count",
        "latest_payment_age_seconds",
        "community_takeover_flag",
    }

    assert required_keys <= saved.keys()
    assert saved["token"] == MINT_ADDRESS
    assert saved["chain"] == "solana"
    assert isinstance(saved["endpoint_status"], dict)
    assert isinstance(saved["has_profile"], bool)
    assert isinstance(saved["has_website"], bool)
    assert isinstance(saved["has_telegram"], bool)
    assert isinstance(saved["has_x"], bool)
    assert isinstance(saved["has_active_ad"], bool)
    assert isinstance(saved["community_takeover_flag"], bool)


def test_extract_market_features_prefers_highest_liquidity_pair():
    pairs = [
        {
            "dexId": "lowdex",
            "pairAddress": "LowPair",
            "priceUsd": "0.01",
            "liquidity": {"usd": 100},
        },
        {
            "dexId": "highdex",
            "pairAddress": "HighPair",
            "priceUsd": "0.05",
            "liquidity": {"usd": 5000, "base": 10, "quote": 250},
            "volume": {"m5": 1, "h1": 2, "h6": 3, "h24": 4},
            "txns": {"h24": {"buys": 7, "sells": 5}},
            "priceChange": {"h24": 42},
            "fdv": 12345,
            "marketCap": 12000,
        },
    ]

    features = dexscreener_api.extract_market_features(pairs)

    assert features["primary_pair_address"] == "HighPair"
    assert features["primary_dex_id"] == "highdex"
    assert features["price_usd"] == 0.05
    assert features["liquidity_usd"] == 5000
    assert features["volume_h24"] == 4
    assert features["txns_h24_buys"] == 7
    assert features["price_change_h24"] == 42
    assert len(features["pairs_compact"]) == 2


def test_collect_snapshot_appends_history_and_timestamped_raw(tmp_path, monkeypatch):
    def fake_request_json(url, sleep_s, debug):
        if "token-pairs" in url:
            body = [
                {
                    "chainId": "solana",
                    "pairAddress": "Pair111",
                    "baseToken": {"address": MINT_ADDRESS},
                    "quoteToken": {"address": "So11111111111111111111111111111111111111112"},
                    "priceUsd": "0.25",
                    "liquidity": {"usd": 1000},
                }
            ]
        else:
            body = []
        return {"url": url, "status_code": 200, "elapsed_seconds": 0.0, "headers": {}, "body": body}

    monkeypatch.setattr(dexscreener_api, "request_json", fake_request_json)

    features = dexscreener_api.collect_dexscreener_snapshot(
        token=MINT_ADDRESS,
        chain="solana",
        out=tmp_path,
        sleep_s=0,
        quiet=True,
        append_history=True,
        timestamped_raw=True,
        endpoint_profile="market",
    )

    out_dir = tmp_path / MINT_ADDRESS / "dexscreener"
    assert features["price_usd"] == 0.25
    assert features["endpoint_profile"] == "market"
    assert features["request_count"] == 1
    assert (out_dir / "features.json").exists()
    assert (out_dir / "features_history.jsonl").exists()
    assert list((out_dir / "raw").glob("*/token_pairs.json"))
