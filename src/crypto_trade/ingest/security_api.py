"""
Fetches security reports for a Solana token and saves them under:

data/raw/analytics/<mint>/security_report.json

Sources:
- RugCheck
- Defade
- GoPlus
- Jupiter
- DexScreener
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from crypto_trade.core.env import get_envs, load_env
from crypto_trade.core.http import request_json
from crypto_trade.core.io import save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ANALYTICS_DIR
from crypto_trade.core.time import now_ts

load_env()

logger = logging.getLogger(__name__)

(
    RUGCHECK_API_KEY,
    DEFADE_API_KEY,
    GOPLUS_API_KEY,
    GOPLUS_API_SECRET,
    JUPITER_API_KEY,
) = get_envs(
    [
        "RUGCHECK_API_KEY",
        "DEFADE_API_KEY",
        "GOPLUS_API_KEY",
        "GOPLUS_API_SECRET",
        "JUPITER_API_KEY",
    ]
)

RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"
DEXSCREENER_BASE = "https://api.dexscreener.com"
DEFADE_BASE = "https://api.defade.org/v1"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"
JUPITER_BASE = "https://api.jup.ag/tokens/v2"

OUTPUT_FILENAME = "security_report.json"


def json_default(obj: Any) -> Any:
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    return str(obj)


async def download_rugcheck(mint: str):
    url = f"{RUGCHECK_BASE}/tokens/{mint}/report"
    response = await request_json("GET", url)

    if response.error_type:
        logger.warning("Failed to download rugcheck security report: %s", response)
    else:
        logger.info("Downloaded rugcheck security report")

    return response


async def download_defade(mint: str):
    url = f"{DEFADE_BASE}/analyze/{mint}"
    response = await request_json(
        "GET",
        url,
        headers={"x-api-key": DEFADE_API_KEY},
    )

    if response.error_type:
        logger.warning("Failed to download defade security report: %s", response)
    else:
        logger.info("Downloaded defade security report")

    return response


async def download_goplus(mint: str):
    timestamp = int(now_ts())
    url = f"{GOPLUS_BASE}/solana/token_security"

    sign = hashlib.sha1(
        f"{GOPLUS_API_KEY}{timestamp}{GOPLUS_API_SECRET}".encode("utf-8")
    ).hexdigest()

    token_response = await request_json(
        "POST",
        f"{GOPLUS_BASE}/token",
        json={
            "app_key": GOPLUS_API_KEY,
            "time": timestamp,
            "sign": sign,
        },
    )

    if token_response.error_type:
        logger.warning("Failed to get GoPlus access token: %s", token_response)
        return token_response

    if not token_response.data or "access_token" not in token_response.data["result"]:
        logger.warning("GoPlus token response missing access_token: %s", token_response)
        return token_response

    access_token = token_response.data["result"]["access_token"]

    response = await request_json(
        "GET",
        url,
        headers={"Authorization": access_token},
        params={"contract_addresses": mint},
    )

    if response.error_type:
        logger.warning("Failed to download goplus security report: %s", response)
    else:
        logger.info("Downloaded goplus security report")

    return response


async def download_jupiter(mint: str):
    url = f"{JUPITER_BASE}/search"
    response = await request_json(
        "GET",
        url,
        headers={"x-api-key": JUPITER_API_KEY},
        params={"query": mint},
    )

    if response.error_type:
        logger.warning("Failed to download jupiter security report: %s", response)
    else:
        logger.info("Downloaded jupiter security report")

    return response


async def download_dexscreener(mint: str):
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{mint}"
    response = await request_json("GET", url)

    if response.error_type:
        logger.warning("Failed to download dexscreener security report: %s", response)
    else:
        logger.info("Downloaded dexscreener security report")

    return response


async def collect_security_report(mint: str) -> dict[str, Any]:
    names = [
        "rugcheck",
        "defade",
        "goplus",
        "jupiter",
        "dexscreener",
    ]

    reports = await asyncio.gather(
        download_rugcheck(mint),
        download_defade(mint),
        download_goplus(mint),
        download_jupiter(mint),
        download_dexscreener(mint),
        return_exceptions=True,
    )

    output = dict(zip(names, reports))
    output["mint"] = mint
    output["time"] = now_ts()

    return output


def security_report_path(mint: str, save_dir: Path | None = None) -> Path:
    if save_dir is not None:
        return save_dir / OUTPUT_FILENAME

    return ANALYTICS_DIR / mint / OUTPUT_FILENAME


async def main(mint: str, save_dir: Path | None = None) -> dict[str, Any]:
    configure_logging()

    report = await collect_security_report(mint)

    path = security_report_path(mint, save_dir)
    save_json(path, report)

    logger.info("Saved security report to %s", path)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mint", help="Solana token mint address")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Optional output directory. Defaults to data/raw/analytics/<mint>/",
    )
    args = parser.parse_args()

    result = asyncio.run(main(args.mint, save_dir=args.out_dir))

    print(
        json.dumps(
            {
                "mint": args.mint,
                "saved_to": str(security_report_path(args.mint, args.out_dir)),
                "sources": ["rugcheck", "defade", "goplus", "jupiter", "dexscreener"],
            },
            indent=2,
            ensure_ascii=False,
            default=json_default,
        )
    )