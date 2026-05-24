"""
This file fetches security report from 5 most common security platforms for Solana:
DEFADE_API_KEY, GOPLUS_API_KEY, GOPLUS_API_SECRET, JUPITER_API_KEY, RUGCHECK_API_KEY
"""
import asyncio
import logging
import hashlib
import json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.env import load_env, get_envs
from crypto_trade.core.http import request_json
from crypto_trade.core.time import now_ts
import argparse

load_env()

logger = logging.getLogger(__name__)

RUGCHECK_API_KEY, DEFADE_API_KEY, GOPLUS_API_KEY, GOPLUS_API_SECRET, JUPITER_API_KEY = get_envs(["RUGCHECK_API_KEY", "DEFADE_API_KEY", "GOPLUS_API_KEY", "GOPLUS_API_SECRET", "JUPITER_API_KEY"])

RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"
DEXSCREENER_BASE = "https://api.dexscreener.com"
DEFADE_BASE = "https://api.defade.org/v1"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"
JUPITER_BASE = "https://api.jup.ag/tokens/v2"


async def download_rugcheck(mint):
    url = f"{RUGCHECK_BASE}/tokens/{mint}/report"
    auth = f"Bearer {RUGCHECK_API_KEY}"
    response = await request_json(
        "GET",
        url
    )
    if response.error_type:
        logger.warning("Failed to download rugcheck security report: %s", response)
    else:
        logger.info("Downloaded rugcheck security report")

    return response


async def download_defade(mint):
    url = f"{DEFADE_BASE}/analyze/{mint}"
    response = await request_json(
        "GET",
        url,
        headers={"x-api-key": DEFADE_API_KEY}
    )
    
    if response.error_type:
        logger.warning("Failed to download defade security report: %s", response)
    else:
        logger.info("Downloaded defade security report")

    return response


async def download_goplus(mint):
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
        headers={
            "Authorization": access_token
        },
        params={
            "contract_addresses": mint,
        },
    )

    if response.error_type:
        logger.warning("Failed to download goplus security report: %s", response)
    else:
        logger.info("Downloaded goplus security report")

    return response


async def download_jupiter(mint):
    url = f"{JUPITER_BASE}/search"
    response = await request_json(
        "GET",
        url,
        headers={"x-api-key": JUPITER_API_KEY},
        params={"query": mint}
    )
    
    if response.error_type:
        logger.warning("Failed to download jupiter security report: %s", response)
    else:
        logger.info("Downloaded jupiter security report")
    return response


async def download_dexscreener(mint):
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{mint}"
    response = await request_json(
        "GET",
        url
    )
    logger.info("Downloaded dexscreener security report")
    return response

async def main(mint):
    configure_logging()
    names = [
        "rugcheck",
        "defade",
        "goplus",
        "jupiter",
        "dexscreener"
    ]
    reports = await asyncio.gather(
        download_rugcheck(mint),
        download_defade(mint),
        download_goplus(mint),
        download_jupiter(mint),
        download_dexscreener(mint),
        return_exceptions=True
    )
    output = dict(zip(names, reports))
    output["time"] = now_ts()
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mint", help="Solana token mint address")
    args = parser.parse_args()
    reports = asyncio.run(main(args.mint))
    print(json.dumps(reports, indent=2, ensure_ascii=False, default=str))


