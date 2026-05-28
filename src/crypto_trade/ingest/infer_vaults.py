from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.http import request_json
from crypto_trade.core.io import save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import TMP_DIR
from crypto_trade.core.time import now_ms

logger = logging.getLogger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"
OUTPUT_DIR = TMP_DIR / "onchain"


async def call_rpc(method: str, params: list[Any]) -> dict[str, Any]:
    api_key = get_env("HELIUS_API_KEY")
    if not api_key:
        raise RuntimeError("Missing HELIUS_API_KEY in .env")

    response = await request_json(
        "POST",
        f"https://mainnet.helius-rpc.com/?api-key={api_key}",
        headers={"Content-Type": "application/json"},
        json={
            "jsonrpc": "2.0",
            "id": now_ms(),
            "method": method,
            "params": params,
        },
        timeout=30,
    )

    if response.error_type:
        raise RuntimeError(f"RPC failed: {response.error_type} {response.error_message}")

    data = response.data or {}
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"RPC failed: {data['error']}")

    return data


async def get_signatures(address: str, limit: int) -> list[str]:
    data = await call_rpc(
        "getSignaturesForAddress",
        [
            address,
            {
                "limit": limit,
                "commitment": "confirmed",
            },
        ],
    )

    rows = data.get("result") or []
    return [row["signature"] for row in rows if row.get("err") is None]


async def get_transaction(signature: str) -> dict[str, Any] | None:
    data = await call_rpc(
        "getTransaction",
        [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    )

    return data.get("result")


def tx_account_keys(tx: dict[str, Any]) -> list[str]:
    keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    out: list[str] = []

    for key in keys:
        if isinstance(key, dict):
            out.append(key.get("pubkey", ""))
        else:
            out.append(str(key))

    loaded = tx.get("meta", {}).get("loadedAddresses") or {}
    out.extend(loaded.get("writable") or [])
    out.extend(loaded.get("readonly") or [])

    return out


def token_amount(balance: dict[str, Any] | None) -> str:
    if not balance:
        return "0"
    return str(balance.get("uiTokenAmount", {}).get("amount") or "0")


def score_vault_candidates(
    txs: list[dict[str, Any]],
    target_mint: str,
) -> dict[str, dict[str, int]]:
    scores: dict[str, dict[str, int]] = {
        target_mint: defaultdict(int),
        WSOL_MINT: defaultdict(int),
    }

    for tx in txs:
        keys = tx_account_keys(tx)
        meta = tx.get("meta") or {}

        pre_by_index = {
            b.get("accountIndex"): b
            for b in meta.get("preTokenBalances") or []
            if b.get("mint") in scores
        }

        for post in meta.get("postTokenBalances") or []:
            mint = post.get("mint")
            index = post.get("accountIndex")

            if mint not in scores or index is None or index >= len(keys):
                continue

            before = token_amount(pre_by_index.get(index))
            after = token_amount(post)

            if before == after:
                continue

            token_account = keys[index]
            if token_account:
                scores[mint][token_account] += 1

    return {
        mint: dict(sorted(accounts.items(), key=lambda x: x[1], reverse=True))
        for mint, accounts in scores.items()
    }


async def infer_vaults_free(mint: str, pair_address: str, limit: int) -> dict[str, Any]:
    signatures = await get_signatures(pair_address, limit)

    txs: list[dict[str, Any]] = []
    for sig in signatures:
        tx = await get_transaction(sig)
        if tx:
            txs.append(tx)

    scores = score_vault_candidates(txs, mint)
    token_candidates = scores.get(mint, {})
    sol_candidates = scores.get(WSOL_MINT, {})

    return {
        "mint": mint,
        "pair_address": pair_address,
        "pool_state": pair_address,
        "token_vault": next(iter(token_candidates), None),
        "sol_vault": next(iter(sol_candidates), None),
        "signatures_requested": limit,
        "signatures_found": len(signatures),
        "transactions_loaded": len(txs),
        "estimated_credits": 1 + len(signatures),
        "scores": scores,
    }


async def main(mint: str, pair_address: str, limit: int) -> dict[str, Any]:
    configure_logging()
    load_env()

    result = await infer_vaults_free(mint, pair_address, limit)

    out_dir = OUTPUT_DIR / mint
    out_dir.mkdir(parents=True, exist_ok=True)
    save_json(out_dir / "vault_inference_free.json", result)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True)
    parser.add_argument("--pair-address", required=True)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    output = asyncio.run(
        main(
            mint=args.mint,
            pair_address=args.pair_address,
            limit=args.limit,
        )
    )

    print(json.dumps(output, indent=2, ensure_ascii=False))