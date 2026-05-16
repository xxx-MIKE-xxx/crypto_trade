"""Mint extraction and PumpPortal event classification.

Pure helpers: no I/O, no DB, no HTTP. Safe to call from tests.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

SOLANA_ADDR_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

MINT_KEYS = {
    "mint",
    "ca",
    "contract",
    "contractaddress",
    "token",
    "tokenaddress",
    "tokenmint",
    "mintaddress",
    "basemint",
    "address",
}


def as_mapping(obj: Any) -> Mapping[str, Any]:
    return obj if isinstance(obj, Mapping) else {}


def looks_like_solana_address(value: Any) -> bool:
    return isinstance(value, str) and bool(SOLANA_ADDR_RE.fullmatch(value.strip()))


def extract_mint(obj: Any) -> str | None:
    """Best-effort extraction across PumpPortal/DexScreener/on-chain payload variants."""
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            key = str(k).replace("_", "").replace("-", "").lower()
            if key in MINT_KEYS:
                if looks_like_solana_address(v):
                    return str(v)
                if isinstance(v, Mapping):
                    nested = extract_mint(v)
                    if nested:
                        return nested

        # DexScreener style nested tokens. Prefer base token for the newly migrated token.
        for token_key in ("baseToken", "base_token", "token"):
            token = obj.get(token_key)
            if isinstance(token, Mapping):
                addr = token.get("address") or token.get("mint") or token.get("tokenAddress")
                if looks_like_solana_address(addr):
                    return str(addr)

        for v in obj.values():
            nested = extract_mint(v)
            if nested:
                return nested

    elif isinstance(obj, list):
        for item in obj:
            nested = extract_mint(item)
            if nested:
                return nested

    return None


def classify_pumpportal_event(payload: Mapping[str, Any]) -> str:
    candidates: list[str] = []
    for key in ("txType", "type", "event", "eventType", "method", "instruction", "name"):
        val = payload.get(key)
        if isinstance(val, str):
            candidates.append(val.lower())
    joined = " ".join(candidates)

    if "migrat" in joined:
        return "migration"
    if "create" in joined or "newtoken" in joined or "new_token" in joined:
        return "new_token"

    low_keys = {str(k).lower() for k in payload.keys()}
    if any(
        k in low_keys
        for k in ("pool", "pooladdress", "pair", "pairaddress", "raydiumpool", "migrated")
    ):
        return "migration"

    return "pumpportal_event"
