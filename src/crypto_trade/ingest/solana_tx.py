"""Solana transaction parsing helpers.

Pure functions over the JSON-parsed ``getTransaction`` response shape. No I/O,
no RPC: they are safe to use from tests and from replay tooling.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from crypto_trade.core.time import now_iso, ts_iso


def normalize_account_key(k: Any) -> Optional[str]:
    if isinstance(k, str):
        return k
    if isinstance(k, dict):
        for field_ in ("pubkey", "account", "address"):
            val = k.get(field_)
            if isinstance(val, str):
                return val
    return None


def get_account_keys(tx: dict[str, Any]) -> list[str]:
    msg = (((tx.get("transaction") or {}).get("message")) or {})
    keys = msg.get("accountKeys") or []
    out: list[str] = []
    for k in keys:
        val = normalize_account_key(k)
        if val:
            out.append(val)
    return out


def token_balance_float(row: dict[str, Any]) -> Optional[float]:
    ui = row.get("uiTokenAmount") or {}
    if ui.get("uiAmount") is not None:
        try:
            return float(ui["uiAmount"])
        except Exception:
            pass
    if ui.get("uiAmountString") is not None:
        try:
            return float(ui["uiAmountString"])
        except Exception:
            pass
    amount = ui.get("amount")
    dec = ui.get("decimals")
    if amount is not None and dec is not None:
        try:
            return float(amount) / (10 ** int(dec))
        except Exception:
            pass
    return None


def summarize_tx_for_mint(
    signature: str,
    tx: Any,
    mint: str,
    seen_by: Iterable[str],
    rpc_name: str,
    attempt: int,
) -> dict[str, Any]:
    """Flatten one ``getTransaction`` response into a CSV-friendly row."""
    base: dict[str, Any] = {
        "fetched_at": now_iso(),
        "signature": signature,
        "attempt": attempt,
        "rpc": rpc_name,
        "slot": None,
        "block_time": None,
        "iso_time": None,
        "err": None,
        "seen_by": ",".join(sorted(set(seen_by))),
        "mentions_mint_in_balances": False,
        "mint_accounts_changed": 0,
        "mint_balance_delta_sum": None,
        "fee_lamports": None,
        "has_inner_instructions": False,
        "log_count": 0,
    }

    if not isinstance(tx, dict):
        base["err"] = "null_or_missing_tx"
        return base

    meta = tx.get("meta") or {}
    pre_map: dict[tuple[int, str], float] = {}
    post_map: dict[tuple[int, str], float] = {}

    for row in meta.get("preTokenBalances") or []:
        if row.get("mint") == mint:
            try:
                idx = int(row.get("accountIndex", -1))
            except Exception:
                idx = -1
            owner = row.get("owner") or ""
            val = token_balance_float(row)
            if val is not None:
                pre_map[(idx, owner)] = val

    for row in meta.get("postTokenBalances") or []:
        if row.get("mint") == mint:
            try:
                idx = int(row.get("accountIndex", -1))
            except Exception:
                idx = -1
            owner = row.get("owner") or ""
            val = token_balance_float(row)
            if val is not None:
                post_map[(idx, owner)] = val

    keys = set(pre_map.keys()) | set(post_map.keys())
    deltas = [post_map.get(k, 0.0) - pre_map.get(k, 0.0) for k in keys]

    err = meta.get("err")
    base.update(
        {
            "slot": tx.get("slot"),
            "block_time": tx.get("blockTime"),
            "iso_time": ts_iso(tx.get("blockTime")),
            "err": json.dumps(err) if err else None,
            "mentions_mint_in_balances": bool(keys),
            "mint_accounts_changed": len(keys),
            "mint_balance_delta_sum": sum(deltas) if deltas else None,
            "fee_lamports": meta.get("fee"),
            "has_inner_instructions": bool(meta.get("innerInstructions")),
            "log_count": len(meta.get("logMessages") or []),
        }
    )
    return base


def discover_token_accounts_from_tx(
    tx: Any, target_mint: str, quote_mints: set[str]
) -> list[dict[str, Any]]:
    """Return newly observed (token_account, mint) pairs from a transaction."""
    if not isinstance(tx, dict):
        return []
    account_keys = get_account_keys(tx)
    meta = tx.get("meta") or {}
    rows = (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or [])

    wanted = set(quote_mints) | {target_mint}
    discovered: dict[tuple[str, str], dict[str, Any]] = {}

    sigs = ((tx.get("transaction") or {}).get("signatures") or [])
    source_sig = sigs[0] if sigs else None

    for row in rows:
        mint = row.get("mint")
        if mint not in wanted:
            continue
        try:
            idx = int(row.get("accountIndex", -1))
        except Exception:
            idx = -1
        token_account = account_keys[idx] if 0 <= idx < len(account_keys) else None
        if not token_account:
            continue

        discovered[(token_account, mint)] = {
            "discovered_at": now_iso(),
            "token_account": token_account,
            "mint": mint,
            "owner": row.get("owner"),
            "program_id": row.get("programId"),
            "source_slot": tx.get("slot"),
            "source_signature": source_sig,
        }

    return list(discovered.values())
