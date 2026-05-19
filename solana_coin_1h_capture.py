#!/usr/bin/env python3
"""
solana_coin_1h_capture.py

Goal:
  Leave this running ~1 hour for ONE Solana coin/pair and collect as much raw data
  as possible for later backtest viability assessment.

What it does:
  - loads API/RPC config from .env before resolving defaults
  - logsSubscribe for mint + pair/pool/watch addresses
  - getSignaturesForAddress polling/backfill
  - persistent signature queue
  - slow getTransaction fetcher with retries
  - rotates across multiple RPC HTTP URLs if provided
  - respects HTTP 429 Retry-After when present
  - accountSubscribe for watched addresses
  - periodic getMultipleAccounts snapshots
  - discovers token accounts from fetched tx pre/post token balances
  - writes a final viability_report.json
  - prints progressive CLI status while data is being captured/fetched

No external dotenv package is required.

Recommended .env:
  HELIUS_API_KEY=YOUR_KEY

Optional .env:
  SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}
  SOLANA_RPC_URLS=https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY},https://api.mainnet-beta.solana.com
  SOLANA_WS_URL=wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}

Install:
  pip install requests websockets

Public RPC fallback:
  python solana_coin_1h_capture.py \
    --mint 33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump \
    --pair ETMhxtENfkMK85TAcveEbZdBv9htziWzDSddmShRP2wB \
    --duration-seconds 3600

Recommended with .env HELIUS_API_KEY:
  python solana_coin_1h_capture.py \
    --mint 33eum82LaAhtv5YkUq1BdwEviSErH5CnFxqVNLT5pump \
    --pair ETMhxtENfkMK85TAcveEbZdBv9htziWzDSddmShRP2wB \
    --duration-seconds 3600

Multiple RPC URLs, rotate fetches:
  python solana_coin_1h_capture.py \
    --mint TOKEN \
    --pair PAIR \
    --rpc https://api.mainnet-beta.solana.com \
    --rpc https://mainnet.helius-rpc.com/?api-key=KEY \
    --duration-seconds 3600

Default output folder:
  data/raw/onchain/<mint>

Outputs:
  manifest.json
  signatures_seen.jsonl
  raw_transactions.jsonl
  tx_index.csv
  failed_attempts.jsonl
  account_notifications.jsonl
  account_snapshots.jsonl
  discovered_token_accounts.jsonl
  ws_log_notifications.jsonl
  viability_report.json

Important:
  For reconstruction, raw_transactions.jsonl is the source of truth.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import random
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
import websockets


PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
PUBLIC_WS = "wss://api.mainnet-beta.solana.com"
HELIUS_RPC_TEMPLATE = "https://mainnet.helius-rpc.com/?api-key={api_key}"
HELIUS_WS_TEMPLATE = "wss://mainnet.helius-rpc.com/?api-key={api_key}"

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


_ENV_LINE_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")


def load_env_file(path: Path, override: bool = False) -> Dict[str, str]:
    """
    Load KEY=VALUE pairs from a .env file into os.environ.

    Supports:
      KEY=value
      export KEY=value
      KEY="quoted value"
      KEY='quoted value'
      inline comments for unquoted values

    Existing environment variables are not overwritten unless override=True.
    Returns the values parsed from the file.
    """
    parsed: Dict[str, str] = {}

    if not path.exists():
        return parsed

    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            match = _ENV_LINE_RE.match(line)
            if not match:
                continue

            key, value = match.group(1), match.group(2).strip()

            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            else:
                value = value.split(" #", 1)[0].strip()

            value = os.path.expandvars(value)
            parsed[key] = value

            if override or key not in os.environ:
                os.environ[key] = value

    # Second pass allows variables earlier in the file to reference variables
    # loaded later in the same .env. This is intentionally simple and deterministic.
    for key, value in parsed.items():
        expanded = os.path.expandvars(value)
        parsed[key] = expanded
        if override or key not in os.environ:
            os.environ[key] = expanded

    return parsed


def now_ts() -> float:
    return time.time()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_iso(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def append_jsonl(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True, default=str) + "\n")


def save_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, default=str)
    tmp.replace(path)


def iter_jsonl(path: Path) -> Iterable[Any]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def append_csv(path: Path, row: Dict[str, Any], fieldnames: List[str]) -> None:
    ensure_dir(path.parent)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def read_csv_col(path: Path, col: str) -> Set[str]:
    out: Set[str] = set()
    if not path.exists():
        return out

    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = row.get(col)
                if val:
                    out.add(val)
    except Exception:
        pass

    return out


def chunked(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i:i + n]


def normalize_account_key(k: Any) -> Optional[str]:
    if isinstance(k, str):
        return k

    if isinstance(k, dict):
        for field in ("pubkey", "account", "address"):
            val = k.get(field)
            if isinstance(val, str):
                return val

    return None


def get_account_keys(tx: Dict[str, Any]) -> List[str]:
    msg = (((tx.get("transaction") or {}).get("message")) or {})
    keys = msg.get("accountKeys") or []
    out: List[str] = []

    for k in keys:
        val = normalize_account_key(k)
        if val:
            out.append(val)

    return out


def token_balance_float(row: Dict[str, Any]) -> Optional[float]:
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


def short_rpc_name(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]

    if "helius" in url:
        return f"helius_{h}"
    if "alchemy" in url:
        return f"alchemy_{h}"
    if "quicknode" in url or "quiknode" in url:
        return f"quicknode_{h}"
    if "solana.com" in url or "mainnet-beta" in url:
        return f"public_{h}"

    return f"rpc_{h}"


def sanitize_url_for_display(url: str) -> str:
    """
    Hide api-key query values in CLI output and manifest-adjacent logs.
    The actual URL used by requests is unchanged.
    """
    return re.sub(r"([?&]api-key=)[^&\s]+", r"\1***", url)


def split_csv_env(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def expand_url(value: str) -> str:
    return os.path.expandvars(value.strip())


def helius_rpc_url(api_key: str) -> str:
    return HELIUS_RPC_TEMPLATE.format(api_key=api_key)


def helius_ws_url(api_key: str) -> str:
    return HELIUS_WS_TEMPLATE.format(api_key=api_key)


def resolve_rpc_urls(cli_rpc_urls: List[str]) -> List[str]:
    """
    Priority:
      1. explicit --rpc values
      2. SOLANA_RPC_URLS from environment/.env
      3. SOLANA_RPC_URL from environment/.env
      4. HELIUS_API_KEY from environment/.env
      5. public Solana RPC
    """
    urls: List[str] = [expand_url(u) for u in cli_rpc_urls if u.strip()]

    env_rpc_urls = split_csv_env(os.getenv("SOLANA_RPC_URLS"))
    urls.extend(expand_url(u) for u in env_rpc_urls)

    env_rpc_url = os.getenv("SOLANA_RPC_URL")
    if env_rpc_url:
        urls.append(expand_url(env_rpc_url))

    if not urls:
        helius_api_key = os.getenv("HELIUS_API_KEY")
        if helius_api_key:
            urls.append(helius_rpc_url(helius_api_key))

    if not urls:
        urls.append(PUBLIC_RPC)

    deduped: List[str] = []
    for url in urls:
        if url and url not in deduped:
            deduped.append(url)

    return deduped


def resolve_ws_url(cli_ws_url: Optional[str]) -> str:
    """
    Priority:
      1. explicit --ws
      2. SOLANA_WS_URL from environment/.env
      3. HELIUS_API_KEY from environment/.env
      4. public Solana WS
    """
    if cli_ws_url:
        return expand_url(cli_ws_url)

    env_ws_url = os.getenv("SOLANA_WS_URL")
    if env_ws_url:
        return expand_url(env_ws_url)

    helius_api_key = os.getenv("HELIUS_API_KEY")
    if helius_api_key:
        return helius_ws_url(helius_api_key)

    return PUBLIC_WS


@dataclass
class RpcResult:
    ok: bool
    result: Any = None
    error: Optional[str] = None
    retry_after: Optional[float] = None
    status_code: Optional[int] = None


class RpcEndpoint:
    def __init__(self, url: str, min_interval: float, debug: bool = False):
        self.url = url
        self.name = short_rpc_name(url)
        self.min_interval = min_interval
        self.debug = debug
        self.session = requests.Session()
        self.req_id = random.randint(1000, 1_000_000)
        self.next_allowed_at = 0.0
        self.stats = {
            "calls": 0,
            "ok": 0,
            "rate_limited": 0,
            "errors": 0,
            "last_error": None,
        }

    def call(self, method: str, params: List[Any]) -> RpcResult:
        wait = self.next_allowed_at - now_ts()
        if wait > 0:
            time.sleep(wait)

        if self.min_interval > 0:
            self.next_allowed_at = max(self.next_allowed_at, now_ts()) + self.min_interval

        self.req_id += 1
        body = {
            "jsonrpc": "2.0",
            "id": self.req_id,
            "method": method,
            "params": params,
        }

        self.stats["calls"] += 1

        try:
            if self.debug:
                print(f"[{self.name}] RPC {method}", flush=True)

            r = self.session.post(self.url, json=body, timeout=60)
            retry_after = None

            if r.headers.get("Retry-After"):
                try:
                    retry_after = float(r.headers["Retry-After"])
                except Exception:
                    retry_after = None

            if r.status_code == 429:
                self.stats["rate_limited"] += 1
                self.stats["last_error"] = "HTTP_429_RATE_LIMIT"

                if retry_after is None:
                    retry_after = 10.0

                self.next_allowed_at = max(self.next_allowed_at, now_ts() + retry_after)
                return RpcResult(False, error="HTTP_429_RATE_LIMIT", retry_after=retry_after, status_code=429)

            if r.status_code == 403:
                self.stats["errors"] += 1
                self.stats["last_error"] = "HTTP_403_FORBIDDEN"
                return RpcResult(False, error="HTTP_403_FORBIDDEN", status_code=403)

            if r.status_code >= 400:
                self.stats["errors"] += 1
                msg = f"HTTP_{r.status_code}: {r.text[:500]}"
                self.stats["last_error"] = msg
                return RpcResult(False, error=msg, status_code=r.status_code)

            data = r.json()

            if "error" in data:
                self.stats["errors"] += 1
                msg = f"RPC_ERROR_{method}: {data['error']}"
                self.stats["last_error"] = msg

                msg_lower = str(data["error"]).lower()
                if "rate" in msg_lower or "limit" in msg_lower or "too many" in msg_lower:
                    self.stats["rate_limited"] += 1
                    self.next_allowed_at = max(self.next_allowed_at, now_ts() + 10.0)
                    return RpcResult(False, error="RPC_RATE_LIMIT", retry_after=10.0)

                return RpcResult(False, error=msg)

            self.stats["ok"] += 1
            return RpcResult(True, result=data.get("result"))

        except requests.RequestException as e:
            self.stats["errors"] += 1
            msg = f"REQUEST_EXCEPTION: {repr(e)}"
            self.stats["last_error"] = msg
            self.next_allowed_at = max(self.next_allowed_at, now_ts() + 3.0)
            return RpcResult(False, error=msg)

        except Exception as e:
            self.stats["errors"] += 1
            msg = f"EXCEPTION: {repr(e)}"
            self.stats["last_error"] = msg
            return RpcResult(False, error=msg)


class RpcPool:
    def __init__(self, urls: List[str], min_interval: float, debug: bool = False):
        if not urls:
            urls = [PUBLIC_RPC]

        self.endpoints = [RpcEndpoint(u, min_interval=min_interval, debug=debug) for u in urls]
        self.idx = 0

    def next_endpoint(self) -> RpcEndpoint:
        ep = self.endpoints[self.idx % len(self.endpoints)]
        self.idx += 1
        return ep

    def call_any(self, method: str, params: List[Any], tries: Optional[int] = None) -> Tuple[RpcResult, str]:
        if tries is None:
            tries = max(1, len(self.endpoints))

        last: Optional[Tuple[RpcResult, str]] = None

        for _ in range(tries):
            ep = self.next_endpoint()
            res = ep.call(method, params)

            if res.ok:
                return res, ep.name

            last = (res, ep.name)

        assert last is not None
        return last

    def stats(self) -> Dict[str, Any]:
        return {ep.name: ep.stats | {"url": sanitize_url_for_display(ep.url)} for ep in self.endpoints}


def rpc_get_transaction(pool: RpcPool, sig: str) -> Tuple[RpcResult, str]:
    return pool.call_any(
        "getTransaction",
        [
            sig,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    )


def rpc_get_signatures(pool: RpcPool, address: str, limit: int) -> Tuple[RpcResult, str]:
    return pool.call_any(
        "getSignaturesForAddress",
        [
            address,
            {
                "limit": limit,
                "commitment": "confirmed",
            },
        ],
    )


def rpc_get_multiple_accounts(pool: RpcPool, addresses: List[str]) -> Tuple[RpcResult, str]:
    return pool.call_any(
        "getMultipleAccounts",
        [
            addresses,
            {
                "encoding": "base64",
                "commitment": "confirmed",
            },
        ],
    )


def summarize_tx_for_mint(
    signature: str,
    tx: Any,
    mint: str,
    seen_by: Iterable[str],
    rpc_name: str,
    attempt: int,
) -> Dict[str, Any]:
    base = {
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
    pre_map: Dict[Tuple[int, str], float] = {}
    post_map: Dict[Tuple[int, str], float] = {}

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
    deltas = []

    for k in keys:
        deltas.append(post_map.get(k, 0.0) - pre_map.get(k, 0.0))

    err = meta.get("err")

    base.update({
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
    })

    return base


def discover_token_accounts_from_tx(
    tx: Any,
    target_mint: str,
    quote_mints: Set[str],
) -> List[Dict[str, Any]]:
    if not isinstance(tx, dict):
        return []

    account_keys = get_account_keys(tx)
    meta = tx.get("meta") or {}
    rows = (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or [])

    wanted = set(quote_mints) | {target_mint}
    discovered: Dict[Tuple[str, str], Dict[str, Any]] = {}

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


class Capture:
    def __init__(
        self,
        mint: str,
        watch_addresses: List[str],
        quote_mints: Set[str],
        rpc_urls: List[str],
        ws_url: str,
        out_dir: Path,
        duration_seconds: int,
        rpc_min_interval: float,
        tx_retry_seconds: float,
        max_tx_attempts: int,
        poll_seconds: float,
        backfill_limit: int,
        snapshot_seconds: float,
        max_queue_size: int,
        display_seconds: float,
        debug: bool,
    ):
        self.mint = mint
        self.watch_addresses = watch_addresses
        self.quote_mints = quote_mints
        self.out_dir = out_dir
        self.duration_seconds = duration_seconds
        self.ws_url = ws_url
        self.debug = debug

        self.rpc_pool = RpcPool(rpc_urls, min_interval=rpc_min_interval, debug=debug)
        self.tx_retry_seconds = tx_retry_seconds
        self.max_tx_attempts = max_tx_attempts
        self.poll_seconds = poll_seconds
        self.backfill_limit = backfill_limit
        self.snapshot_seconds = snapshot_seconds
        self.max_queue_size = max_queue_size
        self.display_seconds = display_seconds

        self.stop_event = asyncio.Event()
        self.started_at = now_ts()

        self.paths = {
            "manifest": out_dir / "manifest.json",
            "signatures": out_dir / "signatures_seen.jsonl",
            "raw": out_dir / "raw_transactions.jsonl",
            "failed": out_dir / "failed_attempts.jsonl",
            "tx_index": out_dir / "tx_index.csv",
            "ws_logs": out_dir / "ws_log_notifications.jsonl",
            "account_notifications": out_dir / "account_notifications.jsonl",
            "snapshots": out_dir / "account_snapshots.jsonl",
            "discovered_accounts": out_dir / "discovered_token_accounts.jsonl",
            "report": out_dir / "viability_report.json",
        }

        self.csv_fields = [
            "fetched_at",
            "signature",
            "attempt",
            "rpc",
            "slot",
            "block_time",
            "iso_time",
            "err",
            "seen_by",
            "mentions_mint_in_balances",
            "mint_accounts_changed",
            "mint_balance_delta_sum",
            "fee_lamports",
            "has_inner_instructions",
            "log_count",
        ]

        self.seen_sigs: Set[str] = set()
        self.fetched_sigs: Set[str] = set()
        self.signature_sources: Dict[str, Set[str]] = {}
        self.attempts: Dict[str, int] = {}
        self.pending: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue_size)

        self.snapshot_accounts: Set[str] = set(watch_addresses)
        self.discovered_token_accounts: Set[str] = set()

        self.stats = {
            "signatures_seen": 0,
            "signatures_new_this_run": 0,
            "transactions_fetched": 0,
            "transactions_failed": 0,
            "transactions_success_err_none": 0,
            "transactions_failed_onchain_err": 0,
            "account_notifications": 0,
            "account_snapshots": 0,
            "discovered_token_accounts": 0,
        }

        self.load_existing_state()

    def load_existing_state(self) -> None:
        for obj in iter_jsonl(self.paths["signatures"]) or []:
            sig = obj.get("signature")
            if sig:
                self.seen_sigs.add(sig)
                src = obj.get("source") or obj.get("seen_by") or "existing"
                self.signature_sources.setdefault(sig, set()).add(str(src))

        self.fetched_sigs |= read_csv_col(self.paths["tx_index"], "signature")

        for obj in iter_jsonl(self.paths["discovered_accounts"]) or []:
            acct = obj.get("token_account")
            if acct:
                self.discovered_token_accounts.add(acct)
                self.snapshot_accounts.add(acct)

        self.stats["signatures_seen"] = len(self.seen_sigs)

    def write_manifest(self, rpc_urls: List[str]) -> None:
        save_json(self.paths["manifest"], {
            "created_at": now_iso(),
            "mint": self.mint,
            "storage_dir": str(self.out_dir),
            "watch_addresses": self.watch_addresses,
            "quote_mints": sorted(self.quote_mints),
            "rpc_urls": [sanitize_url_for_display(u) for u in rpc_urls],
            "ws_url": sanitize_url_for_display(self.ws_url),
            "duration_seconds": self.duration_seconds,
            "poll_seconds": self.poll_seconds,
            "backfill_limit": self.backfill_limit,
            "snapshot_seconds": self.snapshot_seconds,
            "tx_retry_seconds": self.tx_retry_seconds,
            "max_tx_attempts": self.max_tx_attempts,
            "outputs": {k: str(v.name) for k, v in self.paths.items()},
        })

    def display_progress(self, final: bool = False) -> None:
        elapsed = max(1.0, now_ts() - self.started_at)
        remaining = max(0.0, float(self.duration_seconds) - elapsed)

        seen = len(self.seen_sigs)
        fetched = len(self.fetched_sigs)
        coverage = fetched / seen if seen else 0.0
        fetched_per_min = fetched / elapsed * 60.0

        rpc_stats = self.rpc_pool.stats()
        rpc_calls = sum(int(v.get("calls", 0)) for v in rpc_stats.values())
        rpc_ok = sum(int(v.get("ok", 0)) for v in rpc_stats.values())
        rpc_429 = sum(int(v.get("rate_limited", 0)) for v in rpc_stats.values())
        rpc_errors = sum(int(v.get("errors", 0)) for v in rpc_stats.values())

        prefix = "[final]" if final else "[progress]"
        print(
            (
                f"{prefix} "
                f"elapsed={format_duration(elapsed)} "
                f"remaining={format_duration(remaining)} "
                f"seen={seen} "
                f"fetched={fetched} "
                f"coverage={coverage:.1%} "
                f"queue={self.pending.qsize()} "
                f"new_this_run={self.stats['signatures_new_this_run']} "
                f"failed_fetches={self.stats['transactions_failed']} "
                f"discovered_accounts={len(self.discovered_token_accounts)} "
                f"snapshots={self.stats['account_snapshots']} "
                f"tx_rate={fetched_per_min:.2f}/min "
                f"rpc_ok={rpc_ok}/{rpc_calls} "
                f"rpc_429={rpc_429} "
                f"rpc_errors={rpc_errors} "
                f"out={self.out_dir}"
            ),
            flush=True,
        )

    async def add_signature(self, sig: str, seen_by: str, source: str) -> None:
        if not sig:
            return

        self.signature_sources.setdefault(sig, set()).add(seen_by)

        is_new = sig not in self.seen_sigs

        if is_new:
            self.seen_sigs.add(sig)
            self.stats["signatures_new_this_run"] += 1
            self.stats["signatures_seen"] = len(self.seen_sigs)

            append_jsonl(self.paths["signatures"], {
                "first_seen_at": now_iso(),
                "signature": sig,
                "seen_by": seen_by,
                "source": source,
            })

        if sig not in self.fetched_sigs and self.attempts.get(sig, 0) < self.max_tx_attempts:
            try:
                self.pending.put_nowait(sig)
            except asyncio.QueueFull:
                append_jsonl(self.paths["failed"], {
                    "failed_at": now_iso(),
                    "signature": sig,
                    "error": "LOCAL_QUEUE_FULL",
                    "will_retry": True,
                })

    async def websocket_listener(self) -> None:
        """
        WebSocket does discovery only; if it disconnects, poller covers gaps.
        """
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    print(f"[ws] connected {sanitize_url_for_display(self.ws_url)}", flush=True)
                    pending_ids: Dict[int, Tuple[str, str]] = {}
                    req_id = 1

                    for addr in self.watch_addresses:
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "logsSubscribe",
                            "params": [
                                {"mentions": [addr]},
                                {"commitment": "confirmed"},
                            ],
                        }))
                        pending_ids[req_id] = ("logs", addr)
                        req_id += 1

                    for addr in self.watch_addresses:
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0",
                            "id": req_id,
                            "method": "accountSubscribe",
                            "params": [
                                addr,
                                {
                                    "encoding": "base64",
                                    "commitment": "confirmed",
                                },
                            ],
                        }))
                        pending_ids[req_id] = ("account", addr)
                        req_id += 1

                    sub_id_to_target: Dict[int, Tuple[str, str]] = {}

                    while not self.stop_event.is_set():
                        msg = json.loads(await ws.recv())

                        if "result" in msg and "id" in msg:
                            target = pending_ids.get(int(msg["id"]))
                            if target:
                                sub_id_to_target[int(msg["result"])] = target
                                print(f"[ws] subscribed {target[0]} {target[1]}", flush=True)
                            continue

                        params = msg.get("params") or {}
                        sub_id = params.get("subscription")
                        kind, addr = sub_id_to_target.get(sub_id, ("unknown", f"sub_{sub_id}"))

                        if msg.get("method") == "logsNotification":
                            append_jsonl(self.paths["ws_logs"], {
                                "received_at": now_iso(),
                                "address": addr,
                                "message": msg,
                            })
                            value = (params.get("result") or {}).get("value") or {}
                            sig = value.get("signature")
                            if sig:
                                await self.add_signature(sig, addr, "websocket_logs")

                        elif msg.get("method") == "accountNotification":
                            self.stats["account_notifications"] += 1
                            append_jsonl(self.paths["account_notifications"], {
                                "received_at": now_iso(),
                                "address": addr,
                                "message": msg,
                            })

            except asyncio.CancelledError:
                raise

            except Exception as e:
                print(f"[ws] error {e!r}; reconnect in 5s", flush=True)
                await asyncio.sleep(5)

    async def poller(self) -> None:
        while not self.stop_event.is_set():
            for addr in self.watch_addresses:
                res, rpc_name = await asyncio.to_thread(
                    rpc_get_signatures,
                    self.rpc_pool,
                    addr,
                    self.backfill_limit,
                )

                if not res.ok:
                    append_jsonl(self.paths["failed"], {
                        "failed_at": now_iso(),
                        "signature": None,
                        "operation": "getSignaturesForAddress",
                        "address": addr,
                        "rpc": rpc_name,
                        "error": res.error,
                        "will_retry": True,
                    })
                    continue

                for row in res.result or []:
                    sig = row.get("signature")
                    if sig:
                        await self.add_signature(sig, addr, "poll_getSignaturesForAddress")

            await asyncio.sleep(self.poll_seconds)

    async def fetcher(self) -> None:
        while not self.stop_event.is_set():
            try:
                sig = await asyncio.wait_for(self.pending.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if sig in self.fetched_sigs:
                self.pending.task_done()
                continue

            attempt = self.attempts.get(sig, 0) + 1
            self.attempts[sig] = attempt

            res, rpc_name = await asyncio.to_thread(rpc_get_transaction, self.rpc_pool, sig)

            if res.ok:
                tx = res.result

                append_jsonl(self.paths["raw"], {
                    "fetched_at": now_iso(),
                    "signature": sig,
                    "attempt": attempt,
                    "rpc": rpc_name,
                    "seen_by": sorted(self.signature_sources.get(sig, set())),
                    "transaction": tx,
                })

                row = summarize_tx_for_mint(
                    sig,
                    tx,
                    self.mint,
                    self.signature_sources.get(sig, set()),
                    rpc_name,
                    attempt,
                )
                append_csv(self.paths["tx_index"], row, self.csv_fields)

                self.fetched_sigs.add(sig)
                self.stats["transactions_fetched"] += 1

                if row["err"]:
                    self.stats["transactions_failed_onchain_err"] += 1
                else:
                    self.stats["transactions_success_err_none"] += 1

                for acct in discover_token_accounts_from_tx(tx, self.mint, self.quote_mints):
                    token_account = acct.get("token_account")
                    if token_account and token_account not in self.discovered_token_accounts:
                        self.discovered_token_accounts.add(token_account)
                        self.snapshot_accounts.add(token_account)
                        self.stats["discovered_token_accounts"] += 1
                        append_jsonl(self.paths["discovered_accounts"], acct)

            else:
                self.stats["transactions_failed"] += 1
                will_retry = attempt < self.max_tx_attempts

                append_jsonl(self.paths["failed"], {
                    "failed_at": now_iso(),
                    "signature": sig,
                    "attempt": attempt,
                    "operation": "getTransaction",
                    "rpc": rpc_name,
                    "error": res.error,
                    "retry_after": res.retry_after,
                    "will_retry": will_retry,
                })

                if will_retry:
                    delay = res.retry_after if res.retry_after is not None else self.tx_retry_seconds
                    asyncio.create_task(self.requeue_later(sig, delay))

            self.pending.task_done()

    async def requeue_later(self, sig: str, delay: float) -> None:
        await asyncio.sleep(max(1.0, delay))

        if not self.stop_event.is_set() and sig not in self.fetched_sigs:
            try:
                self.pending.put_nowait(sig)
            except asyncio.QueueFull:
                append_jsonl(self.paths["failed"], {
                    "failed_at": now_iso(),
                    "signature": sig,
                    "error": "LOCAL_QUEUE_FULL_ON_REQUEUE",
                    "will_retry": True,
                })

    async def snapshotter(self) -> None:
        while not self.stop_event.is_set():
            accounts = sorted(self.snapshot_accounts)

            for batch in chunked(accounts, 100):
                res, rpc_name = await asyncio.to_thread(rpc_get_multiple_accounts, self.rpc_pool, batch)

                if res.ok:
                    self.stats["account_snapshots"] += 1
                    append_jsonl(self.paths["snapshots"], {
                        "snapshot_at": now_iso(),
                        "rpc": rpc_name,
                        "accounts": batch,
                        "result": res.result,
                    })
                else:
                    append_jsonl(self.paths["failed"], {
                        "failed_at": now_iso(),
                        "signature": None,
                        "operation": "getMultipleAccounts",
                        "rpc": rpc_name,
                        "error": res.error,
                        "will_retry": True,
                    })

            await asyncio.sleep(self.snapshot_seconds)

    async def reporter(self) -> None:
        while not self.stop_event.is_set():
            self.write_report(partial=True)
            await asyncio.sleep(60)

    async def progress_reporter(self) -> None:
        if self.display_seconds <= 0:
            return

        while not self.stop_event.is_set():
            self.display_progress(final=False)
            await asyncio.sleep(self.display_seconds)

    def write_report(self, partial: bool = False) -> Dict[str, Any]:
        elapsed = max(1.0, now_ts() - self.started_at)
        fetched = len(self.fetched_sigs)
        seen = len(self.seen_sigs)
        coverage = fetched / seen if seen else 0.0

        report = {
            "generated_at": now_iso(),
            "partial": partial,
            "elapsed_seconds": elapsed,
            "mint": self.mint,
            "storage_dir": str(self.out_dir),
            "watch_addresses": self.watch_addresses,
            "signatures_seen_total": seen,
            "signatures_fetched_total": fetched,
            "raw_coverage_ratio": coverage,
            "pending_queue_size": self.pending.qsize(),
            "discovered_token_accounts": len(self.discovered_token_accounts),
            "snapshot_accounts": len(self.snapshot_accounts),
            "stats_this_run": self.stats,
            "rpc_stats": self.rpc_pool.stats(),
            "viability": {
                "usable_for_rough_timeline": coverage >= 0.5 and fetched >= 20,
                "usable_for_serious_backtest": coverage >= 0.9 and fetched >= 50,
                "reason": (
                    "Good raw transaction coverage."
                    if coverage >= 0.9 and fetched >= 50
                    else "Need higher raw transaction coverage; keep drain running or use a better RPC."
                ),
            },
        }

        save_json(self.paths["report"], report)
        return report

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self.websocket_listener()),
            asyncio.create_task(self.poller()),
            asyncio.create_task(self.fetcher()),
            asyncio.create_task(self.snapshotter()),
            asyncio.create_task(self.reporter()),
            asyncio.create_task(self.progress_reporter()),
        ]

        async def stop_after() -> None:
            await asyncio.sleep(self.duration_seconds)
            self.stop_event.set()

        tasks.append(asyncio.create_task(stop_after()))

        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.5)
        finally:
            print("[main] stopping; draining queue briefly...", flush=True)

            stop_deadline = now_ts() + 15
            while not self.pending.empty() and now_ts() < stop_deadline:
                await asyncio.sleep(0.5)

            for t in tasks:
                t.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

            report = self.write_report(partial=False)
            self.display_progress(final=True)
            print(json.dumps(report["viability"], indent=2), flush=True)


def parse_args() -> argparse.Namespace:
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument("--env-file", default=".env", help="Path to .env file. Default: .env")
    env_parser.add_argument(
        "--env-override",
        action="store_true",
        help="Allow .env values to override existing process environment variables.",
    )

    env_args, remaining = env_parser.parse_known_args()
    loaded_env = load_env_file(Path(env_args.env_file), override=env_args.env_override)

    parser = argparse.ArgumentParser(parents=[env_parser])
    parser.set_defaults(_loaded_env_file=env_args.env_file, _loaded_env_keys=sorted(loaded_env.keys()))

    parser.add_argument("--mint", required=True, help="Target token mint")
    parser.add_argument("--pair", action="append", default=[], help="Pair/pool address to watch; can pass multiple")
    parser.add_argument("--watch", action="append", default=[], help="Extra address to watch; can pass multiple")

    parser.add_argument(
        "--out",
        default="data/raw/onchain",
        help="Base output directory. Final storage path is <out>/<mint>. Default: data/raw/onchain",
    )

    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--rpc", action="append", default=[], help="HTTP RPC URL; can pass multiple")
    parser.add_argument("--ws", default=None, help="WebSocket RPC URL. Overrides SOLANA_WS_URL / HELIUS_API_KEY.")
    parser.add_argument("--rpc-min-interval", type=float, default=1.1, help="Minimum seconds between calls per RPC URL")
    parser.add_argument("--tx-retry-seconds", type=float, default=20.0)
    parser.add_argument("--max-tx-attempts", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=20.0)
    parser.add_argument("--backfill-limit", type=int, default=5)
    parser.add_argument("--snapshot-seconds", type=float, default=60.0)
    parser.add_argument("--max-queue-size", type=int, default=100000)
    parser.add_argument("--quote-mint", action="append", default=[WSOL_MINT, USDC_MINT])
    parser.add_argument("--display-seconds", type=float, default=10.0, help="CLI progress display interval. Use 0 to disable.")
    parser.add_argument("--debug", action="store_true")

    return parser.parse_args(remaining)


async def async_main() -> int:
    args = parse_args()

    rpc_urls = resolve_rpc_urls(args.rpc)
    ws_url = resolve_ws_url(args.ws)

    watch_addresses: List[str] = []
    for a in [args.mint] + args.pair + args.watch:
        if a and a not in watch_addresses:
            watch_addresses.append(a)

    out_dir = Path(args.out) / args.mint
    ensure_dir(out_dir)

    capture = Capture(
        mint=args.mint,
        watch_addresses=watch_addresses,
        quote_mints=set(args.quote_mint),
        rpc_urls=rpc_urls,
        ws_url=ws_url,
        out_dir=out_dir,
        duration_seconds=args.duration_seconds,
        rpc_min_interval=args.rpc_min_interval,
        tx_retry_seconds=args.tx_retry_seconds,
        max_tx_attempts=args.max_tx_attempts,
        poll_seconds=args.poll_seconds,
        backfill_limit=args.backfill_limit,
        snapshot_seconds=args.snapshot_seconds,
        max_queue_size=args.max_queue_size,
        display_seconds=args.display_seconds,
        debug=args.debug,
    )

    capture.write_manifest(rpc_urls)

    helius_key_loaded = bool(os.getenv("HELIUS_API_KEY"))

    print("Storage:", out_dir, flush=True)
    print("Env file:", args._loaded_env_file, flush=True)
    print("Loaded env keys:", ", ".join(args._loaded_env_keys) if args._loaded_env_keys else "(none)", flush=True)
    print("HELIUS_API_KEY:", "loaded" if helius_key_loaded else "not set", flush=True)

    print("Watch addresses:", flush=True)
    for a in watch_addresses:
        print(" ", a, flush=True)

    print("HTTP RPC endpoints:", flush=True)
    for u in rpc_urls:
        print(" ", short_rpc_name(u), sanitize_url_for_display(u), flush=True)

    print("WS:", sanitize_url_for_display(ws_url), flush=True)
    print(
        (
            "Progress format: elapsed remaining seen fetched coverage queue "
            "new_this_run failed_fetches discovered_accounts snapshots tx_rate rpc stats out"
        ),
        flush=True,
    )

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, sig_name), capture.stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    try:
        await capture.run()
    except KeyboardInterrupt:
        capture.stop_event.set()
        capture.write_report(partial=False)
        capture.display_progress(final=True)

    print("Done. Check viability_report.json", flush=True)
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
