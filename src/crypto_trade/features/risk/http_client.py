"""HTTP layer and multi-source fetch client for the risk package."""

from __future__ import annotations

import hashlib
import time
from typing import Any, Dict, Optional, Tuple

import requests

from crypto_trade.core.http import get_json, make_session

from .constants import (
    DEFADE_BASE,
    DEXSCREENER_BASE,
    GOPLUS_BASE,
    JUPITER_BASE,
    RUGCHECK_BASE,
    SOURCE_NAMES,
)
from .types import ReportConfig, SourceResult

_SESSION = make_session(user_agent="crypto-trade/token-risk")


def http_get_json(
    name: str,
    url: str,
    *,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    timeout: int = 15,
) -> Tuple[Optional[Any], Optional[int], Optional[str], Optional[str], float]:
    """GET JSON with timing. Returns ``(data, http_status, error_type, error_message, latency_ms)``."""
    resp = get_json(
        url,
        session=_SESSION,
        headers=headers,
        params=params,
        timeout=timeout,
        name=name,
    )
    latency_ms = resp.elapsed_seconds * 1000.0
    if resp.ok:
        return resp.body, resp.status_code, None, None, latency_ms
    return resp.body, resp.status_code, resp.error_type, resp.error_message, latency_ms


def _missing_key_result(source: str) -> SourceResult:
    return SourceResult(
        source=source,
        attempted=False,
        success=False,
        available=False,
        requires_key=True,
        error_type="missing_api_key",
        error_message=f"{source}: API key not configured",
        data=None,
        raw=None,
    )


class RiskReportClient:
    """Fetches and normalizes data from all configured risk sources."""

    def __init__(self, config: Optional[ReportConfig] = None) -> None:
        self.config = config or ReportConfig.from_env()

    def fetch_all(self, mint: str) -> Dict[str, SourceResult]:
        return {name: getattr(self, f"fetch_{name}")(mint) for name in SOURCE_NAMES}

    def fetch_rugcheck(self, mint: str) -> SourceResult:
        headers: Dict[str, str] = {}
        if self.config.rugcheck_api_key:
            headers["Authorization"] = f"Bearer {self.config.rugcheck_api_key}"
        url = f"{RUGCHECK_BASE}/tokens/{mint}/report"
        data, status, err_type, err_msg, latency = http_get_json(
            "rugcheck", url, headers=headers, timeout=self.config.timeout
        )
        ok = data is not None
        return SourceResult(
            source="rugcheck",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=False,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=data,
            raw=data,
        )

    def fetch_dexscreener(self, mint: str) -> SourceResult:
        url = f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{mint}"
        data, status, err_type, err_msg, latency = http_get_json(
            "dexscreener", url, timeout=self.config.timeout
        )
        if isinstance(data, dict) and "pairs" in data:
            pairs = data.get("pairs") or []
        elif isinstance(data, list):
            pairs = data
        else:
            pairs = []
        ok = data is not None
        return SourceResult(
            source="dexscreener",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=False,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=pairs,
            raw=data,
        )

    def fetch_defade(self, mint: str) -> SourceResult:
        if not self.config.defade_api_key:
            return _missing_key_result("defade")
        url = f"{DEFADE_BASE}/v1/analyze/{mint}"
        data, status, err_type, err_msg, latency = http_get_json(
            "defade",
            url,
            headers={"x-api-key": self.config.defade_api_key},
            timeout=self.config.timeout,
        )
        ok = data is not None
        return SourceResult(
            source="defade",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=True,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=data,
            raw=data,
        )

    def fetch_goplus(self, mint: str) -> SourceResult:
        if not self.config.goplus_api_key or not self.config.goplus_api_secret:
            return _missing_key_result("goplus")

        now = int(time.time())
        sign = hashlib.sha1(
            f"{self.config.goplus_api_key}{now}{self.config.goplus_api_secret}".encode("utf-8")
        ).hexdigest()
        token_payload = {
            "app_key": self.config.goplus_api_key,
            "time": now,
            "sign": sign,
        }
        start = time.perf_counter()
        try:
            token_resp = _SESSION.post(
                f"{GOPLUS_BASE}/token",
                json=token_payload,
                timeout=self.config.timeout,
            )
            token_latency = (time.perf_counter() - start) * 1000.0
            token_resp.raise_for_status()
            token_json = token_resp.json()
        except requests.RequestException as e:
            token_latency = (time.perf_counter() - start) * 1000.0
            return SourceResult(
                source="goplus",
                attempted=True,
                success=False,
                available=False,
                requires_key=True,
                latency_ms=round(token_latency, 2),
                http_status=getattr(getattr(e, "response", None), "status_code", None),
                error_type="request_error",
                error_message=f"goplus token request failed: {e}",
                data=None,
                raw=None,
            )
        except ValueError as e:
            token_latency = (time.perf_counter() - start) * 1000.0
            return SourceResult(
                source="goplus",
                attempted=True,
                success=False,
                available=False,
                requires_key=True,
                latency_ms=round(token_latency, 2),
                error_type="invalid_json",
                error_message=f"goplus token invalid JSON: {e}",
                data=None,
                raw=None,
            )

        access_token = token_json.get("result", {}).get("access_token") or token_json.get(
            "access_token"
        )
        if not access_token:
            return SourceResult(
                source="goplus",
                attempted=True,
                success=False,
                available=False,
                requires_key=True,
                latency_ms=round(token_latency, 2),
                error_type="auth_error",
                error_message="goplus: access_token missing in token response",
                data=None,
                raw=token_json,
            )

        url = f"{GOPLUS_BASE}/solana/token_security"
        data, status, err_type, err_msg, latency = http_get_json(
            "goplus",
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"contract_addresses": mint},
            timeout=self.config.timeout,
        )
        parsed = None
        if isinstance(data, dict):
            result = data.get("result", data)
            if isinstance(result, dict):
                parsed = result.get(mint) or result.get(mint.lower()) or result
            else:
                parsed = result
        ok = data is not None and parsed is not None
        total_latency = token_latency + latency
        return SourceResult(
            source="goplus",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=True,
            latency_ms=round(total_latency, 2),
            http_status=status,
            error_type=err_type,
            error_message=err_msg,
            data=parsed,
            raw=data,
        )

    def fetch_jupiter(self, mint: str) -> SourceResult:
        if not self.config.jupiter_api_key:
            return _missing_key_result("jupiter")
        url = f"{JUPITER_BASE}/search"
        data, status, err_type, err_msg, latency = http_get_json(
            "jupiter",
            url,
            headers={"x-api-key": self.config.jupiter_api_key},
            params={"query": mint},
            timeout=self.config.timeout,
        )
        item = None
        if isinstance(data, list):
            for row in data:
                if str(row.get("id", "")).lower() == mint.lower():
                    item = row
                    break
            if item is None and data:
                item = data[0]
        ok = item is not None
        return SourceResult(
            source="jupiter",
            attempted=True,
            success=ok,
            available=ok,
            requires_key=True,
            latency_ms=round(latency, 2) if latency is not None else None,
            http_status=status,
            error_type=err_type if not ok else None,
            error_message=err_msg if not ok else None,
            data=item,
            raw=data,
        )
