"""Tests for solana_risk_report — offline with mocked HTTP."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock

import pytest
import requests

import solana_risk_report as srr

SAMPLE_MINT = "9xQeWvG816bUx9EPjHmaT23yvVM2ZW1cRdxWhgn526S"


def make_config(**overrides: Any) -> srr.ReportConfig:
    defaults = {
        "timeout": 5,
        "include_raw": False,
        "defade_api_key": "defade-test-key",
        "goplus_api_key": "goplus-test-key",
        "goplus_api_secret": "goplus-test-secret",
        "jupiter_api_key": "jupiter-test-key",
        "rugcheck_api_key": "rugcheck-test-key",
    }
    defaults.update(overrides)
    return srr.ReportConfig(**defaults)


def make_config_no_keys() -> srr.ReportConfig:
    return make_config(
        defade_api_key=None,
        goplus_api_key=None,
        goplus_api_secret=None,
        jupiter_api_key=None,
        rugcheck_api_key=None,
    )


def _mock_http_response(
    json_data: Any,
    *,
    status_code: int = 200,
) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = Mock()
    if status_code >= 400 and status_code not in (401, 403, 404, 429):
        resp.raise_for_status.side_effect = requests.HTTPError("http error")
    return resp


def _rugcheck_payload() -> dict:
    return {
        "score_normalised": 42,
        "token": {"symbol": "TST", "name": "Test Token"},
    }


def _dex_pair() -> dict:
    return {
        "chainId": "solana",
        "liquidity": {"usd": 50_000},
        "volume": {"h24": 10_000},
        "txns": {"h24": {"buys": 100, "sells": 80}},
        "priceChange": {"h24": 5.5},
        "pairCreatedAt": 1_700_000_000_000,
        "baseToken": {"symbol": "TST", "name": "Test Token"},
        "info": {"websites": [{"url": "https://example.com"}], "socials": []},
    }


def _defade_payload() -> dict:
    return {
        "success": True,
        "token": {"name": "Example", "symbol": "EX", "mint": SAMPLE_MINT},
        "rugScore": 30,
        "riskLevel": "MEDIUM",
        "analysis": {
            "liquidity": {"totalUsd": 485000, "locked": True, "lockPct": 66},
            "holders": {"total": 1823, "top10Pct": 34.2},
            "bundles": {"count": 3, "bundledPct": 8.4},
            "insiderNetwork": {"insiderCount": 6, "networkScore": 72},
            "smartMoney": {"buys": 4, "sells": 1, "netFlow": "bullish"},
            "snipers": {"count": 7, "pct": 4.2},
            "devTracker": {"previousTokens": 3, "rugHistory": 1},
        },
    }


def _goplus_token_payload() -> dict:
    return {"result": {"access_token": "goplus-test-token"}}


def _goplus_security_payload(mint: str) -> dict:
    return {"result": {mint: {"is_honeypot": "0", "is_mintable": "0"}}}


def _jupiter_payload(mint: str) -> list:
    return [
        {
            "id": mint,
            "symbol": "TST",
            "name": "Test Token",
            "audit": {
                "mintAuthorityDisabled": True,
                "freezeAuthorityDisabled": True,
            },
        }
    ]


def install_source_mocks(
    monkeypatch: pytest.MonkeyPatch,
    mint: str = SAMPLE_MINT,
) -> Dict[str, List[Dict[str, Any]]]:
    """Patch requests.get/post with canned security-service responses."""
    calls: Dict[str, List[Dict[str, Any]]] = {"get": [], "post": []}

    def fake_get(url: str, headers=None, params=None, timeout=15, **kwargs):
        calls["get"].append(
            {
                "url": url,
                "headers": dict(headers or {}),
                "params": dict(params or {}),
                "timeout": timeout,
            }
        )
        if "rugcheck.xyz" in url:
            return _mock_http_response(_rugcheck_payload())
        if "dexscreener.com" in url:
            return _mock_http_response([_dex_pair()])
        if "defade.org" in url:
            return _mock_http_response(_defade_payload())
        if "gopluslabs.io" in url and "token_security" in url:
            return _mock_http_response(_goplus_security_payload(mint))
        if "jup.ag" in url:
            return _mock_http_response(_jupiter_payload(mint))
        return _mock_http_response(None, status_code=404)

    def fake_post(url: str, json=None, timeout=15, **kwargs):
        calls["post"].append(
            {
                "url": url,
                "json": json,
                "timeout": timeout,
            }
        )
        if "gopluslabs.io" in url and url.rstrip("/").endswith("/token"):
            return _mock_http_response(_goplus_token_payload())
        return _mock_http_response(None, status_code=404)

    monkeypatch.setattr(srr.requests, "get", fake_get)
    monkeypatch.setattr(srr.requests, "post", fake_post)
    return calls


def make_minimal_report(mint: str = SAMPLE_MINT) -> srr.StandardRiskReport:
    """Build a schema-valid report without HTTP (empty vendor data)."""
    dex = srr.aggregate_dex_pairs([])
    metrics = srr.build_category_metrics(None, dex, None, None, None)
    categories = {
        cat: {"score": 50.0, "level": "MEDIUM", "metrics": metrics[cat]}
        for cat in srr.CATEGORY_NAMES
    }
    source_status = {
        name: {
            "attempted": False,
            "success": False,
            "available": False,
            "requires_key": name in srr.SOURCES_REQUIRING_KEY,
            "latency_ms": None,
            "http_status": None,
            "error_type": "missing_api_key" if name in srr.SOURCES_REQUIRING_KEY else None,
            "error_message": f"{name}: skipped in test fixture",
        }
        for name in srr.SOURCE_NAMES
    }
    report = srr.StandardRiskReport(
        schema_version=srr.SCHEMA_VERSION,
        generated_at_utc=srr.utc_now_iso(),
        token={
            "chain": srr.CHAIN,
            "mint": mint,
            "symbol": "TST",
            "name": "Test",
            "decimals": 9,
        },
        overall={
            "risk_score": 50.0,
            "risk_level": "MEDIUM",
            "confidence_score": 10.0,
            "coverage_ratio": 0.5,
            "model_version": srr.MODEL_VERSION,
        },
        source_status=source_status,
        categories=categories,
        feature_row={},
        warnings=[],
        raw={name: None for name in srr.SOURCE_NAMES},
    )
    report.feature_row = srr.flatten_report(report)
    return report


def analysis_root(tmp_path: Path) -> Path:
    return tmp_path / "data" / "raw" / "analytics"


# ---------------------------------------------------------------------------
# Constants & save path
# ---------------------------------------------------------------------------


def test_default_analysis_root_constant():
    assert srr.DEFAULT_ANALYSIS_ROOT == Path("data/raw/analytics")


def test_write_standard_security_outputs_paths(tmp_path):
    root = analysis_root(tmp_path)
    report = make_minimal_report()
    paths = srr.write_standard_security_outputs(report, root)

    mint_dir = root / SAMPLE_MINT
    assert paths["directory"] == mint_dir
    assert paths["security_report"] == mint_dir / srr.SECURITY_REPORT_FILENAME
    assert paths["security_analysis"] == mint_dir / srr.SECURITY_ANALYSIS_FILENAME


def test_write_standard_security_outputs_creates_files(tmp_path):
    root = analysis_root(tmp_path)
    report = make_minimal_report()
    paths = srr.write_standard_security_outputs(report, root)

    assert paths["security_report"].exists()
    assert paths["security_analysis"].exists()
    assert paths["security_report"].stat().st_size > 0
    assert paths["security_analysis"].stat().st_size > 0


def test_write_standard_security_outputs_requires_mint(tmp_path):
    report = make_minimal_report()
    report.token["mint"] = ""
    with pytest.raises(ValueError, match="token.mint"):
        srr.write_standard_security_outputs(report, analysis_root(tmp_path))


# ---------------------------------------------------------------------------
# Security API connection (mocked)
# ---------------------------------------------------------------------------


def test_fetch_rugcheck_url_and_auth(monkeypatch):
    calls = install_source_mocks(monkeypatch)
    client = srr.RiskReportClient(make_config())
    result = client.fetch_rugcheck(SAMPLE_MINT)

    assert len(calls["get"]) == 1
    assert calls["get"][0]["url"] == f"{srr.RUGCHECK_BASE}/tokens/{SAMPLE_MINT}/report"
    assert calls["get"][0]["headers"]["Authorization"] == "Bearer rugcheck-test-key"
    assert result.success is True
    assert result.source == "rugcheck"


def test_fetch_rugcheck_without_api_key(monkeypatch):
    calls = install_source_mocks(monkeypatch)
    client = srr.RiskReportClient(make_config(rugcheck_api_key=None))
    result = client.fetch_rugcheck(SAMPLE_MINT)

    assert "Authorization" not in calls["get"][0]["headers"]
    assert result.success is True


def test_fetch_dexscreener_url(monkeypatch):
    calls = install_source_mocks(monkeypatch)
    client = srr.RiskReportClient(make_config())
    result = client.fetch_dexscreener(SAMPLE_MINT)

    assert calls["get"][0]["url"] == (
        f"{srr.DEXSCREENER_BASE}/token-pairs/v1/solana/{SAMPLE_MINT}"
    )
    assert result.success is True
    assert isinstance(result.data, list)


def test_fetch_defade_missing_key():
    client = srr.RiskReportClient(make_config(defade_api_key=None))
    result = client.fetch_defade(SAMPLE_MINT)

    assert result.attempted is False
    assert result.error_type == "missing_api_key"
    assert result.requires_key is True


def test_fetch_defade_with_key(monkeypatch):
    calls = install_source_mocks(monkeypatch)
    client = srr.RiskReportClient(make_config())
    result = client.fetch_defade(SAMPLE_MINT)

    defade_call = next(c for c in calls["get"] if "defade.org" in c["url"])
    assert defade_call["url"] == f"{srr.DEFADE_BASE}/v1/analyze/{SAMPLE_MINT}"
    assert defade_call["headers"]["x-api-key"] == "defade-test-key"
    assert result.success is True
    assert result.data["rugScore"] == 30
    assert result.data["insiderScore"] == 72
    assert result.data["bundleScore"] == 8.4
    assert result.data["sniperScore"] == 4.2
    assert result.raw["analysis"]["holders"]["total"] == 1823


def test_normalize_defade_analyze_response_documented_shape():
    normalized = srr.normalize_defade_analyze_response(_defade_payload())
    assert normalized is not None
    assert normalized["rugScore"] == 30
    assert normalized["riskLevel"] == "MEDIUM"
    assert normalized["insiderScore"] == 72
    assert normalized["bundleScore"] == 8.4
    assert normalized["sniperScore"] == 4.2
    assert normalized["holderScore"] == 34.2
    assert normalized["holderCount"] == 1823


def test_normalize_defade_analyze_response_legacy_risk_block():
    normalized = srr.normalize_defade_analyze_response(
        {
            "success": True,
            "token": {"mint": SAMPLE_MINT},
            "risk": {"score": 49, "rating": "HIGH RISK"},
            "holders": {
                "totalHolders": 20,
                "concentration": {"top10": 15.63},
                "bundles": {"bundlePct": "14.04"},
            },
        }
    )
    assert normalized is not None
    assert normalized["rugScore"] == 49
    assert normalized["riskLevel"] == "HIGH"
    assert normalized["bundleScore"] == 14.04
    assert normalized["holderScore"] == 15.63
    assert normalized["holderCount"] == 20


def test_normalize_defade_analyze_response_error_body():
    assert srr.normalize_defade_analyze_response(
        {"error": "Too many scans. Please wait a minute."}
    ) is None
    assert srr.normalize_defade_analyze_response({"success": False}) is None


def test_fetch_defade_api_error_in_200_body(monkeypatch):
    def fake_get(url: str, headers=None, params=None, timeout=15, **kwargs):
        if "defade.org" in url:
            return _mock_http_response({"error": "Too many scans"})
        return _mock_http_response(None, status_code=404)

    monkeypatch.setattr(requests, "get", fake_get)
    client = srr.RiskReportClient(make_config())
    result = client.fetch_defade(SAMPLE_MINT)
    assert result.success is False
    assert result.error_type == "api_error"
    assert "Too many scans" in (result.error_message or "")


def test_fetch_goplus_missing_keys():
    client = srr.RiskReportClient(
        make_config(goplus_api_key=None, goplus_api_secret=None)
    )
    result = client.fetch_goplus(SAMPLE_MINT)

    assert result.attempted is False
    assert result.error_type == "missing_api_key"


def test_fetch_goplus_with_keys(monkeypatch):
    calls = install_source_mocks(monkeypatch)
    client = srr.RiskReportClient(make_config())
    result = client.fetch_goplus(SAMPLE_MINT)

    assert len(calls["post"]) == 1
    assert calls["post"][0]["url"] == f"{srr.GOPLUS_BASE}/token"
    assert calls["post"][0]["json"]["app_key"] == "goplus-test-key"
    assert "sign" in calls["post"][0]["json"]

    security_call = next(
        c for c in calls["get"] if "solana/token_security" in c["url"]
    )
    assert security_call["headers"]["Authorization"] == "Bearer goplus-test-token"
    assert security_call["params"]["contract_addresses"] == SAMPLE_MINT
    assert result.success is True


def test_fetch_jupiter_missing_key():
    client = srr.RiskReportClient(make_config(jupiter_api_key=None))
    result = client.fetch_jupiter(SAMPLE_MINT)

    assert result.attempted is False
    assert result.error_type == "missing_api_key"


def test_fetch_jupiter_with_key(monkeypatch):
    calls = install_source_mocks(monkeypatch)
    client = srr.RiskReportClient(make_config())
    result = client.fetch_jupiter(SAMPLE_MINT)

    jup_call = next(c for c in calls["get"] if "jup.ag" in c["url"])
    assert jup_call["url"] == f"{srr.JUPITER_BASE}/search"
    assert jup_call["headers"]["x-api-key"] == "jupiter-test-key"
    assert jup_call["params"]["query"] == SAMPLE_MINT
    assert result.success is True


def test_build_report_calls_all_sources(monkeypatch):
    install_source_mocks(monkeypatch)
    report = srr.build_report(SAMPLE_MINT, config=make_config())

    for name in srr.SOURCE_NAMES:
        assert name in report.source_status
    assert report.token["mint"] == SAMPLE_MINT
    srr.validate_report_schema(report)


# ---------------------------------------------------------------------------
# Output file structure
# ---------------------------------------------------------------------------


def test_security_report_human_structure(tmp_path):
    root = analysis_root(tmp_path)
    report = make_minimal_report()
    paths = srr.write_standard_security_outputs(report, root)
    text = paths["security_report"].read_text(encoding="utf-8")

    assert "=== Solana Token Risk Report ===" in text
    assert f"Token: {SAMPLE_MINT}" in text
    assert "Overall risk:" in text
    assert "Source status:" in text
    assert f"Schema: {srr.SCHEMA_VERSION}" in text


def test_security_analytics_json_structure(tmp_path):
    root = analysis_root(tmp_path)
    report = make_minimal_report()
    paths = srr.write_standard_security_outputs(report, root)
    raw = paths["security_analysis"].read_text(encoding="utf-8")
    assert raw.endswith("\n")
    lines = [ln for ln in raw.strip().splitlines() if ln.strip()]
    assert len(lines) == 1

    row = json.loads(lines[0])
    for key in (
        "schema_version",
        "mint",
        "chain",
        "overall_risk_score",
        "overall_risk_level",
        "model_version",
    ):
        assert key in row
    assert row["mint"] == SAMPLE_MINT
    assert row["source_rugcheck_attempted"] is not None
    assert row["external_vendor_risk_score"] is not None


def test_write_report_json_structure(tmp_path, monkeypatch):
    install_source_mocks(monkeypatch)
    report = srr.build_report(SAMPLE_MINT, config=make_config())
    out = tmp_path / "report.json"
    srr.write_report(report, out, "json", pretty=True)

    data = json.loads(out.read_text(encoding="utf-8"))
    srr.validate_report_schema(data)
    assert data["token"]["mint"] == SAMPLE_MINT


def test_write_report_jsonl_append(tmp_path):
    report = make_minimal_report()
    out = tmp_path / "reports.jsonl"
    srr.write_report(report, out, "jsonl")
    srr.write_report(report, out, "jsonl", append=True)

    lines = [ln for ln in out.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 2
    assert json.loads(lines[0])["token"]["mint"] == SAMPLE_MINT


def test_write_report_csv_header(tmp_path):
    report = make_minimal_report()
    out = tmp_path / "features.csv"
    srr.write_report(report, out, "csv")

    lines = out.read_text(encoding="utf-8").splitlines()
    assert "mint" in lines[0]
    assert SAMPLE_MINT in lines[1]


# ---------------------------------------------------------------------------
# Schema & pure helpers
# ---------------------------------------------------------------------------


def test_validate_report_schema_passes_on_build_report(monkeypatch):
    install_source_mocks(monkeypatch)
    report = srr.build_report(SAMPLE_MINT, config=make_config())
    srr.validate_report_schema(report)


def test_validate_report_schema_raises_on_missing_metric(monkeypatch):
    install_source_mocks(monkeypatch)
    report = srr.build_report(SAMPLE_MINT, config=make_config())
    report.categories["liquidity_health"]["metrics"].pop("pair_count")
    with pytest.raises(ValueError, match="pair_count"):
        srr.validate_report_schema(report)


def test_rugcheck_score_from_normalized():
    assert srr.rugcheck_score({"score_normalised": 42}) == 42.0


def test_aggregate_dex_pairs_single_pair():
    agg = srr.aggregate_dex_pairs([_dex_pair()])
    assert agg["pair_count"] == 1
    assert agg["total_liquidity_usd"] == 50_000


def test_risk_level_boundaries():
    assert srr.risk_level(10) == "LOW"
    assert srr.risk_level(40) == "MEDIUM"
    assert srr.risk_level(60) == "HIGH"
    assert srr.risk_level(90) == "CRITICAL"
    assert srr.risk_level(None) == "UNKNOWN"


def test_http_get_json_404(monkeypatch):
    def fake_get(url, headers=None, params=None, timeout=15, **kwargs):
        return _mock_http_response(None, status_code=404)

    monkeypatch.setattr(srr.requests, "get", fake_get)
    data, status, err_type, err_msg, latency = srr.http_get_json(
        "test", "https://example.com/missing"
    )
    assert data is None
    assert status == 404
    assert err_type == "not_found"
    assert latency is not None


def test_missing_key_result_shape():
    result = srr._missing_key_result("defade")
    assert result.source == "defade"
    assert result.attempted is False
    assert result.error_type == "missing_api_key"
    assert result.requires_key is True


def test_main_writes_standard_outputs(tmp_path, monkeypatch):
    root = analysis_root(tmp_path)
    monkeypatch.setattr(
        srr,
        "build_report",
        lambda mint, config=None: make_minimal_report(mint),
    )
    rc = srr.main(
        [
            "--mint",
            SAMPLE_MINT,
            "--analysis-root",
            str(root),
            "--human",
            "--no-raw",
        ]
    )
    assert rc == 0
    mint_dir = root / SAMPLE_MINT
    assert (mint_dir / srr.SECURITY_REPORT_FILENAME).exists()
    assert (mint_dir / srr.SECURITY_ANALYSIS_FILENAME).exists()
