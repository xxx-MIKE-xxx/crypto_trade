"""Report assembly, validation, and file/stdout writers."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .constants import (
    CATEGORY_METRIC_KEYS,
    CATEGORY_NAMES,
    MODEL_VERSION,
    SCHEMA_VERSION,
    SOURCE_NAMES,
)
from .http_client import RiskReportClient
from .scoring import (
    aggregate_dex_pairs,
    build_category_metrics,
    build_warnings,
    combine_category_scores,
    extract_token_info,
    score_contract_permissions,
    score_external_vendor_risk,
    score_holder_distribution,
    score_liquidity_health,
    score_trading_behavior,
    score_verification_identity,
    warning_counts,
)
from .types import FeatureRow, ReportConfig, StandardRiskReport
from .utils import as_float, bool_to_ml, json_default, risk_level, risk_level_code, utc_now_iso


def build_report(mint: str, config: Optional[ReportConfig] = None) -> StandardRiskReport:
    """Build a complete standardized risk report for a Solana token mint."""
    cfg = config or ReportConfig.from_env()
    client = RiskReportClient(cfg)
    source_results = client.fetch_all(mint)

    rug = source_results["rugcheck"].data
    dex_pairs = source_results["dexscreener"].data or []
    dex = aggregate_dex_pairs(dex_pairs if isinstance(dex_pairs, list) else [])
    defade = source_results["defade"].data
    goplus = source_results["goplus"].data
    jup = source_results["jupiter"].data

    metrics_by_category = build_category_metrics(rug, dex, defade, goplus, jup)

    raw_scores = {
        "external_vendor_risk": score_external_vendor_risk(rug, defade, goplus),
        "contract_permissions": score_contract_permissions(rug, goplus, jup),
        "holder_distribution": score_holder_distribution(rug, jup, defade),
        "liquidity_health": score_liquidity_health(rug, dex, jup),
        "trading_behavior": score_trading_behavior(dex, jup),
        "verification_identity": score_verification_identity(dex, jup),
    }

    categories: Dict[str, Dict[str, Any]] = {}
    for cat_name in CATEGORY_NAMES:
        scored = raw_scores[cat_name]
        score = scored.get("score")
        categories[cat_name] = {
            "score": round(score, 2) if score is not None else None,
            "level": scored.get("level", "UNKNOWN"),
            "metrics": metrics_by_category[cat_name],
        }

    overall_score, confidence, coverage = combine_category_scores(raw_scores)
    overall_level = risk_level(overall_score)

    source_status = {name: source_results[name].to_status_dict() for name in SOURCE_NAMES}

    warnings = build_warnings(categories, metrics_by_category, source_results)

    top_pair = None
    if isinstance(dex_pairs, list) and dex_pairs:
        def liq(p: dict) -> float:
            return as_float((p.get("liquidity") or {}).get("usd")) or 0.0

        valid = [p for p in dex_pairs if isinstance(p, dict)]
        if valid:
            top_pair = max(valid, key=liq)

    token = extract_token_info(mint, rug, jup, top_pair)

    raw_section: Dict[str, Any] = {name: None for name in SOURCE_NAMES}
    if cfg.include_raw:
        for name, result in source_results.items():
            raw_section[name] = result.raw if result.success else result.raw

    report_stub = StandardRiskReport(
        schema_version=SCHEMA_VERSION,
        generated_at_utc=utc_now_iso(),
        token=token,
        overall={
            "risk_score": round(overall_score, 2) if overall_score is not None else None,
            "risk_level": overall_level,
            "confidence_score": round(confidence, 1),
            "coverage_ratio": coverage,
            "model_version": MODEL_VERSION,
        },
        source_status=source_status,
        categories=categories,
        feature_row={},
        warnings=warnings,
        raw=raw_section,
    )

    feature_row = flatten_report(report_stub)
    report_stub.feature_row = feature_row
    return report_stub


def flatten_report(report: StandardRiskReport) -> FeatureRow:
    """Flatten a standardized report into an ML-friendly feature dict."""
    row: FeatureRow = {
        "schema_version": report.schema_version,
        "generated_at_utc": report.generated_at_utc,
        "chain": report.token.get("chain"),
        "mint": report.token.get("mint"),
        "token_symbol": report.token.get("symbol"),
        "token_name": report.token.get("name"),
        "token_decimals": report.token.get("decimals"),
        "overall_risk_score": report.overall.get("risk_score"),
        "overall_risk_level": report.overall.get("risk_level"),
        "overall_risk_level_code": risk_level_code(str(report.overall.get("risk_level", "UNKNOWN"))),
        "confidence_score": report.overall.get("confidence_score"),
        "coverage_ratio": report.overall.get("coverage_ratio"),
        "model_version": report.overall.get("model_version"),
    }

    for src in SOURCE_NAMES:
        status = report.source_status.get(src, {})
        row[f"source_{src}_attempted"] = bool_to_ml(status.get("attempted"))
        row[f"source_{src}_success"] = bool_to_ml(status.get("success"))
        row[f"source_{src}_available"] = bool_to_ml(status.get("available"))
        row[f"source_{src}_latency_ms"] = status.get("latency_ms")
        row[f"source_{src}_http_status"] = status.get("http_status")

    for cat_name in CATEGORY_NAMES:
        cat = report.categories.get(cat_name, {})
        row[f"{cat_name}_score"] = cat.get("score")
        row[f"{cat_name}_level_code"] = risk_level_code(str(cat.get("level", "UNKNOWN")))
        metrics = cat.get("metrics") or {}
        for metric_key, value in metrics.items():
            field_name = f"{cat_name}__{metric_key}"
            if isinstance(value, bool):
                row[field_name] = bool_to_ml(value)
            else:
                row[field_name] = value

    row.update(warning_counts(report.warnings))
    return row


def validate_report_schema(report: Union[StandardRiskReport, Dict[str, Any]]) -> None:
    """Validate required sections and fixed metric keys. Raises ``ValueError`` on failure."""
    data = report.to_dict() if isinstance(report, StandardRiskReport) else report

    required_top = (
        "schema_version",
        "generated_at_utc",
        "token",
        "overall",
        "source_status",
        "categories",
        "feature_row",
        "warnings",
        "raw",
    )
    for key in required_top:
        if key not in data:
            raise ValueError(f"Missing top-level key: {key}")

    for src in SOURCE_NAMES:
        if src not in data["source_status"]:
            raise ValueError(f"Missing source_status entry: {src}")
        status = data["source_status"][src]
        for field_name in (
            "attempted",
            "success",
            "available",
            "requires_key",
            "latency_ms",
            "http_status",
            "error_type",
            "error_message",
        ):
            if field_name not in status:
                raise ValueError(f"source_status.{src} missing field: {field_name}")

    for cat_name in CATEGORY_NAMES:
        if cat_name not in data["categories"]:
            raise ValueError(f"Missing category: {cat_name}")
        cat = data["categories"][cat_name]
        for field_name in ("score", "level", "metrics"):
            if field_name not in cat:
                raise ValueError(f"categories.{cat_name} missing field: {field_name}")
        expected_metrics = CATEGORY_METRIC_KEYS[cat_name]
        metrics = cat.get("metrics") or {}
        for mk in expected_metrics:
            if mk not in metrics:
                raise ValueError(f"categories.{cat_name}.metrics missing key: {mk}")


# ---------------------------------------------------------------------------
# File output
# ---------------------------------------------------------------------------


def write_report(
    report: StandardRiskReport,
    path: Union[str, Path],
    fmt: str,
    *,
    append: bool = False,
    pretty: bool = False,
) -> None:
    """Write a report to disk in the requested format."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fmt_lower = fmt.lower()

    if fmt_lower == "json":
        payload = report.to_dict()
        indent = 2 if pretty else None
        text = json.dumps(payload, indent=indent, ensure_ascii=False, default=json_default)
        out_path.write_text(text + ("\n" if pretty else ""), encoding="utf-8")
        return

    if fmt_lower == "jsonl":
        line = json.dumps(report.to_dict(), ensure_ascii=False, default=json_default)
        mode = "a" if append else "w"
        needs_sep = False
        if append and out_path.exists() and out_path.stat().st_size > 0:
            with out_path.open("rb") as rf:
                rf.seek(-1, os.SEEK_END)
                needs_sep = rf.read(1) != b"\n"
        with out_path.open(mode, encoding="utf-8") as f:
            if needs_sep:
                f.write("\n")
            f.write(line + "\n")
        return

    if fmt_lower == "csv":
        row = flatten_report(report)
        _write_csv_row(out_path, row, append=append)
        return

    if fmt_lower == "parquet":
        if append:
            raise NotImplementedError(
                "Parquet append is not supported. Use --format jsonl --append for "
                "streaming append logs, then batch-convert to Parquet."
            )
        _write_parquet_row(out_path, flatten_report(report))
        return

    raise ValueError(f"Unsupported format: {fmt}")


def _write_csv_row(path: Path, row: FeatureRow, *, append: bool) -> None:
    fieldnames: List[str]
    if append and path.exists() and path.stat().st_size > 0:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    else:
        fieldnames = list(row.keys())

    mode = "a" if append else "w"
    write_header = not (append and path.exists() and path.stat().st_size > 0)
    with path.open(mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({k: row.get(k) for k in fieldnames})


def _write_parquet_row(path: Path, row: FeatureRow) -> None:
    try:
        import pandas as pd
    except ImportError as e:
        raise ImportError(
            "Parquet output requires pandas and pyarrow. "
            "Install with: pip install pandas pyarrow"
        ) from e
    try:
        import pyarrow  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Parquet output requires pyarrow. Install with: pip install pyarrow"
        ) from e

    df = pd.DataFrame([row])
    df.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Human-readable output
# ---------------------------------------------------------------------------


def print_human(report: StandardRiskReport) -> None:
    """Print a human-readable summary."""
    print("\n=== Solana Token Risk Report ===")

    symbol = report.token.get("symbol")
    name = report.token.get("name")
    label = f"{symbol} ({name})" if symbol and name else symbol or name

    print(f"Token: {report.token.get('mint')}")
    if label:
        print(f"Name: {label}")

    overall = report.overall
    print(
        f"Overall risk: {overall.get('risk_score')} / 100 "
        f"({overall.get('risk_level')})"
    )
    print(f"Confidence: {overall.get('confidence_score')}%")
    print(f"Coverage ratio: {overall.get('coverage_ratio')}")

    print("\nSource status:")
    for src, status in report.source_status.items():
        if status.get("success"):
            print(
                f"  - {src}: ok "
                f"(latency_ms={status.get('latency_ms')}, http_status={status.get('http_status')})"
            )
        elif status.get("attempted"):
            print(
                f"  - {src}: failed "
                f"({status.get('error_type')}: {status.get('error_message')})"
            )
        else:
            print(f"  - {src}: skipped ({status.get('error_message')})")

    print("\nSub-category scores:")
    for cat_name, cat in report.categories.items():
        print(f"\n  {cat_name}: {cat.get('score')} / 100 ({cat.get('level')})")
        metrics = cat.get("metrics") or {}
        for metric_name, value in metrics.items():
            print(f"    - {metric_name}: {value}")

    if report.warnings:
        print("\nTop warnings:")
        for w in report.warnings[:12]:
            print(
                f"  - [{w.get('severity')}] {w.get('code')}: "
                f"{w.get('message')} (value={w.get('value')})"
            )

    liq = report.categories.get("liquidity_health", {}).get("metrics", {})
    trading = report.categories.get("trading_behavior", {}).get("metrics", {})

    print("\nMarket snapshot:")
    print(f"  Pair count: {liq.get('pair_count')}")
    print(f"  Total liquidity: {liq.get('total_liquidity_usd')}")
    print(f"  Top pair liquidity: {liq.get('top_pair_liquidity_usd')}")
    print(f"  Newest pair age hours: {liq.get('newest_pair_age_hours')}")
    print(f"  24h buys/sells: {trading.get('h24_buys')}/{trading.get('h24_sells')}")
    print(f"  24h volume: {trading.get('h24_volume_usd')}")
    print(f"  24h price change: {trading.get('h24_price_change_pct')}%")

    print("\nSchema:", report.schema_version)
    print(
        "\nDisclaimer: This is a heuristic risk screen, not financial advice "
        "and not a guarantee. Always inspect raw API reports, liquidity lockers, "
        "deployer wallets, and recent transactions."
    )
