"""Argparse CLI for the Solana token risk scoring tool."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from typing import Optional, Sequence

from crypto_trade.core.env import load_env
from crypto_trade.core.logging_config import configure_logging

from .report import (
    build_report,
    flatten_report,
    print_human,
    validate_report_schema,
    write_report,
)
from .types import ReportConfig, StandardRiskReport
from .utils import json_default

logger = logging.getLogger(__name__)


def _resolve_mint(args: argparse.Namespace) -> str:
    mint = args.mint or getattr(args, "mint_positional", None)
    if not mint:
        raise SystemExit("Error: provide a mint address as argument or --mint <MINT>")
    return mint.strip()


def _format_was_explicit(argv: Sequence[str]) -> bool:
    return any(arg == "--format" or arg.startswith("--format=") for arg in argv)


def _print_machine_stdout(report: StandardRiskReport, fmt: str, *, pretty: bool) -> int:
    fmt_lower = fmt.lower()

    if fmt_lower == "json":
        indent = 2 if pretty else None
        text = json.dumps(report.to_dict(), indent=indent, ensure_ascii=False, default=json_default)
        try:
            sys.stdout.buffer.write(text.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")
            sys.stdout.buffer.flush()
        except (AttributeError, OSError):
            print(text)
        return 0

    if fmt_lower == "jsonl":
        text = json.dumps(report.to_dict(), ensure_ascii=False, default=json_default)
        print(text)
        return 0

    if fmt_lower == "csv":
        row = flatten_report(report)
        writer = csv.DictWriter(sys.stdout, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)
        return 0

    if fmt_lower == "parquet":
        print("Parquet cannot be written to stdout. Use --out <path>.", file=sys.stderr)
        return 1

    print(f"Unsupported format: {fmt}", file=sys.stderr)
    return 1


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Solana token risk report with human display and machine-readable exports.",
    )
    parser.add_argument("mint_positional", nargs="?", help="Solana token mint address")
    parser.add_argument("--mint", dest="mint", help="Solana token mint address")
    parser.add_argument(
        "--format",
        choices=["json", "jsonl", "csv", "parquet"],
        default=None,
        help="Machine-readable output format. Default is json when --out or --machine is used.",
    )
    parser.add_argument("--out", help="Output file path for machine-readable export")
    parser.add_argument("--append", action="store_true", help="Append to JSONL/CSV output")
    parser.add_argument("--no-raw", action="store_true", help="Exclude raw vendor payloads")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout in seconds")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate schema before writing output",
    )
    parser.add_argument(
        "--human",
        action="store_true",
        help="Force human-readable summary. This is already the default when not exporting.",
    )
    parser.add_argument(
        "--machine",
        action="store_true",
        help="Print machine-readable output to stdout instead of the human summary.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    load_env()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]

    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    configure_logging("DEBUG" if args.verbose else "WARNING")

    mint = _resolve_mint(args)
    output_format = args.format or "json"
    explicit_machine_request = args.machine or _format_was_explicit(raw_argv)

    config = ReportConfig.from_env(timeout=args.timeout, include_raw=not args.no_raw)

    logger.info("Building risk report for mint=%s", mint)
    report = build_report(mint, config=config)

    if args.validate:
        validate_report_schema(report)
        logger.info("Schema validation passed")

    if args.out:
        try:
            write_report(
                report,
                args.out,
                output_format,
                append=args.append,
                pretty=args.pretty,
            )
        except (ImportError, NotImplementedError) as e:
            print(str(e), file=sys.stderr)
            return 1

        logger.info("Wrote %s report to %s", output_format, args.out)
        print(f"Wrote {output_format.upper()} report to {args.out}")

        if args.human:
            print_human(report)

        return 0

    if args.human or not explicit_machine_request:
        print_human(report)
        return 0

    return _print_machine_stdout(report, output_format, pretty=args.pretty)


if __name__ == "__main__":
    raise SystemExit(main())
