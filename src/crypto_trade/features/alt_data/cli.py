"""CLI entrypoint for the website grader."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path

from crypto_trade.core.env import load_env
from crypto_trade.core.logging_config import configure_logging

from .fetch import (
    check_dns,
    check_ssl_cert,
    fetch_rdap,
    fetch_url,
    hostname,
    normalize_url,
    parse_html,
    root_domain,
    run_pagespeed,
    run_urlscan,
    try_playwright_render,
    visible_text,
    collect_dom_stats,
    detect_tech_stack,
)
from .grading import (
    build_report,
    grade_label,
    print_section,
    score_content_quality,
    score_domain_quality,
    score_https_security,
    score_performance_seo_accessibility,
    score_risk_flags,
    score_screenshot_dom_quality,
    score_social_consistency,
    score_tech_stack,
    weighted_overall,
)


def main() -> int:
    load_env()
    configure_logging()

    parser = argparse.ArgumentParser(description="Grade a crypto/meme-coin website.")
    parser.add_argument("positional_url", nargs="?", help="Website URL to grade")
    parser.add_argument("--url", dest="url_flag", help="Website URL to grade")

    parser.add_argument("--out", default="./website_grade", help="Output directory")
    parser.add_argument("--json", action="store_true", help="Print JSON report to stdout")

    parser.add_argument("--token-name", default=None)
    parser.add_argument("--token-symbol", default=None)
    parser.add_argument("--contract", default=None)
    parser.add_argument("--expected-x", default=None)
    parser.add_argument("--expected-telegram", default=None)
    parser.add_argument("--expected-website", default=None)

    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--screenshot", action="store_true", help="Try Playwright screenshot/rendered DOM check")
    parser.add_argument("--render-timeout-ms", type=int, default=25000)

    parser.add_argument("--use-pagespeed", action="store_true", help="Call Google PageSpeed Insights API")
    parser.add_argument("--pagespeed-key", default=os.getenv("PAGESPEED_API_KEY"))

    parser.add_argument(
        "--use-urlscan",
        action="store_true",
        help="Submit URL to urlscan.io as a public scan; requires URLSCAN_API_KEY",
    )
    parser.add_argument("--urlscan-key", default=os.getenv("URLSCAN_API_KEY"))

    parser.add_argument(
        "--check-social-links",
        action="store_true",
        help="Sample HEAD requests against social links; slower but useful for pipeline features",
    )

    args = parser.parse_args()
    args.url = args.url_flag or args.positional_url

    if not args.url:
        parser.error("Provide a URL as positional argument or with --url.")

    url = normalize_url(args.url)
    start_all = time.time()

    input_host = hostname(url)
    input_domain = root_domain(input_host)
    safe_domain = re.sub(r"[^a-zA-Z0-9_.-]+", "_", input_domain or "unknown")
    out_dir = Path(args.out) / safe_domain
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.json:
        print(f"Grading: {url}")
        print(f"Output:  {out_dir}")

    fetch = fetch_url(url, timeout=args.timeout)

    final_url = fetch.final_url or url
    final_host = hostname(final_url) or input_host
    final_domain = root_domain(final_host)

    dns = check_dns(final_host)
    ssl_info = check_ssl_cert(final_host)
    rdap = fetch_rdap(final_domain)

    soup = parse_html(fetch.html)
    text = visible_text(soup)
    dom = collect_dom_stats(soup, final_url, text)
    tech = detect_tech_stack(fetch.headers, soup, final_url)

    pagespeed = {"ok": False, "skipped": True, "reason": "not requested"}
    if args.use_pagespeed:
        if not args.json:
            print("Calling PageSpeed Insights...")
        pagespeed = run_pagespeed(final_url, args.pagespeed_key)

    screenshot = {"ok": False, "skipped": True, "reason": "not requested"}
    if args.screenshot and fetch.ok:
        if not args.json:
            print("Capturing screenshot/rendered DOM with Playwright...")
        screenshot = try_playwright_render(final_url, out_dir, timeout_ms=args.render_timeout_ms)

    urlscan = {"ok": False, "skipped": True, "reason": "not requested"}
    if args.use_urlscan:
        if not args.json:
            print("Submitting public urlscan.io scan...")
        urlscan = run_urlscan(final_url, args.urlscan_key)

    sections = [
        score_domain_quality(url, fetch, rdap, dns),
        score_https_security(fetch, ssl_info),
        score_performance_seo_accessibility(fetch, dom, pagespeed, final_url),
        score_tech_stack(tech),
        score_content_quality(text, fetch.html, dom, args.token_name, args.token_symbol, args.contract),
        score_social_consistency(
            dom,
            final_url,
            args.expected_x,
            args.expected_telegram,
            args.expected_website,
            args.check_social_links,
        ),
        score_risk_flags(fetch, soup, text, dom, urlscan),
        score_screenshot_dom_quality(dom, screenshot),
    ]

    overall, weighted_parts = weighted_overall(sections)

    report = build_report(
        args=args,
        fetch=fetch,
        dns=dns,
        ssl_info=ssl_info,
        rdap=rdap,
        dom=dom,
        tech=tech,
        pagespeed=pagespeed,
        screenshot=screenshot,
        urlscan=urlscan,
        sections=sections,
        overall=overall,
        weighted_parts=weighted_parts,
    )

    report["runtime_seconds"] = round(time.time() - start_all, 3)

    report_path = out_dir / "website_quality_report.json"
    html_path = out_dir / "page.html"
    text_path = out_dir / "page_text.txt"

    report_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    html_path.write_text(fetch.html, encoding="utf-8", errors="ignore")
    text_path.write_text(text, encoding="utf-8", errors="ignore")

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0

    print("\n" + "#" * 88)
    print(f"OVERALL WEBSITE QUALITY SCORE: {overall:.1f}/100 ({grade_label(overall)})")
    print("#" * 88)

    print(f"Input URL:       {url}")
    print(f"Final URL:       {fetch.final_url}")
    print(f"HTTP status:     {fetch.status_code}")
    print(f"Fetch time:      {fetch.elapsed_seconds:.2f}s" if fetch.elapsed_seconds is not None else "Fetch time:      unknown")
    print(f"Downloaded:      {fetch.bytes_downloaded} bytes")
    print(f"Redirect count:  {len(fetch.redirects)}")
    if fetch.error:
        print(f"Fetch error:     {fetch.error}")

    for section in sections:
        print_section(section)

    print("\n" + "=" * 88)
    print("Weighted contribution:")
    for key, value in weighted_parts.items():
        print(f"  {key}: {value:.2f}")

    print("=" * 88)
    print(f"Saved report: {report_path}")
    print(f"Saved HTML:   {html_path}")
    print(f"Saved text:   {text_path}")
    print()
    print("Pipeline note:")
    print("  Treat this as one feature source. For 6h survival prediction, combine with")
    print("  liquidity, volume, holder distribution, deployer history, tax/honeypot checks,")
    print("  social velocity, exchange listings, and contract-level risk features.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
