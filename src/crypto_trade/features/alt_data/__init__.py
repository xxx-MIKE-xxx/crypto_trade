"""Website grader package.

Split from the former ``crypto_trade.features.alt_data`` monolith into:

* :mod:`.fetch` -- shared constants, dataclasses, URL helpers, network probes
  (HTTP, DNS, TLS, RDAP, sample HEAD, PageSpeed, urlscan, Playwright), and HTML
  parsing primitives.
* :mod:`.grading` -- per-section scorers, overall weighting, human/JSON
  rendering, and :func:`build_report`.
* :mod:`.cli` -- the ``grade-website`` argparse entry point.

The public API matches the old module so existing callers keep working through
:mod:`crypto_trade.features.alt_data`.
"""

from .cli import main
from .fetch import (
    FetchResult,
    Finding,
    SectionResult,
    USER_AGENT,
    analyze_screenshot,
    canonical_social_url,
    check_dns,
    check_ssl_cert,
    clamp,
    collect_dom_stats,
    count_terms,
    detect_tech_stack,
    domain_token,
    extract_rdap_dates,
    extract_socials,
    fetch_rdap,
    fetch_url,
    hostname,
    normalize_url,
    parse_datetime,
    parse_html,
    root_domain,
    run_pagespeed,
    run_urlscan,
    sample_head,
    shannon_entropy,
    suffix_of,
    try_playwright_render,
    visible_text,
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
    section_to_json,
    weighted_overall,
)

__all__ = [
    "FetchResult",
    "Finding",
    "SectionResult",
    "USER_AGENT",
    "analyze_screenshot",
    "build_report",
    "canonical_social_url",
    "check_dns",
    "check_ssl_cert",
    "clamp",
    "collect_dom_stats",
    "count_terms",
    "detect_tech_stack",
    "domain_token",
    "extract_rdap_dates",
    "extract_socials",
    "fetch_rdap",
    "fetch_url",
    "grade_label",
    "hostname",
    "main",
    "normalize_url",
    "parse_datetime",
    "parse_html",
    "print_section",
    "root_domain",
    "run_pagespeed",
    "run_urlscan",
    "sample_head",
    "score_content_quality",
    "score_domain_quality",
    "score_https_security",
    "score_performance_seo_accessibility",
    "score_risk_flags",
    "score_screenshot_dom_quality",
    "score_social_consistency",
    "score_tech_stack",
    "section_to_json",
    "shannon_entropy",
    "suffix_of",
    "try_playwright_render",
    "visible_text",
    "weighted_overall",
]
