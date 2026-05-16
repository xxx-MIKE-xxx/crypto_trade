"""Per-section grading, overall scoring, and report assembly."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .fetch import (
    CONTENT_POSITIVE_TERMS,
    DANGEROUS_WEB3_TERMS,
    EVM_CONTRACT_RE,
    FREE_OR_LOW_EFFORT_HOSTS,
    Finding,
    HYPE_TERMS,
    LINK_SHORTENERS,
    PARKED_OR_PLACEHOLDER_PATTERNS,
    RISKY_TLDS,
    SECURITY_HEADERS,
    SOLANA_LIKE_RE,
    SectionResult,
    FetchResult,
    analyze_screenshot,
    canonical_social_url,
    clamp,
    count_terms,
    domain_token,
    extract_rdap_dates,
    extract_socials,
    hostname,
    normalize_url,
    root_domain,
    sample_head,
    shannon_entropy,
    suffix_of,
)


# ---------------------------------------------------------------------------
# Per-section scoring
# ---------------------------------------------------------------------------


def score_domain_quality(
    url: str,
    fetch: FetchResult,
    rdap: Dict[str, Any],
    dns: Dict[str, Any],
) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 40.0

    final_url = fetch.final_url or url
    host = hostname(final_url) or hostname(url)
    rd = root_domain(host)
    token = domain_token(host)
    suffix = suffix_of(host)

    if dns.get("ok"):
        score += 15
        positives.append(f"DNS resolves for {host}.")
        findings.append(Finding("DNS IPs", dns.get("ips", []), +15))
    else:
        score -= 25
        negatives.append(f"DNS resolution failed: {dns.get('error')}.")
        findings.append(Finding("DNS resolution", dns, -25))

    is_free_host = any(rd == h or rd.endswith("." + h) for h in FREE_OR_LOW_EFFORT_HOSTS)
    if is_free_host:
        score -= 15
        negatives.append(f"Uses free/low-effort hosting or link-hub domain: {rd}.")
    else:
        score += 12
        positives.append(f"Uses a custom/root domain: {rd}.")

    if suffix in RISKY_TLDS:
        score -= 5
        negatives.append(f"TLD .{suffix} is common in short-lived/speculative launches.")
    else:
        score += 3

    entropy = shannon_entropy(token)
    if len(token) >= 12 and entropy > 3.6:
        score -= 8
        negatives.append(f"Domain token looks high-entropy/random: {token}.")
    else:
        score += 3

    hyphen_digit_count = len(re.findall(r"[-\d]", token))
    if hyphen_digit_count >= 4:
        score -= 6
        negatives.append("Domain has heavy hyphen/digit usage.")
    elif hyphen_digit_count == 0:
        score += 3

    if len(fetch.redirects) <= 2:
        score += 8
        positives.append("Redirect chain is short.")
    else:
        penalty = min(12, len(fetch.redirects) * 3)
        score -= penalty
        negatives.append(f"Long redirect chain: {len(fetch.redirects)} redirects.")

    input_rd = root_domain(hostname(fetch.input_url))
    final_rd = root_domain(hostname(fetch.final_url))
    if input_rd and final_rd and input_rd != final_rd:
        score -= 8
        negatives.append(f"Redirect crosses root domains: {input_rd} -> {final_rd}.")
    else:
        score += 4

    registrar = None
    created = None
    expires = None

    if rdap.get("ok"):
        created, expires, registrar = extract_rdap_dates(rdap.get("data", {}))
        positives.append("RDAP lookup succeeded.")

        if registrar:
            score += 3
            findings.append(Finding("Registrar", registrar, +3))

        if created:
            now = dt.datetime.now(dt.timezone.utc)
            age_days = max(0, (now - created).days)
            findings.append(Finding("Domain age days", age_days, 0))

            if age_days >= 365:
                score += 18
                positives.append("Domain is at least one year old.")
            elif age_days >= 90:
                score += 10
                positives.append("Domain has some age.")
            elif age_days >= 30:
                score += 4
                negatives.append("Domain is fairly new.")
            elif age_days >= 7:
                score -= 6
                negatives.append("Domain is new, under 30 days old.")
            else:
                score -= 14
                negatives.append("Domain is extremely new, under 7 days old.")
        else:
            score -= 2
            negatives.append("Could not extract creation date from RDAP.")

        if expires:
            days_to_expiry = (expires - dt.datetime.now(dt.timezone.utc)).days
            findings.append(Finding("Domain expiry days", days_to_expiry, 0))

            if days_to_expiry >= 180:
                score += 8
                positives.append("Domain expiry is not immediate.")
            elif days_to_expiry >= 30:
                score += 2
                negatives.append("Domain expires relatively soon.")
            else:
                score -= 8
                negatives.append("Domain expires very soon.")
    else:
        score -= 2
        negatives.append(f"RDAP lookup failed or unavailable: {rdap.get('status_code') or rdap.get('error')}.")

    if urlparse(final_url).scheme == "https":
        score += 7
        positives.append("Final URL uses HTTPS.")

    raw = {
        "host": host,
        "root_domain": rd,
        "domain_token": token,
        "suffix": suffix,
        "entropy": round(entropy, 3),
        "rdap_ok": rdap.get("ok"),
        "registrar": registrar,
        "created": created.isoformat() if created else None,
        "expires": expires.isoformat() if expires else None,
        "dns": dns,
    }

    return SectionResult("Domain quality", clamp(score), findings, positives, negatives, raw)


def score_https_security(fetch: FetchResult, ssl_info: Dict[str, Any]) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 35.0

    final_url = fetch.final_url or fetch.input_url
    headers = fetch.headers

    if urlparse(final_url).scheme == "https":
        score += 18
        positives.append("Final URL is HTTPS.")
    else:
        score -= 30
        negatives.append("Final URL is not HTTPS.")

    if ssl_info.get("ok") and ssl_info.get("valid_now"):
        score += 20
        positives.append(f"TLS certificate is valid; expires in {ssl_info.get('days_to_expiry')} days.")
    else:
        score -= 20
        negatives.append(f"TLS certificate check failed or invalid: {ssl_info.get('error')}.")

    if ssl_info.get("san_matches_host"):
        score += 8
        positives.append("TLS certificate appears to match host.")
    else:
        score -= 5
        negatives.append("TLS SAN host match was not confirmed.")

    days = ssl_info.get("days_to_expiry")
    if days is not None:
        if days > 30:
            score += 5
        elif days >= 0:
            score -= 8
            negatives.append("TLS certificate expires soon.")
        else:
            score -= 20
            negatives.append("TLS certificate appears expired.")

    present_headers = []
    missing_headers = []

    for h, label in SECURITY_HEADERS.items():
        if headers.get(h):
            present_headers.append(label)
            score += 4
        else:
            missing_headers.append(label)

    if present_headers:
        positives.append(f"Security headers present: {', '.join(present_headers)}.")
        findings.append(Finding("Security headers present", present_headers, len(present_headers) * 4))

    if missing_headers:
        negatives.append(f"Missing security headers: {', '.join(missing_headers)}.")
        findings.append(Finding("Security headers missing", missing_headers, 0))

    set_cookie = headers.get("set-cookie", "")
    if set_cookie:
        insecure_cookies = []
        for cookie in set_cookie.split(","):
            c = cookie.lower()
            if "secure" not in c and urlparse(final_url).scheme == "https":
                insecure_cookies.append(cookie[:100])

        if insecure_cookies:
            penalty = min(8, len(insecure_cookies) * 3)
            score -= penalty
            negatives.append("Some cookies may lack the Secure flag.")
            findings.append(Finding("Potential insecure cookies", len(insecure_cookies), -penalty))
        else:
            score += 2

    mixed_content = len(re.findall(r"""(?:src|href)=["']http://""", fetch.html, flags=re.I))
    if mixed_content:
        penalty = min(15, mixed_content * 3)
        score -= penalty
        negatives.append(f"Found {mixed_content} apparent HTTP asset references on page.")
    else:
        score += 3
        positives.append("No obvious mixed-content asset references.")

    if fetch.status_code and 200 <= fetch.status_code < 400:
        score += 7
        positives.append(f"HTTP status is usable: {fetch.status_code}.")
    else:
        score -= 8
        negatives.append(f"Unexpected HTTP status: {fetch.status_code}.")

    raw = {
        "ssl": ssl_info,
        "headers_present": present_headers,
        "headers_missing": missing_headers,
        "mixed_content_count": mixed_content,
    }

    return SectionResult("HTTPS/security", clamp(score), findings, positives, negatives, raw)


def score_performance_seo_accessibility(
    fetch: FetchResult,
    dom: Dict[str, Any],
    pagespeed: Dict[str, Any],
    base_url: str,
) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 30.0

    cat_scores: Dict[str, float] = {}

    if pagespeed.get("ok"):
        cats = (((pagespeed.get("data") or {}).get("lighthouseResult") or {}).get("categories") or {})
        for key in ("performance", "accessibility", "best-practices", "seo"):
            val = cats.get(key, {}).get("score")
            if val is not None:
                cat_scores[key] = round(float(val) * 100, 1)

        if cat_scores:
            avg = sum(cat_scores.values()) / len(cat_scores)
            score += avg * 0.35
            positives.append(f"PageSpeed/Lighthouse scores available: {cat_scores}.")
            findings.append(Finding("PageSpeed category scores", cat_scores, avg * 0.35))
        else:
            negatives.append("PageSpeed returned no category scores.")
    else:
        findings.append(Finding("PageSpeed", pagespeed.get("reason") or pagespeed.get("error") or "not used", 0))

    elapsed = fetch.elapsed_seconds
    if elapsed is not None:
        if elapsed < 1.0:
            score += 12
            positives.append(f"Fast initial HTML fetch: {elapsed:.2f}s.")
        elif elapsed < 3.0:
            score += 7
            positives.append(f"Acceptable initial HTML fetch: {elapsed:.2f}s.")
        elif elapsed < 6.0:
            score -= 3
            negatives.append(f"Slow initial HTML fetch: {elapsed:.2f}s.")
        else:
            score -= 10
            negatives.append(f"Very slow initial HTML fetch: {elapsed:.2f}s.")

    size = fetch.bytes_downloaded
    if size < 500_000:
        score += 6
        positives.append(f"HTML response size is light: {size} bytes.")
    elif size < 2_000_000:
        score += 2
    else:
        score -= 8
        negatives.append(f"Large HTML response: {size} bytes.")

    title = dom.get("title", "")
    if 10 <= len(title) <= 70:
        score += 7
        positives.append("Title tag has a reasonable length.")
    elif title:
        score += 2
        negatives.append("Title exists but length is not ideal.")
    else:
        score -= 7
        negatives.append("Missing <title>.")

    meta_description = dom.get("meta_description", "")
    if 50 <= len(meta_description) <= 180:
        score += 7
        positives.append("Meta description has a reasonable length.")
    elif meta_description:
        score += 2
        negatives.append("Meta description exists but length is not ideal.")
    else:
        score -= 6
        negatives.append("Missing meta description.")

    if dom.get("has_viewport"):
        score += 5
        positives.append("Mobile viewport tag present.")
    else:
        score -= 5
        negatives.append("Missing mobile viewport meta tag.")

    h1_count = dom.get("headings", {}).get("h1", 0)
    if h1_count == 1:
        score += 5
        positives.append("Exactly one H1 heading found.")
    elif h1_count > 1:
        score += 1
        negatives.append(f"Multiple H1 headings found: {h1_count}.")
    else:
        score -= 5
        negatives.append("No H1 heading found.")

    alt_ratio = dom.get("image_alt_ratio")
    if alt_ratio is None:
        score += 2
    elif alt_ratio >= 0.75:
        score += 5
        positives.append(f"Good image alt ratio: {alt_ratio:.2f}.")
    elif alt_ratio >= 0.35:
        score += 1
    else:
        score -= 5
        negatives.append(f"Low image alt ratio: {alt_ratio:.2f}.")

    robots_status, robots_err = sample_head(urljoin(base_url, "/robots.txt"))
    if robots_status and robots_status < 500:
        score += 2
        findings.append(Finding("robots.txt", robots_status, +2))
    else:
        findings.append(Finding("robots.txt", robots_err or robots_status, 0))

    sitemap_status, sitemap_err = sample_head(urljoin(base_url, "/sitemap.xml"))
    if sitemap_status and sitemap_status < 500:
        score += 2
        findings.append(Finding("sitemap.xml", sitemap_status, +2))
    else:
        findings.append(Finding("sitemap.xml", sitemap_err or sitemap_status, 0))

    raw = {
        "pagespeed_ok": pagespeed.get("ok"),
        "pagespeed_scores": cat_scores,
        "elapsed_seconds": elapsed,
        "content_length": size,
        "title": title,
        "meta_description": meta_description,
    }

    return SectionResult("Performance/SEO/accessibility", clamp(score), findings, positives, negatives, raw)


def score_tech_stack(tech: Dict[str, Any]) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 42.0

    stack = tech.get("tech", [])

    if stack:
        score += min(24, len(stack) * 4)
        positives.append(f"Detected technologies: {', '.join(stack)}.")
    else:
        score -= 5
        negatives.append("No recognizable tech stack detected.")

    if any(x in stack for x in ["Cloudflare/CDN", "Vercel", "Netlify"]):
        score += 8
        positives.append("Detected CDN or modern hosting signal.")

    if any(x in stack for x in ["Next.js", "React", "Vue", "Nuxt", "Svelte", "Angular", "Webflow"]):
        score += 8
        positives.append("Detected real frontend framework or site builder.")

    if any(x in stack for x in ["WordPress", "Wix"]):
        score -= 3
        negatives.append("Template/CMS signal detected; this is common in rushed launches.")

    script_count = tech.get("script_count", 0)
    if script_count <= 20:
        score += 6
        positives.append(f"External script count is reasonable: {script_count}.")
    elif script_count <= 60:
        findings.append(Finding("External script count", script_count, 0))
    else:
        score -= 10
        negatives.append(f"Very high external script count: {script_count}.")

    inline_chars = tech.get("inline_script_chars", 0)
    if inline_chars > 300_000:
        score -= 8
        negatives.append("Very large inline script volume.")
    elif inline_chars < 50_000:
        score += 3

    if tech.get("source_maps_referenced"):
        score -= 4
        negatives.append("Public source map references detected.")
    else:
        score += 2

    if any(x in stack for x in ["Google Analytics/GTM", "Meta Pixel"]):
        score += 4
        positives.append("Analytics/measurement signal detected.")

    if any(x in stack for x in ["WalletConnect", "ethers.js", "web3.js", "wagmi", "RainbowKit", "Solana wallet adapter"]):
        findings.append(Finding("Web3/wallet libraries", [x for x in stack if x in {
            "WalletConnect", "ethers.js", "web3.js", "wagmi", "RainbowKit", "Solana wallet adapter"
        }], 0, "Not automatically bad, but handled again in risk scoring."))

    raw = tech

    return SectionResult("Tech stack", clamp(score), findings, positives, negatives, raw)


def score_content_quality(
    text: str,
    html: str,
    dom: Dict[str, Any],
    token_name: Optional[str],
    token_symbol: Optional[str],
    contract: Optional[str],
) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 25.0

    lower_text = text.lower()
    lower_html = html.lower()
    word_count = dom.get("word_count", 0)

    title = dom.get("title", "")
    desc = dom.get("meta_description", "")

    if title:
        score += 7
        positives.append(f"Title exists: {title[:120]}.")
    else:
        score -= 5
        negatives.append("No title.")

    if desc:
        score += 7
        positives.append("Meta description exists.")
    else:
        score -= 4
        negatives.append("No meta description.")

    if word_count >= 700:
        score += 20
        positives.append(f"Substantial visible text: {word_count} words.")
    elif word_count >= 250:
        score += 12
        positives.append(f"Moderate visible text: {word_count} words.")
    elif word_count >= 80:
        score += 2
        negatives.append(f"Thin visible text: {word_count} words.")
    else:
        score -= 14
        negatives.append(f"Very thin visible text: {word_count} words.")

    if token_name and token_name.lower() in lower_text:
        score += 8
        positives.append("Expected token/project name appears in page content.")
    elif token_name:
        score -= 4
        negatives.append("Expected token/project name does not appear in page content.")

    if token_symbol and re.search(rf"\b{re.escape(token_symbol.lower())}\b", lower_text):
        score += 5
        positives.append("Expected token symbol appears in page content.")
    elif token_symbol:
        score -= 3
        negatives.append("Expected token symbol does not appear in page content.")

    evm_contracts = sorted(set(EVM_CONTRACT_RE.findall(html)))
    solana_like = sorted(set(x for x in SOLANA_LIKE_RE.findall(text) if len(x) >= 32))[:10]

    if contract:
        if contract.lower() in lower_text or contract.lower() in lower_html:
            score += 10
            positives.append("Expected contract address appears on website.")
        else:
            score -= 8
            negatives.append("Expected contract address was not found on website.")
    elif evm_contracts:
        score += 8
        positives.append("EVM contract address detected.")
        findings.append(Finding("EVM contract addresses", evm_contracts[:5], +8))
    elif solana_like:
        score += 5
        positives.append("Possible Solana-style address detected.")
        findings.append(Finding("Solana-like addresses", solana_like[:5], +5))
    else:
        score -= 5
        negatives.append("No obvious contract address detected.")

    if dom.get("has_og_title") and dom.get("has_og_image"):
        score += 8
        positives.append("OpenGraph title and image exist.")
    elif dom.get("has_og_title") or dom.get("has_og_image"):
        score += 3
        findings.append(Finding("OpenGraph metadata", "partial", +3))
    else:
        score -= 2

    if dom.get("favicon"):
        score += 4
        positives.append("Favicon exists.")

    placeholder_hits = [p for p in PARKED_OR_PLACEHOLDER_PATTERNS if p in lower_text]
    if placeholder_hits:
        penalty = min(30, len(placeholder_hits) * 8)
        score -= penalty
        negatives.append(f"Placeholder/parked patterns detected: {placeholder_hits}.")

    hype_hits = count_terms(lower_text, HYPE_TERMS)
    hype_penalty = min(20, sum(hype_hits.values()) * 2)
    if hype_hits:
        score -= hype_penalty
        negatives.append(f"Hype/risk language detected: {list(hype_hits.keys())[:10]}.")
        findings.append(Finding("Hype language", hype_hits, -hype_penalty))

    useful = [t for t in CONTENT_POSITIVE_TERMS if t in lower_text]
    if len(useful) >= 6:
        score += 15
        positives.append(f"Many useful project terms found: {', '.join(useful[:12])}.")
    elif len(useful) >= 3:
        score += 9
        positives.append(f"Useful project terms found: {', '.join(useful)}.")
    elif useful:
        score += 4
        findings.append(Finding("Useful project terms", useful, +4))
    else:
        score -= 6
        negatives.append("No obvious roadmap/tokenomics/docs/audit/FAQ-style content terms found.")

    words = re.findall(r"\b[a-zA-Z]{4,}\b", lower_text)
    repeated_ratio = 0.0
    if words:
        c = Counter(words)
        repeated_ratio = sum(1 for _, n in c.items() if n >= 8) / max(1, len(c))

    if repeated_ratio > 0.08:
        score -= 5
        negatives.append("Visible text appears repetitive.")
    else:
        score += 2

    raw = {
        "word_count": word_count,
        "title": title,
        "description": desc,
        "placeholder_hits": placeholder_hits,
        "hype_hits": hype_hits,
        "useful_terms": useful,
        "repeated_word_ratio": repeated_ratio,
        "evm_contracts": evm_contracts[:10],
        "solana_like_addresses": solana_like[:10],
    }

    return SectionResult("Content quality", clamp(score), findings, positives, negatives, raw)


def score_social_consistency(
    dom: Dict[str, Any],
    final_url: str,
    expected_x: Optional[str],
    expected_telegram: Optional[str],
    expected_website: Optional[str],
    check_social_links: bool,
) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 28.0

    links = dom.get("social_links", [])
    socials = extract_socials(links)

    major = {k: v for k, v in socials.items() if k in {"twitter", "telegram", "discord", "github"}}
    market = {k: v for k, v in socials.items() if k in {
        "dexscreener", "dextools", "geckoterminal", "coinmarketcap", "coingecko"
    }}

    if len(major) >= 3:
        score += 20
        positives.append("Several major social/community links found.")
    elif len(major) == 2:
        score += 13
        positives.append("Two major social/community links found.")
    elif len(major) == 1:
        score += 5
        positives.append("One major social/community link found.")
    else:
        score -= 15
        negatives.append("No X/Twitter, Telegram, Discord, or GitHub links found.")

    if links:
        findings.append(Finding("Social links", links[:20], 0))
    if market:
        score += 8
        positives.append("Market/data links found.")
        findings.append(Finding("Market/data links", market, +8))

    found_set = {canonical_social_url(x) for x in links}

    if expected_x:
        exp = canonical_social_url(expected_x)
        if exp in found_set:
            score += 14
            positives.append("Expected X/Twitter link is present.")
        else:
            score -= 10
            negatives.append("Expected X/Twitter link was not found.")

    if expected_telegram:
        exp = canonical_social_url(expected_telegram)
        if exp in found_set:
            score += 14
            positives.append("Expected Telegram link is present.")
        else:
            score -= 10
            negatives.append("Expected Telegram link was not found.")

    if expected_website:
        expected_domain = root_domain(hostname(expected_website))
        final_domain = root_domain(hostname(final_url))
        if expected_domain == final_domain:
            score += 10
            positives.append("Website domain matches expected website domain.")
        else:
            score -= 12
            negatives.append(f"Website domain mismatch: expected {expected_domain}, got {final_domain}.")

    shorteners = []
    for link in links + dom.get("external_links", []):
        h = hostname(link).lower().replace("www.", "")
        if h in LINK_SHORTENERS:
            shorteners.append(link)

    if shorteners:
        penalty = min(15, len(shorteners) * 5)
        score -= penalty
        negatives.append("Link shorteners detected.")
        findings.append(Finding("Link shorteners", shorteners[:10], -penalty))
    else:
        score += 3

    final_token = domain_token(hostname(final_url)).lower()
    handles = []

    for link in links:
        p = urlparse(link)
        if "x.com" in p.netloc or "twitter.com" in p.netloc or "t.me" in p.netloc:
            parts = [x for x in p.path.split("/") if x]
            if parts:
                handles.append(parts[0].lower().replace("@", ""))

    handle_matches = [h for h in handles if final_token and (final_token in h or h in final_token)]
    if handle_matches:
        score += 6
        positives.append("At least one social handle appears brand-consistent with the domain.")
    elif handles:
        score -= 4
        negatives.append("Could not confirm brand consistency between domain and social handles.")

    x_handles = []
    for link in links:
        p = urlparse(link)
        if "x.com" in p.netloc or "twitter.com" in p.netloc:
            parts = [x for x in p.path.split("/") if x]
            if parts:
                x_handles.append(parts[0].lower())

    if len(set(x_handles)) > 1:
        score -= 12
        negatives.append(f"Multiple X/Twitter handles found: {sorted(set(x_handles))}.")

    reachability: List[Dict[str, Any]] = []
    if check_social_links:
        for link in sorted(set(links))[:8]:
            status, err = sample_head(link)
            reachability.append({"url": link, "status": status, "error": err})
        ok_count = sum(1 for item in reachability if item["status"] and item["status"] < 500)
        score += min(6, ok_count)
        findings.append(Finding("Sampled social reachability", reachability, min(6, ok_count)))

    raw = {
        "found_social_links": links,
        "socials_by_type": socials,
        "expected_x": expected_x,
        "expected_telegram": expected_telegram,
        "expected_website": expected_website,
        "handles": handles,
        "reachability": reachability,
    }

    return SectionResult("Social consistency", clamp(score), findings, positives, negatives, raw)


def score_risk_flags(
    fetch: FetchResult,
    soup: BeautifulSoup,
    text: str,
    dom: Dict[str, Any],
    urlscan: Dict[str, Any],
) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 100.0

    html = fetch.html or ""
    lower_text = text.lower()
    lower_html = html.lower()
    flags: List[str] = []

    if not fetch.ok:
        penalty = 40
        score -= penalty
        flags.append(f"Fetch failed or non-usable response: {fetch.error or fetch.status_code}.")

    if fetch.status_code and int(fetch.status_code) >= 400:
        score -= 25
        flags.append(f"Bad HTTP status: {fetch.status_code}.")

    if len(fetch.redirects) > 3:
        score -= 12
        flags.append("Long redirect chain.")

    placeholder_hits = [p for p in PARKED_OR_PLACEHOLDER_PATTERNS if p in lower_text]
    if placeholder_hits:
        penalty = min(35, len(placeholder_hits) * 10)
        score -= penalty
        flags.append(f"Parked/placeholder text: {placeholder_hits}.")

    hype_hits = count_terms(lower_text, HYPE_TERMS)
    if hype_hits:
        penalty = min(30, sum(hype_hits.values()) * 3)
        score -= penalty
        flags.append(f"Hype/scam-risk words: {list(hype_hits.keys())[:10]}.")
        findings.append(Finding("Hype terms", hype_hits, -penalty))

    dangerous_hits = count_terms(lower_html + " " + lower_text, DANGEROUS_WEB3_TERMS)
    if dangerous_hits:
        penalty = min(55, sum(dangerous_hits.values()) * 8)
        score -= penalty
        flags.append(f"Dangerous wallet/Web3 terms detected: {list(dangerous_hits.keys())[:10]}.")
        findings.append(Finding("Dangerous Web3 terms", dangerous_hits, -penalty))

    if "connect wallet" in lower_text or "walletconnect" in lower_html:
        score -= 8
        flags.append("Wallet-connect/call-to-connect detected.")

    seed_form = False
    for field in soup.find_all(["input", "textarea"]):
        attrs = " ".join(str(field.get(x, "")) for x in ["name", "id", "placeholder", "aria-label"]).lower()
        if any(term in attrs for term in ["seed", "phrase", "private", "mnemonic", "secret"]):
            seed_form = True
            break

    if seed_form:
        score -= 45
        flags.append("Page appears to ask for seed phrase, private key, mnemonic, or secret material.")

    password_inputs = soup.find_all("input", attrs={"type": "password"})
    if password_inputs:
        score -= 18
        flags.append("Password input detected.")

    forms = soup.find_all("form")
    if len(forms) > 3:
        score -= 8
        flags.append(f"Many forms detected: {len(forms)}.")

    hidden_iframes = 0
    for iframe in soup.find_all("iframe"):
        style = str(iframe.get("style", "")).lower()
        width = str(iframe.get("width", "")).lower()
        height = str(iframe.get("height", "")).lower()
        if "display:none" in style or "visibility:hidden" in style or width in {"0", "1"} or height in {"0", "1"}:
            hidden_iframes += 1

    if hidden_iframes:
        penalty = min(20, hidden_iframes * 8)
        score -= penalty
        flags.append(f"Hidden iframes detected: {hidden_iframes}.")

    obfuscation = {
        "eval(": lower_html.count("eval("),
        "atob(": lower_html.count("atob("),
        "fromCharCode": html.count("fromCharCode"),
    }
    obf_count = sum(obfuscation.values())

    if obf_count:
        penalty = min(25, obf_count * 5)
        score -= penalty
        flags.append("JavaScript obfuscation indicators found.")
        findings.append(Finding("JS obfuscation indicators", obfuscation, -penalty))

    external_count = len(dom.get("external_links", []))
    if external_count > 30:
        score -= 8
        flags.append(f"High external-link count: {external_count}.")

    if urlscan.get("ok"):
        positives.append("urlscan.io submission succeeded.")
        findings.append(Finding("urlscan", urlscan.get("data", {}), 0))
    elif urlscan and not urlscan.get("skipped", True):
        findings.append(Finding("urlscan", urlscan.get("error") or urlscan.get("status_code"), 0))

    if flags:
        negatives.extend(flags)
    else:
        positives.append("No major obvious risk flags detected by static checks.")

    raw = {
        "flags": flags,
        "placeholder_hits": placeholder_hits,
        "hype_hits": hype_hits,
        "dangerous_hits": dangerous_hits,
        "hidden_iframes": hidden_iframes,
        "obfuscation": obfuscation,
        "urlscan": urlscan,
    }

    return SectionResult("Risk flags", clamp(score), findings, positives, negatives, raw)


def score_screenshot_dom_quality(
    dom: Dict[str, Any],
    screenshot: Dict[str, Any],
) -> SectionResult:
    findings: List[Finding] = []
    positives: List[str] = []
    negatives: List[str] = []
    score = 20.0

    nodes = dom.get("node_count", 0)
    links = dom.get("link_count", 0)
    imgs = dom.get("image_count", 0)
    word_count = dom.get("word_count", 0)

    if screenshot.get("ok"):
        rendered_dom = screenshot.get("dom") or {}
        shot_stats = analyze_screenshot(screenshot.get("path"))

        score += 15
        positives.append(f"Screenshot captured: {screenshot.get('path')}.")

        if screenshot.get("render_seconds") is not None:
            rs = screenshot["render_seconds"]
            if rs < 3:
                score += 7
                positives.append(f"Rendered quickly: {rs:.2f}s.")
            elif rs < 8:
                score += 3
            else:
                score -= 6
                negatives.append(f"Rendered slowly: {rs:.2f}s.")

        rendered_words = rendered_dom.get("bodyWordCount")
        if rendered_words is not None:
            if rendered_words >= 150:
                score += 8
                positives.append(f"Rendered DOM has meaningful text: {rendered_words} words.")
            elif rendered_words >= 40:
                score += 2
            else:
                score -= 8
                negatives.append("Rendered DOM is thin or mostly graphical.")

        broken = rendered_dom.get("brokenImageCount", 0)
        rendered_imgs = rendered_dom.get("imageCount", 0)
        if rendered_imgs and broken / max(1, rendered_imgs) > 0.2:
            score -= 8
            negatives.append("Many rendered images appear broken.")
        else:
            score += 3

        console_errors = screenshot.get("console_errors", [])
        if console_errors:
            penalty = min(12, len(console_errors) * 3)
            score -= penalty
            negatives.append(f"Console errors captured: {len(console_errors)}.")
            findings.append(Finding("Console errors", console_errors[:10], -penalty))
        else:
            score += 5
            positives.append("No console errors captured.")

        failures = screenshot.get("request_failures", [])
        if failures:
            penalty = min(10, len(failures) * 2)
            score -= penalty
            negatives.append(f"Failed network requests captured: {len(failures)}.")
            findings.append(Finding("Request failures", failures[:10], -penalty))
        else:
            score += 4

        if shot_stats.get("available"):
            if shot_stats.get("blank_like"):
                score -= 18
                negatives.append("Screenshot appears visually blank or near-uniform.")
            else:
                score += 6
                positives.append("Screenshot has visible variance.")
            findings.append(Finding("Screenshot stats", shot_stats, 0))

        findings.append(Finding("Rendered DOM path", screenshot.get("rendered_dom_path"), 0))

    else:
        findings.append(Finding(
            "Screenshot",
            screenshot.get("reason") or screenshot.get("error") or "not requested",
            0,
        ))

    if 80 <= nodes <= 5000:
        score += 15
        positives.append(f"Static DOM node count looks reasonable: {nodes}.")
    elif nodes < 80:
        score += 4
        negatives.append(f"Very small static DOM: {nodes} nodes.")
    else:
        score += 7
        negatives.append(f"Very large static DOM: {nodes} nodes.")

    if word_count >= 150:
        score += 10
        positives.append(f"Visible text is sufficient for a basic project site: {word_count} words.")
    elif word_count >= 50:
        score += 3
    else:
        score -= 8
        negatives.append(f"Very low visible text: {word_count} words.")

    if imgs >= 1:
        score += 6
        positives.append(f"Images present: {imgs}.")
    else:
        negatives.append("No images detected.")

    if dom.get("favicon"):
        score += 5
    if dom.get("has_og_image"):
        score += 5
    if dom.get("has_viewport"):
        score += 5

    if links >= 3:
        score += 6
        positives.append(f"Navigation/external links present: {links}.")
    else:
        negatives.append(f"Few links/navigation items: {links}.")

    headings = dom.get("headings", {})
    if sum(headings.values()) >= 2 and headings.get("h1", 0) >= 1:
        score += 5
        positives.append("Headings are present.")
    else:
        negatives.append("Weak heading structure.")

    raw = {"dom": dom, "screenshot": screenshot}

    return SectionResult("Screenshot/DOM quality", clamp(score), findings, positives, negatives, raw)


# ---------------------------------------------------------------------------
# Overall scoring and report assembly
# ---------------------------------------------------------------------------


def weighted_overall(sections: List[SectionResult]) -> Tuple[float, Dict[str, float]]:
    weights = {
        "Domain quality": 0.15,
        "HTTPS/security": 0.15,
        "Performance/SEO/accessibility": 0.14,
        "Tech stack": 0.10,
        "Content quality": 0.16,
        "Social consistency": 0.12,
        "Risk flags": 0.13,
        "Screenshot/DOM quality": 0.05,
    }

    total = 0.0
    parts: Dict[str, float] = {}

    for section in sections:
        w = weights.get(section.name, 0.0)
        contribution = section.score * w
        parts[section.name] = round(contribution, 2)
        total += contribution

    return round(clamp(total), 1), parts


def grade_label(score: float) -> str:
    if score >= 80:
        return "strong"
    if score >= 65:
        return "acceptable"
    if score >= 50:
        return "weak/mixed"
    if score >= 35:
        return "poor / high risk"
    return "very poor / very high risk"


def print_section(section: SectionResult) -> None:
    print("\n" + "=" * 88)
    print(f"{section.name}: {section.score:.1f}/100 ({grade_label(section.score)})")
    print("-" * 88)

    if section.positives:
        print("Positive:")
        for item in section.positives:
            print(f"  + {item}")

    if section.findings:
        print("Findings:")
        for item in section.findings:
            value = item.value
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value, ensure_ascii=False, default=str)
            else:
                value_str = str(value)

            if len(value_str) > 500:
                value_str = value_str[:497] + "..."

            impact = f"{item.impact:+.1f}" if item.impact else "0"
            print(f"  - {item.label}: {value_str} | impact {impact}")
            if item.note:
                print(f"    note: {item.note}")

    if section.negatives:
        print("Negative/risk:")
        for item in section.negatives:
            print(f"  - {item}")


def section_to_json(section: SectionResult) -> Dict[str, Any]:
    return {
        "score": round(section.score, 2),
        "label": grade_label(section.score),
        "positives": section.positives,
        "findings": [dataclasses.asdict(f) for f in section.findings],
        "negatives": section.negatives,
        "raw": section.raw,
    }


def build_report(
    args: argparse.Namespace,
    fetch: FetchResult,
    dns: Dict[str, Any],
    ssl_info: Dict[str, Any],
    rdap: Dict[str, Any],
    dom: Dict[str, Any],
    tech: Dict[str, Any],
    pagespeed: Dict[str, Any],
    screenshot: Dict[str, Any],
    urlscan: Dict[str, Any],
    sections: List[SectionResult],
    overall: float,
    weighted_parts: Dict[str, float],
) -> Dict[str, Any]:
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input_url": normalize_url(args.url),
        "final_url": fetch.final_url,
        "overall_score": overall,
        "overall_label": grade_label(overall),
        "weighted_contribution": weighted_parts,
        "sections": {s.name: section_to_json(s) for s in sections},
        "raw": {
            "request": {
                "input_url": fetch.input_url,
                "final_url": fetch.final_url,
                "status_code": fetch.status_code,
                "ok": fetch.ok,
                "elapsed_seconds": fetch.elapsed_seconds,
                "headers": fetch.headers,
                "error": fetch.error,
                "redirects": fetch.redirects,
                "bytes_downloaded": fetch.bytes_downloaded,
            },
            "dns": dns,
            "ssl": ssl_info,
            "rdap": rdap,
            "dom": dom,
            "tech": tech,
            "pagespeed": pagespeed,
            "screenshot": screenshot,
            "urlscan": urlscan,
        },
    }
