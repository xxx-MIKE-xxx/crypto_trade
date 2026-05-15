#!/usr/bin/env python3
"""
coin_website_grader.py

Grades any crypto / meme-coin website by URL.

Categories:
  - Domain quality
  - HTTPS/security
  - Performance/SEO/accessibility
  - Tech stack
  - Content quality
  - Social consistency
  - Risk flags
  - Screenshot/DOM quality

Install:
  pip install requests beautifulsoup4

Recommended optional deps:
  pip install pillow playwright
  python -m playwright install chromium

Optional APIs:
  export PAGESPEED_API_KEY=YOUR_KEY
  export URLSCAN_API_KEY=YOUR_KEY

Run:
  python coin_website_grader.py https://examplecoin.xyz
  python coin_website_grader.py --url https://examplecoin.xyz
  python coin_website_grader.py --url https://examplecoin.xyz --json
  python coin_website_grader.py --url https://examplecoin.xyz --screenshot
  python coin_website_grader.py --url https://examplecoin.xyz --use-pagespeed

With expected token/social identity:
  python coin_website_grader.py \
    --url https://examplecoin.xyz \
    --token-name "Example Coin" \
    --token-symbol EXMP \
    --contract 0x0000000000000000000000000000000000000000 \
    --expected-x https://x.com/examplecoin \
    --expected-telegram https://t.me/examplecoin
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import re
import socket
import ssl
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Install with: pip install requests beautifulsoup4", file=sys.stderr)
    raise


USER_AGENT = (
    "Mozilla/5.0 (compatible; CoinWebsiteGrader/2.0; "
    "+https://example.local/research-bot)"
)

FREE_OR_LOW_EFFORT_HOSTS = {
    "linktr.ee", "carrd.co", "wixsite.com", "wordpress.com", "blogspot.com",
    "github.io", "netlify.app", "vercel.app", "pages.dev", "notion.site",
    "webflow.io", "sites.google.com", "bio.link", "beacons.ai", "taplink.cc",
    "solo.to", "about.me", "lnk.bio", "framer.website",
}

RISKY_TLDS = {
    "zip", "mov", "click", "country", "kim", "gq", "work", "quest",
    "top", "xyz", "icu", "cyou", "rest", "support", "surf", "monster",
}

SECURITY_HEADERS = {
    "strict-transport-security": "HSTS",
    "content-security-policy": "CSP",
    "x-content-type-options": "X-Content-Type-Options",
    "x-frame-options": "X-Frame-Options",
    "referrer-policy": "Referrer-Policy",
    "permissions-policy": "Permissions-Policy",
}

SOCIAL_DOMAINS = {
    "twitter.com": "twitter",
    "x.com": "twitter",
    "t.me": "telegram",
    "telegram.me": "telegram",
    "discord.gg": "discord",
    "discord.com": "discord",
    "github.com": "github",
    "medium.com": "medium",
    "mirror.xyz": "mirror",
    "reddit.com": "reddit",
    "youtube.com": "youtube",
    "linktr.ee": "linktree",
    "dexscreener.com": "dexscreener",
    "dextools.io": "dextools",
    "geckoterminal.com": "geckoterminal",
    "coinmarketcap.com": "coinmarketcap",
    "coingecko.com": "coingecko",
}

LINK_SHORTENERS = {
    "bit.ly", "tinyurl.com", "t.co", "cutt.ly", "is.gd", "buff.ly", "shorturl.at",
    "rebrand.ly", "ow.ly", "soo.gd",
}

PARKED_OR_PLACEHOLDER_PATTERNS = [
    "domain is parked", "buy this domain", "this domain is for sale",
    "coming soon", "under construction", "lorem ipsum", "template",
    "sedo", "afternic", "dan.com", "parkingcrew", "namecheap parking",
    "insert text", "your text here", "untitled", "sample token", "example token",
]

HYPE_TERMS = [
    "100x", "1000x", "moon", "moonshot", "guaranteed", "risk-free", "no risk",
    "gem", "ape in", "send it", "lambo", "pump", "next shib", "next doge",
    "financial freedom", "life changing", "don't miss", "last chance",
    "free money", "presale bonus", "moon soon",
]

CONTENT_POSITIVE_TERMS = [
    "tokenomics", "roadmap", "whitepaper", "docs", "documentation", "audit",
    "liquidity", "vesting", "renounced", "locked", "contract", "ca:",
    "community", "governance", "utility", "burn", "supply", "chain",
    "faq", "privacy", "terms", "about",
]

DANGEROUS_WEB3_TERMS = [
    "setapprovalforall",
    "approve(address,uint256)",
    "increaseallowance",
    "permit(",
    "personal_sign",
    "eth_sign",
    "wallet_requestpermissions",
    "unlimited approval",
    "infinite approval",
    "seed phrase",
    "secret phrase",
    "private key",
    "mnemonic",
    "claim airdrop",
    "connect wallet to claim",
    "drainer",
    "sweeper",
    "send sol",
]

EVM_CONTRACT_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
SOLANA_LIKE_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")


@dataclasses.dataclass
class Finding:
    label: str
    value: Any
    impact: float = 0.0
    note: str = ""


@dataclasses.dataclass
class SectionResult:
    name: str
    score: float
    findings: List[Finding]
    positives: List[str]
    negatives: List[str]
    raw: Dict[str, Any]


@dataclasses.dataclass
class FetchResult:
    input_url: str
    final_url: str
    status_code: Optional[int]
    ok: bool
    elapsed_seconds: Optional[float]
    headers: Dict[str, str]
    html: str
    error: Optional[str]
    redirects: List[Dict[str, Any]]
    bytes_downloaded: int


def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def normalize_url(url: str) -> str:
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def hostname(url: str) -> str:
    return urlparse(url).hostname or ""


def root_domain(host: str) -> str:
    host = (host or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]

    parts = host.split(".")
    if len(parts) <= 2:
        return host

    two_level_suffixes = {
        "co.uk", "org.uk", "ac.uk", "com.au", "net.au", "co.jp",
        "com.br", "com.tr", "co.in", "com.sg",
    }
    suffix2 = ".".join(parts[-2:])
    if suffix2 in two_level_suffixes and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def domain_token(host: str) -> str:
    rd = root_domain(host)
    parts = rd.split(".")
    return parts[0] if parts else rd


def suffix_of(host: str) -> str:
    rd = root_domain(host)
    parts = rd.split(".")
    return parts[-1] if len(parts) >= 2 else ""


def shannon_entropy(s: str) -> float:
    if not s:
        return 0.0
    counts = Counter(s)
    total = len(s)
    return -sum((n / total) * math.log2(n / total) for n in counts.values())


def count_terms(text: str, terms: List[str]) -> Dict[str, int]:
    lower = text.lower()
    return {t: lower.count(t.lower()) for t in terms if lower.count(t.lower()) > 0}


def parse_datetime(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None

    if isinstance(value, list):
        for item in value:
            parsed = parse_datetime(item)
            if parsed:
                return parsed
        return None

    text = str(value).replace("Z", "+00:00")

    try:
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except Exception:
        pass

    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            parsed = dt.datetime.strptime(text[:25], fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
        except Exception:
            continue

    return None


def fetch_url(url: str, timeout: float = 12.0) -> FetchResult:
    session = requests.Session()
    session.headers.update({
        "user-agent": USER_AGENT,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    start = time.time()
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        elapsed = time.time() - start
        return FetchResult(
            input_url=url,
            final_url=resp.url,
            status_code=resp.status_code,
            ok=200 <= resp.status_code < 400,
            elapsed_seconds=elapsed,
            headers={k.lower(): v for k, v in resp.headers.items()},
            html=resp.text or "",
            error=None,
            redirects=[{"status_code": r.status_code, "url": r.url} for r in resp.history],
            bytes_downloaded=len(resp.content or b""),
        )
    except Exception as e:
        elapsed = time.time() - start
        return FetchResult(
            input_url=url,
            final_url=url,
            status_code=None,
            ok=False,
            elapsed_seconds=elapsed,
            headers={},
            html="",
            error=f"{type(e).__name__}: {e}",
            redirects=[],
            bytes_downloaded=0,
        )


def check_dns(host: str) -> Dict[str, Any]:
    try:
        records = socket.getaddrinfo(host, 443)
        ips = sorted({r[4][0] for r in records if r and len(r) >= 5})
        return {"ok": True, "ips": ips[:12]}
    except Exception as e:
        return {"ok": False, "ips": [], "error": f"{type(e).__name__}: {e}"}


def check_ssl_cert(host: str, port: int = 443, timeout: float = 8.0) -> Dict[str, Any]:
    out = {
        "ok": False,
        "valid_now": False,
        "issuer": None,
        "subject": None,
        "not_before": None,
        "not_after": None,
        "days_to_expiry": None,
        "san_matches_host": False,
        "error": None,
    }

    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()

        out["ok"] = True
        out["issuer"] = dict(x[0] for x in cert.get("issuer", [])).get("organizationName")
        out["subject"] = dict(x[0] for x in cert.get("subject", [])).get("commonName")
        out["not_before"] = cert.get("notBefore")
        out["not_after"] = cert.get("notAfter")

        if cert.get("notAfter"):
            not_after = dt.datetime.strptime(
                cert["notAfter"],
                "%b %d %H:%M:%S %Y %Z",
            ).replace(tzinfo=dt.timezone.utc)
            days = (not_after - dt.datetime.now(dt.timezone.utc)).days
            out["days_to_expiry"] = days
            out["valid_now"] = days > 0

        san_hosts = []
        for typ, val in cert.get("subjectAltName", []):
            if typ.lower() == "dns":
                san_hosts.append(val.lower())

        host_l = host.lower()
        out["san_matches_host"] = any(
            h == host_l or (h.startswith("*.") and host_l.endswith(h[1:]))
            for h in san_hosts
        )

    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"

    return out


def fetch_rdap(domain: str, timeout: float = 10.0) -> Dict[str, Any]:
    url = f"https://rdap.org/domain/{domain}"
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={
                "accept": "application/rdap+json, application/json",
                "user-agent": USER_AGENT,
            },
        )
        data = r.json() if "json" in r.headers.get("content-type", "") else {"raw": r.text[:2000]}
        return {"ok": r.status_code == 200, "status_code": r.status_code, "url": url, "data": data}
    except Exception as e:
        return {"ok": False, "status_code": None, "url": url, "data": {}, "error": f"{type(e).__name__}: {e}"}


def extract_rdap_dates(rdap_data: Dict[str, Any]) -> Tuple[Optional[dt.datetime], Optional[dt.datetime], Optional[str]]:
    created = None
    expires = None
    registrar = None

    for event in rdap_data.get("events", []) or []:
        if not isinstance(event, dict):
            continue
        action = str(event.get("eventAction", "")).lower()
        date = parse_datetime(event.get("eventDate"))
        if "registration" in action or "created" in action:
            created = created or date
        if "expiration" in action or "expiry" in action:
            expires = expires or date

    for ent in rdap_data.get("entities", []) or []:
        if not isinstance(ent, dict):
            continue
        roles = [str(x).lower() for x in ent.get("roles", [])]
        if "registrar" not in roles:
            continue

        vcard = ent.get("vcardArray")
        if isinstance(vcard, list) and len(vcard) > 1:
            for row in vcard[1]:
                if isinstance(row, list) and row and row[0] == "fn":
                    registrar = row[-1]
                    break

    return created, expires, registrar


def parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", "html.parser")


def visible_text(soup: BeautifulSoup) -> str:
    soup2 = BeautifulSoup(str(soup), "html.parser")
    for tag in soup2(["script", "style", "noscript", "template", "svg"]):
        tag.extract()
    return re.sub(r"\s+", " ", soup2.get_text(" ", strip=True)).strip()


def all_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:")):
            continue
        links.append(urljoin(base_url, href))
    return sorted(set(links))


def canonical_social_url(url: str) -> str:
    try:
        p = urlparse(url)
        host_l = p.netloc.lower().replace("www.", "")
        path = p.path.rstrip("/").lower()
        return f"{host_l}{path}"
    except Exception:
        return url.lower().strip().rstrip("/")


def extract_socials(links: List[str]) -> Dict[str, List[str]]:
    socials: Dict[str, List[str]] = {}
    for link in links:
        host_l = hostname(link).lower().replace("www.", "")
        for domain, name in SOCIAL_DOMAINS.items():
            if host_l == domain or host_l.endswith("." + domain):
                socials.setdefault(name, []).append(link)
    return {k: sorted(set(v)) for k, v in socials.items()}


def sample_head(url: str, timeout: float = 8.0) -> Tuple[Optional[int], Optional[str]]:
    try:
        r = requests.head(
            url,
            headers={"user-agent": USER_AGENT},
            timeout=timeout,
            allow_redirects=True,
        )
        return r.status_code, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def collect_dom_stats(soup: BeautifulSoup, final_url: str, text: str) -> Dict[str, Any]:
    tags = soup.find_all(True)
    links = all_links(soup, final_url)
    imgs = soup.find_all("img")
    buttons = soup.find_all(["button", "input", "textarea", "select"])
    headings = {f"h{i}": len(soup.find_all(f"h{i}")) for i in range(1, 7)}
    img_alt_count = sum(1 for img in imgs if img.get("alt", "").strip())

    og_tags = {
        m.get("property") or m.get("name"): m.get("content")
        for m in soup.find_all("meta")
        if m.get("property") or m.get("name")
    }

    favicon = None
    for link in soup.find_all("link"):
        rel = link.get("rel")
        rel_text = " ".join(rel).lower() if isinstance(rel, list) else str(rel or "").lower()
        if "icon" in rel_text:
            href = link.get("href")
            if href:
                favicon = urljoin(final_url, href)
            break

    social_links = []
    external_links = []
    final_root = root_domain(hostname(final_url))

    for link in links:
        link_host = hostname(link)
        if not link_host:
            continue

        link_root = root_domain(link_host)
        if any(d in link_host.lower() for d in SOCIAL_DOMAINS):
            social_links.append(link)
        elif link_root != final_root:
            external_links.append(link)

    meta_desc = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    title = soup.find("title")

    return {
        "node_count": len(tags),
        "link_count": len(links),
        "image_count": len(imgs),
        "image_alt_ratio": (img_alt_count / len(imgs)) if imgs else None,
        "button_input_count": len(buttons),
        "headings": headings,
        "has_viewport": soup.find("meta", attrs={"name": re.compile("^viewport$", re.I)}) is not None,
        "has_title": bool(title and title.get_text(strip=True)),
        "title": title.get_text(" ", strip=True) if title else "",
        "meta_description": meta_desc.get("content", "").strip() if meta_desc else "",
        "has_og_image": bool(og_tags.get("og:image")),
        "has_og_title": bool(og_tags.get("og:title")),
        "favicon": favicon,
        "text_length": len(text),
        "word_count": len(re.findall(r"\b\w+\b", text)),
        "social_links": sorted(set(social_links)),
        "external_links": sorted(set(external_links))[:100],
        "og_tags": og_tags,
    }


def detect_tech_stack(headers: Dict[str, str], soup: BeautifulSoup, final_url: str) -> Dict[str, Any]:
    header_l = {k.lower(): v for k, v in headers.items()}
    scripts = [s.get("src", "") for s in soup.find_all("script") if s.get("src")]
    links = [l.get("href", "") for l in soup.find_all("link") if l.get("href")]
    html = str(soup).lower()
    urls = " ".join(scripts + links).lower()

    tech = []
    signals = []

    server = header_l.get("server")
    powered = header_l.get("x-powered-by")

    if server:
        signals.append(f"server={server}")
    if powered:
        signals.append(f"x-powered-by={powered}")

    checks = [
        ("Cloudflare/CDN", "cloudflare" in str(server).lower() or "cf-ray" in header_l),
        ("Vercel", "vercel" in urls or "x-vercel-id" in header_l or "vercel" in final_url),
        ("Netlify", "netlify" in urls or "x-nf-request-id" in header_l or "netlify" in final_url),
        ("Next.js", "_next/" in urls or "__next_data__" in html or "next.js" in html),
        ("React", "react" in urls or "__next_data__" in html or "data-reactroot" in html),
        ("Vue", "vue" in urls or "__vue" in html),
        ("Nuxt", "_nuxt/" in urls),
        ("Svelte", "svelte" in urls),
        ("Angular", "ng-version" in html or "angular" in urls),
        ("Tailwind-like CSS", re.search(r"\b(?:text|bg|flex|grid|rounded|shadow)-[a-z0-9\-\[\]/]+", html) is not None),
        ("Bootstrap", "bootstrap" in urls),
        ("jQuery", "jquery" in urls),
        ("Webflow", "webflow" in urls or "webflow" in html),
        ("Wix", "wixstatic" in urls or "wix.com" in html),
        ("WordPress", "wp-content" in urls or "wordpress" in html),
        ("Shopify", "shopify" in urls or "cdn.shopify.com" in urls),
        ("Google Analytics/GTM", "googletagmanager" in urls or "google-analytics" in urls or "gtag(" in html),
        ("Meta Pixel", "connect.facebook.net" in urls or "fbq(" in html),
        ("WalletConnect", "walletconnect" in urls or "walletconnect" in html),
        ("ethers.js", "ethers.js" in urls or "ethers.min.js" in urls or "ethersproject" in html),
        ("web3.js", "web3.js" in urls or "web3.min.js" in urls),
        ("wagmi", "wagmi" in html),
        ("RainbowKit", "rainbowkit" in html),
        ("Solana wallet adapter", "wallet-adapter" in html or "@solana" in html),
    ]

    for name, ok in checks:
        if ok:
            tech.append(name)

    inline_script_chars = sum(len(s.get_text(" ", strip=True)) for s in soup.find_all("script") if not s.get("src"))

    return {
        "tech": sorted(set(tech)),
        "signals": signals,
        "script_count": len(scripts),
        "stylesheet_count": len([x for x in links if ".css" in x or "stylesheet" in x.lower()]),
        "inline_script_chars": inline_script_chars,
        "external_script_hosts": sorted(set(urlparse(x).netloc.lower() for x in scripts if urlparse(x).netloc))[:50],
        "source_maps_referenced": bool(re.search(r"\.map(?:\?|['\"<])", str(soup), flags=re.I)),
    }


def run_pagespeed(url: str, api_key: Optional[str]) -> Dict[str, Any]:
    endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = [
        ("url", url),
        ("strategy", "mobile"),
        ("category", "PERFORMANCE"),
        ("category", "ACCESSIBILITY"),
        ("category", "BEST_PRACTICES"),
        ("category", "SEO"),
    ]
    if api_key:
        params.append(("key", api_key))

    try:
        r = requests.get(endpoint, params=params, timeout=60)
        return {"ok": r.status_code == 200, "status_code": r.status_code, "data": r.json()}
    except Exception as e:
        return {"ok": False, "status_code": None, "data": {}, "error": f"{type(e).__name__}: {e}"}


def run_urlscan(url: str, api_key: Optional[str]) -> Dict[str, Any]:
    if not api_key:
        return {"ok": False, "skipped": True, "reason": "No URLSCAN_API_KEY"}

    try:
        r = requests.post(
            "https://urlscan.io/api/v1/scan/",
            headers={"API-Key": api_key, "Content-Type": "application/json"},
            json={"url": url, "visibility": "public"},
            timeout=20,
        )
        return {"ok": r.status_code in (200, 201), "status_code": r.status_code, "data": r.json()}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def try_playwright_render(url: str, out_dir: Path, timeout_ms: int = 25000) -> Dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        return {"ok": False, "reason": f"Playwright not installed: {e}"}

    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(url.encode()).hexdigest()[:12]
        shot_path = out_dir / f"screenshot_{digest}.png"
        dom_path = out_dir / f"rendered_dom_{digest}.html"

        console_errors = []
        request_failures = []

        start = time.time()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                viewport={"width": 1365, "height": 900},
                user_agent=USER_AGENT,
            )

            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("requestfailed", lambda req: request_failures.append(req.url))

            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            page.screenshot(path=str(shot_path), full_page=True)
            dom_path.write_text(page.content(), encoding="utf-8", errors="ignore")

            rendered_dom = page.evaluate(
                """() => {
                    const text = document.body ? document.body.innerText : "";
                    const els = Array.from(document.querySelectorAll("*"));
                    const imgs = Array.from(document.images || []);
                    const controls = Array.from(document.querySelectorAll("button, a, input, textarea, select"));
                    const hidden = els.filter(e => {
                        const s = window.getComputedStyle(e);
                        const r = e.getBoundingClientRect();
                        return s.display === "none" || s.visibility === "hidden" || r.width === 0 || r.height === 0;
                    }).length;
                    const brokenImgs = imgs.filter(img => img.complete && img.naturalWidth === 0).length;
                    return {
                        title: document.title,
                        bodyTextLength: text.length,
                        bodyWordCount: (text.match(/\\b\\w+\\b/g) || []).length,
                        elementCount: els.length,
                        imageCount: imgs.length,
                        brokenImageCount: brokenImgs,
                        interactiveCount: controls.length,
                        iframeCount: document.querySelectorAll("iframe").length,
                        hiddenElementCount: hidden,
                        scrollHeight: document.documentElement.scrollHeight,
                        viewportHeight: window.innerHeight
                    };
                }"""
            )

            browser.close()

        return {
            "ok": True,
            "path": str(shot_path),
            "rendered_dom_path": str(dom_path),
            "render_seconds": time.time() - start,
            "dom": rendered_dom,
            "console_errors": console_errors[:50],
            "request_failures": request_failures[:50],
        }

    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def analyze_screenshot(path: Optional[str]) -> Dict[str, Any]:
    out = {
        "available": False,
        "error": None,
        "size": None,
        "blank_like": None,
        "brightness_mean": None,
        "brightness_std": None,
    }

    if not path:
        return out

    try:
        from PIL import Image, ImageStat

        img = Image.open(path).convert("L")
        stat = ImageStat.Stat(img)
        mean = stat.mean[0]
        std = stat.stddev[0]

        out.update({
            "available": True,
            "size": img.size,
            "blank_like": std < 5,
            "brightness_mean": round(mean, 2),
            "brightness_std": round(std, 2),
        })
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"

    return out


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

    cat_scores = {}

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

    reachability = []
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
    flags = []

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
    parts = {}

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


def main() -> int:
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