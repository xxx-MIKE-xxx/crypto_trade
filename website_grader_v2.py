#!/usr/bin/env python3
"""
coin_website_grader_v3.py

Pipeline-grade website quality / developer-effort grader for freshly migrated
Solana meme coins.

Primary goal:
  Produce stable, analytics-friendly features that can be joined into a 6-24h
  post-migration survival model. The score is not a trading signal by itself;
  it is a developer-effort / website-quality feature source.

Default output:
  ./data/raw/analytics/<mint>/

Artifacts written per mint:
  - input.json                  normalized pipeline input
  - report.json                 full nested human/debug report
  - features.json               one flat ML feature row
  - features.jsonl              same row as one JSONL line
  - features.csv                same row as one CSV row
  - summary.md                  compact human-readable report
  - artifacts_index.json        paths + schema/version metadata
  - raw/page.html               fetched HTML
  - raw/page_text.txt           visible text
  - raw/headers.json            HTTP response headers
  - raw/rendered_dom_*.html     if Playwright is available
  - raw/screenshot_*.png        if Playwright is available

Install:
  pip install requests beautifulsoup4

Recommended optional deps:
  pip install pillow playwright
  python -m playwright install chromium

Optional APIs:
  export PAGESPEED_API_KEY=...
  export URLSCAN_API_KEY=...

Examples:
  python coin_website_grader_v3.py \
    --mint 9xQeWvG816bUx9EPjHmaT23yvVM2ZW1cRdxWhgn526S \
    --website https://examplecoin.xyz \
    --coin-name "Example Coin" \
    --symbol EXMP

  python coin_website_grader_v3.py \
    --mint <mint> \
    --website <url> \
    --metadata-json '{"name":"Coin","symbol":"COIN","twitter":"https://x.com/coin"}' \
    --stdout-json
"""

from __future__ import annotations

import argparse
import csv
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
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as exc:  # pragma: no cover - import-time operator error
    print("Missing dependencies. Install with: pip install requests beautifulsoup4", file=sys.stderr)
    raise exc

SCHEMA_VERSION = "website-effort-grader.v3.1.0"
USER_AGENT = (
    "Mozilla/5.0 (compatible; SolanaMemeCoinWebsiteGrader/3.0; "
    "+https://example.local/research-bot)"
)

DEFAULT_OUTPUT_ROOT = "./data/raw/analytics"

FREE_OR_LOW_EFFORT_HOSTS = {
    "linktr.ee", "carrd.co", "wixsite.com", "wordpress.com", "blogspot.com",
    "github.io", "netlify.app", "vercel.app", "pages.dev", "notion.site",
    "webflow.io", "sites.google.com", "bio.link", "beacons.ai", "taplink.cc",
    "solo.to", "about.me", "lnk.bio", "framer.website", "strikingly.com",
    "weebly.com", "glitch.me", "neocities.org", "godaddysites.com",
}

RISKY_TLDS = {
    "zip", "mov", "click", "country", "kim", "gq", "work", "quest",
    "top", "xyz", "icu", "cyou", "rest", "support", "surf", "monster",
    "buzz", "live", "sbs", "shop", "cam", "bond", "skin",
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
    "tiktok.com": "tiktok",
    "instagram.com": "instagram",
    "linktr.ee": "linktree",
    "dexscreener.com": "dexscreener",
    "dextools.io": "dextools",
    "geckoterminal.com": "geckoterminal",
    "birdeye.so": "birdeye",
    "coinmarketcap.com": "coinmarketcap",
    "coingecko.com": "coingecko",
}

MARKET_LINK_TYPES = {
    "dexscreener", "dextools", "geckoterminal", "birdeye", "coinmarketcap", "coingecko"
}
MAJOR_SOCIAL_TYPES = {"twitter", "telegram", "discord", "github"}

LINK_SHORTENERS = {
    "bit.ly", "tinyurl.com", "t.co", "cutt.ly", "is.gd", "buff.ly", "shorturl.at",
    "rebrand.ly", "ow.ly", "soo.gd", "lnkd.in", "s.id",
}

PARKED_OR_PLACEHOLDER_PATTERNS = [
    "domain is parked", "buy this domain", "this domain is for sale",
    "coming soon", "under construction", "lorem ipsum", "template",
    "sedo", "afternic", "dan.com", "parkingcrew", "namecheap parking",
    "insert text", "your text here", "untitled", "sample token", "example token",
    "replace this", "coming soon page", "this website is for sale",
]

HYPE_TERMS = [
    "100x", "1000x", "moon", "moonshot", "guaranteed", "risk-free", "no risk",
    "gem", "ape in", "send it", "lambo", "pump", "next shib", "next doge",
    "financial freedom", "life changing", "don't miss", "last chance",
    "free money", "presale bonus", "moon soon", "x100", "x1000",
]

CONTENT_POSITIVE_TERMS = [
    "tokenomics", "roadmap", "whitepaper", "docs", "documentation", "audit",
    "liquidity", "vesting", "renounced", "locked", "contract", "ca:",
    "community", "governance", "utility", "burn", "supply", "chain",
    "faq", "privacy", "terms", "about", "mission", "ecosystem", "staking",
    "partnership", "team", "bridge", "game", "telegram", "twitter", "x.com",
]

EFFORT_SECTION_TERMS = {
    "roadmap": ["roadmap", "phase 1", "phase 2", "milestone", "timeline"],
    "tokenomics": ["tokenomics", "supply", "liquidity", "burn", "allocation", "tax"],
    "docs": ["whitepaper", "docs", "documentation", "learn more", "gitbook"],
    "community": ["community", "telegram", "discord", "twitter", "x.com"],
    "risk_disclosure": ["terms", "privacy", "disclaimer", "not financial advice"],
    "product_utility": ["utility", "use case", "game", "bot", "app", "platform", "ecosystem"],
}

GENERIC_SITE_SECTION_TERMS = {
    "about_brand": ["about", "story", "mission", "vision", "company", "team"],
    "products_or_services": ["product", "products", "service", "services", "features", "solutions"],
    "support_or_contact": ["support", "help", "contact", "faq", "customer service"],
    "legal_or_privacy": ["privacy", "terms", "legal", "cookies", "policy"],
    "navigation_or_store": ["shop", "store", "buy", "learn more", "compare", "account"],
    "media_or_assets": ["gallery", "video", "image", "press", "news", "blog"],
}

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
    "airdrop claim",
]

EVM_CONTRACT_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
SOLANA_LIKE_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
PERCENT_RE = re.compile(r"\b\d+(?:\.\d+)?\s?%\b")
SUPPLY_RE = re.compile(r"\b\d+(?:[,.]\d{3})*(?:\.\d+)?\s?(?:k|m|b|bn|million|billion)?\b", re.I)


@dataclasses.dataclass(frozen=True)
class CoinInput:
    mint: str
    website_url: str
    coin_name: Optional[str] = None
    symbol: Optional[str] = None
    address: Optional[str] = None
    expected_x: Optional[str] = None
    expected_telegram: Optional[str] = None
    expected_discord: Optional[str] = None
    expected_website: Optional[str] = None
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class Finding:
    label: str
    value: Any
    impact: float = 0.0
    note: str = ""


@dataclasses.dataclass(frozen=True)
class SectionResult:
    name: str
    score: float
    findings: list[Finding]
    positives: list[str]
    negatives: list[str]
    raw: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class FetchResult:
    input_url: str
    final_url: str
    status_code: Optional[int]
    ok: bool
    elapsed_seconds: Optional[float]
    headers: dict[str, str]
    html: str
    error: Optional[str]
    redirects: list[dict[str, Any]]
    bytes_downloaded: int


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def safe_slug(value: str, fallback: str = "unknown") -> str:
    value = (value or "").strip()
    value = re.sub(r"[^a-zA-Z0-9_.=-]+", "_", value)
    value = value.strip("._-")
    return value[:128] or fallback


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        raise ValueError("website URL is empty")
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def hostname(url: str) -> str:
    return urlparse(url).hostname or ""


def root_domain(host: str) -> str:
    host = (host or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return host
    two_level_suffixes = {
        "co.uk", "org.uk", "ac.uk", "com.au", "net.au", "co.jp",
        "com.br", "com.tr", "co.in", "com.sg", "com.mx",
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


def count_terms(text: str, terms: Iterable[str]) -> dict[str, int]:
    lower = text.lower()
    return {term: lower.count(term.lower()) for term in terms if lower.count(term.lower()) > 0}


def normalized_similarity(a: Optional[str], b: Optional[str]) -> float:
    def clean(x: Optional[str]) -> str:
        return re.sub(r"[^a-z0-9]+", "", (x or "").lower())

    aa = clean(a)
    bb = clean(b)
    if not aa or not bb:
        return 0.0
    return round(SequenceMatcher(None, aa, bb).ratio(), 4)


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


def json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if dataclasses.is_dataclass(value):
        return json_safe(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    return str(value)


def load_json_arg(json_text: Optional[str], json_file: Optional[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if json_file:
        path = Path(json_file)
        payload.update(json.loads(path.read_text(encoding="utf-8")))
    if json_text:
        raw = json_text.strip()
        maybe_path = Path(raw)
        if maybe_path.exists() and maybe_path.is_file():
            payload.update(json.loads(maybe_path.read_text(encoding="utf-8")))
        else:
            payload.update(json.loads(raw))
    return payload


def coalesce(*values: Any) -> Optional[str]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def build_coin_input(args: argparse.Namespace) -> CoinInput:
    metadata = load_json_arg(args.metadata_json, args.metadata_file)

    mint = coalesce(args.mint, metadata.get("mint"), metadata.get("address"), metadata.get("contract"))
    website = coalesce(
        args.website,
        args.positional_website,
        metadata.get("website"),
        metadata.get("website_url"),
        metadata.get("url"),
    )
    if not mint:
        raise ValueError("--mint is required, or provide mint/address/contract in metadata JSON")
    if not website:
        raise ValueError("--website/--url is required, or provide website/website_url/url in metadata JSON")

    return CoinInput(
        mint=mint,
        website_url=normalize_url(website),
        coin_name=coalesce(args.coin_name, args.token_name, metadata.get("name"), metadata.get("coin_name")),
        symbol=coalesce(args.symbol, args.token_symbol, metadata.get("symbol"), metadata.get("ticker")),
        address=coalesce(args.address, args.contract, metadata.get("address"), metadata.get("contract"), mint),
        expected_x=coalesce(args.expected_x, metadata.get("twitter"), metadata.get("x"), metadata.get("expected_x")),
        expected_telegram=coalesce(args.expected_telegram, metadata.get("telegram"), metadata.get("expected_telegram")),
        expected_discord=coalesce(args.expected_discord, metadata.get("discord"), metadata.get("expected_discord")),
        expected_website=coalesce(args.expected_website, metadata.get("expected_website"), metadata.get("website")),
        metadata=metadata,
    )


def make_output_dirs(output_root: str, mint: str) -> tuple[Path, Path]:
    root = Path(output_root)
    mint_dir = root / safe_slug(mint)
    raw_dir = mint_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return mint_dir, raw_dir


def fetch_url(url: str, timeout: float = 12.0, max_bytes: int = 5_000_000) -> FetchResult:
    session = requests.Session()
    session.headers.update({
        "user-agent": USER_AGENT,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.8",
    })
    start = time.time()
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True, stream=True)
        chunks: list[bytes] = []
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            downloaded += len(chunk)
            if downloaded <= max_bytes:
                chunks.append(chunk)
            if downloaded > max_bytes:
                break
        content = b"".join(chunks)
        encoding = resp.encoding or resp.apparent_encoding or "utf-8"
        html = content.decode(encoding, errors="replace")
        elapsed = time.time() - start
        return FetchResult(
            input_url=url,
            final_url=resp.url,
            status_code=resp.status_code,
            ok=200 <= resp.status_code < 400,
            elapsed_seconds=elapsed,
            headers={k.lower(): v for k, v in resp.headers.items()},
            html=html,
            error="truncated_at_max_bytes" if downloaded > max_bytes else None,
            redirects=[{"status_code": r.status_code, "url": r.url} for r in resp.history],
            bytes_downloaded=downloaded,
        )
    except Exception as exc:
        elapsed = time.time() - start
        return FetchResult(
            input_url=url,
            final_url=url,
            status_code=None,
            ok=False,
            elapsed_seconds=elapsed,
            headers={},
            html="",
            error=f"{type(exc).__name__}: {exc}",
            redirects=[],
            bytes_downloaded=0,
        )


def check_dns(host: str) -> dict[str, Any]:
    try:
        records = socket.getaddrinfo(host, 443)
        ips = sorted({record[4][0] for record in records if record and len(record) >= 5})
        return {"ok": True, "ips": ips[:20], "ip_count": len(ips)}
    except Exception as exc:
        return {"ok": False, "ips": [], "ip_count": 0, "error": f"{type(exc).__name__}: {exc}"}


def check_ssl_cert(host: str, port: int = 443, timeout: float = 8.0) -> dict[str, Any]:
    out: dict[str, Any] = {
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
            not_after = dt.datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=dt.timezone.utc)
            days = (not_after - utc_now()).days
            out["days_to_expiry"] = days
            out["valid_now"] = days > 0
        san_hosts = [val.lower() for typ, val in cert.get("subjectAltName", []) if typ.lower() == "dns"]
        host_l = host.lower()
        out["san_matches_host"] = any(h == host_l or (h.startswith("*.") and host_l.endswith(h[1:])) for h in san_hosts)
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def fetch_rdap(domain: str, timeout: float = 10.0) -> dict[str, Any]:
    if not domain:
        return {"ok": False, "status_code": None, "url": None, "data": {}, "error": "empty domain"}
    url = f"https://rdap.org/domain/{domain}"
    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={"accept": "application/rdap+json, application/json", "user-agent": USER_AGENT},
        )
        if "json" in response.headers.get("content-type", ""):
            data: Any = response.json()
        else:
            data = {"raw": response.text[:2000]}
        return {"ok": response.status_code == 200, "status_code": response.status_code, "url": url, "data": data}
    except Exception as exc:
        return {"ok": False, "status_code": None, "url": url, "data": {}, "error": f"{type(exc).__name__}: {exc}"}


def extract_rdap_dates(rdap_data: dict[str, Any]) -> tuple[Optional[dt.datetime], Optional[dt.datetime], Optional[str]]:
    created = None
    expires = None
    registrar = None
    for event in rdap_data.get("events", []) or []:
        if not isinstance(event, dict):
            continue
        action = str(event.get("eventAction", "")).lower()
        parsed = parse_datetime(event.get("eventDate"))
        if "registration" in action or "created" in action:
            created = created or parsed
        if "expiration" in action or "expiry" in action:
            expires = expires or parsed
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


def all_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        links.append(urljoin(base_url, href))
    return sorted(set(links))


def canonical_social_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        host_l = parsed.netloc.lower().replace("www.", "")
        path = parsed.path.rstrip("/").lower()
        return f"{host_l}{path}"
    except Exception:
        return url.lower().strip().rstrip("/")


def extract_socials(links: Iterable[str]) -> dict[str, list[str]]:
    socials: dict[str, list[str]] = {}
    for link in links:
        host_l = hostname(link).lower().replace("www.", "")
        for domain, name in SOCIAL_DOMAINS.items():
            if host_l == domain or host_l.endswith("." + domain):
                socials.setdefault(name, []).append(link)
    return {k: sorted(set(v)) for k, v in socials.items()}


def sample_head(url: str, timeout: float = 8.0) -> tuple[Optional[int], Optional[str], Optional[str]]:
    try:
        response = requests.head(url, headers={"user-agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
        if response.status_code in {403, 405}:
            response = requests.get(url, headers={"user-agent": USER_AGENT}, timeout=timeout, allow_redirects=True, stream=True)
        return response.status_code, response.url, None
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"


def collect_dom_stats(soup: BeautifulSoup, final_url: str, text: str) -> dict[str, Any]:
    tags = soup.find_all(True)
    links = all_links(soup, final_url)
    imgs = soup.find_all("img")
    buttons = soup.find_all(["button", "input", "textarea", "select"])
    headings = {f"h{i}": len(soup.find_all(f"h{i}")) for i in range(1, 7)}
    heading_text = [h.get_text(" ", strip=True)[:200] for h in soup.find_all(re.compile(r"^h[1-6]$"))]
    img_alt_count = sum(1 for img in imgs if img.get("alt", "").strip())
    meta_tags = soup.find_all("meta")
    og_tags = {
        meta.get("property") or meta.get("name"): meta.get("content")
        for meta in meta_tags
        if meta.get("property") or meta.get("name")
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
    final_root = root_domain(hostname(final_url))
    internal_links: list[str] = []
    external_links: list[str] = []
    for link in links:
        link_host = hostname(link)
        if not link_host:
            continue
        if root_domain(link_host) == final_root:
            internal_links.append(link)
        else:
            external_links.append(link)
    meta_desc = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
    title = soup.find("title")
    scripts = soup.find_all("script")
    css_links = [l for l in soup.find_all("link") if "stylesheet" in " ".join(l.get("rel", [])).lower() or ".css" in str(l.get("href", "")).lower()]
    words = re.findall(r"\b[a-zA-Z]{3,}\b", text.lower())
    unique_words = len(set(words))
    word_count = len(words)
    repeated_word_ratio = 0.0
    if words:
        counts = Counter(words)
        repeated_word_ratio = sum(1 for _, count in counts.items() if count >= 8) / max(1, len(counts))
    text_l = text.lower()
    section_hits = {
        section: sorted({term for term in terms if term in text_l})
        for section, terms in EFFORT_SECTION_TERMS.items()
    }
    section_hits = {k: v for k, v in section_hits.items() if v}
    generic_section_hits = {
        section: sorted({term for term in terms if term in text_l})
        for section, terms in GENERIC_SITE_SECTION_TERMS.items()
    }
    generic_section_hits = {k: v for k, v in generic_section_hits.items() if v}
    return {
        "node_count": len(tags),
        "link_count": len(links),
        "internal_link_count": len(set(internal_links)),
        "external_link_count": len(set(external_links)),
        "image_count": len(imgs),
        "image_alt_ratio": (img_alt_count / len(imgs)) if imgs else None,
        "button_input_count": len(buttons),
        "form_count": len(soup.find_all("form")),
        "iframe_count": len(soup.find_all("iframe")),
        "canvas_count": len(soup.find_all("canvas")),
        "svg_count": len(soup.find_all("svg")),
        "video_count": len(soup.find_all("video")),
        "script_tag_count": len(scripts),
        "stylesheet_tag_count": len(css_links),
        "headings": headings,
        "heading_text": heading_text[:30],
        "has_viewport": soup.find("meta", attrs={"name": re.compile("^viewport$", re.I)}) is not None,
        "has_title": bool(title and title.get_text(strip=True)),
        "title": title.get_text(" ", strip=True) if title else "",
        "meta_description": meta_desc.get("content", "").strip() if meta_desc else "",
        "has_og_image": bool(og_tags.get("og:image")),
        "has_og_title": bool(og_tags.get("og:title")),
        "has_twitter_card": bool(og_tags.get("twitter:card")),
        "favicon": favicon,
        "text_length": len(text),
        "word_count": word_count,
        "unique_word_count": unique_words,
        "unique_word_ratio": round(unique_words / max(1, word_count), 4),
        "repeated_word_ratio": round(repeated_word_ratio, 4),
        "social_links": sorted(set(link for link in links if any(d in hostname(link).lower() for d in SOCIAL_DOMAINS))),
        "internal_links": sorted(set(internal_links))[:100],
        "external_links": sorted(set(external_links))[:200],
        "og_tags": og_tags,
        "section_hits": section_hits,
        "generic_section_hits": generic_section_hits,
        "percentage_count": len(PERCENT_RE.findall(text)),
        "numeric_count": len(SUPPLY_RE.findall(text)),
    }


def detect_tech_stack(headers: dict[str, str], soup: BeautifulSoup, final_url: str) -> dict[str, Any]:
    header_l = {k.lower(): v for k, v in headers.items()}
    scripts = [s.get("src", "") for s in soup.find_all("script") if s.get("src")]
    links = [l.get("href", "") for l in soup.find_all("link") if l.get("href")]
    html = str(soup)
    html_l = html.lower()
    urls = " ".join(scripts + links).lower()
    tech: list[str] = []
    signals: list[str] = []
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
        ("Next.js", "_next/" in urls or "__next_data__" in html_l or "next.js" in html_l),
        ("React", "react" in urls or "__next_data__" in html_l or "data-reactroot" in html_l),
        ("Vue", "vue" in urls or "__vue" in html_l),
        ("Nuxt", "_nuxt/" in urls),
        ("Svelte", "svelte" in urls),
        ("Angular", "ng-version" in html_l or "angular" in urls),
        ("Tailwind-like CSS", re.search(r"\b(?:text|bg|flex|grid|rounded|shadow|border|p|m|px|py)-[a-z0-9\-\[\]/]+", html_l) is not None),
        ("Bootstrap", "bootstrap" in urls),
        ("jQuery", "jquery" in urls),
        ("Webflow", "webflow" in urls or "webflow" in html_l),
        ("Wix", "wixstatic" in urls or "wix.com" in html_l),
        ("WordPress", "wp-content" in urls or "wordpress" in html_l),
        ("Framer", "framer" in urls or "data-framer" in html_l),
        ("GSAP", "gsap" in urls or "greensock" in urls),
        ("Three.js", "three" in urls and ".js" in urls),
        ("Lottie", "lottie" in urls or "bodymovin" in urls),
        ("Google Analytics/GTM", "googletagmanager" in urls or "google-analytics" in urls or "gtag(" in html_l),
        ("Meta Pixel", "connect.facebook.net" in urls or "fbq(" in html_l),
        ("WalletConnect", "walletconnect" in urls or "walletconnect" in html_l),
        ("Solana wallet adapter", "wallet-adapter" in html_l or "@solana" in html_l),
        ("Phantom wallet", "phantom" in html_l),
        ("ethers.js", "ethers.js" in urls or "ethers.min.js" in urls or "ethersproject" in html_l),
        ("web3.js", "web3.js" in urls or "web3.min.js" in urls),
        ("wagmi", "wagmi" in html_l),
        ("RainbowKit", "rainbowkit" in html_l),
    ]
    for name, ok in checks:
        if ok:
            tech.append(name)
    inline_script_chars = sum(len(s.get_text(" ", strip=True)) for s in soup.find_all("script") if not s.get("src"))
    external_hosts = sorted(set(urlparse(src).netloc.lower() for src in scripts if urlparse(src).netloc))
    return {
        "tech": sorted(set(tech)),
        "signals": signals,
        "script_count": len(scripts),
        "stylesheet_count": len([x for x in links if ".css" in x or "stylesheet" in x.lower()]),
        "inline_script_chars": inline_script_chars,
        "external_script_hosts": external_hosts[:75],
        "external_script_host_count": len(external_hosts),
        "source_maps_referenced": bool(re.search(r"\.map(?:\?|['\"<])", html, flags=re.I)),
        "minified_asset_hint": bool(re.search(r"\.(?:min|bundle|chunk)\.(?:js|css)", html_l)),
    }


def run_pagespeed(url: str, api_key: Optional[str], timeout: float = 60.0) -> dict[str, Any]:
    endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params: list[tuple[str, str]] = [
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
        response = requests.get(endpoint, params=params, timeout=timeout, headers={"user-agent": USER_AGENT})
        return {"ok": response.status_code == 200, "status_code": response.status_code, "data": response.json()}
    except Exception as exc:
        return {"ok": False, "status_code": None, "data": {}, "error": f"{type(exc).__name__}: {exc}"}


def run_urlscan(url: str, api_key: Optional[str]) -> dict[str, Any]:
    if not api_key:
        return {"ok": False, "skipped": True, "reason": "No URLSCAN_API_KEY"}
    try:
        response = requests.post(
            "https://urlscan.io/api/v1/scan/",
            headers={"API-Key": api_key, "Content-Type": "application/json", "User-Agent": USER_AGENT},
            json={"url": url, "visibility": "public"},
            timeout=20,
        )
        return {"ok": response.status_code in (200, 201), "status_code": response.status_code, "data": response.json()}
    except Exception as exc:
        return {"ok": False, "skipped": False, "error": f"{type(exc).__name__}: {exc}"}


def try_playwright_render(url: str, out_dir: Path, timeout_ms: int = 25_000) -> dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return {"ok": False, "skipped": True, "reason": f"Playwright not installed: {exc}"}
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(url.encode()).hexdigest()[:12]
        shot_path = out_dir / f"screenshot_{digest}.png"
        dom_path = out_dir / f"rendered_dom_{digest}.html"
        console_errors: list[str] = []
        console_warnings: list[str] = []
        request_failures: list[str] = []
        start = time.time()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1365, "height": 900}, user_agent=USER_AGENT)
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("console", lambda msg: console_warnings.append(msg.text) if msg.type == "warning" else None)
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
                        viewportHeight: window.innerHeight,
                        aboveFoldTextLength: document.body ? document.body.innerText.slice(0, 2000).length : 0
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
            "console_warnings": console_warnings[:50],
            "request_failures": request_failures[:50],
        }
    except Exception as exc:
        return {"ok": False, "skipped": False, "error": f"{type(exc).__name__}: {exc}"}


def analyze_screenshot(path: Optional[str]) -> dict[str, Any]:
    out: dict[str, Any] = {
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
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def extract_address_evidence(text: str, html: str, coin: CoinInput) -> dict[str, Any]:
    lower_text = text.lower()
    lower_html = html.lower()
    mint = coin.mint.strip()
    address = (coin.address or coin.mint).strip()
    evm_contracts = sorted(set(EVM_CONTRACT_RE.findall(html)))[:20]
    solana_like_visible = sorted(set(x for x in SOLANA_LIKE_RE.findall(text) if len(x) >= 32))[:30]
    exact_mint_in_text = bool(mint and mint.lower() in lower_text)
    exact_mint_in_html = bool(mint and mint.lower() in lower_html)
    exact_address_in_text = bool(address and address.lower() in lower_text)
    exact_address_in_html = bool(address and address.lower() in lower_html)
    return {
        "evm_contracts": evm_contracts,
        "solana_like_visible": solana_like_visible,
        "exact_mint_in_text": exact_mint_in_text,
        "exact_mint_in_html": exact_mint_in_html,
        "exact_address_in_text": exact_address_in_text,
        "exact_address_in_html": exact_address_in_html,
        "visible_solana_address_count": len(solana_like_visible),
        "any_address_detected": bool(evm_contracts or solana_like_visible or exact_mint_in_html or exact_address_in_html),
    }


def score_availability(fetch: FetchResult, dns: dict[str, Any]) -> SectionResult:
    score = 20.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    if dns.get("ok"):
        score += 20
        positives.append("DNS resolves.")
    else:
        score -= 25
        negatives.append(f"DNS resolution failed: {dns.get('error')}.")
    if fetch.ok:
        score += 35
        positives.append(f"Usable HTTP response: {fetch.status_code}.")
    else:
        score -= 35
        negatives.append(f"Fetch failed or unusable response: {fetch.error or fetch.status_code}.")
    if fetch.elapsed_seconds is not None:
        if fetch.elapsed_seconds < 1.0:
            score += 12
            positives.append(f"Fast initial fetch: {fetch.elapsed_seconds:.2f}s.")
        elif fetch.elapsed_seconds < 3.0:
            score += 7
        elif fetch.elapsed_seconds > 8.0:
            score -= 12
            negatives.append(f"Very slow initial fetch: {fetch.elapsed_seconds:.2f}s.")
        else:
            score -= 4
    if len(fetch.redirects) <= 1:
        score += 8
    elif len(fetch.redirects) <= 3:
        score += 2
        findings.append(Finding("redirect_count", len(fetch.redirects), 0))
    else:
        score -= min(15, len(fetch.redirects) * 3)
        negatives.append(f"Long redirect chain: {len(fetch.redirects)} redirects.")
    if fetch.bytes_downloaded == 0:
        score -= 8
    elif fetch.bytes_downloaded < 1_500_000:
        score += 5
    else:
        score -= 5
        negatives.append(f"Large initial HTML payload: {fetch.bytes_downloaded} bytes.")
    findings.append(Finding("dns", dns, 0))
    return SectionResult("availability", clamp(score), findings, positives, negatives, {
        "status_code": fetch.status_code,
        "ok": fetch.ok,
        "elapsed_seconds": fetch.elapsed_seconds,
        "redirect_count": len(fetch.redirects),
        "bytes_downloaded": fetch.bytes_downloaded,
        "fetch_error": fetch.error,
        "dns": dns,
    })


def score_domain_hosting(coin: CoinInput, fetch: FetchResult, rdap: dict[str, Any], ssl_info: dict[str, Any]) -> SectionResult:
    score = 35.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    final_url = fetch.final_url or coin.website_url
    host = hostname(final_url) or hostname(coin.website_url)
    rd = root_domain(host)
    token = domain_token(host)
    suffix = suffix_of(host)
    is_free_host = any(rd == h or rd.endswith("." + h) for h in FREE_OR_LOW_EFFORT_HOSTS)
    if is_free_host:
        score -= 22
        negatives.append(f"Uses low-effort/free hosting or link-hub domain: {rd}.")
    else:
        score += 14
        positives.append(f"Custom/root domain detected: {rd}.")
    if suffix in RISKY_TLDS:
        score -= 7
        negatives.append(f"TLD .{suffix} is common in short-lived speculative launches.")
    else:
        score += 4
    entropy = shannon_entropy(token)
    if len(token) >= 12 and entropy > 3.6:
        score -= 8
        negatives.append(f"Domain token looks random/high-entropy: {token}.")
    else:
        score += 3
    if urlparse(final_url).scheme == "https":
        score += 12
        positives.append("Final URL uses HTTPS.")
    else:
        score -= 25
        negatives.append("Final URL is not HTTPS.")
    if ssl_info.get("ok") and ssl_info.get("valid_now"):
        score += 12
        positives.append("TLS certificate is valid.")
    else:
        score -= 12
        negatives.append(f"TLS check failed/invalid: {ssl_info.get('error')}.")
    if ssl_info.get("san_matches_host"):
        score += 5
    else:
        score -= 3
    days_to_expiry = ssl_info.get("days_to_expiry")
    if days_to_expiry is not None and days_to_expiry < 30:
        score -= 8
        negatives.append("TLS certificate expires soon.")
    created = None
    expires = None
    registrar = None
    domain_age_days = None
    domain_expiry_days = None
    if rdap.get("ok"):
        created, expires, registrar = extract_rdap_dates(rdap.get("data", {}))
        if created:
            domain_age_days = max(0, (utc_now() - created).days)
            if domain_age_days >= 365:
                score += 14
            elif domain_age_days >= 90:
                score += 8
            elif domain_age_days >= 14:
                score += 1
                negatives.append("Domain is recent.")
            else:
                score -= 10
                negatives.append("Domain is extremely new.")
        else:
            score -= 2
        if expires:
            domain_expiry_days = (expires - utc_now()).days
            if domain_expiry_days >= 180:
                score += 6
            elif domain_expiry_days < 30:
                score -= 8
        if registrar:
            score += 2
    else:
        findings.append(Finding("rdap", rdap.get("status_code") or rdap.get("error"), 0))
    input_rd = root_domain(hostname(fetch.input_url))
    final_rd = root_domain(hostname(fetch.final_url))
    if input_rd and final_rd and input_rd != final_rd:
        score -= 8
        negatives.append(f"Cross-domain redirect: {input_rd} -> {final_rd}.")
    raw = {
        "host": host,
        "root_domain": rd,
        "domain_token": token,
        "suffix": suffix,
        "is_free_or_low_effort_host": is_free_host,
        "domain_entropy": round(entropy, 4),
        "rdap_ok": rdap.get("ok"),
        "registrar": registrar,
        "domain_created_at": created.isoformat() if created else None,
        "domain_expires_at": expires.isoformat() if expires else None,
        "domain_age_days": domain_age_days,
        "domain_expiry_days": domain_expiry_days,
        "ssl": ssl_info,
    }
    return SectionResult("domain_hosting", clamp(score), findings, positives, negatives, raw)


def score_identity_consistency(coin: CoinInput, fetch: FetchResult, dom: dict[str, Any], text: str, html: str) -> SectionResult:
    score = 25.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    final_url = fetch.final_url or coin.website_url
    host_token = domain_token(hostname(final_url))
    lower_text = text.lower()
    lower_html = html.lower()
    title = dom.get("title", "")
    desc = dom.get("meta_description", "")
    h_text = " ".join(dom.get("heading_text", []))
    name_sim = normalized_similarity(coin.coin_name, host_token)
    symbol_sim = normalized_similarity(coin.symbol, host_token)
    best_brand_sim = max(name_sim, symbol_sim)
    if best_brand_sim >= 0.55:
        score += 14
        positives.append("Domain appears brand-consistent with coin name/symbol.")
    elif coin.coin_name or coin.symbol:
        score -= 6
        negatives.append("Domain does not look brand-consistent with coin name/symbol.")
    if coin.coin_name:
        name_l = coin.coin_name.lower()
        if name_l in lower_text or name_l in title.lower() or name_l in desc.lower():
            score += 14
            positives.append("Coin name appears in website content/metadata.")
        else:
            score -= 8
            negatives.append("Coin name not found in visible content/metadata.")
    if coin.symbol:
        symbol_l = coin.symbol.lower().strip("$")
        symbol_pattern = rf"(?<![a-z0-9])\$?{re.escape(symbol_l)}(?![a-z0-9])"
        if re.search(symbol_pattern, lower_text) or re.search(symbol_pattern, title.lower()) or re.search(symbol_pattern, h_text.lower()):
            score += 10
            positives.append("Ticker/symbol appears on the website.")
        else:
            score -= 5
            negatives.append("Ticker/symbol not found on the website.")
    address_evidence = extract_address_evidence(text, html, coin)
    if address_evidence["exact_mint_in_text"]:
        score += 16
        positives.append("Exact mint/address is visible on page.")
    elif address_evidence["exact_mint_in_html"] or address_evidence["exact_address_in_html"]:
        score += 8
        positives.append("Exact mint/address appears in HTML.")
    elif address_evidence["any_address_detected"]:
        score += 3
        findings.append(Finding("address_detected_not_exact", address_evidence, 3))
    else:
        score -= 10
        negatives.append("No mint/contract/address evidence found.")
    if dom.get("has_og_title") and dom.get("has_og_image"):
        score += 8
    elif dom.get("has_og_title") or dom.get("has_og_image"):
        score += 3
    else:
        score -= 3
    socials = extract_socials(dom.get("social_links", []))
    handles: list[str] = []
    for link in dom.get("social_links", []):
        parsed = urlparse(link)
        if any(domain in parsed.netloc for domain in ["x.com", "twitter.com", "t.me", "discord.gg"]):
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                handles.append(parts[0].lower().replace("@", ""))
    handle_sim = max([normalized_similarity(coin.coin_name, h) for h in handles] + [normalized_similarity(coin.symbol, h) for h in handles] + [0.0])
    if handles and handle_sim >= 0.55:
        score += 8
        positives.append("At least one social handle appears brand-consistent.")
    elif handles:
        score -= 3
    if coin.expected_website:
        expected_rd = root_domain(hostname(normalize_url(coin.expected_website)))
        actual_rd = root_domain(hostname(final_url))
        if expected_rd == actual_rd:
            score += 8
        else:
            score -= 10
            negatives.append(f"Expected website domain mismatch: expected {expected_rd}, got {actual_rd}.")
    raw = {
        "domain_token": host_token,
        "coin_name_domain_similarity": name_sim,
        "symbol_domain_similarity": symbol_sim,
        "best_brand_domain_similarity": best_brand_sim,
        "address_evidence": address_evidence,
        "socials_by_type": socials,
        "social_handles": sorted(set(handles)),
        "best_social_handle_similarity": handle_sim,
        "title": title,
        "meta_description": desc,
    }
    return SectionResult("identity_consistency", clamp(score), findings, positives, negatives, raw)



def score_generic_identity(coin: CoinInput, fetch: FetchResult, dom: dict[str, Any], text: str) -> SectionResult:
    """Brand/site identity quality without crypto-specific mint/social requirements."""
    score = 30.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    final_url = fetch.final_url or coin.website_url
    host_token = domain_token(hostname(final_url))
    title = dom.get("title", "") or ""
    desc = dom.get("meta_description", "") or ""
    heading_text = " ".join(dom.get("heading_text", []) or [])
    visible = " ".join([title, desc, heading_text, text[:4000]]).lower()

    name_sim = normalized_similarity(coin.coin_name, host_token)
    symbol_sim = normalized_similarity(coin.symbol, host_token)
    title_host_sim = normalized_similarity(title, host_token)
    desc_host_sim = normalized_similarity(desc, host_token)
    best_declared_brand_sim = max(name_sim, symbol_sim)
    best_page_host_sim = max(title_host_sim, desc_host_sim)

    if coin.coin_name or coin.symbol:
        if best_declared_brand_sim >= 0.60:
            score += 16
            positives.append("Domain token is consistent with supplied name/symbol.")
        elif best_declared_brand_sim >= 0.35:
            score += 6
        else:
            score -= 10
            negatives.append("Domain token is weakly related to supplied name/symbol.")
    elif best_page_host_sim >= 0.45:
        score += 10

    if coin.coin_name and coin.coin_name.lower() in visible:
        score += 12
        positives.append("Supplied name appears in page title, metadata, headings, or visible content.")
    elif coin.coin_name:
        score -= 8
        negatives.append("Supplied name not found in title/metadata/headings/visible content.")

    if coin.symbol:
        symbol_l = coin.symbol.lower().strip("$")
        symbol_pattern = rf"(?<![a-z0-9])\$?{re.escape(symbol_l)}(?![a-z0-9])"
        if re.search(symbol_pattern, visible):
            score += 8
            positives.append("Supplied ticker/symbol appears in generic page identity signals.")
        else:
            score -= 4
            negatives.append("Supplied ticker/symbol not found in generic page identity signals.")

    if title:
        score += 8
    else:
        score -= 8
        negatives.append("Missing HTML title.")
    if desc:
        score += 6
    else:
        score -= 5
        negatives.append("Missing meta description.")
    if dom.get("has_og_title"):
        score += 5
    if dom.get("has_og_image"):
        score += 7
    if dom.get("has_twitter_card"):
        score += 3
    if dom.get("favicon"):
        score += 5
    h1_count = int((dom.get("headings") or {}).get("h1", 0) or 0)
    if h1_count >= 1:
        score += 4
    else:
        score -= 3
        negatives.append("No H1 heading found.")

    raw = {
        "domain_token": host_token,
        "coin_name_domain_similarity": name_sim,
        "symbol_domain_similarity": symbol_sim,
        "title_domain_similarity": title_host_sim,
        "description_domain_similarity": desc_host_sim,
        "best_declared_brand_domain_similarity": best_declared_brand_sim,
        "best_page_host_similarity": best_page_host_sim,
        "title": title,
        "meta_description": desc,
        "h1_count": h1_count,
        "has_og_title": bool(dom.get("has_og_title")),
        "has_og_image": bool(dom.get("has_og_image")),
        "has_twitter_card": bool(dom.get("has_twitter_card")),
        "has_favicon": bool(dom.get("favicon")),
    }
    return SectionResult("generic_identity", clamp(score), findings, positives, negatives, raw)


def score_crypto_identity(coin: CoinInput, fetch: FetchResult, dom: dict[str, Any], text: str, html: str) -> SectionResult:
    """Crypto-specific identity fit: mint/address evidence, ticker evidence, and coin/social consistency."""
    score = 20.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    final_url = fetch.final_url or coin.website_url
    host_token = domain_token(hostname(final_url))
    lower_text = text.lower()
    lower_html = html.lower()
    title = dom.get("title", "") or ""
    desc = dom.get("meta_description", "") or ""
    h_text = " ".join(dom.get("heading_text", []) or [])

    name_sim = normalized_similarity(coin.coin_name, host_token)
    symbol_sim = normalized_similarity(coin.symbol, host_token)
    best_brand_sim = max(name_sim, symbol_sim)
    if best_brand_sim >= 0.55:
        score += 10
    elif coin.coin_name or coin.symbol:
        score -= 5
        negatives.append("Website domain is weakly related to coin name/symbol.")

    if coin.coin_name:
        name_l = coin.coin_name.lower()
        if name_l in lower_text or name_l in title.lower() or name_l in desc.lower():
            score += 10
        else:
            score -= 8
            negatives.append("Coin name not found on the website.")
    if coin.symbol:
        symbol_l = coin.symbol.lower().strip("$")
        symbol_pattern = rf"(?<![a-z0-9])\$?{re.escape(symbol_l)}(?![a-z0-9])"
        if re.search(symbol_pattern, lower_text) or re.search(symbol_pattern, title.lower()) or re.search(symbol_pattern, h_text.lower()):
            score += 8
        else:
            score -= 6
            negatives.append("Ticker/symbol not found on the website.")

    address_evidence = extract_address_evidence(text, html, coin)
    if address_evidence["exact_mint_in_text"]:
        score += 24
        positives.append("Exact mint/address is visible to users.")
    elif address_evidence["exact_mint_in_html"] or address_evidence["exact_address_in_html"]:
        score += 12
        positives.append("Exact mint/address appears in HTML.")
    elif address_evidence["any_address_detected"]:
        score += 5
        findings.append(Finding("address_detected_not_exact", address_evidence, 5))
    else:
        score -= 18
        negatives.append("No mint/contract/address evidence found.")

    socials = extract_socials(dom.get("social_links", []) or [])
    handles: list[str] = []
    for link in dom.get("social_links", []) or []:
        parsed = urlparse(link)
        if any(domain in parsed.netloc for domain in ["x.com", "twitter.com", "t.me", "discord.gg"]):
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                handles.append(parts[0].lower().replace("@", ""))
    handle_sim = max(
        [normalized_similarity(coin.coin_name, h) for h in handles]
        + [normalized_similarity(coin.symbol, h) for h in handles]
        + [0.0]
    )
    if handles and handle_sim >= 0.55:
        score += 10
        positives.append("Social handle appears consistent with coin name/symbol.")
    elif handles:
        score -= 3

    if coin.expected_website:
        expected_rd = root_domain(hostname(normalize_url(coin.expected_website)))
        actual_rd = root_domain(hostname(final_url))
        if expected_rd == actual_rd:
            score += 8
        else:
            score -= 10
            negatives.append(f"Expected website domain mismatch: expected {expected_rd}, got {actual_rd}.")

    raw = {
        "domain_token": host_token,
        "coin_name_domain_similarity": name_sim,
        "symbol_domain_similarity": symbol_sim,
        "best_brand_domain_similarity": best_brand_sim,
        "address_evidence": address_evidence,
        "socials_by_type": socials,
        "social_handles": sorted(set(handles)),
        "best_social_handle_similarity": handle_sim,
        "exact_mint_visible_text": address_evidence["exact_mint_in_text"],
        "exact_mint_present_html": address_evidence["exact_mint_in_html"],
    }
    return SectionResult("crypto_identity", clamp(score), findings, positives, negatives, raw)


def score_generic_content_depth(dom: dict[str, Any], text: str) -> SectionResult:
    """Generic site content depth, independent of crypto-specific terms."""
    score = 20.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    lower_text = text.lower()
    word_count = int(dom.get("word_count", 0) or 0)
    unique_word_ratio = float(dom.get("unique_word_ratio", 0.0) or 0.0)
    repeated_word_ratio = float(dom.get("repeated_word_ratio", 0.0) or 0.0)
    link_count = int(dom.get("link_count", 0) or 0)
    internal_link_count = int(dom.get("internal_link_count", 0) or 0)
    image_count = int(dom.get("image_count", 0) or 0)
    generic_section_hits = dom.get("generic_section_hits", {}) or {}
    heading_count = sum((dom.get("headings") or {}).values())

    if word_count >= 800:
        score += 24
        positives.append(f"Substantial visible copy: {word_count} words.")
    elif word_count >= 300:
        score += 16
    elif word_count >= 100:
        score += 6
        negatives.append(f"Thin visible copy: {word_count} words.")
    else:
        score -= 18
        negatives.append(f"Very thin visible copy: {word_count} words.")

    generic_section_count = len(generic_section_hits)
    if generic_section_count >= 5:
        score += 16
        positives.append("Multiple generic site sections detected.")
    elif generic_section_count >= 3:
        score += 10
    elif generic_section_count >= 1:
        score += 4
    else:
        score -= 6
        negatives.append("Few generic content/navigation sections detected.")

    if heading_count >= 5:
        score += 8
    elif heading_count >= 2:
        score += 4
    else:
        score -= 5
        negatives.append("Weak heading structure.")

    if link_count >= 20 and internal_link_count >= 10:
        score += 10
    elif link_count >= 6:
        score += 5
    elif link_count <= 2:
        score -= 5
        negatives.append("Very few links/navigation elements.")

    if image_count >= 5:
        score += 7
    elif image_count >= 1:
        score += 3
    else:
        score -= 3

    if unique_word_ratio < 0.25 and word_count > 100:
        score -= 8
        negatives.append("Low unique-word ratio; copy may be repetitive/template-like.")
    elif unique_word_ratio > 0.45 and word_count > 100:
        score += 5
    if repeated_word_ratio > 0.08:
        score -= 6
        negatives.append("Visible text is unusually repetitive.")

    placeholder_hits = [p for p in PARKED_OR_PLACEHOLDER_PATTERNS if p in lower_text]
    if placeholder_hits:
        penalty = min(35, len(placeholder_hits) * 10)
        score -= penalty
        negatives.append(f"Placeholder/parked patterns detected: {placeholder_hits}.")

    if dom.get("has_title"):
        score += 4
    else:
        score -= 4
    if dom.get("meta_description"):
        score += 4
    else:
        score -= 3
    if any(term in lower_text for term in ["privacy", "terms", "legal", "contact", "support"]):
        score += 4

    raw = {
        "word_count": word_count,
        "unique_word_count": dom.get("unique_word_count"),
        "unique_word_ratio": unique_word_ratio,
        "repeated_word_ratio": repeated_word_ratio,
        "generic_section_hits": generic_section_hits,
        "generic_section_count": generic_section_count,
        "heading_count": heading_count,
        "link_count": link_count,
        "internal_link_count": internal_link_count,
        "image_count": image_count,
        "placeholder_hits": placeholder_hits,
    }
    return SectionResult("generic_content_depth", clamp(score), findings, positives, negatives, raw)


def score_crypto_content_completeness(coin: CoinInput, dom: dict[str, Any], text: str, html: str) -> SectionResult:
    """Crypto-specific content completeness: tokenomics, roadmap, utility, mint, and launch disclosures."""
    score = 18.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    lower_text = text.lower()
    section_hits = dom.get("section_hits", {}) or {}
    section_count = len(section_hits)
    useful_terms = [term for term in CONTENT_POSITIVE_TERMS if term in lower_text]

    if section_count >= 5:
        score += 24
        positives.append("Multiple crypto project sections detected.")
    elif section_count >= 3:
        score += 14
    elif section_count >= 1:
        score += 5
    else:
        score -= 12
        negatives.append("No obvious tokenomics/roadmap/docs/community/utility sections detected.")

    if len(useful_terms) >= 8:
        score += 16
    elif len(useful_terms) >= 4:
        score += 9
    elif useful_terms:
        score += 3
    else:
        score -= 8
        negatives.append("Few crypto/project-completeness terms detected.")

    tokenomics_numeric_detail = bool(
        dom.get("percentage_count", 0) >= 3
        and any(term in lower_text for term in ["tokenomics", "supply", "allocation", "liquidity"])
    )
    if tokenomics_numeric_detail:
        score += 12
        positives.append("Tokenomics appears to include numeric detail.")
    elif any(term in lower_text for term in ["tokenomics", "supply", "allocation", "liquidity"]):
        score += 4
    else:
        score -= 5

    address_evidence = extract_address_evidence(text, html, coin)
    if address_evidence["exact_mint_in_text"]:
        score += 12
    elif address_evidence["exact_mint_in_html"] or address_evidence["any_address_detected"]:
        score += 5
    else:
        score -= 8
        negatives.append("No mint/address evidence in crypto content.")

    hype_hits = count_terms(lower_text, HYPE_TERMS)
    if hype_hits:
        hype_count = sum(hype_hits.values())
        penalty = min(18, hype_count * 2)
        score -= penalty
        negatives.append(f"Hype-heavy language detected: {list(hype_hits.keys())[:8]}.")
        findings.append(Finding("hype_terms", hype_hits, -penalty))

    if any(term in lower_text for term in ["privacy", "terms", "disclaimer", "not financial advice"]):
        score += 4
    else:
        score -= 2

    raw = {
        "section_hits": section_hits,
        "section_count": section_count,
        "useful_terms": useful_terms,
        "useful_term_count": len(useful_terms),
        "tokenomics_numeric_detail": tokenomics_numeric_detail,
        "percentage_count": dom.get("percentage_count"),
        "numeric_count": dom.get("numeric_count"),
        "hype_hits": hype_hits,
        "address_evidence": address_evidence,
    }
    return SectionResult("crypto_content_completeness", clamp(score), findings, positives, negatives, raw)

def score_content_depth(coin: CoinInput, dom: dict[str, Any], text: str, html: str) -> SectionResult:
    score = 20.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    lower_text = text.lower()
    word_count = int(dom.get("word_count", 0) or 0)
    unique_word_ratio = float(dom.get("unique_word_ratio", 0.0) or 0.0)
    repeated_word_ratio = float(dom.get("repeated_word_ratio", 0.0) or 0.0)
    if word_count >= 900:
        score += 22
        positives.append(f"Substantial visible copy: {word_count} words.")
    elif word_count >= 350:
        score += 15
        positives.append(f"Moderate visible copy: {word_count} words.")
    elif word_count >= 120:
        score += 5
        negatives.append(f"Thin copy: {word_count} words.")
    else:
        score -= 18
        negatives.append(f"Very thin copy: {word_count} words.")
    section_hits = dom.get("section_hits", {}) or {}
    section_count = len(section_hits)
    if section_count >= 5:
        score += 18
        positives.append("Multiple effort sections detected: roadmap/tokenomics/docs/community/etc.")
    elif section_count >= 3:
        score += 10
    elif section_count >= 1:
        score += 3
    else:
        score -= 8
        negatives.append("No obvious roadmap/tokenomics/docs/community effort sections detected.")
    useful_terms = [term for term in CONTENT_POSITIVE_TERMS if term in lower_text]
    if len(useful_terms) >= 8:
        score += 13
    elif len(useful_terms) >= 4:
        score += 8
    elif useful_terms:
        score += 3
    else:
        score -= 6
    if dom.get("percentage_count", 0) >= 3 and any(term in lower_text for term in ["tokenomics", "supply", "allocation", "liquidity"]):
        score += 8
        positives.append("Tokenomics appears to include numeric detail.")
    if dom.get("numeric_count", 0) >= 10:
        score += 3
    if unique_word_ratio < 0.25 and word_count > 100:
        score -= 8
        negatives.append("Low unique-word ratio; copy may be repetitive/template-like.")
    elif unique_word_ratio > 0.45 and word_count > 100:
        score += 4
    if repeated_word_ratio > 0.08:
        score -= 6
        negatives.append("Visible text is unusually repetitive.")
    placeholder_hits = [p for p in PARKED_OR_PLACEHOLDER_PATTERNS if p in lower_text]
    if placeholder_hits:
        penalty = min(35, len(placeholder_hits) * 10)
        score -= penalty
        negatives.append(f"Placeholder/parked patterns detected: {placeholder_hits}.")
    hype_hits = count_terms(lower_text, HYPE_TERMS)
    if hype_hits:
        hype_count = sum(hype_hits.values())
        penalty = min(18, hype_count * 2)
        score -= penalty
        negatives.append(f"Hype-heavy language detected: {list(hype_hits.keys())[:8]}.")
        findings.append(Finding("hype_terms", hype_hits, -penalty))
    has_privacy_terms = any(term in lower_text for term in ["privacy", "terms", "disclaimer"])
    if has_privacy_terms:
        score += 4
    if dom.get("has_title"):
        score += 4
    else:
        score -= 4
    if dom.get("meta_description"):
        score += 4
    else:
        score -= 3
    raw = {
        "word_count": word_count,
        "unique_word_count": dom.get("unique_word_count"),
        "unique_word_ratio": unique_word_ratio,
        "repeated_word_ratio": repeated_word_ratio,
        "section_hits": section_hits,
        "section_count": section_count,
        "useful_terms": useful_terms,
        "placeholder_hits": placeholder_hits,
        "hype_hits": hype_hits,
        "percentage_count": dom.get("percentage_count"),
        "numeric_count": dom.get("numeric_count"),
        "has_privacy_or_terms": has_privacy_terms,
    }
    return SectionResult("content_depth", clamp(score), findings, positives, negatives, raw)


def score_build_effort(fetch: FetchResult, dom: dict[str, Any], tech: dict[str, Any], screenshot: dict[str, Any]) -> SectionResult:
    score = 25.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    stack = tech.get("tech", [])
    node_count = int(dom.get("node_count", 0) or 0)
    img_count = int(dom.get("image_count", 0) or 0)
    links = int(dom.get("link_count", 0) or 0)
    if stack:
        score += min(18, len(stack) * 3)
        positives.append(f"Detected tech stack: {', '.join(stack[:10])}.")
    else:
        score -= 4
    if any(x in stack for x in ["Next.js", "React", "Vue", "Nuxt", "Svelte", "Angular", "Framer", "Webflow"]):
        score += 10
    if any(x in stack for x in ["Cloudflare/CDN", "Vercel", "Netlify"]):
        score += 7
    if any(x in stack for x in ["Wix", "WordPress"]):
        score -= 3
        negatives.append("Template/CMS signal detected; not fatal, but common in rushed sites.")
    if 120 <= node_count <= 5_000:
        score += 10
    elif node_count < 80:
        score -= 8
        negatives.append(f"Very small DOM: {node_count} nodes.")
    else:
        score += 3
        negatives.append(f"Very large DOM: {node_count} nodes.")
    if img_count >= 6:
        score += 8
    elif img_count >= 1:
        score += 4
    else:
        score -= 4
        negatives.append("No images detected.")
    if dom.get("favicon"):
        score += 4
    if dom.get("has_og_image"):
        score += 5
    if dom.get("has_twitter_card"):
        score += 3
    if dom.get("svg_count", 0) >= 3 or dom.get("canvas_count", 0) >= 1 or dom.get("video_count", 0) >= 1:
        score += 4
    if links >= 6:
        score += 5
    elif links <= 2:
        score -= 4
    script_count = int(tech.get("script_count", 0) or 0)
    if 1 <= script_count <= 35:
        score += 5
    elif script_count > 80:
        score -= 8
        negatives.append(f"Very high external script count: {script_count}.")
    inline_chars = int(tech.get("inline_script_chars", 0) or 0)
    if inline_chars > 500_000:
        score -= 8
        negatives.append("Very large inline JavaScript volume.")
    elif 1_000 <= inline_chars <= 200_000:
        score += 2
    if tech.get("source_maps_referenced"):
        score -= 4
        negatives.append("Public source map references detected.")
    if screenshot.get("ok"):
        score += 6
        rendered = screenshot.get("dom") or {}
        rendered_words = rendered.get("bodyWordCount")
        if rendered_words is not None and rendered_words >= max(80, int(dom.get("word_count", 0) * 0.5)):
            score += 4
        if screenshot.get("console_errors"):
            penalty = min(10, len(screenshot["console_errors"]) * 2)
            score -= penalty
            findings.append(Finding("console_errors", screenshot["console_errors"][:10], -penalty))
    else:
        findings.append(Finding("render_check", screenshot.get("reason") or screenshot.get("error") or "not available", 0))
    raw = {
        "tech": tech,
        "node_count": node_count,
        "image_count": img_count,
        "link_count": links,
        "favicon_present": bool(dom.get("favicon")),
        "og_image_present": bool(dom.get("has_og_image")),
        "twitter_card_present": bool(dom.get("has_twitter_card")),
        "render_ok": screenshot.get("ok", False),
    }
    return SectionResult("build_effort", clamp(score), findings, positives, negatives, raw)


def score_social_market(coin: CoinInput, dom: dict[str, Any], final_url: str, check_social_links: bool) -> SectionResult:
    score = 25.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    links = dom.get("social_links", []) or []
    socials = extract_socials(links)
    major = {k: v for k, v in socials.items() if k in MAJOR_SOCIAL_TYPES}
    market = {k: v for k, v in socials.items() if k in MARKET_LINK_TYPES}
    if len(major) >= 3:
        score += 20
        positives.append("Several major social/community links found.")
    elif len(major) == 2:
        score += 14
    elif len(major) == 1:
        score += 6
    else:
        score -= 16
        negatives.append("No X/Twitter, Telegram, Discord, or GitHub links found.")
    if market:
        score += min(14, 5 + len(market) * 3)
        positives.append("Market/data links found.")
    else:
        score -= 4
    found_set = {canonical_social_url(x) for x in links}
    expected = {
        "twitter": coin.expected_x,
        "telegram": coin.expected_telegram,
        "discord": coin.expected_discord,
    }
    for label, expected_url in expected.items():
        if not expected_url:
            continue
        canonical = canonical_social_url(expected_url)
        if canonical in found_set:
            score += 8
        else:
            score -= 7
            negatives.append(f"Expected {label} link not found.")
    shorteners = []
    for link in links + (dom.get("external_links", []) or []):
        host_l = hostname(link).lower().replace("www.", "")
        if host_l in LINK_SHORTENERS:
            shorteners.append(link)
    if shorteners:
        penalty = min(16, len(shorteners) * 5)
        score -= penalty
        negatives.append("Link shorteners detected.")
        findings.append(Finding("shorteners", shorteners[:10], -penalty))
    else:
        score += 3
    x_handles: list[str] = []
    for link in links:
        parsed = urlparse(link)
        if "x.com" in parsed.netloc or "twitter.com" in parsed.netloc:
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                x_handles.append(parts[0].lower())
    if len(set(x_handles)) > 1:
        score -= 10
        negatives.append(f"Multiple X/Twitter handles found: {sorted(set(x_handles))}.")
    reachability = []
    if check_social_links:
        for link in sorted(set(links))[:10]:
            status, resolved_url, error = sample_head(link, timeout=6.0)
            reachability.append({"url": link, "status": status, "resolved_url": resolved_url, "error": error})
        ok_count = sum(1 for item in reachability if item["status"] and item["status"] < 500)
        score += min(6, ok_count)
        bad_count = sum(1 for item in reachability if not item["status"] or item["status"] >= 500)
        if bad_count >= 3:
            score -= 5
    raw = {
        "found_social_links": links,
        "socials_by_type": socials,
        "major_social_count": len(major),
        "market_link_count": len(market),
        "x_handles": sorted(set(x_handles)),
        "shortener_count": len(shorteners),
        "reachability": reachability,
    }
    return SectionResult("social_market_wiring", clamp(score), findings, positives, negatives, raw)


def score_safety_risk(fetch: FetchResult, soup: BeautifulSoup, text: str, dom: dict[str, Any], urlscan: dict[str, Any]) -> SectionResult:
    score = 100.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    html = fetch.html or ""
    lower_text = text.lower()
    lower_html = html.lower()
    flags: list[str] = []
    if not fetch.ok:
        score -= 40
        flags.append(f"Fetch failed/non-usable response: {fetch.error or fetch.status_code}.")
    if fetch.status_code and int(fetch.status_code) >= 400:
        score -= 25
        flags.append(f"Bad HTTP status: {fetch.status_code}.")
    placeholder_hits = [p for p in PARKED_OR_PLACEHOLDER_PATTERNS if p in lower_text]
    if placeholder_hits:
        penalty = min(40, len(placeholder_hits) * 10)
        score -= penalty
        flags.append(f"Parked/placeholder text: {placeholder_hits}.")
    hype_hits = count_terms(lower_text, HYPE_TERMS)
    if hype_hits:
        penalty = min(24, sum(hype_hits.values()) * 3)
        score -= penalty
        flags.append(f"Hype/scam-risk words: {list(hype_hits.keys())[:10]}.")
        findings.append(Finding("hype_terms", hype_hits, -penalty))
    dangerous_hits = count_terms(lower_html + " " + lower_text, DANGEROUS_WEB3_TERMS)
    if dangerous_hits:
        penalty = min(60, sum(dangerous_hits.values()) * 8)
        score -= penalty
        flags.append(f"Dangerous wallet/Web3 terms detected: {list(dangerous_hits.keys())[:10]}.")
        findings.append(Finding("dangerous_web3_terms", dangerous_hits, -penalty))
    if "connect wallet" in lower_text or "walletconnect" in lower_html:
        score -= 8
        flags.append("Wallet-connect call detected.")
    seed_form = False
    for field in soup.find_all(["input", "textarea"]):
        attrs = " ".join(str(field.get(x, "")) for x in ["name", "id", "placeholder", "aria-label"]).lower()
        if any(term in attrs for term in ["seed", "phrase", "private", "mnemonic", "secret"]):
            seed_form = True
            break
    if seed_form:
        score -= 50
        flags.append("Page appears to ask for seed phrase/private key/mnemonic/secret material.")
    password_inputs = soup.find_all("input", attrs={"type": "password"})
    if password_inputs:
        score -= 18
        flags.append("Password input detected.")
    hidden_iframes = 0
    for iframe in soup.find_all("iframe"):
        style = str(iframe.get("style", "")).lower().replace(" ", "")
        width = str(iframe.get("width", "")).lower()
        height = str(iframe.get("height", "")).lower()
        if "display:none" in style or "visibility:hidden" in style or width in {"0", "1"} or height in {"0", "1"}:
            hidden_iframes += 1
    if hidden_iframes:
        penalty = min(24, hidden_iframes * 8)
        score -= penalty
        flags.append(f"Hidden iframes detected: {hidden_iframes}.")
    obfuscation = {
        "eval": lower_html.count("eval("),
        "atob": lower_html.count("atob("),
        "fromCharCode": html.count("fromCharCode"),
        "document_write": lower_html.count("document.write"),
    }
    obf_count = sum(obfuscation.values())
    if obf_count:
        penalty = min(30, obf_count * 5)
        score -= penalty
        flags.append("JavaScript obfuscation indicators found.")
        findings.append(Finding("js_obfuscation", obfuscation, -penalty))
    mixed_content = len(re.findall(r"""(?:src|href)=['\"]http://""", html, flags=re.I))
    if mixed_content:
        penalty = min(18, mixed_content * 3)
        score -= penalty
        flags.append(f"HTTP asset references on page: {mixed_content}.")
    external_count = len(dom.get("external_links", []) or [])
    if external_count > 50:
        score -= 8
        flags.append(f"High external-link count: {external_count}.")
    if urlscan.get("ok"):
        findings.append(Finding("urlscan", urlscan.get("data", {}), 0))
    elif urlscan and not urlscan.get("skipped", True):
        findings.append(Finding("urlscan", urlscan.get("error") or urlscan.get("status_code"), 0))
    if flags:
        negatives.extend(flags)
    else:
        positives.append("No major static scam/drainer flags detected.")
    raw = {
        "flags": flags,
        "flag_count": len(flags),
        "placeholder_hits": placeholder_hits,
        "hype_hits": hype_hits,
        "dangerous_hits": dangerous_hits,
        "hidden_iframes": hidden_iframes,
        "password_input_count": len(password_inputs),
        "obfuscation": obfuscation,
        "mixed_content_count": mixed_content,
        "urlscan": urlscan,
    }
    return SectionResult("safety_risk", clamp(score), findings, positives, negatives, raw)


def score_performance_render(fetch: FetchResult, dom: dict[str, Any], screenshot: dict[str, Any], pagespeed: dict[str, Any]) -> SectionResult:
    score = 35.0
    positives: list[str] = []
    negatives: list[str] = []
    findings: list[Finding] = []
    cat_scores: dict[str, float] = {}
    lighthouse_metrics: dict[str, Any] = {}
    if pagespeed.get("ok"):
        lighthouse = (pagespeed.get("data") or {}).get("lighthouseResult") or {}
        categories = lighthouse.get("categories") or {}
        for key in ("performance", "accessibility", "best-practices", "seo"):
            val = categories.get(key, {}).get("score")
            if val is not None:
                cat_scores[key] = round(float(val) * 100, 1)
        audits = lighthouse.get("audits") or {}
        for key in ["first-contentful-paint", "largest-contentful-paint", "total-blocking-time", "cumulative-layout-shift", "speed-index"]:
            audit = audits.get(key) or {}
            lighthouse_metrics[key] = {
                "numeric_value": audit.get("numericValue"),
                "display_value": audit.get("displayValue"),
                "score": audit.get("score"),
            }
        if cat_scores:
            avg = sum(cat_scores.values()) / len(cat_scores)
            score += avg * 0.25
            positives.append("PageSpeed/Lighthouse scores available.")
    else:
        findings.append(Finding("pagespeed", pagespeed.get("reason") or pagespeed.get("error") or pagespeed.get("status_code"), 0))
    if fetch.elapsed_seconds is not None:
        if fetch.elapsed_seconds < 1.5:
            score += 9
        elif fetch.elapsed_seconds < 4.0:
            score += 4
        else:
            score -= 7
    if dom.get("has_viewport"):
        score += 6
    else:
        score -= 7
        negatives.append("Missing mobile viewport tag.")
    title = dom.get("title", "")
    if 8 <= len(title) <= 80:
        score += 5
    elif not title:
        score -= 5
    desc = dom.get("meta_description", "")
    if 40 <= len(desc) <= 220:
        score += 5
    elif not desc:
        score -= 4
    alt_ratio = dom.get("image_alt_ratio")
    if alt_ratio is None:
        score += 1
    elif alt_ratio >= 0.65:
        score += 4
    elif alt_ratio < 0.25:
        score -= 4
    if screenshot.get("ok"):
        shot_stats = analyze_screenshot(screenshot.get("path"))
        rendered = screenshot.get("dom") or {}
        score += 8
        if screenshot.get("render_seconds") is not None:
            render_seconds = float(screenshot["render_seconds"])
            if render_seconds < 4:
                score += 5
            elif render_seconds > 10:
                score -= 6
        if rendered.get("brokenImageCount", 0) and rendered.get("imageCount", 0):
            broken_ratio = rendered["brokenImageCount"] / max(1, rendered["imageCount"])
            if broken_ratio > 0.20:
                score -= 8
                negatives.append("Many rendered images appear broken.")
        if screenshot.get("console_errors"):
            score -= min(10, len(screenshot["console_errors"]) * 2)
        if screenshot.get("request_failures"):
            score -= min(8, len(screenshot["request_failures"]) * 2)
        if shot_stats.get("available"):
            if shot_stats.get("blank_like"):
                score -= 18
                negatives.append("Screenshot appears blank/near-uniform.")
            else:
                score += 4
        findings.append(Finding("screenshot_stats", shot_stats, 0))
    else:
        findings.append(Finding("screenshot", screenshot.get("reason") or screenshot.get("error") or "not available", 0))
    raw = {
        "pagespeed_ok": pagespeed.get("ok"),
        "pagespeed_scores": cat_scores,
        "lighthouse_metrics": lighthouse_metrics,
        "elapsed_seconds": fetch.elapsed_seconds,
        "has_viewport": dom.get("has_viewport"),
        "image_alt_ratio": alt_ratio,
        "screenshot": screenshot,
    }
    return SectionResult("performance_render", clamp(score), findings, positives, negatives, raw)


GENERIC_WEBSITE_QUALITY_WEIGHTS = {
    "availability": 0.14,
    "domain_hosting": 0.14,
    "generic_identity": 0.12,
    "generic_content_depth": 0.20,
    "build_effort": 0.22,
    "safety_risk": 0.12,
    "performance_render": 0.06,
}

DEVELOPER_EFFORT_WEIGHTS = {
    "availability": 0.08,
    "domain_hosting": 0.15,
    "generic_identity": 0.15,
    "generic_content_depth": 0.25,
    "build_effort": 0.25,
    "safety_risk": 0.07,
    "performance_render": 0.05,
}

CRYPTO_PROJECT_FIT_WEIGHTS = {
    "availability": 0.05,
    "domain_hosting": 0.08,
    "crypto_identity": 0.26,
    "crypto_content_completeness": 0.20,
    "social_market_wiring": 0.26,
    "safety_risk": 0.10,
    "performance_render": 0.05,
}

SURVIVAL_FEATURE_WEIGHTS = {
    "availability": 0.08,
    "domain_hosting": 0.10,
    "generic_identity": 0.05,
    "crypto_identity": 0.16,
    "generic_content_depth": 0.07,
    "crypto_content_completeness": 0.13,
    "build_effort": 0.12,
    "social_market_wiring": 0.16,
    "safety_risk": 0.10,
    "performance_render": 0.03,
}

SCORE_WEIGHT_SETS = {
    "generic_website_quality_score": GENERIC_WEBSITE_QUALITY_WEIGHTS,
    "developer_effort_score": DEVELOPER_EFFORT_WEIGHTS,
    "crypto_project_fit_score": CRYPTO_PROJECT_FIT_WEIGHTS,
    "survival_feature_score": SURVIVAL_FEATURE_WEIGHTS,
}


def weighted_score(sections: list[SectionResult], weights: dict[str, float]) -> tuple[float, dict[str, float]]:
    section_map = {section.name: section.score for section in sections}
    contributions: dict[str, float] = {}
    total = 0.0
    for name, weight in weights.items():
        section_score = section_map.get(name, 0.0)
        contribution = section_score * weight
        contributions[name] = round(contribution, 4)
        total += contribution
    return round(clamp(total), 2), contributions


def compute_score_bundle(sections: list[SectionResult]) -> dict[str, dict[str, Any]]:
    bundle: dict[str, dict[str, Any]] = {}
    for score_name, weights in SCORE_WEIGHT_SETS.items():
        score, contributions = weighted_score(sections, weights)
        bundle[score_name] = {
            "score": score,
            "label": grade_label(score),
            "weights": weights,
            "weighted_contribution": contributions,
        }
    return bundle


def weighted_overall(sections: list[SectionResult]) -> tuple[float, dict[str, float], dict[str, float]]:
    """Backward-compatible alias: the old overall now means survival_feature_score."""
    score, contributions = weighted_score(sections, SURVIVAL_FEATURE_WEIGHTS)
    return score, contributions, SURVIVAL_FEATURE_WEIGHTS

def grade_label(score: float) -> str:
    if score >= 82:
        return "strong_effort"
    if score >= 68:
        return "good_effort"
    if score >= 54:
        return "mixed_effort"
    if score >= 38:
        return "low_effort"
    return "very_low_effort"


def section_to_json(section: SectionResult) -> dict[str, Any]:
    return {
        "score": round(section.score, 4),
        "label": grade_label(section.score),
        "positives": section.positives,
        "findings": [json_safe(f) for f in section.findings],
        "negatives": section.negatives,
        "raw": json_safe(section.raw),
    }


def build_report(
    coin: CoinInput,
    fetch: FetchResult,
    dns: dict[str, Any],
    ssl_info: dict[str, Any],
    rdap: dict[str, Any],
    dom: dict[str, Any],
    tech: dict[str, Any],
    pagespeed: dict[str, Any],
    screenshot: dict[str, Any],
    urlscan: dict[str, Any],
    sections: list[SectionResult],
    overall: float,
    contributions: dict[str, float],
    weights: dict[str, float],
    runtime_seconds: float,
    score_bundle: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    score_bundle = score_bundle or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now().isoformat(),
        "runtime_seconds": round(runtime_seconds, 4),
        "coin": json_safe(coin),
        "input_url": coin.website_url,
        "final_url": fetch.final_url,
        "overall_score": overall,
        "overall_label": grade_label(overall),
        "overall_score_meaning": "survival_feature_score",
        "generic_website_quality_score": (score_bundle.get("generic_website_quality_score") or {}).get("score"),
        "developer_effort_score": (score_bundle.get("developer_effort_score") or {}).get("score"),
        "crypto_project_fit_score": (score_bundle.get("crypto_project_fit_score") or {}).get("score"),
        "survival_feature_score": (score_bundle.get("survival_feature_score") or {}).get("score", overall),
        "objective": "separate_generic_quality_developer_effort_crypto_fit_and_6h_to_24h_survival_features",
        "scores": score_bundle,
        "weighted_contribution": contributions,
        "section_weights": weights,
        "sections": {section.name: section_to_json(section) for section in sections},
        "raw": {
            "request": json_safe(fetch),
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


def flatten_feature_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        return value
    return json.dumps(json_safe(value), sort_keys=True, ensure_ascii=False)


def build_feature_row(coin: CoinInput, report: dict[str, Any]) -> dict[str, Any]:
    sections = report["sections"]
    raw = report["raw"]
    dom = raw.get("dom", {})
    tech = raw.get("tech", {})
    req = raw.get("request", {})
    dns = raw.get("dns", {})
    ssl_info = raw.get("ssl", {})
    feature: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": report["generated_at_utc"],
        "mint": coin.mint,
        "address": coin.address,
        "coin_name": coin.coin_name,
        "symbol": coin.symbol,
        "website_url": coin.website_url,
        "final_url": report.get("final_url"),
        "overall_score": report.get("overall_score"),
        "overall_label": report.get("overall_label"),
        "overall_score_meaning": report.get("overall_score_meaning"),
        "generic_website_quality_score": report.get("generic_website_quality_score"),
        "developer_effort_score": report.get("developer_effort_score"),
        "crypto_project_fit_score": report.get("crypto_project_fit_score"),
        "survival_feature_score": report.get("survival_feature_score"),
        "runtime_seconds": report.get("runtime_seconds"),
    }
    for score_name, score_payload in (report.get("scores") or {}).items():
        feature[f"{score_name}_label"] = score_payload.get("label")
    for section_name, section_payload in sections.items():
        feature[f"score_{section_name}"] = section_payload.get("score")
        feature[f"neg_count_{section_name}"] = len(section_payload.get("negatives") or [])
        feature[f"pos_count_{section_name}"] = len(section_payload.get("positives") or [])
    feature.update({
        "fetch_ok": req.get("ok"),
        "http_status_code": req.get("status_code"),
        "fetch_elapsed_seconds": req.get("elapsed_seconds"),
        "redirect_count": len(req.get("redirects") or []),
        "bytes_downloaded": req.get("bytes_downloaded"),
        "dns_ok": dns.get("ok"),
        "dns_ip_count": dns.get("ip_count", len(dns.get("ips") or [])),
        "tls_ok": ssl_info.get("ok"),
        "tls_valid_now": ssl_info.get("valid_now"),
        "tls_days_to_expiry": ssl_info.get("days_to_expiry"),
        "tls_san_matches_host": ssl_info.get("san_matches_host"),
        "dom_node_count": dom.get("node_count"),
        "dom_word_count": dom.get("word_count"),
        "dom_unique_word_count": dom.get("unique_word_count"),
        "dom_unique_word_ratio": dom.get("unique_word_ratio"),
        "dom_repeated_word_ratio": dom.get("repeated_word_ratio"),
        "dom_text_length": dom.get("text_length"),
        "dom_link_count": dom.get("link_count"),
        "dom_internal_link_count": dom.get("internal_link_count"),
        "dom_external_link_count": dom.get("external_link_count"),
        "dom_social_link_count": len(dom.get("social_links") or []),
        "dom_image_count": dom.get("image_count"),
        "dom_image_alt_ratio": dom.get("image_alt_ratio"),
        "dom_form_count": dom.get("form_count"),
        "dom_iframe_count": dom.get("iframe_count"),
        "dom_canvas_count": dom.get("canvas_count"),
        "dom_svg_count": dom.get("svg_count"),
        "dom_video_count": dom.get("video_count"),
        "has_title": dom.get("has_title"),
        "title_length": len(dom.get("title") or ""),
        "has_meta_description": bool(dom.get("meta_description")),
        "meta_description_length": len(dom.get("meta_description") or ""),
        "has_viewport": dom.get("has_viewport"),
        "has_og_title": dom.get("has_og_title"),
        "has_og_image": dom.get("has_og_image"),
        "has_twitter_card": dom.get("has_twitter_card"),
        "has_favicon": bool(dom.get("favicon")),
        "percentage_count": dom.get("percentage_count"),
        "numeric_count": dom.get("numeric_count"),
        "section_hit_count": len(dom.get("section_hits") or {}),
        "tech_count": len(tech.get("tech") or []),
        "script_count": tech.get("script_count"),
        "stylesheet_count": tech.get("stylesheet_count"),
        "inline_script_chars": tech.get("inline_script_chars"),
        "external_script_host_count": tech.get("external_script_host_count"),
        "source_maps_referenced": tech.get("source_maps_referenced"),
        "minified_asset_hint": tech.get("minified_asset_hint"),
    })
    # Pull highly model-relevant raw metrics from sections.
    for section_name in ["domain_hosting", "generic_identity", "crypto_identity", "generic_content_depth", "crypto_content_completeness", "social_market_wiring", "safety_risk", "performance_render"]:
        section_raw = (sections.get(section_name) or {}).get("raw") or {}
        if section_name == "domain_hosting":
            keys = ["is_free_or_low_effort_host", "domain_entropy", "domain_age_days", "domain_expiry_days", "rdap_ok"]
        elif section_name == "generic_identity":
            keys = ["coin_name_domain_similarity", "symbol_domain_similarity", "best_declared_brand_domain_similarity", "best_page_host_similarity", "title_domain_similarity", "description_domain_similarity"]
        elif section_name == "crypto_identity":
            keys = ["coin_name_domain_similarity", "symbol_domain_similarity", "best_brand_domain_similarity", "best_social_handle_similarity", "exact_mint_visible_text", "exact_mint_present_html"]
            address = section_raw.get("address_evidence") or {}
            for k, v in address.items():
                feature[f"address_{k}"] = v
        elif section_name == "generic_content_depth":
            keys = ["generic_section_count", "heading_count", "link_count", "internal_link_count", "image_count"]
            feature["placeholder_hit_count"] = len(section_raw.get("placeholder_hits") or [])
        elif section_name == "crypto_content_completeness":
            keys = ["section_count", "useful_term_count", "tokenomics_numeric_detail"]
            feature["crypto_hype_hit_total"] = sum((section_raw.get("hype_hits") or {}).values())
            address = section_raw.get("address_evidence") or {}
            for k, v in address.items():
                feature[f"crypto_content_address_{k}"] = v
        elif section_name == "social_market_wiring":
            keys = ["major_social_count", "market_link_count", "shortener_count"]
        elif section_name == "safety_risk":
            keys = ["flag_count", "hidden_iframes", "password_input_count", "mixed_content_count"]
            feature["dangerous_term_total"] = sum((section_raw.get("dangerous_hits") or {}).values())
            feature["risk_hype_term_total"] = sum((section_raw.get("hype_hits") or {}).values())
            feature["obfuscation_total"] = sum((section_raw.get("obfuscation") or {}).values())
        elif section_name == "performance_render":
            keys = ["pagespeed_ok"]
            ps_scores = section_raw.get("pagespeed_scores") or {}
            for k, v in ps_scores.items():
                feature[f"pagespeed_{k.replace('-', '_')}"] = v
            shot = section_raw.get("screenshot") or {}
            feature["render_ok"] = shot.get("ok")
            feature["render_seconds"] = shot.get("render_seconds")
            render_dom = shot.get("dom") or {}
            for k in ["bodyWordCount", "elementCount", "imageCount", "brokenImageCount", "interactiveCount", "scrollHeight"]:
                feature[f"render_{k}"] = render_dom.get(k)
        else:
            keys = []
        for key in keys:
            feature[f"{section_name}_{key}"] = section_raw.get(key)
    return {k: flatten_feature_value(v) for k, v in feature.items()}


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")


def write_feature_csv(path: Path, row: dict[str, Any]) -> None:
    fieldnames = sorted(row.keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerow(row)


def write_summary_md(path: Path, report: dict[str, Any]) -> None:
    lines = [
        f"# Website effort report: {report['coin'].get('mint')}",
        "",
        f"- Generated UTC: `{report['generated_at_utc']}`",
        f"- Website: `{report['input_url']}`",
        f"- Final URL: `{report.get('final_url')}`",
        f"- Survival feature score: **{report['survival_feature_score']}/100** (`{report['overall_label']}`)",
        f"- Generic website quality: **{report['generic_website_quality_score']}/100**",
        f"- Developer effort: **{report['developer_effort_score']}/100**",
        f"- Crypto project fit: **{report['crypto_project_fit_score']}/100**",
        "",
        "## Sections",
        "",
        "| Section | Score | Label | Key negatives |",
        "|---|---:|---|---|",
    ]
    for name, payload in report["sections"].items():
        negatives = "; ".join((payload.get("negatives") or [])[:3]).replace("|", "-")
        lines.append(f"| {name} | {payload.get('score')} | {payload.get('label')} | {negatives} |")
    lines.extend([
        "",
        "## Model-use note",
        "",
        "Use `features.jsonl` or `features.csv` as the ML feature row. Do not train only on the overall score; prefer the raw section metrics, counts, exact identity-match booleans, risk flags, social wiring, and render-health fields.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def run_analysis(coin: CoinInput, args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    start_all = time.time()
    out_dir, raw_dir = make_output_dirs(args.output_root, coin.mint)
    fetch = fetch_url(coin.website_url, timeout=args.timeout, max_bytes=args.max_html_bytes)
    final_url = fetch.final_url or coin.website_url
    final_host = hostname(final_url) or hostname(coin.website_url)
    final_domain = root_domain(final_host)
    dns = check_dns(final_host)
    ssl_info = check_ssl_cert(final_host) if urlparse(final_url).scheme == "https" else {"ok": False, "valid_now": False, "error": "not https"}
    rdap = fetch_rdap(final_domain, timeout=args.timeout)
    soup = parse_html(fetch.html)
    text = visible_text(soup)
    dom = collect_dom_stats(soup, final_url, text)
    tech = detect_tech_stack(fetch.headers, soup, final_url)
    pagespeed = {"ok": False, "skipped": True, "reason": "disabled"}
    if not args.no_pagespeed:
        pagespeed = run_pagespeed(final_url, args.pagespeed_key, timeout=args.pagespeed_timeout)
    screenshot = {"ok": False, "skipped": True, "reason": "disabled"}
    if not args.no_screenshot and fetch.ok:
        screenshot = try_playwright_render(final_url, raw_dir, timeout_ms=args.render_timeout_ms)
    urlscan = {"ok": False, "skipped": True, "reason": "disabled"}
    if not args.no_urlscan:
        if args.urlscan_key:
            urlscan = run_urlscan(final_url, args.urlscan_key)
        else:
            urlscan = {"ok": False, "skipped": True, "reason": "No URLSCAN_API_KEY"}
    check_social_links = not args.no_social_head
    sections = [
        score_availability(fetch, dns),
        score_domain_hosting(coin, fetch, rdap, ssl_info),
        score_generic_identity(coin, fetch, dom, text),
        score_crypto_identity(coin, fetch, dom, text, fetch.html),
        score_generic_content_depth(dom, text),
        score_crypto_content_completeness(coin, dom, text, fetch.html),
        score_build_effort(fetch, dom, tech, screenshot),
        score_social_market(coin, dom, final_url, check_social_links),
        score_safety_risk(fetch, soup, text, dom, urlscan),
        score_performance_render(fetch, dom, screenshot, pagespeed),
    ]
    score_bundle = compute_score_bundle(sections)
    overall = score_bundle["survival_feature_score"]["score"]
    contributions = score_bundle["survival_feature_score"]["weighted_contribution"]
    weights = score_bundle["survival_feature_score"]["weights"]
    runtime_seconds = time.time() - start_all
    report = build_report(
        coin=coin,
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
        contributions=contributions,
        weights=weights,
        runtime_seconds=runtime_seconds,
        score_bundle=score_bundle,
    )
    features = build_feature_row(coin, report)
    paths = {
        "output_dir": str(out_dir),
        "raw_dir": str(raw_dir),
        "input_json": str(out_dir / "input.json"),
        "report_json": str(out_dir / "report.json"),
        "features_json": str(out_dir / "features.json"),
        "features_jsonl": str(out_dir / "features.jsonl"),
        "features_csv": str(out_dir / "features.csv"),
        "summary_md": str(out_dir / "summary.md"),
        "artifacts_index_json": str(out_dir / "artifacts_index.json"),
        "raw_html": str(raw_dir / "page.html"),
        "raw_text": str(raw_dir / "page_text.txt"),
        "raw_headers": str(raw_dir / "headers.json"),
    }
    write_json(Path(paths["input_json"]), coin)
    write_json(Path(paths["report_json"]), report)
    write_json(Path(paths["features_json"]), features)
    Path(paths["features_jsonl"]).write_text(json.dumps(features, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    write_feature_csv(Path(paths["features_csv"]), features)
    write_summary_md(Path(paths["summary_md"]), report)
    Path(paths["raw_html"]).write_text(fetch.html, encoding="utf-8", errors="ignore")
    Path(paths["raw_text"]).write_text(text, encoding="utf-8", errors="ignore")
    write_json(Path(paths["raw_headers"]), fetch.headers)
    artifact_index = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": report["generated_at_utc"],
        "mint": coin.mint,
        "website_url": coin.website_url,
        "final_url": report.get("final_url"),
        "overall_score": report.get("overall_score"),
        "overall_score_meaning": report.get("overall_score_meaning"),
        "generic_website_quality_score": report.get("generic_website_quality_score"),
        "developer_effort_score": report.get("developer_effort_score"),
        "crypto_project_fit_score": report.get("crypto_project_fit_score"),
        "survival_feature_score": report.get("survival_feature_score"),
        "paths": paths,
        "feature_field_count": len(features),
    }
    write_json(Path(paths["artifacts_index_json"]), artifact_index)
    return report, features, paths


def print_human_summary(report: dict[str, Any], paths: dict[str, str]) -> None:
    print("#" * 88)
    print(f"SURVIVAL FEATURE SCORE:        {report['survival_feature_score']:.2f}/100 ({report['overall_label']})")
    print(f"GENERIC WEBSITE QUALITY:       {report['generic_website_quality_score']:.2f}/100")
    print(f"DEVELOPER EFFORT SCORE:        {report['developer_effort_score']:.2f}/100")
    print(f"CRYPTO PROJECT FIT SCORE:      {report['crypto_project_fit_score']:.2f}/100")
    print("#" * 88)
    print(f"Mint:       {report['coin'].get('mint')}")
    print(f"Coin:       {report['coin'].get('coin_name') or ''} {report['coin'].get('symbol') or ''}".strip())
    print(f"Input URL:  {report['input_url']}")
    print(f"Final URL:  {report.get('final_url')}")
    print(f"Output dir: {paths['output_dir']}")
    print("")
    print("Sections:")
    for name, payload in report["sections"].items():
        negatives = payload.get("negatives") or []
        neg_text = f" | risk: {negatives[0]}" if negatives else ""
        print(f"  - {name}: {payload.get('score'):.2f}/100{neg_text}")
    print("")
    print("Saved artifacts:")
    for key in ["report_json", "features_jsonl", "features_csv", "summary_md", "raw_html", "raw_text"]:
        print(f"  - {key}: {paths[key]}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Grade Solana meme-coin website developer effort and write ML-ready artifacts.")
    parser.add_argument("positional_website", nargs="?", help="Website URL; prefer --website in pipelines")
    parser.add_argument("--mint", required=False, help="Solana mint address / unique coin id. Used as <output_root>/<mint>/")
    parser.add_argument("--website", "--url", dest="website", help="Website URL to grade")
    parser.add_argument("--coin-name", "--name", dest="coin_name", default=None)
    parser.add_argument("--token-name", dest="token_name", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--symbol", "--ticker", dest="symbol", default=None)
    parser.add_argument("--token-symbol", dest="token_symbol", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--address", default=None, help="Expected address/contract; defaults to mint")
    parser.add_argument("--contract", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--metadata-json", default=None, help="Coin metadata JSON string or path to JSON file")
    parser.add_argument("--metadata-file", default=None, help="Path to coin metadata JSON file")
    parser.add_argument("--expected-x", default=None)
    parser.add_argument("--expected-telegram", default=None)
    parser.add_argument("--expected-discord", default=None)
    parser.add_argument("--expected-website", default=None)
    parser.add_argument("--output-root", "--out-root", dest="output_root", default=DEFAULT_OUTPUT_ROOT, help="Root output directory; default ./data/raw/analytics")
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--max-html-bytes", type=int, default=5_000_000)
    parser.add_argument("--render-timeout-ms", type=int, default=25_000)
    parser.add_argument("--pagespeed-timeout", type=float, default=60.0)
    parser.add_argument("--pagespeed-key", default=os.getenv("PAGESPEED_API_KEY"))
    parser.add_argument("--urlscan-key", default=os.getenv("URLSCAN_API_KEY"))
    # Broad by default. These flags exist so batch jobs can explicitly trade coverage for latency.
    parser.add_argument("--no-pagespeed", action="store_true", help="Disable PageSpeed. Default: enabled/attempted.")
    parser.add_argument("--no-screenshot", action="store_true", help="Disable Playwright render/screenshot. Default: enabled if installed.")
    parser.add_argument("--no-urlscan", action="store_true", help="Disable urlscan.io. Default: enabled only when URLSCAN_API_KEY exists.")
    parser.add_argument("--no-social-head", action="store_true", help="Disable sampled HEAD checks for social links.")
    parser.add_argument("--stdout-json", action="store_true", help="Print full report JSON to stdout after writing artifacts")
    parser.add_argument("--quiet", action="store_true", help="Only print output directory unless --stdout-json")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        coin = build_coin_input(args)
        report, _features, paths = run_analysis(coin, args)
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    if args.stdout_json:
        print(json.dumps(json_safe(report), indent=2, sort_keys=True, ensure_ascii=False))
    elif args.quiet:
        print(paths["output_dir"])
    else:
        print_human_summary(report, paths)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
