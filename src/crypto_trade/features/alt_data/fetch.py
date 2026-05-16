"""Fetching layer for the website grader.

Holds: shared constants, dataclasses, URL/string helpers, HTTP/DNS/TLS/RDAP
probes, HTML parsing primitives, and optional Playwright/PageSpeed/urlscan
integrations. Stateless and side-effect-free beyond the underlying network call.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import math
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
    print(
        "Missing dependencies. Install with: pip install requests beautifulsoup4",
        file=sys.stderr,
    )
    raise


USER_AGENT = (
    "Mozilla/5.0 (compatible; CoinWebsiteGrader/2.0; "
    "+https://example.local/research-bot)"
)


def _build_fetch_session() -> "requests.Session":
    """Module-level session for HTML fetches; reuses TCP/TLS connections."""
    s = requests.Session()
    s.headers.update(
        {
            "user-agent": USER_AGENT,
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return s


_FETCH_SESSION: "requests.Session | None" = None


def _fetch_session() -> "requests.Session":
    global _FETCH_SESSION
    if _FETCH_SESSION is None:
        _FETCH_SESSION = _build_fetch_session()
    return _FETCH_SESSION


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# URL / string utilities
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Network probes
# ---------------------------------------------------------------------------


def fetch_url(url: str, timeout: float = 12.0) -> FetchResult:
    session = _fetch_session()

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
    out: Dict[str, Any] = {
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


def extract_rdap_dates(
    rdap_data: Dict[str, Any],
) -> Tuple[Optional[dt.datetime], Optional[dt.datetime], Optional[str]]:
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


# ---------------------------------------------------------------------------
# HTML parsing / DOM extraction
# ---------------------------------------------------------------------------


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
    links = [link.get("href", "") for link in soup.find_all("link") if link.get("href")]
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

    inline_script_chars = sum(
        len(s.get_text(" ", strip=True)) for s in soup.find_all("script") if not s.get("src")
    )

    return {
        "tech": sorted(set(tech)),
        "signals": signals,
        "script_count": len(scripts),
        "stylesheet_count": len([x for x in links if ".css" in x or "stylesheet" in x.lower()]),
        "inline_script_chars": inline_script_chars,
        "external_script_hosts": sorted(set(urlparse(x).netloc.lower() for x in scripts if urlparse(x).netloc))[:50],
        "source_maps_referenced": bool(re.search(r"\.map(?:\?|['\"<])", str(soup), flags=re.I)),
    }


# ---------------------------------------------------------------------------
# Optional external services
# ---------------------------------------------------------------------------


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
    out: Dict[str, Any] = {
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
