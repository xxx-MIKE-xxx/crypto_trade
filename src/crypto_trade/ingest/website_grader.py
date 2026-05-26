#!/usr/bin/env python3
"""Website report collector for freshly migrated Solana meme coins.

This module is designed to be imported by pipeline code. It accepts the fixed
coin website input, fetches static and optionally rendered page signals, and
writes one JSON report.

Imported usage:
    run_and_save_report(
        coin_name="Example Coin",
        coin_symbol="EXMP",
        coin_mint="...",
        website_url="https://example.xyz",
        x_account="https://x.com/example",
        telegram_link="https://t.me/example",
    )

CLI usage writes to TMP_DIR / "website_report.json":
    python -m crypto_trade.ingest.website_report \
      --coin-name "Example Coin" \
      --coin-symbol EXMP \
      --coin-mint 9xQeWvG816bUx9EPjHmaT23yvVM2ZW1cRdxWhgn526S \
      --website-url https://example.xyz
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from crypto_trade.core import paths as core_paths
from crypto_trade.core.io import ensure_dir, save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.text import safe_part
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.core.yaml import get_yaml_value, load_yaml

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "website_report.v1"
CONFIG_PATH = core_paths.CONFIG_DIR / "website_quality.yaml"
ANALYTICS_DIR = getattr(core_paths, "ANALYTICS_DIR", core_paths.RAW_DIR / "analytics")
TMP_DIR = getattr(core_paths, "TMP_DIR", core_paths.PROJECT_ROOT / "tmp")

SOLANA_ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
EVM_ADDRESS_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
WORD_RE = re.compile(r"\b[\w'$-]+\b", re.UNICODE)

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
PHONE_RE = re.compile(r"(?<!\d)(?:\+\d{1,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?){2,4}\d{2,4}(?!\d)")
PERCENT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:\.\d{1,2})?)\s*%")
MULTIPLIER_RE = re.compile(r"(?<![\w.])(\d{2,5})\s*x(?![\w.])", re.I)
SUPPLY_RE = re.compile(r"\b(?:total\s+)?supply\b[^\n.!?]{0,80}", re.I)
TAX_FEE_RE = re.compile(r"\b(?:buy|sell|transfer|tx|transaction|trading|token)?\s*(?:tax|fee|fees)\b[^\n.!?]{0,80}", re.I)
CONTRACT_RE = re.compile(r"\b(?:contract|ca|token\s+address|mint)\b[^\n.!?]{0,100}", re.I)
ROADMAP_DATE_RE = re.compile(r"\b(?:q[1-4]\s*20\d{2}|20\d{2}|phase\s*\d+|stage\s*\d+)\b", re.I)
ADDRESS_HINT_RE = re.compile(r"\b(?:street|st\.|road|rd\.|avenue|ave\.|suite|floor|city|state|province|ltd|llc|inc\.|limited|gmbh|foundation)\b", re.I)
PLACEHOLDER_RE = re.compile(r"\b(?:lorem ipsum|coming soon|to be announced|tba|todo|your text|your token|your project|example\.com|template)\b", re.I)
COPYRIGHT_YEAR_RE = re.compile(r"(?:copyright|©)\s*(20\d{2})", re.I)

DEFAULT_TERM_GROUPS: dict[str, list[str]] = {
    "hype": [
        "moon", "mooning", "lambo", "100x", "1000x", "gem", "hidden gem", "next big",
        "pump", "send it", "ape", "degen", "diamond hands", "to the moon", "viral",
        "explosive", "parabolic", "life changing", "generational wealth",
    ],
    "guaranteed_returns": [
        "guaranteed", "risk free", "no risk", "safe returns", "guaranteed profit",
        "guaranteed returns", "daily returns", "passive income", "fixed return",
        "double your money", "multiply your money", "profit guaranteed", "insured returns",
    ],
    "urgency_pressure": [
        "limited time", "act now", "don't miss", "dont miss", "last chance", "hurry",
        "buy now", "presale live", "fair launch now", "only today", "countdown", "whitelist now",
    ],
    "technical_substance": [
        "smart contract", "contract address", "mint address", "liquidity pool", "lp",
        "staking", "bridge", "api", "sdk", "governance", "dao", "utility", "protocol",
        "mainnet", "testnet", "oracle", "burn", "mint authority", "freeze authority",
    ],
    "tokenomics": [
        "tokenomics", "supply", "total supply", "circulating supply", "allocation",
        "vesting", "cliff", "tax", "fee", "liquidity", "liquidity locked", "lp locked",
        "burn", "airdrop", "presale", "treasury", "renounced", "ownership", "mint authority",
        "freeze authority", "holders", "distribution",
    ],
    "audit_security": [
        "audit", "audited", "certik", "coinsult", "solidproof", "bug bounty", "security review",
        "verified contract", "contract verified", "kyc", "doxxed",
    ],
    "team_governance": [
        "team", "founder", "co-founder", "developer", "advisor", "ceo", "cto",
        "doxxed", "governance", "dao", "foundation", "community owned",
    ],
    "anonymous_team": [
        "anonymous", "anon", "pseudonymous", "no team", "community takeover", "cto token",
        "stealth launch", "fair launch", "no founders",
    ],
    "legal_contact": [
        "terms", "privacy", "disclaimer", "risk disclosure", "contact", "support", "jurisdiction",
        "company", "limited", "llc", "foundation", "not financial advice", "nfa",
    ],
    "roadmap": [
        "roadmap", "phase 1", "phase 2", "phase 3", "milestone", "launch", "listing",
        "partnership", "exchange", "cex", "dex", "marketing", "development",
    ],
    "meme_identity": [
        "meme", "memecoin", "community", "mascot", "dog", "cat", "frog", "pepe", "doge",
        "bonk", "wif", "shib", "viral", "culture", "fun", "joke",
    ],
    "ai_hype": [
        "ai", "artificial intelligence", "machine learning", "trading bot", "automated trading",
        "algorithmic", "neural", "agent", "agentic",
    ],
}

LINK_HOST_CATEGORIES: dict[str, tuple[str, ...]] = {
    "github": ("github.com", "gitlab.com", "bitbucket.org"),
    "docs": ("gitbook.io", "docs.", "notion.site", "readme.io", "mirror.xyz", "medium.com"),
    "explorer": (
        "solscan.io", "solana.fm", "explorer.solana.com", "etherscan.io", "basescan.org",
        "bscscan.com", "polygonscan.com", "arbiscan.io", "optimistic.etherscan.io",
    ),
    "dex_chart": (
        "dexscreener.com", "dextools.io", "birdeye.so", "geckoterminal.com", "defined.fi",
        "photon-sol.tinyastro.io", "bullx.io", "gmgn.ai", "pump.fun", "raydium.io",
        "jup.ag", "uniswap.org", "pancakeswap.finance",
    ),
    "audit": ("certik.com", "coinsult.net", "solidproof.io", "hacken.io", "cyberscope.io"),
    "liquidity_lock": ("team.finance", "unicrypt.network", "pinksale.finance", "dx.app", "gempad.app", "mudra.website"),
    "social": ("x.com", "twitter.com", "t.me", "telegram.me", "discord.gg", "discord.com", "youtube.com", "reddit.com", "linkedin.com"),
    "market_data": ("coinmarketcap.com", "coingecko.com", "coinpaprika.com", "livecoinwatch.com"),
}

LINK_KEYWORD_CATEGORIES: dict[str, tuple[str, ...]] = {
    "whitepaper": ("whitepaper", "white-paper", "white_paper", "litepaper", "paper", ".pdf"),
    "docs": ("docs", "documentation", "gitbook", "notion", "litepaper", "wiki"),
    "audit": ("audit", "security-review", "security_review", "certik", "solidproof", "hacken", "coinsult"),
    "tokenomics": ("tokenomics", "supply", "allocation", "vesting", "tax", "fees"),
    "roadmap": ("roadmap", "milestone", "phase"),
    "legal": ("terms", "privacy", "disclaimer", "risk", "legal"),
    "app": ("app", "swap", "stake", "staking", "claim", "airdrop", "bridge", "dashboard"),
    "liquidity_lock": ("lock", "locked", "liquidity", "lp-lock", "lp_lock"),
}

POSITIVE_SIGNAL_TERMS = [
    "audited", "verified", "transparent", "open source", "doxxed", "liquidity locked",
    "lp locked", "renounced", "bug bounty", "docs", "whitepaper", "roadmap",
]
NEGATIVE_SIGNAL_TERMS = [
    "guaranteed", "risk free", "no risk", "double your money", "urgent", "last chance",
    "anonymous", "stealth", "unlimited", "coming soon", "tba",
]


@dataclass(frozen=True)
class WebsiteInput:
    coin_name: str
    coin_symbol: str
    coin_mint: str
    website_url: str
    x_account: str | None = None
    telegram_link: str | None = None


@dataclass(frozen=True)
class StaticFetch:
    input_url: str
    final_url: str | None
    http_status: int | None
    ok: bool
    elapsed_ms: float | None
    headers: dict[str, str]
    redirects: list[dict[str, Any]]
    html: str
    error_type: str | None = None
    error_message: str | None = None


def load_website_quality_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Load project website-quality config from config/website_quality.yaml."""
    try:
        data = load_yaml(path)
    except FileNotFoundError:
        logger.warning("Website quality config missing: %s", path)
        return {"_config_error": f"missing: {path}"}
    except Exception as exc:
        logger.warning("Failed to load website quality config: %s", exc)
        return {"_config_error": f"{type(exc).__name__}: {exc}"}
    return data or {}


def config_get(config: dict[str, Any], *keys: str, default: Any = None) -> Any:
    data: Any = config
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return default
        data = data[key]
    return data


def configured_value(*keys: str, default: Any = None) -> Any:
    try:
        return get_yaml_value(CONFIG_PATH, *keys)
    except Exception:
        return default


def normalize_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        raise ValueError("website_url is required")
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    return value


def normalize_social_url(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.startswith("@"):
        return f"https://x.com/{text[1:]}"
    if "://" not in text and not text.startswith("t.me/"):
        return f"https://x.com/{text}"
    return normalize_url(text)


def hostname(url: str | None) -> str:
    if not url:
        return ""
    return (urlparse(url).hostname or "").lower().strip(".")


def root_domain(host: str) -> str:
    host = host.lower().removeprefix("www.").strip(".")
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return host
    two_level_suffixes = {
        "co.uk", "org.uk", "ac.uk", "com.au", "net.au", "co.jp",
        "com.br", "com.tr", "co.in", "com.sg", "com.mx",
    }
    suffix = ".".join(parts[-2:])
    if suffix in two_level_suffixes and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def canonical_url_key(url: str | None) -> str | None:
    if not url:
        return None
    try:
        normalized = normalize_url(url)
    except Exception:
        normalized = url
    parsed = urlparse(normalized)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    path = parsed.path.rstrip("/").lower()
    return f"{host}{path}"


def json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    if hasattr(value, "__dataclass_fields__"):
        return json_safe(asdict(value))
    return str(value)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def clipped(value: str, max_chars: int | None) -> str:
    if not max_chars or max_chars <= 0 or len(value) <= max_chars:
        return value
    return value[:max_chars]


def clean_visible_text(soup: BeautifulSoup) -> str:
    copy = BeautifulSoup(str(soup), "html.parser")
    for tag in copy(["script", "style", "noscript", "template", "svg"]):
        tag.extract()
    return re.sub(r"\s+", " ", copy.get_text(" ", strip=True)).strip()


def all_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    links: set[str] = set()
    for tag in soup.find_all("a", href=True):
        href = str(tag.get("href", "")).strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        links.add(urljoin(base_url, href))
    return sorted(links)


def tag_attr(tag: Any, attr: str) -> str | None:
    if tag is None:
        return None
    value = tag.get(attr)
    return str(value).strip() if value else None


def meta_content(soup: BeautifulSoup, *, name: str | None = None, prop: str | None = None) -> str | None:
    attrs: dict[str, Any] = {}
    if name:
        attrs["name"] = re.compile(f"^{re.escape(name)}$", re.I)
    if prop:
        attrs["property"] = re.compile(f"^{re.escape(prop)}$", re.I)
    return tag_attr(soup.find("meta", attrs=attrs), "content")


def get_meta_maps(soup: BeautifulSoup) -> dict[str, dict[str, str]]:
    out = {"name": {}, "property": {}}
    for tag in soup.find_all("meta"):
        content = tag_attr(tag, "content")
        if not content:
            continue
        name = tag_attr(tag, "name")
        prop = tag_attr(tag, "property")
        if name:
            out["name"][name.lower()] = content
        if prop:
            out["property"][prop.lower()] = content
    return out


def term_hits(text: str, terms: Iterable[str]) -> dict[str, int]:
    lower = text.lower()
    hits: dict[str, int] = {}
    for term in terms or []:
        t = str(term).strip().lower()
        if not t:
            continue
        count = lower.count(t)
        if count:
            hits[t] = count
    return hits


def regex_presence(text: str, value: str | None) -> bool:
    if not value:
        return False
    clean = value.strip()
    if not clean:
        return False
    return re.search(rf"(?<![a-zA-Z0-9]){re.escape(clean)}(?![a-zA-Z0-9])", text, re.I) is not None


def classify_domains(links: list[str], configured_domains: dict[str, Any]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for link in links:
        host = hostname(link).removeprefix("www.")
        for label, domains in (configured_domains or {}).items():
            if isinstance(domains, str):
                domains = [domains]
            for domain in domains or []:
                d = str(domain).lower().removeprefix("www.")
                if host == d or host.endswith("." + d):
                    out.setdefault(str(label), []).append(link)
                    break
    return {k: sorted(set(v)) for k, v in out.items()}




def safe_ratio(numerator: int | float, denominator: int | float, *, digits: int = 6) -> float | None:
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), digits)


def dedupe_preserve_order(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def all_link_records(soup: BeautifulSoup, base_url: str) -> list[dict[str, str | None]]:
    records: list[dict[str, str | None]] = []
    for tag in soup.find_all("a", href=True):
        href = str(tag.get("href", "")).strip()
        if not href or href.startswith(("javascript:", "#")):
            continue
        normalized = href if href.startswith(("mailto:", "tel:")) else urljoin(base_url, href)
        records.append({
            "url": normalized,
            "text": tag.get_text(" ", strip=True) or None,
            "title": tag_attr(tag, "title"),
            "aria_label": tag_attr(tag, "aria-label"),
        })
    return records


def extract_control_texts(soup: BeautifulSoup) -> list[str]:
    texts: list[str] = []
    for tag in soup.find_all(["button", "input", "textarea", "select", "a"]):
        label = (
            tag.get_text(" ", strip=True)
            or tag_attr(tag, "value")
            or tag_attr(tag, "placeholder")
            or tag_attr(tag, "aria-label")
            or tag_attr(tag, "title")
        )
        if label:
            texts.append(label)
    return dedupe_preserve_order(texts)[:120]


def count_term_occurrences(text: str, terms: Iterable[str]) -> dict[str, int]:
    hits: dict[str, int] = {}
    for term in terms or []:
        raw = str(term).strip().lower()
        if not raw:
            continue
        if re.search(r"[a-z0-9]", raw, re.I):
            pattern = r"(?<![\w$])" + re.escape(raw).replace(r"\ ", r"\s+") + r"(?![\w$])"
            count = len(re.findall(pattern, text, flags=re.I))
        else:
            count = text.lower().count(raw)
        if count:
            hits[raw] = count
    return hits


def grouped_term_hits(text: str, groups: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    term_groups = groups or DEFAULT_TERM_GROUPS
    return {name: count_term_occurrences(text, terms) for name, terms in term_groups.items()}


def flatten_hit_counts(group_hits: dict[str, dict[str, int]], word_count: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for group, hits in group_hits.items():
        total = sum(hits.values())
        out[f"{group}_term_count"] = total
        out[f"{group}_unique_term_count"] = len(hits)
        out[f"{group}_density_per_1k_words"] = round((total / word_count) * 1000, 6) if word_count else None
    return out


def sentence_count(text: str) -> int:
    chunks = [s for s in re.split(r"[.!?]+\s+", text.strip()) if s.strip()]
    return max(1, len(chunks)) if text.strip() else 0


def syllable_count(word: str) -> int:
    value = re.sub(r"[^a-z]", "", word.lower())
    if not value:
        return 0
    groups = re.findall(r"[aeiouy]+", value)
    count = len(groups)
    if value.endswith("e") and count > 1:
        count -= 1
    return max(1, count)


def readability_metrics(text: str, words: list[str]) -> dict[str, Any]:
    alpha_words = [w for w in words if re.search(r"[a-zA-Z]", w)]
    word_count = len(words)
    alpha_word_count = len(alpha_words)
    sentences = sentence_count(text)
    syllables = sum(syllable_count(w) for w in alpha_words)
    avg_sentence_length = safe_ratio(word_count, sentences)
    avg_word_chars = safe_ratio(sum(len(w) for w in alpha_words), alpha_word_count)
    flesch_reading_ease = None
    flesch_kincaid_grade = None
    if sentences and alpha_word_count:
        words_per_sentence = word_count / sentences
        syllables_per_word = syllables / alpha_word_count
        flesch_reading_ease = round(206.835 - (1.015 * words_per_sentence) - (84.6 * syllables_per_word), 4)
        flesch_kincaid_grade = round((0.39 * words_per_sentence) + (11.8 * syllables_per_word) - 15.59, 4)
    unique_words = {w.lower() for w in words}
    uppercase_words = [w for w in words if len(w) >= 3 and w.isupper()]
    numeric_words = [w for w in words if re.search(r"\d", w)]
    return {
        "sentence_count": sentences,
        "syllable_count_estimate": syllables,
        "alpha_word_count": alpha_word_count,
        "avg_sentence_length_words": avg_sentence_length,
        "avg_word_char_count": avg_word_chars,
        "type_token_ratio": safe_ratio(len(unique_words), word_count),
        "uppercase_word_ratio": safe_ratio(len(uppercase_words), word_count),
        "numeric_token_ratio": safe_ratio(len(numeric_words), word_count),
        "flesch_reading_ease": flesch_reading_ease,
        "flesch_kincaid_grade": flesch_kincaid_grade,
    }


def classify_links_by_research_signal(link_records: list[dict[str, str | None]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {name: [] for name in set(LINK_HOST_CATEGORIES) | set(LINK_KEYWORD_CATEGORIES)}
    for record in link_records:
        url = str(record.get("url") or "")
        if not url:
            continue
        parsed = urlparse(url if "://" in url else "https://" + url)
        host = (parsed.hostname or "").lower().removeprefix("www.")
        path_and_text = " ".join([
            parsed.path.lower(),
            parsed.query.lower(),
            str(record.get("text") or "").lower(),
            str(record.get("title") or "").lower(),
            str(record.get("aria_label") or "").lower(),
        ])
        for category, host_patterns in LINK_HOST_CATEGORIES.items():
            if any(host == pat or host.endswith("." + pat) or pat in host for pat in host_patterns):
                grouped.setdefault(category, []).append(url)
        for category, keywords in LINK_KEYWORD_CATEGORIES.items():
            if any(keyword in path_and_text for keyword in keywords):
                grouped.setdefault(category, []).append(url)
    return {k: sorted(set(v)) for k, v in grouped.items() if v}


def extract_contact_signals(soup: BeautifulSoup, text: str, link_records: list[dict[str, str | None]]) -> dict[str, Any]:
    html = str(soup)
    email_hits = sorted(set(EMAIL_RE.findall(f"{text}\n{html}")))
    mailto_links = sorted(set(str(r.get("url")) for r in link_records if str(r.get("url") or "").startswith("mailto:")))
    tel_links = sorted(set(str(r.get("url")) for r in link_records if str(r.get("url") or "").startswith("tel:")))
    phone_hits = [hit.strip() for hit in PHONE_RE.findall(text) if len(re.sub(r"\D", "", hit)) >= 8]
    company_entity_hits = sorted(set(re.findall(r"\b[A-Z][A-Za-z0-9&.,' -]{2,60}\s+(?:LLC|LTD|Ltd|Limited|Inc\.?|Foundation|GmbH)\b", text)))
    return {
        "email_count": len(email_hits),
        "email_samples": email_hits[:10],
        "mailto_count": len(mailto_links),
        "mailto_samples": mailto_links[:10],
        "tel_link_count": len(tel_links),
        "phone_like_count": len(phone_hits),
        "phone_like_samples": phone_hits[:5],
        "physical_address_hint": bool(ADDRESS_HINT_RE.search(text)),
        "company_entity_count": len(company_entity_hits),
        "company_entity_samples": company_entity_hits[:8],
    }


def extract_script_signals(soup: BeautifulSoup, base_url: str, final_root: str) -> dict[str, Any]:
    srcs = []
    for tag in soup.find_all("script"):
        src = tag_attr(tag, "src")
        if src:
            srcs.append(urljoin(base_url, src))
    hosts = sorted(set(hostname(src) for src in srcs if hostname(src)))
    third_party = [src for src in srcs if root_domain(hostname(src)) and root_domain(hostname(src)) != final_root]
    tracker_keywords = ("google-analytics", "googletagmanager", "gtag", "pixel", "hotjar", "mixpanel", "segment", "clarity")
    tracker_srcs = [src for src in srcs if any(k in src.lower() for k in tracker_keywords)]
    return {
        "script_src_count": len(srcs),
        "script_host_count": len(hosts),
        "script_hosts": hosts[:50],
        "third_party_script_count": len(third_party),
        "third_party_script_hosts": sorted(set(hostname(src) for src in third_party))[:50],
        "tracker_script_count": len(tracker_srcs),
        "tracker_script_hosts": sorted(set(hostname(src) for src in tracker_srcs))[:20],
    }


def extract_security_header_signals(headers: dict[str, str]) -> dict[str, Any]:
    lower_headers = {k.lower(): v for k, v in (headers or {}).items()}
    important = {
        "strict_transport_security": "strict-transport-security" in lower_headers,
        "content_security_policy": "content-security-policy" in lower_headers,
        "x_frame_options": "x-frame-options" in lower_headers,
        "x_content_type_options": "x-content-type-options" in lower_headers,
        "referrer_policy": "referrer-policy" in lower_headers,
        "permissions_policy": "permissions-policy" in lower_headers,
    }
    return {
        **important,
        "present_security_header_count": sum(1 for value in important.values() if value),
        "server_header_present": "server" in lower_headers,
        "powered_by_header_present": "x-powered-by" in lower_headers,
        "cache_control_present": "cache-control" in lower_headers,
    }


def extract_provenance_signals(fetch: StaticFetch, final_url: str) -> dict[str, Any]:
    input_host = hostname(fetch.input_url)
    final_host = hostname(final_url)
    final_root = root_domain(final_host)
    input_root = root_domain(input_host)
    tld = final_root.split(".")[-1] if "." in final_root else None
    return {
        "uses_https": urlparse(final_url).scheme == "https",
        "input_host": input_host,
        "final_host": final_host,
        "input_root_domain": input_root,
        "final_root_domain": final_root,
        "final_tld": tld,
        "redirect_count": len(fetch.redirects),
        "root_domain_changed_after_redirect": bool(input_root and final_root and input_root != final_root),
        "domain_age_days": None,
        "domain_age_source": "not_collected_by_static_fetch",
    }


def extract_social_presence_signals(link_groups: dict[str, list[str]], link_records: list[dict[str, str | None]]) -> dict[str, Any]:
    social_urls = link_groups.get("social", [])
    hosts = [hostname(url).removeprefix("www.") for url in social_urls if hostname(url)]
    host_counts = {host: hosts.count(host) for host in sorted(set(hosts))}
    text_blob = "\n".join(str(r.get("text") or "") for r in link_records)
    return {
        "social_link_count": len(social_urls),
        "social_links": social_urls[:50],
        "unique_social_host_count": len(set(hosts)),
        "social_host_counts": host_counts,
        "has_x_or_twitter": any(host in {"x.com", "twitter.com"} or host.endswith(".twitter.com") for host in hosts),
        "has_telegram": any(host in {"t.me", "telegram.me"} for host in hosts),
        "has_discord": any(host in {"discord.gg", "discord.com"} for host in hosts),
        "has_reddit": any("reddit.com" in host for host in hosts),
        "has_youtube": any("youtube.com" in host for host in hosts),
        "social_cta_hits": count_term_occurrences(text_blob, ["join", "community", "follow", "telegram", "discord", "twitter", "x"]),
    }


def extract_static_link_integrity_signals(soup: BeautifulSoup, link_records: list[dict[str, str | None]]) -> dict[str, Any]:
    anchor_tags = soup.find_all("a")
    missing_href_count = sum(1 for tag in anchor_tags if not tag.get("href"))
    placeholder_href_count = 0
    malformed_count = 0
    for tag in anchor_tags:
        href = str(tag.get("href", "")).strip()
        if not href:
            continue
        if href in {"#", "/#"} or href.lower().startswith("javascript:"):
            placeholder_href_count += 1
        parsed = urlparse(href if "://" in href or href.startswith(("mailto:", "tel:")) else "https://example.local" + (href if href.startswith("/") else "/" + href))
        if not parsed.scheme:
            malformed_count += 1
    return {
        "anchor_tag_count": len(anchor_tags),
        "recorded_link_count": len(link_records),
        "missing_href_count": missing_href_count,
        "placeholder_href_count": placeholder_href_count,
        "malformed_href_count": malformed_count,
        "placeholder_href_ratio": safe_ratio(placeholder_href_count, len(anchor_tags)),
        "network_broken_link_check_performed": False,
    }


def extract_language_and_risk_signals(text: str, words: list[str]) -> dict[str, Any]:
    word_count = len(words)
    group_hits = grouped_term_hits(text)
    flat_counts = flatten_hit_counts(group_hits, word_count)
    positive_hits = count_term_occurrences(text, POSITIVE_SIGNAL_TERMS)
    negative_hits = count_term_occurrences(text, NEGATIVE_SIGNAL_TERMS)
    positive_total = sum(positive_hits.values())
    negative_total = sum(negative_hits.values())
    percentages = [float(match) for match in PERCENT_RE.findall(text)]
    multipliers = [int(match) for match in MULTIPLIER_RE.findall(text)]
    return {
        "term_hits": group_hits,
        "term_count_features": flat_counts,
        "positive_signal_hits": positive_hits,
        "negative_signal_hits": negative_hits,
        "sentiment_proxy_score": safe_ratio(positive_total - negative_total, positive_total + negative_total),
        "percentage_mentions": percentages[:80],
        "max_percentage_mention": max(percentages) if percentages else None,
        "high_percentage_mention_count": sum(1 for p in percentages if p >= 20),
        "multiplier_mentions": multipliers[:40],
        "max_multiplier_mention": max(multipliers) if multipliers else None,
        "return_promise_risk": bool(group_hits.get("guaranteed_returns") or multipliers or any(p >= 50 for p in percentages)),
        "urgency_pressure_risk": bool(group_hits.get("urgency_pressure")),
    }


def extract_tokenomics_signals(text: str, link_groups: dict[str, list[str]]) -> dict[str, Any]:
    supply_mentions = [m.group(0).strip() for m in SUPPLY_RE.finditer(text)]
    tax_fee_mentions = [m.group(0).strip() for m in TAX_FEE_RE.finditer(text)]
    contract_mentions = [m.group(0).strip() for m in CONTRACT_RE.finditer(text)]
    percentages = [float(match) for match in PERCENT_RE.findall(text)]
    return {
        "tokenomics_link_count": len(link_groups.get("tokenomics", [])),
        "tokenomics_links": link_groups.get("tokenomics", [])[:20],
        "supply_mention_count": len(supply_mentions),
        "supply_mention_samples": supply_mentions[:10],
        "tax_fee_mention_count": len(tax_fee_mentions),
        "tax_fee_mention_samples": tax_fee_mentions[:10],
        "contract_phrase_count": len(contract_mentions),
        "contract_phrase_samples": contract_mentions[:10],
        "percentage_count": len(percentages),
        "percentage_samples": percentages[:30],
        "has_liquidity_lock_link": bool(link_groups.get("liquidity_lock")),
        "has_explorer_link": bool(link_groups.get("explorer")),
        "has_dex_chart_link": bool(link_groups.get("dex_chart")),
    }


def extract_team_signals(text: str, link_groups: dict[str, list[str]]) -> dict[str, Any]:
    linkedins = [u for u in link_groups.get("social", []) if "linkedin.com" in u.lower()]
    githubs = link_groups.get("github", [])
    founder_titles = count_term_occurrences(text, DEFAULT_TERM_GROUPS["team_governance"])
    anonymous_hits = count_term_occurrences(text, DEFAULT_TERM_GROUPS["anonymous_team"])
    title_name_patterns = re.findall(
        r"\b(?:Founder|Co-Founder|CEO|CTO|Developer|Advisor)\b[:\s,-]{0,20}([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",
        text,
    )
    return {
        "team_section_hint": bool(founder_titles),
        "founder_title_hits": founder_titles,
        "anonymous_team_hits": anonymous_hits,
        "anonymous_team_risk": bool(anonymous_hits) and not linkedins,
        "linkedin_link_count": len(linkedins),
        "linkedin_links": linkedins[:20],
        "github_link_count": len(githubs),
        "github_links": githubs[:20],
        "possible_named_people_count": len(set(title_name_patterns)),
        "possible_named_people_samples": sorted(set(title_name_patterns))[:10],
    }


def extract_roadmap_signals(text: str, link_groups: dict[str, list[str]]) -> dict[str, Any]:
    roadmap_dates = [m.group(0) for m in ROADMAP_DATE_RE.finditer(text)]
    roadmap_hits = count_term_occurrences(text, DEFAULT_TERM_GROUPS["roadmap"])
    return {
        "roadmap_link_count": len(link_groups.get("roadmap", [])),
        "roadmap_links": link_groups.get("roadmap", [])[:20],
        "roadmap_term_hits": roadmap_hits,
        "roadmap_date_or_phase_count": len(roadmap_dates),
        "roadmap_date_or_phase_samples": roadmap_dates[:20],
        "has_specific_roadmap_hint": bool(roadmap_dates),
    }


def extract_document_and_proof_signals(link_groups: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "whitepaper_link_count": len(link_groups.get("whitepaper", [])),
        "whitepaper_links": link_groups.get("whitepaper", [])[:20],
        "docs_link_count": len(link_groups.get("docs", [])),
        "docs_links": link_groups.get("docs", [])[:20],
        "github_link_count": len(link_groups.get("github", [])),
        "github_links": link_groups.get("github", [])[:20],
        "audit_link_count": len(link_groups.get("audit", [])),
        "audit_links": link_groups.get("audit", [])[:20],
        "explorer_link_count": len(link_groups.get("explorer", [])),
        "explorer_links": link_groups.get("explorer", [])[:20],
        "app_link_count": len(link_groups.get("app", [])),
        "app_links": link_groups.get("app", [])[:20],
        "has_whitepaper_or_docs": bool(link_groups.get("whitepaper") or link_groups.get("docs")),
        "has_technical_proof_link": bool(link_groups.get("github") or link_groups.get("audit") or link_groups.get("explorer")),
    }


def extract_cta_signals(text: str, control_texts: list[str]) -> dict[str, Any]:
    buy_terms = ("buy", "swap", "ape", "trade", "presale", "claim", "airdrop", "join", "whitelist")
    urgent_terms = tuple(DEFAULT_TERM_GROUPS["urgency_pressure"])
    cta_text = "\n".join(control_texts)
    buy_ctas = [label for label in control_texts if any(term in label.lower() for term in buy_terms)]
    urgent_ctas = [label for label in control_texts if any(term in label.lower() for term in urgent_terms)]
    return {
        "control_text_count": len(control_texts),
        "control_text_samples": control_texts[:60],
        "buy_cta_count": len(buy_ctas),
        "buy_cta_samples": buy_ctas[:20],
        "urgent_cta_count": len(urgent_ctas),
        "urgent_cta_samples": urgent_ctas[:20],
        "cta_hype_hits": count_term_occurrences(cta_text or text, DEFAULT_TERM_GROUPS["hype"]),
    }


def extract_quality_signals(
    soup: BeautifulSoup,
    text: str,
    words: list[str],
    links: list[str],
    internal_links: list[str],
    external_links: list[str],
    images: list[Any],
    title: str,
    meta_description: str | None,
    viewport: str | None,
    canonical: str | None,
) -> dict[str, Any]:
    image_alt_count = sum(1 for img in images if tag_attr(img, "alt"))
    placeholder_hits = sorted(set(match.group(0).lower() for match in PLACEHOLDER_RE.finditer(text)))
    copyright_years = [int(y) for y in COPYRIGHT_YEAR_RE.findall(text)]
    node_count = len(soup.find_all(True))
    return {
        "has_title": bool(title),
        "title_char_count": len(title or ""),
        "has_meta_description": bool(meta_description),
        "meta_description_char_count": len(meta_description or ""),
        "has_viewport": bool(viewport),
        "has_canonical": bool(canonical),
        "image_alt_ratio": safe_ratio(image_alt_count, len(images)),
        "link_to_word_ratio": safe_ratio(len(links), len(words)),
        "external_to_internal_link_ratio": safe_ratio(len(external_links), len(internal_links)),
        "text_to_node_ratio": safe_ratio(len(text), node_count),
        "thin_content_hint": len(words) < 120,
        "single_page_hint": len(internal_links) <= 2,
        "placeholder_hit_count": len(placeholder_hits),
        "placeholder_hits": placeholder_hits[:20],
        "latest_copyright_year": max(copyright_years) if copyright_years else None,
    }


def build_flat_ml_features(research: dict[str, Any]) -> dict[str, Any]:
    language_counts = research.get("language_risk", {}).get("term_count_features", {})
    docs = research.get("documents_and_proof", {})
    tokenomics = research.get("tokenomics", {})
    team = research.get("team", {})
    legal = research.get("legal_contact", {})
    cta = research.get("cta", {})
    quality = research.get("page_quality", {})
    security = research.get("security_headers", {})
    readability = research.get("readability", {})
    provenance = research.get("provenance", {})
    social = research.get("social_presence", {})
    link_integrity = research.get("static_link_integrity", {})
    return {
        **language_counts,
        "uses_https": provenance.get("uses_https"),
        "redirect_count": provenance.get("redirect_count"),
        "root_domain_changed_after_redirect": provenance.get("root_domain_changed_after_redirect"),
        "social_link_count": social.get("social_link_count"),
        "unique_social_host_count": social.get("unique_social_host_count"),
        "has_x_or_twitter": social.get("has_x_or_twitter"),
        "has_telegram": social.get("has_telegram"),
        "has_discord": social.get("has_discord"),
        "has_whitepaper_or_docs": docs.get("has_whitepaper_or_docs"),
        "whitepaper_link_count": docs.get("whitepaper_link_count"),
        "docs_link_count": docs.get("docs_link_count"),
        "github_link_count": docs.get("github_link_count"),
        "audit_link_count": docs.get("audit_link_count"),
        "explorer_link_count": docs.get("explorer_link_count"),
        "has_technical_proof_link": docs.get("has_technical_proof_link"),
        "has_explorer_link": tokenomics.get("has_explorer_link"),
        "has_liquidity_lock_link": tokenomics.get("has_liquidity_lock_link"),
        "supply_mention_count": tokenomics.get("supply_mention_count"),
        "tax_fee_mention_count": tokenomics.get("tax_fee_mention_count"),
        "contract_phrase_count": tokenomics.get("contract_phrase_count"),
        "team_section_hint": team.get("team_section_hint"),
        "anonymous_team_risk": team.get("anonymous_team_risk"),
        "linkedin_link_count": team.get("linkedin_link_count"),
        "possible_named_people_count": team.get("possible_named_people_count"),
        "email_count": legal.get("email_count"),
        "physical_address_hint": legal.get("physical_address_hint"),
        "company_entity_count": legal.get("company_entity_count"),
        "buy_cta_count": cta.get("buy_cta_count"),
        "urgent_cta_count": cta.get("urgent_cta_count"),
        "thin_content_hint": quality.get("thin_content_hint"),
        "single_page_hint": quality.get("single_page_hint"),
        "placeholder_hit_count": quality.get("placeholder_hit_count"),
        "image_alt_ratio": quality.get("image_alt_ratio"),
        "present_security_header_count": security.get("present_security_header_count"),
        "placeholder_href_ratio": link_integrity.get("placeholder_href_ratio"),
        "missing_href_count": link_integrity.get("missing_href_count"),
        "flesch_reading_ease": readability.get("flesch_reading_ease"),
        "flesch_kincaid_grade": readability.get("flesch_kincaid_grade"),
        "type_token_ratio": readability.get("type_token_ratio"),
    }


def build_research_features(
    *,
    soup: BeautifulSoup,
    fetch: StaticFetch,
    final_url: str,
    final_root: str,
    text: str,
    words: list[str],
    links: list[str],
    internal_links: list[str],
    external_links: list[str],
    images: list[Any],
    title: str,
    meta_description: str | None,
    viewport: str | None,
    canonical: str | None,
    link_records: list[dict[str, str | None]],
    control_texts: list[str],
) -> dict[str, Any]:
    link_groups = classify_links_by_research_signal(link_records)
    research: dict[str, Any] = {
        "readability": readability_metrics(text, words),
        "provenance": extract_provenance_signals(fetch, final_url),
        "social_presence": extract_social_presence_signals(link_groups, link_records),
        "language_risk": extract_language_and_risk_signals(text, words),
        "documents_and_proof": extract_document_and_proof_signals(link_groups),
        "tokenomics": extract_tokenomics_signals(text, link_groups),
        "team": extract_team_signals(text, link_groups),
        "roadmap": extract_roadmap_signals(text, link_groups),
        "legal_contact": extract_contact_signals(soup, text, link_records),
        "cta": extract_cta_signals(text, control_texts),
        "link_research_groups": link_groups,
        "script_supply_chain": extract_script_signals(soup, final_url, final_root),
        "security_headers": extract_security_header_signals(fetch.headers),
        "static_link_integrity": extract_static_link_integrity_signals(soup, link_records),
        "page_quality": extract_quality_signals(
            soup,
            text,
            words,
            links,
            internal_links,
            external_links,
            images,
            title,
            meta_description,
            viewport,
            canonical,
        ),
    }
    research["ml_feature_vector"] = build_flat_ml_features(research)
    return research


def make_input(
    coin_name: str,
    coin_symbol: str,
    coin_mint: str,
    website_url: str,
    x_account: str | None = None,
    telegram_link: str | None = None,
) -> WebsiteInput:
    return WebsiteInput(
        coin_name=coin_name.strip(),
        coin_symbol=coin_symbol.strip().lstrip("$"),
        coin_mint=coin_mint.strip(),
        website_url=normalize_url(website_url),
        x_account=normalize_social_url(x_account),
        telegram_link=normalize_social_url(telegram_link),
    )


def fetch_html(url: str, config: dict[str, Any]) -> StaticFetch:
    timeout_s = float(config_get(config, "fetch", "timeout_s", default=12))
    max_bytes = int(config_get(config, "fetch", "max_bytes", default=5_000_000))
    user_agent = str(config_get(config, "fetch", "user_agent", default="Mozilla/5.0 (compatible; crypto_trade website report)"))
    headers = {
        "user-agent": user_agent,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.8",
    }
    start = time.perf_counter()
    try:
        with httpx.Client(follow_redirects=True, timeout=timeout_s, headers=headers) as client:
            response = client.get(url)
        elapsed_ms = (time.perf_counter() - start) * 1000
        content = response.content[:max_bytes]
        html = content.decode(response.encoding or "utf-8", errors="replace")
        return StaticFetch(
            input_url=url,
            final_url=str(response.url),
            http_status=response.status_code,
            ok=200 <= response.status_code < 400,
            elapsed_ms=elapsed_ms,
            headers={k.lower(): v for k, v in response.headers.items()},
            redirects=[{"status_code": r.status_code, "url": str(r.url)} for r in response.history],
            html=html,
            error_type="truncated" if len(response.content) > max_bytes else None,
            error_message=None,
        )
    except httpx.TimeoutException as exc:
        return StaticFetch(url, None, None, False, None, {}, [], "", "timeout", str(exc))
    except httpx.RequestError as exc:
        return StaticFetch(url, None, None, False, None, {}, [], "", "request_error", str(exc))


def parse_static_page(fetch: StaticFetch, coin: WebsiteInput, config: dict[str, Any]) -> dict[str, Any]:
    final_url = fetch.final_url or fetch.input_url
    soup = BeautifulSoup(fetch.html or "", "html.parser")
    text = clean_visible_text(soup)
    words = WORD_RE.findall(text)
    links = all_links(soup, final_url)
    link_records = all_link_records(soup, final_url)
    control_texts = extract_control_texts(soup)
    final_host = hostname(final_url)
    final_root = root_domain(final_host)
    internal_links = [link for link in links if root_domain(hostname(link)) == final_root]
    external_links = [link for link in links if root_domain(hostname(link)) != final_root]
    meta = get_meta_maps(soup)
    headings = {
        f"h{i}": [h.get_text(" ", strip=True) for h in soup.find_all(f"h{i}") if h.get_text(" ", strip=True)]
        for i in range(1, 7)
    }
    images = soup.find_all("img")
    forms = soup.find_all("form")
    controls = soup.find_all(["button", "input", "textarea", "select"])
    canonical = tag_attr(soup.find("link", rel=re.compile("canonical", re.I)), "href")
    icon_tag = soup.find("link", rel=lambda value: value and "icon" in " ".join(value).lower() if isinstance(value, list) else value and "icon" in str(value).lower())
    favicon = urljoin(final_url, tag_attr(icon_tag, "href") or "") if icon_tag else None
    title_tag = soup.find("title")
    title = title_tag.get_text(" ", strip=True) if title_tag else ""
    configured_terms = config_get(config, "terms", default={}) or {}
    configured_domains = config_get(config, "domains", default={}) or {}
    text_and_html = f"{text}\n{fetch.html}"
    social_links = classify_domains(links, configured_domains.get("social", {}))
    market_links = classify_domains(links, configured_domains.get("market", {}))
    expected_socials = {
        "x_account": coin.x_account,
        "telegram_link": coin.telegram_link,
    }
    canonical_link_keys = {canonical_url_key(link) for link in links}
    expected_social_presence = {
        name: {
            "expected_url": value,
            "found_exact_link": canonical_url_key(value) in canonical_link_keys if value else None,
        }
        for name, value in expected_socials.items()
    }
    text_max_chars = config_get(config, "report", "text_max_chars", default=12_000)
    html_max_chars = config_get(config, "report", "html_max_chars", default=0)
    meta_description = meta_content(soup, name="description")
    viewport = meta_content(soup, name="viewport")
    research_features = build_research_features(
        soup=soup,
        fetch=fetch,
        final_url=final_url,
        final_root=final_root,
        text=text,
        words=words,
        links=links,
        internal_links=internal_links,
        external_links=external_links,
        images=images,
        title=title,
        meta_description=meta_description,
        viewport=viewport,
        canonical=canonical,
        link_records=link_records,
        control_texts=control_texts,
    )
    return {
        "page": {
            "title": title,
            "meta_description": meta_description,
            "canonical_url": urljoin(final_url, canonical) if canonical else None,
            "favicon_url": favicon,
            "html_lang": tag_attr(soup.find("html"), "lang"),
            "viewport": viewport,
            "charset": tag_attr(soup.find("meta", charset=True), "charset"),
            "meta": meta,
            "headings": headings,
        },
        "domain": {
            "scheme": urlparse(final_url).scheme,
            "host": final_host,
            "root_domain": final_root,
            "domain_label": final_root.split(".")[0] if final_root else None,
            "tld": final_root.split(".")[-1] if "." in final_root else None,
            "configured_low_effort_host_match": final_root in set(configured_domains.get("low_effort_hosts", []) or []),
        },
        "content": {
            "visible_text": clipped(text, int(text_max_chars) if text_max_chars is not None else None),
            "visible_text_truncated": bool(text_max_chars and len(text) > int(text_max_chars)),
            "visible_text_sha256": sha256_text(text),
            "html_sample": clipped(fetch.html or "", int(html_max_chars) if html_max_chars else 0),
            "html_sample_included": bool(html_max_chars),
            "text_char_count": len(text),
            "word_count": len(words),
            "unique_word_count": len({w.lower() for w in words}),
            "paragraph_count": len(soup.find_all("p")),
            "heading_count": sum(len(v) for v in headings.values()),
        },
        "structure": {
            "node_count": len(soup.find_all(True)),
            "link_count": len(links),
            "internal_link_count": len(internal_links),
            "external_link_count": len(external_links),
            "image_count": len(images),
            "image_alt_count": sum(1 for img in images if tag_attr(img, "alt")),
            "form_count": len(forms),
            "control_count": len(controls),
            "iframe_count": len(soup.find_all("iframe")),
            "script_count": len(soup.find_all("script")),
            "stylesheet_count": len(soup.find_all("link", rel=lambda value: value and "stylesheet" in " ".join(value).lower() if isinstance(value, list) else value and "stylesheet" in str(value).lower())),
            "canvas_count": len(soup.find_all("canvas")),
            "svg_count": len(soup.find_all("svg")),
            "video_count": len(soup.find_all("video")),
        },
        "links": {
            "all_links": links,
            "internal_links": internal_links,
            "external_links": external_links,
            "social_links": social_links,
            "market_links": market_links,
            "expected_social_presence": expected_social_presence,
            "link_text_records_sample": link_records[:120],
        },
        "coin_identity": {
            "coin_name_in_text": coin.coin_name.lower() in text.lower(),
            "coin_symbol_in_text": regex_presence(text, coin.coin_symbol),
            "coin_mint_in_text": coin.coin_mint in text,
            "coin_mint_in_html": coin.coin_mint in (fetch.html or ""),
            "solana_like_addresses_in_text": sorted(set(SOLANA_ADDRESS_RE.findall(text))),
            "evm_addresses_in_html": sorted(set(EVM_ADDRESS_RE.findall(fetch.html or ""))),
        },
        "configured_term_hits": {
            name: term_hits(text_and_html, terms)
            for name, terms in configured_terms.items()
            if isinstance(terms, list)
        },
        "research_features": research_features,
    }


def browser_snapshot_js(overlay_selectors: list[str]) -> str:
    selectors_json = json.dumps(overlay_selectors or [])
    return f"""
    () => {{
      const text = document.body ? document.body.innerText : "";
      const words = text.match(/\\b[\\w'$-]+\\b/g) || [];
      const elements = Array.from(document.querySelectorAll("*"));
      const controls = Array.from(document.querySelectorAll("button,a,input,textarea,select,[role=button]"));
      const buttonTexts = controls.map(el => (el.innerText || el.textContent || el.getAttribute("aria-label") || "").trim()).filter(Boolean).slice(0, 80);
      const overlays = [];
      for (const selector of {selectors_json}) {{
        for (const el of Array.from(document.querySelectorAll(selector)).slice(0, 20)) {{
          overlays.push({{
            selector,
            text: (el.innerText || el.textContent || "").trim().slice(0, 1000),
            visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length)
          }});
        }}
      }}
      return {{
        title: document.title,
        url: location.href,
        text_char_count: text.length,
        word_count: words.length,
        unique_word_count: new Set(words.map(w => w.toLowerCase())).size,
        node_count: elements.length,
        link_count: document.links.length,
        image_count: document.images.length,
        form_count: document.forms.length,
        control_count: controls.length,
        iframe_count: document.querySelectorAll("iframe").length,
        canvas_count: document.querySelectorAll("canvas").length,
        video_count: document.querySelectorAll("video").length,
        scroll_height: document.documentElement.scrollHeight,
        viewport_height: window.innerHeight,
        button_texts: buttonTexts,
        configured_overlay_matches: overlays,
        visible_text_hash32: Array.from(new TextEncoder().encode(text)).reduce((h, b) => ((h << 5) - h + b) | 0, 0).toString()
      }};
    }}
    """


def render_page(url: str, config: dict[str, Any]) -> dict[str, Any]:
    browser_config = config_get(config, "browser", default={}) or {}
    if browser_config.get("enabled") is False:
        return {"ok": False, "skipped": True, "reason": "disabled_by_config"}
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        logger.info("Playwright render skipped: %s", exc)
        return {"ok": False, "skipped": True, "reason": "playwright_not_available"}

    timeout_ms = int(browser_config.get("timeout_ms", 25_000))
    load_wait_until = str(browser_config.get("load_wait_until", "load"))
    content_read_delay_ms = int(browser_config.get("content_read_delay_ms", 10_000))
    scroll_steps = int(browser_config.get("scroll_steps", 0))
    scroll_wait_ms = int(browser_config.get("scroll_wait_ms", 250))
    click_selectors = list(browser_config.get("click_selectors", []) or [])
    click_texts = list(browser_config.get("click_texts", []) or [])
    overlay_selectors = list(browser_config.get("overlay_selectors", []) or [])

    actions: list[dict[str, Any]] = []
    console_errors: list[str] = []
    request_failures: list[str] = []
    start = time.perf_counter()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                viewport=browser_config.get("viewport", {"width": 1365, "height": 900}),
                user_agent=str(config_get(config, "fetch", "user_agent", default="Mozilla/5.0")),
            )
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("requestfailed", lambda req: request_failures.append(req.url))

            navigation: dict[str, Any] = {
                "ok": True,
                "wait_until": load_wait_until,
                "timed_out": False,
                "error": None,
            }
            try:
                page.goto(url, wait_until=load_wait_until, timeout=timeout_ms)
            except PlaywrightTimeoutError as exc:
                navigation.update({"ok": False, "timed_out": True, "error": str(exc)[:500]})
                logger.warning(
                    "Playwright page load timed out with wait_until=%s; waiting before reading current DOM",
                    load_wait_until,
                )

            page.wait_for_timeout(content_read_delay_ms)

            before_actions = page.evaluate(browser_snapshot_js(overlay_selectors))

            for text in click_texts:
                try:
                    page.get_by_text(str(text), exact=False).first.click(timeout=1500)
                    actions.append({"type": "click_text", "target": text, "ok": True})
                    page.wait_for_timeout(scroll_wait_ms)
                except PlaywrightTimeoutError as exc:
                    actions.append({"type": "click_text", "target": text, "ok": False, "error": str(exc)[:200]})

            for selector in click_selectors:
                try:
                    page.locator(str(selector)).first.click(timeout=1500)
                    actions.append({"type": "click_selector", "target": selector, "ok": True})
                    page.wait_for_timeout(scroll_wait_ms)
                except PlaywrightTimeoutError as exc:
                    actions.append({"type": "click_selector", "target": selector, "ok": False, "error": str(exc)[:200]})

            for step in range(1, scroll_steps + 1):
                page.evaluate("step => window.scrollTo(0, document.documentElement.scrollHeight * step)", step / max(1, scroll_steps))
                page.wait_for_timeout(scroll_wait_ms)
                actions.append({"type": "scroll", "step": step, "ok": True})

            after_actions = page.evaluate(browser_snapshot_js(overlay_selectors))
            rendered_html = page.content()
            browser.close()

        return {
            "ok": True,
            "skipped": False,
            "elapsed_ms": (time.perf_counter() - start) * 1000,
            "navigation": navigation,
            "content_read_delay_ms": content_read_delay_ms,
            "before_actions": before_actions,
            "after_actions": after_actions,
            "actions": actions,
            "console_errors": console_errors[:100],
            "request_failures": request_failures[:100],
            "rendered_html_sha256": sha256_text(rendered_html),
            "rendered_html_char_count": len(rendered_html),
            "content_delta": {
                "text_char_count": after_actions.get("text_char_count", 0) - before_actions.get("text_char_count", 0),
                "word_count": after_actions.get("word_count", 0) - before_actions.get("word_count", 0),
                "node_count": after_actions.get("node_count", 0) - before_actions.get("node_count", 0),
                "link_count": after_actions.get("link_count", 0) - before_actions.get("link_count", 0),
            },
        }
    except Exception as exc:
        logger.warning("Rendered website capture failed: %s", exc)
        return {"ok": False, "skipped": False, "error_type": type(exc).__name__, "error_message": str(exc)}


def build_report(coin: WebsiteInput, config: dict[str, Any]) -> dict[str, Any]:
    start = time.perf_counter()
    logger.info("Fetching website report for mint=%s url=%s", coin.coin_mint, coin.website_url)
    fetch = fetch_html(coin.website_url, config)
    static = parse_static_page(fetch, coin, config)
    rendered = render_page(fetch.final_url or coin.website_url, config) if fetch.ok else {"ok": False, "skipped": True, "reason": "static_fetch_failed"}
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso_ms_z(),
        "runtime_ms": (time.perf_counter() - start) * 1000,
        "config": {
            "path": str(CONFIG_PATH),
            "loaded": "_config_error" not in config,
            "error": config.get("_config_error"),
            "research_features": config.get("research_features"),
        },
        "input": json_safe(coin),
        "fetch": json_safe({k: v for k, v in asdict(fetch).items() if k != "html"}),
        "static_page": static,
        "rendered_page": rendered,
    }


def report_output_path(coin_mint: str) -> Path:
    return ANALYTICS_DIR / safe_part(coin_mint, fallback="unknown_mint") / "website_report.json"


def save_report(report: dict[str, Any], path: Path) -> Path:
    ensure_dir(path.parent)
    save_json(path, json_safe(report))
    logger.info("Saved website report: %s", path)
    return path


def run_report(
    coin_name: str,
    coin_symbol: str,
    coin_mint: str,
    website_url: str,
    x_account: str | None = None,
    telegram_link: str | None = None,
    *,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    coin = make_input(coin_name, coin_symbol, coin_mint, website_url, x_account, telegram_link)
    loaded_config = load_website_quality_config() if config is None else config
    return build_report(coin, loaded_config)


def run_and_save_report(
    coin_name: str,
    coin_symbol: str,
    coin_mint: str,
    website_url: str,
    x_account: str | None = None,
    telegram_link: str | None = None,
    *,
    config: dict[str, Any] | None = None,
) -> Path:
    report = run_report(coin_name, coin_symbol, coin_mint, website_url, x_account, telegram_link, config=config)
    return save_report(report, report_output_path(coin_mint))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch one coin website report JSON.")
    parser.add_argument("--coin-name", required=True)
    parser.add_argument("--coin-symbol", "--symbol", dest="coin_symbol", required=True)
    parser.add_argument("--coin-mint", "--mint", dest="coin_mint", required=True)
    parser.add_argument("--website-url", "--website", dest="website_url", required=True)
    parser.add_argument("--x-account", default=None)
    parser.add_argument("--telegram-link", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_arg_parser().parse_args(argv)
    report = run_report(
        coin_name=args.coin_name,
        coin_symbol=args.coin_symbol,
        coin_mint=args.coin_mint,
        website_url=args.website_url,
        x_account=args.x_account,
        telegram_link=args.telegram_link,
    )
    
    return report


if __name__ == "__main__":
    report = main()
    save_report(report, TMP_DIR / "website_report.json")
