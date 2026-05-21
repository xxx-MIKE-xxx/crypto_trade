#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html.parser
import json
import mimetypes
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_OUT_DIR = Path("data/raw/tmp")
USER_AGENT = "linked-site-social-extractor/1.0"

BAD_URIS = {
    "",
    "https://arweave.net/fallback",
    "http://arweave.net/fallback",
}

X_HOSTS = {
    "x.com",
    "www.x.com",
    "twitter.com",
    "www.twitter.com",
    "mobile.twitter.com",
}

TELEGRAM_HOSTS = {
    "t.me",
    "www.t.me",
    "telegram.me",
    "www.telegram.me",
    "telegram.dog",
    "www.telegram.dog",
}

WEBSITE_KEYS = {
    "website",
    "site",
    "homepage",
    "home_page",
    "external_url",
    "externalUrl",
    "externalURL",
}

X_KEYS = {
    "x",
    "twitter",
    "twitter_url",
    "twitterUrl",
    "x_url",
    "xUrl",
}

TELEGRAM_KEYS = {
    "telegram",
    "telegram_url",
    "telegramUrl",
    "tg",
    "tg_url",
}

IGNORED_WEBSITE_KEYS = {
    "image",
    "image_url",
    "imageUrl",
    "logo",
    "logoURI",
    "animation_url",
    "animationUrl",
    "uri",
}

MEDIA_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".mp4",
    ".webm",
    ".mov",
    ".mp3",
    ".wav",
    ".json",
}

METADATA_HOSTS = {
    "ipfs.io",
    "gateway.pinata.cloud",
    "cloudflare-ipfs.com",
    "arweave.net",
}


@dataclass(frozen=True)
class FetchInfo:
    ok: bool
    requested_uri: str | None
    fetched_url: str | None
    final_url: str | None
    status_code: int | None
    content_type: str | None
    body_path: str | None
    parsed_json_path: str | None
    error: str | None


@dataclass(frozen=True)
class ExtractedLinks:
    mint: str
    source_uri: str | None
    fetched_url: str | None
    website: list[str]
    x: list[str]
    telegram: list[str]


class LinkHTMLParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for key, value in attrs:
            if key.lower() in {"href", "content"} and value:
                self.links.append(value.strip())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_no}: {exc}") from exc

            if not isinstance(obj, dict):
                raise ValueError(f"Line {line_no} is not a JSON object")

            events.append(obj)

    return events


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    tmp_path.replace(path)


def write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("wb") as f:
        f.write(value)

    tmp_path.replace(path)


def safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned[:180] or "unknown"


def event_data(event: dict[str, Any]) -> dict[str, Any]:
    data = event.get("data")
    return data if isinstance(data, dict) else event


def get_mint_and_uri(event: dict[str, Any]) -> tuple[str, str | None]:
    data = event_data(event)

    mint = event.get("mint") or data.get("mint")
    if not isinstance(mint, str) or not mint.strip():
        raise ValueError("event missing mint")

    uri = data.get("uri")
    if not isinstance(uri, str) or not uri.strip():
        uri = None

    return mint.strip(), uri.strip() if uri else None


def normalize_uri_candidates(uri: str | None) -> list[str]:
    if not uri:
        return []

    uri = uri.strip()
    if uri in BAD_URIS:
        return []

    if uri.startswith("ipfs://"):
        cid_path = uri.removeprefix("ipfs://").lstrip("/")
        return [
            f"https://ipfs.io/ipfs/{cid_path}",
            f"https://gateway.pinata.cloud/ipfs/{cid_path}",
            f"https://cloudflare-ipfs.com/ipfs/{cid_path}",
        ]

    if uri.startswith("ar://"):
        arweave_id = uri.removeprefix("ar://").lstrip("/")
        return [f"https://arweave.net/{arweave_id}"]

    if uri.startswith(("http://", "https://")) and "/ipfs/" in uri:
        cid_path = uri.split("/ipfs/", 1)[1]
        return [
            uri,
            f"https://ipfs.io/ipfs/{cid_path}",
            f"https://gateway.pinata.cloud/ipfs/{cid_path}",
            f"https://cloudflare-ipfs.com/ipfs/{cid_path}",
        ]

    return [uri]


def guess_body_path(out_dir: Path, content_type: str | None, final_url: str | None) -> Path:
    ext = ".bin"

    if content_type:
        mime = content_type.split(";", 1)[0].strip().lower()
        guessed = mimetypes.guess_extension(mime)
        if guessed:
            ext = guessed

    if final_url:
        suffix = Path(urllib.parse.urlparse(final_url).path).suffix
        if suffix:
            ext = suffix[:16]

    return out_dir / f"linked_site_body{ext}"


def fetch_first_available(
    uri: str | None,
    out_dir: Path,
    timeout: float,
    max_bytes: int,
) -> tuple[FetchInfo, bytes | None]:
    if not uri or uri in BAD_URIS:
        return (
            FetchInfo(
                ok=False,
                requested_uri=uri,
                fetched_url=None,
                final_url=None,
                status_code=None,
                content_type=None,
                body_path=None,
                parsed_json_path=None,
                error=f"missing_or_bad_uri: {uri}",
            ),
            None,
        )

    errors: list[str] = []

    for url in normalize_uri_candidates(uri):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,text/html,text/plain,*/*",
                },
                method="GET",
            )

            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status_code = int(resp.status)
                final_url = str(resp.geturl())
                headers = {str(k): str(v) for k, v in resp.headers.items()}
                content_type = headers.get("Content-Type") or headers.get("content-type")
                body = resp.read()

            if len(body) > max_bytes:
                raise ValueError(f"response_too_large: {len(body)} > {max_bytes}")

            body_path = guess_body_path(out_dir, content_type, final_url)
            write_bytes(body_path, body)

            parsed_json_path: Path | None = None
            try:
                parsed = json.loads(body.decode("utf-8"))
                if isinstance(parsed, dict):
                    parsed_json_path = out_dir / "linked_site.json"
                    write_json(parsed_json_path, parsed)
            except Exception:
                pass

            return (
                FetchInfo(
                    ok=200 <= status_code < 300,
                    requested_uri=uri,
                    fetched_url=url,
                    final_url=final_url,
                    status_code=status_code,
                    content_type=content_type,
                    body_path=str(body_path),
                    parsed_json_path=str(parsed_json_path) if parsed_json_path else None,
                    error=None if 200 <= status_code < 300 else f"http_status_{status_code}",
                ),
                body,
            )

        except urllib.error.HTTPError as exc:
            errors.append(f"{url}: HTTPError {exc.code}")
        except urllib.error.URLError as exc:
            errors.append(f"{url}: URLError {exc.reason}")
        except Exception as exc:
            errors.append(f"{url}: {type(exc).__name__}: {exc}")

    return (
        FetchInfo(
            ok=False,
            requested_uri=uri,
            fetched_url=None,
            final_url=None,
            status_code=None,
            content_type=None,
            body_path=None,
            parsed_json_path=None,
            error="; ".join(errors),
        ),
        None,
    )


def walk_json(obj: Any) -> list[tuple[str, Any]]:
    items: list[tuple[str, Any]] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                items.append((str(k), v))
                walk(v)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(obj)
    return items


def normalize_url(raw: str, base_url: str | None) -> str | None:
    value = raw.strip()
    if not value:
        return None

    if value.startswith("@"):
        return None

    if value.startswith("//"):
        value = "https:" + value

    if re.match(r"^(x\.com|twitter\.com|t\.me|telegram\.me|telegram\.dog|www\.)", value, re.I):
        value = "https://" + value

    if base_url:
        value = urllib.parse.urljoin(base_url, value)

    parsed = urllib.parse.urlparse(value)

    if parsed.scheme not in {"http", "https"}:
        return None

    if not parsed.netloc:
        return None

    return urllib.parse.urlunparse(parsed._replace(fragment=""))


def host_of(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower().split("@")[-1].split(":")[0]


def is_x_url(url: str) -> bool:
    host = host_of(url)
    return host in X_HOSTS


def is_telegram_url(url: str) -> bool:
    host = host_of(url)
    return host in TELEGRAM_HOSTS


def is_media_or_metadata_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    suffix = Path(parsed.path).suffix.lower()

    if suffix in MEDIA_EXTENSIONS:
        return True

    if host in METADATA_HOSTS:
        return True

    return False


def add_unique(dst: list[str], url: str) -> None:
    if url not in dst:
        dst.append(url)


def extract_links_from_json(parsed: dict[str, Any], base_url: str | None) -> dict[str, list[str]]:
    result = {
        "website": [],
        "x": [],
        "telegram": [],
    }

    for key, value in walk_json(parsed):
        if not isinstance(value, str):
            continue

        key_norm = key.strip()

        url = normalize_url(value, base_url)
        if not url:
            continue

        if key_norm in X_KEYS or is_x_url(url):
            add_unique(result["x"], url)
            continue

        if key_norm in TELEGRAM_KEYS or is_telegram_url(url):
            add_unique(result["telegram"], url)
            continue

        if key_norm in WEBSITE_KEYS:
            if not is_x_url(url) and not is_telegram_url(url) and not is_media_or_metadata_url(url):
                add_unique(result["website"], url)
            continue

        if key_norm not in IGNORED_WEBSITE_KEYS:
            if is_x_url(url):
                add_unique(result["x"], url)
            elif is_telegram_url(url):
                add_unique(result["telegram"], url)

    return result


def extract_links_from_html(body: bytes, base_url: str | None) -> dict[str, list[str]]:
    result = {
        "website": [],
        "x": [],
        "telegram": [],
    }

    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:
        return result

    parser = LinkHTMLParser()
    parser.feed(text)

    for raw_link in parser.links:
        url = normalize_url(raw_link, base_url)
        if not url:
            continue

        if is_x_url(url):
            add_unique(result["x"], url)
        elif is_telegram_url(url):
            add_unique(result["telegram"], url)
        elif not is_media_or_metadata_url(url):
            add_unique(result["website"], url)

    return result


def merge_links(*groups: dict[str, list[str]]) -> dict[str, list[str]]:
    merged = {
        "website": [],
        "x": [],
        "telegram": [],
    }

    for group in groups:
        for key in merged:
            for url in group.get(key, []):
                add_unique(merged[key], url)

    return merged


def extract_links_from_fetched_body(body: bytes | None, final_url: str | None) -> dict[str, list[str]]:
    if body is None:
        return {
            "website": [],
            "x": [],
            "telegram": [],
        }

    json_links = {
        "website": [],
        "x": [],
        "telegram": [],
    }

    try:
        parsed = json.loads(body.decode("utf-8"))
        if isinstance(parsed, dict):
            json_links = extract_links_from_json(parsed, base_url=final_url)
    except Exception:
        pass

    html_links = extract_links_from_html(body, base_url=final_url)

    return merge_links(json_links, html_links)


def process_event(
    event: dict[str, Any],
    out_dir: Path,
    timeout: float,
    max_bytes: int,
) -> ExtractedLinks:
    mint, uri = get_mint_and_uri(event)
    mint_dir = out_dir / safe_filename(mint)
    mint_dir.mkdir(parents=True, exist_ok=True)

    fetch_info, body = fetch_first_available(
        uri=uri,
        out_dir=mint_dir,
        timeout=timeout,
        max_bytes=max_bytes,
    )

    links = extract_links_from_fetched_body(body, fetch_info.final_url)

    extracted = ExtractedLinks(
        mint=mint,
        source_uri=uri,
        fetched_url=fetch_info.final_url,
        website=links["website"],
        x=links["x"],
        telegram=links["telegram"],
    )

    write_json(mint_dir / "linked_site_fetch.json", asdict(fetch_info))
    write_json(mint_dir / "social_links.json", asdict(extracted))

    return extracted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch each PumpPortal event's data.uri and extract only website/X/Telegram "
            "links from the linked site content."
        )
    )
    parser.add_argument(
        "input",
        type=Path,
        help="JSONL file with one PumpPortal event per line.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Default: data/raw/tmp",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=8.0,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=2_000_000,
        help="Maximum linked-site response size. Default: 2MB.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()

    try:
        events = read_jsonl(args.input)
    except Exception as exc:
        print(f"ERROR: failed to read input: {exc}", file=sys.stderr)
        return 1

    summary: list[dict[str, Any]] = []
    failures = 0

    for i, event in enumerate(events, start=1):
        try:
            extracted = process_event(
                event=event,
                out_dir=args.out_dir,
                timeout=args.timeout,
                max_bytes=args.max_bytes,
            )
            row = asdict(extracted)
            summary.append(row)

            printable = {
                "mint": extracted.mint,
                "website": extracted.website,
                "x": extracted.x,
                "telegram": extracted.telegram,
            }
            print(json.dumps(printable, ensure_ascii=False, separators=(",", ":")))

        except Exception as exc:
            failures += 1
            err = {
                "index": i,
                "error": f"{type(exc).__name__}: {exc}",
            }
            summary.append(err)
            print(json.dumps(err, separators=(",", ":")), file=sys.stderr)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.out_dir / "_social_links_summary.json", summary)

    elapsed = round(time.time() - started, 3)
    print(
        json.dumps(
            {
                "events": len(events),
                "failures": failures,
                "out_dir": str(args.out_dir),
                "elapsed_seconds": elapsed,
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )

    return 0 if failures == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())