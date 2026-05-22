#!/usr/bin/env python3
"""
x_account_info.py

Acquire basic public X/Twitter account profile information from a profile URL.

Input:
  python x_account_info.py https://x.com/some_account

Output:
  Appends one JSON object per profile to:
    data/raw/analytics/x_acc.jsonl

Design:
  - Uses Playwright to open the profile page.
  - Captures X web GraphQL responses when available.
  - Falls back to DOM/meta extraction without long locator waits.
  - Does not bypass login walls, captchas, rate limits, or access controls.
  - Optionally supports Playwright storage-state from an account you control.

Install:
  pip install playwright
  python -m playwright install chromium

Examples:
  python x_account_info.py https://x.com/babytrollsolana
  python x_account_info.py @babytrollsolana
  python x_account_info.py https://x.com/babytrollsolana --headful
  python x_account_info.py https://x.com/babytrollsolana --storage-state data/raw/analytics/x_storage_state.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)


DEFAULT_OUTPUT_PATH = Path("data/raw/analytics/x_acc.jsonl")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")

RESERVED_X_PATHS = {
    "home",
    "i",
    "intent",
    "messages",
    "notifications",
    "search",
    "settings",
    "share",
    "explore",
    "compose",
    "login",
    "logout",
    "signup",
}


@dataclass(frozen=True)
class ProfileTarget:
    input_url: str
    normalized_url: str
    username: str


@dataclass
class ScrapeDiagnostics:
    acquired_at: str
    input_url: str
    normalized_url: str
    username: str
    ok: bool = True
    method: str | None = None
    page_url_after_load: str | None = None
    graphql_urls_seen: list[str] = field(default_factory=list)
    graphql_errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: str | None = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_profile_target(profile_url: str) -> ProfileTarget:
    raw = profile_url.strip()
    if not raw:
        raise ValueError("profile_url is empty")

    if raw.startswith("@"):
        username = raw[1:]
        if not USERNAME_RE.fullmatch(username):
            raise ValueError(f"Invalid X username: {username!r}")
        return ProfileTarget(
            input_url=profile_url,
            normalized_url=f"https://x.com/{username}",
            username=username,
        )

    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    host = parsed.netloc.lower().removeprefix("www.")

    if host not in {"x.com", "twitter.com", "mobile.twitter.com"}:
        raise ValueError(
            "Expected an X/Twitter profile URL with host x.com, twitter.com, "
            f"or mobile.twitter.com; got {parsed.netloc!r}"
        )

    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        raise ValueError("URL does not contain a profile path")

    username = path_parts[0].lstrip("@")
    if username.lower() in RESERVED_X_PATHS or not USERNAME_RE.fullmatch(username):
        raise ValueError(f"URL does not look like a profile URL: {profile_url!r}")

    return ProfileTarget(
        input_url=profile_url,
        normalized_url=f"https://x.com/{username}",
        username=username,
    )


def iter_dicts(value: Any) -> Any:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for item in value:
            yield from iter_dicts(item)


def first_expanded_url(entity: Any) -> str | None:
    if not isinstance(entity, dict):
        return None

    urls = entity.get("urls")
    if not isinstance(urls, list) or not urls:
        return None

    first = urls[0]
    if not isinstance(first, dict):
        return None

    return first.get("expanded_url") or first.get("display_url") or first.get("url")


def normalize_count_text(value: str | None) -> str | None:
    if not value:
        return None
    return " ".join(value.replace("\n", " ").split())


def parse_x_user_result(user_result: dict[str, Any], expected_username: str) -> dict[str, Any] | None:
    """
    Parse an X User result object. X frequently changes exact response paths, but
    user objects usually contain:
      - rest_id
      - legacy.screen_name
      - legacy.followers_count, etc.
    """
    legacy = user_result.get("legacy")
    if not isinstance(legacy, dict):
        return None

    screen_name = legacy.get("screen_name")
    if not isinstance(screen_name, str):
        return None

    if screen_name.lower() != expected_username.lower():
        return None

    entities = legacy.get("entities") if isinstance(legacy.get("entities"), dict) else {}
    url_entity = entities.get("url") if isinstance(entities, dict) else None
    description_entity = entities.get("description") if isinstance(entities, dict) else None

    external_url = first_expanded_url(url_entity)

    description_urls: list[dict[str, Any]] = []
    if isinstance(description_entity, dict) and isinstance(description_entity.get("urls"), list):
        for item in description_entity["urls"]:
            if isinstance(item, dict):
                description_urls.append(
                    {
                        "url": item.get("url"),
                        "expanded_url": item.get("expanded_url"),
                        "display_url": item.get("display_url"),
                    }
                )

    return {
        "platform": "x",
        "id": user_result.get("rest_id"),
        "username": screen_name,
        "display_name": legacy.get("name"),
        "description": legacy.get("description"),
        "location": legacy.get("location"),
        "external_url": external_url,
        "created_at": legacy.get("created_at"),
        "followers_count": legacy.get("followers_count"),
        "following_count": legacy.get("friends_count"),
        "normal_followers_count": legacy.get("normal_followers_count"),
        "fast_followers_count": legacy.get("fast_followers_count"),
        "listed_count": legacy.get("listed_count"),
        "statuses_count": legacy.get("statuses_count"),
        "media_count": legacy.get("media_count"),
        "favourites_count": legacy.get("favourites_count"),
        "verified": legacy.get("verified"),
        "blue_verified": user_result.get("is_blue_verified"),
        "verified_type": user_result.get("verified_type"),
        "protected": legacy.get("protected"),
        "possibly_sensitive": legacy.get("possibly_sensitive"),
        "default_profile": legacy.get("default_profile"),
        "default_profile_image": legacy.get("default_profile_image"),
        "profile_image_url": legacy.get("profile_image_url_https") or legacy.get("profile_image_url"),
        "profile_banner_url": legacy.get("profile_banner_url"),
        "profile_image_shape": user_result.get("profile_image_shape"),
        "pinned_tweet_ids": legacy.get("pinned_tweet_ids_str") or legacy.get("pinned_tweet_ids"),
        "can_dm": legacy.get("can_dm"),
        "can_media_tag": legacy.get("can_media_tag"),
        "has_custom_timelines": legacy.get("has_custom_timelines"),
        "is_translator": legacy.get("is_translator"),
        "translator_type": legacy.get("translator_type"),
        "withheld_in_countries": legacy.get("withheld_in_countries"),
        "description_urls": description_urls,
        "raw_entities": entities,
        "affiliates_highlighted_label": user_result.get("affiliates_highlighted_label"),
        "business_account": user_result.get("business_account"),
        "creator_subscriptions_count": user_result.get("creator_subscriptions_count"),
    }


def extract_user_from_graphql(payload: Any, expected_username: str) -> dict[str, Any] | None:
    """
    Search recursively for a user object matching expected_username.
    This is more robust than relying on one hardcoded GraphQL path.
    """
    for node in iter_dicts(payload):
        profile = parse_x_user_result(node, expected_username)
        if profile is not None:
            return profile
    return None


async def get_meta_tags_fast(page: Page) -> dict[str, str | None]:
    """
    Extract meta tags via JS. This avoids Playwright locator waits and prevents
    the long fallback hang that happened in the previous version.
    """
    names = ["og:title", "og:description", "og:image", "twitter:title", "twitter:description"]

    try:
        result = await page.evaluate(
            """
            (names) => {
              const out = {};
              for (const name of names) {
                const el =
                  document.querySelector(`meta[property="${name}"]`) ||
                  document.querySelector(`meta[name="${name}"]`);
                out[name] = el ? el.getAttribute("content") : null;
              }
              return out;
            }
            """,
            names,
        )
    except Exception:
        return {name: None for name in names}

    if not isinstance(result, dict):
        return {name: None for name in names}

    return {name: normalize_count_text(result.get(name)) for name in names}


async def safe_inner_text(page: Page, selector: str, timeout_ms: int = 500) -> str | None:
    try:
        locator = page.locator(selector).first
        text = await locator.inner_text(timeout=timeout_ms)
        return normalize_count_text(text)
    except Exception:
        return None


async def extract_dom_counts(page: Page) -> dict[str, str | None]:
    """
    Best-effort fallback for visible following/follower counts.
    X DOM is unstable, so these fields are text-form and should be parsed later.
    """
    following_text: str | None = None
    followers_text: str | None = None

    candidates = [
        ('a[href$="/following"]', "following_text"),
        ('a[href$="/verified_followers"]', "verified_followers_text"),
        ('a[href$="/followers"]', "followers_text"),
    ]

    result: dict[str, str | None] = {
        "following_text": None,
        "followers_text": None,
        "verified_followers_text": None,
    }

    for selector, key in candidates:
        try:
            value = await page.locator(selector).first.inner_text(timeout=500)
            result[key] = normalize_count_text(value)
        except Exception:
            result[key] = None

    if following_text:
        result["following_text"] = following_text
    if followers_text:
        result["followers_text"] = followers_text

    return result


async def extract_meta_fallback(page: Page, target: ProfileTarget) -> dict[str, Any]:
    """
    Fallback when GraphQL capture fails. Everything here must be fast and
    best-effort; pipeline ingestion should never block waiting for perfect data.
    """
    meta = await get_meta_tags_fast(page)

    user_name_text = await safe_inner_text(page, '[data-testid="UserName"]')
    display_name = await safe_inner_text(page, '[data-testid="UserName"] div[dir="ltr"]')
    description = await safe_inner_text(page, '[data-testid="UserDescription"]')
    location = await safe_inner_text(page, '[data-testid="UserLocation"]')
    join_date = await safe_inner_text(page, '[data-testid="UserJoinDate"]')
    url = await safe_inner_text(page, '[data-testid="UserUrl"]')
    counts = await extract_dom_counts(page)

    description_from_meta = meta.get("twitter:description") or meta.get("og:description")
    title_from_meta = meta.get("twitter:title") or meta.get("og:title")

    return {
        "platform": "x",
        "id": None,
        "username": target.username,
        "display_name": display_name or title_from_meta,
        "description": description or description_from_meta,
        "location": location,
        "external_url": url,
        "created_at": join_date,
        "followers_count": None,
        "following_count": None,
        "normal_followers_count": None,
        "fast_followers_count": None,
        "listed_count": None,
        "statuses_count": None,
        "media_count": None,
        "favourites_count": None,
        "verified": None,
        "blue_verified": None,
        "verified_type": None,
        "protected": None,
        "possibly_sensitive": None,
        "default_profile": None,
        "default_profile_image": None,
        "profile_image_url": meta.get("og:image"),
        "profile_banner_url": None,
        "profile_image_shape": None,
        "pinned_tweet_ids": None,
        "can_dm": None,
        "can_media_tag": None,
        "has_custom_timelines": None,
        "is_translator": None,
        "translator_type": None,
        "withheld_in_countries": None,
        "description_urls": [],
        "raw_entities": {},
        "affiliates_highlighted_label": None,
        "business_account": None,
        "creator_subscriptions_count": None,
        "dom_user_name_text": user_name_text,
        "dom_counts": counts,
        "raw_meta": meta,
    }


async def classify_page_state(page: Page) -> list[str]:
    warnings: list[str] = []

    try:
        body_text = (await page.locator("body").inner_text(timeout=1_000)).lower()
    except Exception:
        return warnings

    checks = [
        ("this account doesn", "Page appears to report that the account does not exist."),
        ("these posts are protected", "Page appears to be a protected account."),
        ("something went wrong", "Page contains an X error message: something went wrong."),
        ("rate limit", "Page may be rate-limited."),
        ("try again", "Page may have failed to load completely."),
    ]

    for needle, warning in checks:
        if needle in body_text:
            warnings.append(warning)

    if "log in" in body_text or "sign in" in body_text:
        warnings.append("Page may be login-gated; consider passing --storage-state.")

    return warnings


def should_parse_response_url(url: str) -> bool:
    if "/graphql/" in url:
        return True

    lowered = url.lower()
    return (
        "userby" in lowered
        or "userresultby" in lowered
        or "usertweets" in lowered
        or "userhighlights" in lowered
    )


async def collect_profile(
    target: ProfileTarget,
    *,
    output_path: Path,
    headless: bool,
    storage_state: Path | None,
    timeout_ms: int,
    settle_ms: int,
    verbose: bool,
) -> dict[str, Any]:
    diagnostics = ScrapeDiagnostics(
        acquired_at=utc_now_iso(),
        input_url=target.input_url,
        normalized_url=target.normalized_url,
        username=target.username,
    )

    found_profile: dict[str, Any] | None = None
    profile_event = asyncio.Event()
    response_tasks: set[asyncio.Task[Any]] = set()

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=headless)

        context_kwargs: dict[str, Any] = {
            "viewport": {"width": 1365, "height": 900},
            "locale": "en-US",
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

        if storage_state is not None:
            if not storage_state.exists():
                await browser.close()
                raise FileNotFoundError(f"storage state file not found: {storage_state}")
            context_kwargs["storage_state"] = str(storage_state)

        context: BrowserContext = await browser.new_context(**context_kwargs)
        context.set_default_timeout(2_000)
        context.set_default_navigation_timeout(timeout_ms)

        page: Page = await context.new_page()

        async def on_response(response: Response) -> None:
            nonlocal found_profile

            url = response.url
            if not should_parse_response_url(url):
                return

            if url not in diagnostics.graphql_urls_seen:
                diagnostics.graphql_urls_seen.append(url)

            try:
                payload = await response.json()
            except Exception:
                return

            if isinstance(payload, dict) and isinstance(payload.get("errors"), list):
                for err in payload["errors"]:
                    if isinstance(err, dict):
                        msg = err.get("message")
                        if isinstance(msg, str) and msg not in diagnostics.graphql_errors:
                            diagnostics.graphql_errors.append(msg)

            if found_profile is not None:
                return

            profile = extract_user_from_graphql(payload, target.username)
            if profile:
                found_profile = profile
                diagnostics.method = "graphql_response"
                profile_event.set()

        def schedule_response_task(response: Response) -> None:
            task = asyncio.create_task(on_response(response))
            response_tasks.add(task)

            def _done_callback(done_task: asyncio.Task[Any]) -> None:
                response_tasks.discard(done_task)
                try:
                    done_task.result()
                except Exception:
                    pass

            task.add_done_callback(_done_callback)

        page.on("response", schedule_response_task)

        try:
            if verbose:
                print(f"[x] opening {target.normalized_url}", file=sys.stderr)

            try:
                await page.goto(target.normalized_url, wait_until="domcontentloaded", timeout=timeout_ms)
                diagnostics.page_url_after_load = page.url
            except PlaywrightTimeoutError as exc:
                diagnostics.warnings.append(f"Navigation timed out after {timeout_ms} ms: {exc}")
                diagnostics.page_url_after_load = page.url
            except PlaywrightError as exc:
                diagnostics.warnings.append(f"Navigation error: {exc}")
                diagnostics.page_url_after_load = page.url

            try:
                await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 5_000))
            except Exception:
                pass

            try:
                await asyncio.wait_for(profile_event.wait(), timeout=max(settle_ms / 1000.0, 0.1))
            except asyncio.TimeoutError:
                pass

            if found_profile is None:
                diagnostics.method = "dom_meta_fallback"
                found_profile = await extract_meta_fallback(page, target)
                diagnostics.warnings.append(
                    "GraphQL user object was not captured; returned best-effort DOM/meta fallback."
                )

            diagnostics.warnings.extend(await classify_page_state(page))

        except Exception as exc:
            diagnostics.ok = False
            diagnostics.error = f"{type(exc).__name__}: {exc}"
            diagnostics.method = diagnostics.method or "error"
            if found_profile is None:
                found_profile = {
                    "platform": "x",
                    "username": target.username,
                    "error": diagnostics.error,
                }

        finally:
            try:
                page.remove_listener("response", schedule_response_task)
            except Exception:
                pass

            if response_tasks:
                try:
                    await asyncio.wait(response_tasks, timeout=1.0)
                except Exception:
                    pass

            try:
                await context.close()
            except Exception:
                pass

            try:
                await browser.close()
            except Exception:
                pass

    record = {
        "acquisition": asdict(diagnostics),
        "profile": found_profile,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    return record


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Open an X profile with Playwright and append profile info to JSONL."
    )
    parser.add_argument(
        "profile_url",
        help="X profile URL or @username, for example: https://x.com/example",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"JSONL output path. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--storage-state",
        type=Path,
        default=None,
        help="Optional Playwright storage-state JSON for an account you control.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=15_000,
        help="Navigation timeout in milliseconds. Default: 15000.",
    )
    parser.add_argument(
        "--settle-ms",
        type=int,
        default=1_500,
        help="Extra time to wait for profile GraphQL responses after page load. Default: 1500.",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run browser with a visible window for debugging.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress to stderr.",
    )
    return parser


async def async_main() -> int:
    args = build_arg_parser().parse_args()

    try:
        target = parse_profile_target(args.profile_url)
    except Exception as exc:
        print(f"ERROR: invalid profile URL: {exc}", file=sys.stderr)
        return 2

    try:
        record = await collect_profile(
            target,
            output_path=args.output,
            headless=not args.headful,
            storage_state=args.storage_state,
            timeout_ms=args.timeout_ms,
            settle_ms=args.settle_ms,
            verbose=args.verbose,
        )
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    acquisition = record.get("acquisition", {})
    profile = record.get("profile", {})

    if not isinstance(profile, dict):
        profile = {}

    summary = {
        "ok": acquisition.get("ok", False),
        "username": profile.get("username"),
        "display_name": profile.get("display_name"),
        "followers_count": profile.get("followers_count"),
        "following_count": profile.get("following_count"),
        "method": acquisition.get("method"),
        "output": str(args.output),
        "warnings": acquisition.get("warnings", []),
        "error": acquisition.get("error"),
    }

    print(json.dumps(summary, ensure_ascii=False))
    return 0 if acquisition.get("ok", False) else 1


def main() -> None:
    raise SystemExit(asyncio.run(async_main()))


if __name__ == "__main__":
    main()
