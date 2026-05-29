#!/usr/bin/env python3
"""X/Twitter profile data ingestion."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from twscrape import API, gather

from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.io import save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ANALYTICS_DIR, PROJECT_ROOT, TMP_DIR
from crypto_trade.core.time import utc_now_iso_ms_z

load_env()

logger = logging.getLogger(__name__)

OUTPUT_FILENAME = "twitter.json"
LITE_OUTPUT_FILENAME = "twitter_lite.json"
DEFAULT_ACCOUNTS_DB = PROJECT_ROOT / "app_data" / "accounts.db"
DEFAULT_POST_LIMIT = 20


def json_default(obj: Any) -> str:
    return str(obj)


def parse_account_link(link: str) -> str:
    raw = link.strip()

    if not raw:
        raise ValueError("Twitter link is empty")

    if raw.startswith("@"):
        raw = raw[1:]

    if "://" not in raw and "/" not in raw:
        username = raw
    else:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = parsed.netloc.lower().removeprefix("www.")

        if host not in {"x.com", "twitter.com"}:
            raise ValueError(f"Expected x.com/twitter.com profile link, got: {link}")

        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError(f"Missing username in link: {link}")

        username = parts[0].lstrip("@")

    if not re.fullmatch(r"[A-Za-z0-9_]{1,15}", username):
        raise ValueError(f"Invalid X/Twitter username: {username!r}")

    return username


def get_accounts_db_path() -> Path:
    configured = get_env("TWITTER_ACCOUNTS_DB")
    return Path(configured).expanduser() if configured else DEFAULT_ACCOUNTS_DB


def get_post_limit(posts_limit: int | None = None) -> int:
    if posts_limit is not None:
        return max(0, posts_limit)

    raw = get_env("TWITTER_POST_LIMIT")
    if raw is None:
        return DEFAULT_POST_LIMIT

    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("Invalid TWITTER_POST_LIMIT=%r, using default", raw)
        return DEFAULT_POST_LIMIT


def to_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "dict"):
        return obj.dict()
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return {"value": obj}


def compact_link(link: Any) -> dict[str, Any]:
    data = to_dict(link)
    return {
        "url": data.get("url"),
        "text": data.get("text"),
        "tcourl": data.get("tcourl"),
    }


def compact_user_ref(user: Any) -> dict[str, Any]:
    data = to_dict(user)
    return {
        "id": data.get("id"),
        "id_str": data.get("id_str"),
        "username": data.get("username"),
        "displayname": data.get("displayname"),
    }


def compact_media(media: Any) -> dict[str, Any]:
    data = to_dict(media)

    if not data:
        return {
            "photos": [],
            "videos": [],
            "animated": [],
        }

    return {
        "photos": data.get("photos") or [],
        "videos": data.get("videos") or [],
        "animated": data.get("animated") or [],
    }


def compact_profile(profile_obj: Any) -> dict[str, Any]:
    data = to_dict(profile_obj)

    return {
        "id": data.get("id"),
        "id_str": data.get("id_str"),
        "url": data.get("url"),
        "username": data.get("username"),
        "displayname": data.get("displayname"),
        "bio": data.get("rawDescription"),
        "created": data.get("created"),
        "followersCount": data.get("followersCount"),
        "followingCount": data.get("friendsCount"),
        "friendsCount": data.get("friendsCount"),
        "statusesCount": data.get("statusesCount"),
        "favouritesCount": data.get("favouritesCount"),
        "listedCount": data.get("listedCount"),
        "mediaCount": data.get("mediaCount"),
        "location": data.get("location"),
        "profileImageUrl": data.get("profileImageUrl"),
        "profileBannerUrl": data.get("profileBannerUrl"),
        "protected": data.get("protected"),
        "verified": data.get("verified"),
        "blue": data.get("blue"),
        "blueType": data.get("blueType"),
        "descriptionLinks": [compact_link(link) for link in data.get("descriptionLinks", []) or []],
        "pinnedIds": data.get("pinnedIds") or [],
    }


def compact_profile_lite(profile_obj: Any) -> dict[str, Any]:
    profile = compact_profile(profile_obj)
    return {
        "id": profile.get("id"),
        "id_str": profile.get("id_str"),
        "url": profile.get("url"),
        "username": profile.get("username"),
        "displayname": profile.get("displayname"),
        "created": profile.get("created"),
        "followersCount": profile.get("followersCount"),
        "followingCount": profile.get("followingCount"),
        "statusesCount": profile.get("statusesCount"),
        "favouritesCount": profile.get("favouritesCount"),
        "listedCount": profile.get("listedCount"),
        "mediaCount": profile.get("mediaCount"),
        "protected": profile.get("protected"),
        "verified": profile.get("verified"),
        "blue": profile.get("blue"),
        "blueType": profile.get("blueType"),
        "has_bio": bool(profile.get("bio")),
        "bio_length": len(profile.get("bio") or ""),
        "has_location": bool(profile.get("location")),
        "has_profile_image": bool(profile.get("profileImageUrl")),
        "has_banner": bool(profile.get("profileBannerUrl")),
        "description_link_count": len(profile.get("descriptionLinks") or []),
        "pinned_count": len(profile.get("pinnedIds") or []),
    }


def compact_post(tweet_obj: Any) -> dict[str, Any]:
    data = to_dict(tweet_obj)

    return {
        "id": data.get("id"),
        "id_str": data.get("id_str"),
        "url": data.get("url"),
        "date": data.get("date"),
        "replyCount": data.get("replyCount"),
        "retweetCount": data.get("retweetCount"),
        "likeCount": data.get("likeCount"),
        "quoteCount": data.get("quoteCount"),
        "viewCount": data.get("viewCount"),
        "bookmarkCount": data.get("bookmarkCount"),
        "lang": data.get("lang"),
        "source": data.get("source"),
        "hashtags": data.get("hashtags") or [],
        "cashtags": data.get("cashtags") or [],
        "links": [compact_link(link) for link in data.get("links", []) or []],
        "mentionedUsers": [compact_user_ref(user) for user in data.get("mentionedUsers", []) or []],
        "media": compact_media(data.get("media")),
    }


async def get_api() -> API:
    db_path = get_accounts_db_path()

    if not db_path.exists():
        raise FileNotFoundError(
            f"twscrape accounts DB not found at {db_path}. "
            "Move accounts.db there or set TWITTER_ACCOUNTS_DB."
        )

    return API(
        str(db_path),
        proxy=get_env("TWS_PROXY"),
        raise_when_no_account=True,
    )


async def fetch_profile(api: API, username: str) -> Any:
    profile_obj = await api.user_by_login(username)
    if profile_obj is None:
        raise RuntimeError(f"Profile not found for @{username}")
    return profile_obj


async def download_twitter_lite_data(link: str) -> dict[str, Any]:
    username = parse_account_link(link)
    api = await get_api()

    logger.info("Downloading lite X profile for @%s", username)
    profile_obj = await fetch_profile(api, username)

    return {
        "time": utc_now_iso_ms_z(),
        "source": "twscrape",
        "mode": "lite",
        "input": {
            "link": link,
            "username": username,
            "posts_limit": 0,
        },
        "profile": compact_profile_lite(profile_obj),
        "posts": [],
    }


async def download_twitter_profile_data(
    link: str,
    posts_limit: int | None = None,
) -> dict[str, Any]:
    username = parse_account_link(link)
    posts_limit = get_post_limit(posts_limit)
    api = await get_api()

    logger.info("Downloading X profile for @%s", username)
    profile_obj = await fetch_profile(api, username)
    profile = compact_profile(profile_obj)
    user_id = profile.get("id")

    if user_id is None:
        raise RuntimeError(f"Missing user id for @{username}")

    posts: list[dict[str, Any]] = []

    if posts_limit > 0:
        logger.info("Downloading up to %s recent posts for @%s", posts_limit, username)
        tweet_objs = await gather(api.user_tweets(int(user_id), limit=posts_limit))
        posts = [compact_post(tweet) for tweet in tweet_objs]

    return {
        "time": utc_now_iso_ms_z(),
        "source": "twscrape",
        "mode": "posts",
        "input": {
            "link": link,
            "username": username,
            "posts_limit": posts_limit,
        },
        "profile": profile,
        "posts": posts,
    }


async def main(
    link: str,
    save_dir: Path = ANALYTICS_DIR,
    posts_limit: int | None = None,
    lite: bool = False,
) -> dict[str, Any]:
    configure_logging()

    output = (
        await download_twitter_lite_data(link)
        if lite
        else await download_twitter_profile_data(link=link, posts_limit=posts_limit)
    )

    output_path = save_dir / (LITE_OUTPUT_FILENAME if lite else OUTPUT_FILENAME)
    save_json(output_path, output)

    logger.info("Saved Twitter profile data to %s", output_path)

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("link", help="X/Twitter profile link, e.g. https://x.com/purple_bitcoin_")
    parser.add_argument("--lite", action="store_true")
    parser.add_argument(
        "--posts-limit",
        type=int,
        default=None,
        help="Number of recent posts to fetch. Defaults to TWITTER_POST_LIMIT or 20.",
    )
    args = parser.parse_args()

    result = asyncio.run(
        main(
            args.link,
            save_dir=TMP_DIR,
            posts_limit=args.posts_limit,
            lite=args.lite,
        )
    )

    print(
        json.dumps(
            {
                "saved_to": str(TMP_DIR / (LITE_OUTPUT_FILENAME if args.lite else OUTPUT_FILENAME)),
                "username": result["input"]["username"],
                "mode": result["mode"],
                "followersCount": result["profile"].get("followersCount"),
                "followingCount": result["profile"].get("followingCount"),
                "posts": len(result["posts"]),
            },
            indent=2,
            ensure_ascii=False,
            default=json_default,
        )
    )
