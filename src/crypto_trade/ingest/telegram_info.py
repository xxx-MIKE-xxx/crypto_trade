from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from urllib.parse import urlparse
from typing import Any

from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.io import save_json
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.paths import ANALYTICS_DIR, PROJECT_ROOT, TELEGRAM_CONFIG
from crypto_trade.core.telegram import TELEGRAM
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.core.yaml import get_yaml_value

load_env()

OUTPUT_FILENAME = "telegram_messages.json"
LITE_OUTPUT_FILENAME = "telegram_lite.json"

TG_MSG_LIMIT = int(get_yaml_value(TELEGRAM_CONFIG, "TG_MSG_LIMIT"))
TG_API_HASH = get_env("TG_API_HASH")
TG_API_ID = int(get_env("TG_API_ID"))
TG_SESSION_PATH = PROJECT_ROOT / "app_data" / "meme_metrics_session"


async def collect_history(channel_name: str) -> list[dict[str, Any]]:
    async with TELEGRAM(TG_API_ID, TG_API_HASH, session_path=TG_SESSION_PATH) as tg:
        await tg.join_channel(channel_name)
        return await tg.collect_messages(channel_name, limit=TG_MSG_LIMIT)


async def collect_lite(channel_name: str) -> dict[str, Any]:
    async with TELEGRAM(TG_API_ID, TG_API_HASH, session_path=TG_SESSION_PATH) as tg:
        return await tg.collect_lite_info(channel_name)


def extract_group_name(link: str) -> str:
    value = link.strip()

    if value.startswith("@"):
        return value[1:]

    parsed = urlparse(value if "://" in value else f"https://{value}")
    return parsed.path.strip("/")


def telegram_output_path(mint: str, save_dir: Path | None = None, lite: bool = False) -> Path:
    filename = LITE_OUTPUT_FILENAME if lite else OUTPUT_FILENAME
    if save_dir is not None:
        return save_dir / filename

    return ANALYTICS_DIR / mint / filename


async def main(
    mint: str,
    invite_link: str,
    save_dir: Path | None = None,
    lite: bool = False,
) -> dict[str, Any]:
    channel_name = extract_group_name(invite_link)

    if lite:
        output = {
            "time": utc_now_iso_ms_z(),
            "mode": "lite",
            "mint": mint,
            "invite_link": invite_link,
            "channel_name": channel_name,
            "metrics": await collect_lite(channel_name),
        }
    else:
        messages = await collect_history(channel_name)
        output = {
            "time": utc_now_iso_ms_z(),
            "mode": "messages",
            "mint": mint,
            "invite_link": invite_link,
            "channel_name": channel_name,
            "message_count": len(messages),
            "messages": messages,
        }

    save_json(telegram_output_path(mint, save_dir, lite=lite), output)
    return output


if __name__ == "__main__":
    configure_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True)
    parser.add_argument("--invite-link", "--invite_link", dest="invite_link", required=True)
    parser.add_argument("--lite", action="store_true")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Optional output directory. Defaults to data/raw/analytics/<mint>/",
    )
    args = parser.parse_args()

    result = asyncio.run(
        main(
            mint=args.mint,
            invite_link=args.invite_link,
            save_dir=args.out_dir,
            lite=args.lite,
        )
    )

    print(
        json.dumps(
            {
                "mint": result["mint"],
                "channel_name": result["channel_name"],
                "mode": result["mode"],
                "saved_to": str(telegram_output_path(args.mint, args.out_dir, lite=args.lite)),
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        )
    )
