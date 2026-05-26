import asyncio
import argparse
from urllib.parse import urlparse
import json
from crypto_trade.core.telegram import TELEGRAM
from crypto_trade.core.paths import TELEGRAM_CONFIG, ANALYTICS_DIR
from crypto_trade.core.yaml import get_yaml_value
from crypto_trade.core.env import load_env, get_env
from crypto_trade.core.io import save_json

load_env()

TG_MSG_LIMIT = int(get_yaml_value(TELEGRAM_CONFIG, "TG_MSG_LIMIT"))
TG_API_HASH = get_env("TG_API_HASH")
TG_API_ID = int(get_env("TG_API_ID"))


async def collect_history(channel_name):
    async with TELEGRAM(TG_API_ID, TG_API_HASH) as tg:
        await tg.join_channel(channel_name)
        messages = await tg.collect_messages(channel_name, limit=TG_MSG_LIMIT)
        return messages


def extract_group_name(link: str) -> str:
    path = urlparse(link).path.strip("/")
    return path


async def main(mint, invite_link):
    channel_name = extract_group_name(invite_link)

    data = await collect_history(channel_name)

    save_path = ANALYTICS_DIR / mint / "telegram_messages.json"
    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", required=True)
    parser.add_argument("--invite_link", required=True)
    args = parser.parse_args()
    results = asyncio.run(main(args.mint, args.invite_link))
    print(results)
    with open("tmp/telegram.json", "w", encoding="utf-8") as f:
        json.dump(results, fp=f, indent=2, ensure_ascii=False)