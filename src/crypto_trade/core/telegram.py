from telethon import TelegramClient
from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
import logging
import json
from crypto_trade.core.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()


class TELEGRAM:
    def __init__(self, TG_API_ID, TG_API_HASH, session_path="meme_metrics_session"):
        self.TG_API_ID = TG_API_ID
        self.TG_API_HASH = TG_API_HASH
        self.session_path = str(session_path)
        self.client = TelegramClient(self.session_path, self.TG_API_ID, self.TG_API_HASH)
    async def __aenter__(self):
        await self.client.start()
        return self
    
    async def __aexit__(self, exc_type, exc, traceback):
        await self.client.disconnect()
    
    async def start(self):
        await self.client.start()
        logger.info("TELEGRAM CLIENT CONNECTED")

    async def disconnect(self):
        await self.client.disconnect()
        logger.info("TELEGRAM CLIENT DISCONNECTED")

    async def join_channel(self, channel_name):
        try:
            await self.client(JoinChannelRequest(channel_name))
            logger.info("Joined telegram channel: %s", channel_name)
        except UserAlreadyParticipantError:
            logger.info("Already joined telegram channel: %s", channel_name)

    async def collect_messages(self, channel_name, limit=500):
        messages = []
        async for message in self.client.iter_messages(channel_name, limit):
            msg = {
                "id": message.id,
                "date": str(message.date),
                "sender_id": message.sender_id,
                "text": message.text,
                "views": message.views,
                "forwards": message.forwards,
                "replies": message.replies.replies if message.replies else None,
                "edit_date": str(message.edit_date) if message.edit_date else None,
                "reply_to_msg_id": (
                    message.reply_to.reply_to_msg_id
                    if message.reply_to else None
                ),
                "has_media": message.media is not None
            }
            messages.append(msg)
        return messages

    def serialise_messages(self, messages):
        return json.dumps(messages, ensure_ascii=False, indent=2)


async def main():
    pass


if __name__ == "__main__":
    exit()