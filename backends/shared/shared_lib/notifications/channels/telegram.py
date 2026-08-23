import logging
import requests
import os

logger = logging.getLogger(__name__)

class TelegramChannel:
    @staticmethod
    def send(chat_id: str, text: str) -> bool:
        """
        Sends telegram message via Bot API.
        """
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            logger.warning("TelegramChannel: Token not configured.")
            return False

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML"
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info(f"TelegramChannel: Sent to {chat_id}")
                return True
            else:
                logger.error(f"TelegramChannel: API Error {resp.text}")
                return False
        except Exception as e:
            logger.error(f"TelegramChannel: Network Error {e}")
            return False
