"""Telegram Bot API Notification Channel Adapter."""

import logging
import os
from typing import Optional, Tuple
import httpx
from src.alert_dispatcher.channels.base import BaseNotificationChannel
from src.alert_dispatcher.formatter import format_telegram_markdown
from src.alert_dispatcher.models import AlertPayload

logger = logging.getLogger("alert_dispatcher.telegram")


class TelegramChannel(BaseNotificationChannel):
    """Dispatches alerts to Telegram Group / Channel via Bot API."""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        timeout_seconds: float = 5.0,
    ):
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.timeout_seconds = timeout_seconds

    @property
    def channel_name(self) -> str:
        return "telegram"

    def is_configured(self) -> bool:
        return bool(self.bot_token and self.chat_id and not self.bot_token.startswith("YOUR_"))

    async def send_alert(self, alert: AlertPayload) -> Tuple[bool, str]:
        if not self.is_configured():
            msg = "Telegram Bot Token or Chat ID is not configured."
            logger.warning(msg)
            return False, msg

        formatted_text = format_telegram_markdown(alert)
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": formatted_text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(url, json=payload)
                if response.status_code == 200:
                    logger.info(f"Telegram alert sent successfully for event: {alert.event}")
                    return True, "Telegram delivery successful (HTTP 200)"
                else:
                    error_msg = f"Telegram API error {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    return False, error_msg
        except Exception as ex:
            error_msg = f"Exception sending Telegram alert: {str(ex)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
