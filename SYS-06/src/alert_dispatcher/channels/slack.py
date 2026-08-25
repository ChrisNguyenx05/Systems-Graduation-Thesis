"""Slack Incoming Webhook Notification Channel Adapter."""

import logging
import os
from typing import Optional, Tuple
import httpx
from src.alert_dispatcher.channels.base import BaseNotificationChannel
from src.alert_dispatcher.formatter import format_slack_payload
from src.alert_dispatcher.models import AlertPayload

logger = logging.getLogger("alert_dispatcher.slack")


class SlackChannel(BaseNotificationChannel):
    """Dispatches structured Block Kit alerts to Slack channels via Incoming Webhooks."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        timeout_seconds: float = 5.0,
    ):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL", "").strip()
        self.timeout_seconds = timeout_seconds

    @property
    def channel_name(self) -> str:
        return "slack"

    def is_configured(self) -> bool:
        return bool(self.webhook_url and self.webhook_url.startswith("http"))

    async def send_alert(self, alert: AlertPayload) -> Tuple[bool, str]:
        if not self.is_configured():
            msg = "Slack Webhook URL is not configured."
            logger.debug(msg)
            return False, msg

        payload = format_slack_payload(alert)

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.post(self.webhook_url, json=payload)
                if response.status_code == 200 and response.text == "ok":
                    logger.info(f"Slack alert sent successfully for event: {alert.event}")
                    return True, "Slack delivery successful (HTTP 200)"
                else:
                    error_msg = f"Slack Webhook error {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    return False, error_msg
        except Exception as ex:
            error_msg = f"Exception sending Slack alert: {str(ex)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
