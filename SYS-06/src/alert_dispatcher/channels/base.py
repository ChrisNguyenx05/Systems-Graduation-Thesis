"""Base interface for all notification channel adapters."""

from abc import ABC, abstractmethod
from typing import Tuple
from src.alert_dispatcher.models import AlertPayload


class BaseNotificationChannel(ABC):
    """Abstract Base Class for notification delivery channels."""

    @property
    @abstractmethod
    def channel_name(self) -> str:
        """Name identifier of the channel (e.g. 'telegram', 'slack', 'console')."""
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if required credentials and endpoints are configured."""
        pass

    @abstractmethod
    async def send_alert(self, alert: AlertPayload) -> Tuple[bool, str]:
        """
        Deliver the alert asynchronously.
        Returns:
            (success: bool, detail_message: str)
        """
        pass
