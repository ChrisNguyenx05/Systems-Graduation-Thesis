"""Routing logic for alerts based on severity, source system, and target roles."""

from typing import List, Set
from src.alert_dispatcher.models import (
    AlertPayload,
    NotificationChannel,
    ReceiverRole,
    SeverityLevel,
)


class AlertRouter:
    """Calculates active dispatch channels and resolved recipient roles."""

    @staticmethod
    def resolve_channels(alert: AlertPayload, available_channels: Set[str]) -> List[NotificationChannel]:
        """
        Determine final delivery channels based on requested channels and severity policies:
        - CRITICAL alerts always ensure Telegram is included if configured.
        - INFO alerts prefer Slack or Console to minimize noise.
        """
        resolved: Set[NotificationChannel] = set(alert.channels)

        # Policy: CRITICAL alerts must route to Telegram & Slack if available
        if alert.level == SeverityLevel.CRITICAL:
            resolved.add(NotificationChannel.TELEGRAM)
            if "slack" in available_channels:
                resolved.add(NotificationChannel.SLACK)

        # Policy: INFO alerts should drop Telegram push to avoid waking on-call engineers
        elif alert.level == SeverityLevel.INFO:
            if NotificationChannel.TELEGRAM in resolved and (
                NotificationChannel.SLACK in resolved or NotificationChannel.CONSOLE in resolved
            ):
                resolved.discard(NotificationChannel.TELEGRAM)

        return list(resolved)

    @staticmethod
    def resolve_roles(alert: AlertPayload) -> List[ReceiverRole]:
        """
        Ensure default roles are properly assigned if omitted.
        """
        if not alert.receiver_roles:
            return [ReceiverRole.DEVOPS_SRE]
        return alert.receiver_roles
