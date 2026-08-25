"""Core Dispatcher Engine for SYS-06."""

import logging
import os
from typing import Dict, List, Optional
from src.alert_dispatcher.channels.base import BaseNotificationChannel
from src.alert_dispatcher.channels.dry_run import DryRunChannel
from src.alert_dispatcher.channels.slack import SlackChannel
from src.alert_dispatcher.channels.telegram import TelegramChannel
from src.alert_dispatcher.dedup_cache import DeduplicationCache
from src.alert_dispatcher.formatter import format_telegram_markdown
from src.alert_dispatcher.models import (
    AlertPayload,
    DispatchResult,
    NotificationChannel,
)
from src.alert_dispatcher.rate_limiter import AlertRateLimiter
from src.alert_dispatcher.router import AlertRouter

logger = logging.getLogger("alert_dispatcher.engine")


class AlertDispatcherEngine:
    """
    Main orchestration service for validating, deduplicating, throttling, routing,
    and delivering operational alerts across multiple notification channels.
    """

    def __init__(
        self,
        dedup_cache: Optional[DeduplicationCache] = None,
        rate_limiter: Optional[AlertRateLimiter] = None,
        dry_run: Optional[bool] = None,
        channels: Optional[Dict[str, BaseNotificationChannel]] = None,
    ):
        self.dedup_cache = dedup_cache or DeduplicationCache(ttl_seconds=300.0)
        self.rate_limiter = rate_limiter or AlertRateLimiter(max_messages=10, window_seconds=60.0)
        
        # Determine dry run mode
        if dry_run is not None:
            self.dry_run = dry_run
        else:
            self.dry_run = os.getenv("ALERT_DRY_RUN", "true").lower() in ("true", "1", "yes")

        # Initialize delivery channels
        if channels is not None:
            self.channels = channels
        else:
            self.channels = self._init_default_channels()

    def _init_default_channels(self) -> Dict[str, BaseNotificationChannel]:
        adapters: Dict[str, BaseNotificationChannel] = {}
        
        if self.dry_run:
            adapters["console"] = DryRunChannel()
            adapters["telegram"] = DryRunChannel()
            adapters["slack"] = DryRunChannel()
            logger.info("Alert Dispatcher initialized in DRY-RUN mode (all channels route to console).")
        else:
            adapters["telegram"] = TelegramChannel()
            adapters["slack"] = SlackChannel()
            adapters["console"] = DryRunChannel()
            logger.info("Alert Dispatcher initialized in PRODUCTION mode.")

        return adapters

    async def dispatch(self, alert: AlertPayload) -> DispatchResult:
        """
        Process and deliver a single alert payload through the full pipeline.
        """
        result = DispatchResult(success=False)
        result.formatted_message = format_telegram_markdown(alert)

        # 1. Deduplication Filter (300s window)
        if self.dedup_cache.is_duplicate(alert.deduplicate_key):
            logger.info(f"Duplicate alert suppressed: '{alert.deduplicate_key}'")
            result.suppressed = True
            result.success = True
            return result

        # 2. Rate Limiting Check (10 msgs / min)
        if not self.rate_limiter.allow_send():
            logger.warning(f"Rate limit reached. Enqueueing alert: '{alert.event}'")
            enqueued = self.rate_limiter.enqueue_delayed(alert)
            result.rate_limited = True
            result.success = enqueued
            if not enqueued:
                result.errors.append("Rate limit buffer queue overflow. Alert dropped.")
            return result

        # 3. Route Resolution
        available_names = set(self.channels.keys())
        target_channels = AlertRouter.resolve_channels(alert, available_names)

        # 4. Dispatch to resolved channels
        sent_any = False
        for channel_enum in target_channels:
            chan_name = channel_enum.value if hasattr(channel_enum, "value") else str(channel_enum)
            channel_adapter = self.channels.get(chan_name)
            
            if not channel_adapter:
                # Fallback to console if channel not found
                channel_adapter = self.channels.get("console")

            if channel_adapter:
                try:
                    success, detail = await channel_adapter.send_alert(alert)
                    result.channel_results[chan_name] = success
                    if success:
                        sent_any = True
                    else:
                        result.errors.append(f"Channel '{chan_name}' delivery failed: {detail}")
                except Exception as ex:
                    err = f"Exception in channel '{chan_name}': {str(ex)}"
                    logger.error(err, exc_info=True)
                    result.channel_results[chan_name] = False
                    result.errors.append(err)

        # 5. Record state on successful dispatch
        if sent_any:
            self.dedup_cache.record_alert(alert.deduplicate_key)
            self.rate_limiter.record_send()
            result.success = True
            logger.info(f"Alert successfully dispatched: '{alert.event}' ({alert.deduplicate_key})")
        else:
            if not result.errors:
                result.errors.append("No active or configured notification channels were available.")
            logger.error(f"Failed to dispatch alert '{alert.event}': {result.errors}")

        return result
