"""Durable, concurrency-safe SYS-06 dispatch core used by the v2 HTTP service."""

import threading
import time
from collections import deque
from typing import Deque, Dict, Iterable, Tuple

from src.alert_dispatcher.channels.base import BaseNotificationChannel
from src.alert_dispatcher.dedup_cache import DeduplicationCache
from src.alert_dispatcher.formatter import format_telegram_markdown
from src.alert_dispatcher.models import AlertPayload, DispatchResult, NotificationChannel, SeverityLevel


class AlertDispatcherV2:
    """Serializes dispatch state and counts every channel delivery against the quota."""

    def __init__(self, channels: Dict[str, BaseNotificationChannel], max_messages: int = 10, window_seconds: float = 60.0):
        self.channels = channels
        self.max_messages = max_messages
        self.window_seconds = window_seconds
        self.dedup = DeduplicationCache(ttl_seconds=300.0)
        self._sent_at: Deque[float] = deque()
        self._queue: Deque[AlertPayload] = deque()
        self._pending_keys: set[str] = set()
        self._lock = threading.Lock()

    def _targets(self, alert: AlertPayload) -> list[NotificationChannel]:
        configured = {name for name, channel in self.channels.items() if channel.is_configured()}
        targets = {NotificationChannel(channel) for channel in alert.channels}
        if alert.level == SeverityLevel.CRITICAL:
            targets.update(channel for channel in (NotificationChannel.TELEGRAM, NotificationChannel.SLACK) if channel.value in configured)
        elif alert.level == SeverityLevel.INFO and len(targets) > 1:
            targets.discard(NotificationChannel.TELEGRAM)
        return [channel for channel in targets if channel.value in configured]

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._sent_at and self._sent_at[0] <= cutoff:
            self._sent_at.popleft()

    async def dispatch(self, alert: AlertPayload) -> DispatchResult:
        with self._lock:
            return await self._dispatch_locked(alert)

    async def _dispatch_locked(self, alert: AlertPayload) -> DispatchResult:
        result = DispatchResult(success=False, formatted_message=format_telegram_markdown(alert))
        if self.dedup.is_duplicate(alert.deduplicate_key) or alert.deduplicate_key in self._pending_keys:
            result.success = True
            result.suppressed = True
            return result

        targets = self._targets(alert)
        if not targets:
            result.errors.append("No configured channel matches this alert's routing policy.")
            return result

        now = time.time()
        self._prune(now)
        if len(self._sent_at) + len(targets) > self.max_messages:
            self._queue.append(alert)
            self._pending_keys.add(alert.deduplicate_key)
            result.success = True
            result.rate_limited = True

            return result

        successes = 0
        for target in targets:
            channel = self.channels[target.value]
            try:
                ok, detail = await channel.send_alert(alert)
            except Exception as exc:  # Channel isolation: one failure never aborts other targets.
                ok, detail = False, str(exc)
            result.channel_results[target.value] = ok
            if ok:
                successes += 1
            else:
                result.errors.append(f"Channel '{target.value}' delivery failed: {detail}")

        if successes:
            self.dedup.record_alert(alert.deduplicate_key)
            self._sent_at.extend([now] * successes)
            result.success = True
        return result

    async def drain_queue(self) -> int:
        """Process ready queued alerts in FIFO order; leave the first blocked alert in place."""
        delivered = 0
        with self._lock:
            while self._queue:
                alert = self._queue.popleft()
                self._pending_keys.discard(alert.deduplicate_key)
                result = await self._dispatch_locked(alert)
                if result.rate_limited and result.success:
                    return delivered
                if result.success:
                    delivered += 1
        return delivered

    def health(self) -> dict:
        return {
            "status": "ok",
            "queue_depth": len(self._queue),
            "configured_channels": {name: channel.is_configured() for name, channel in self.channels.items()},
        }

