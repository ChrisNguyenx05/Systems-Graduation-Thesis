"""Rate Limiter to throttle outbound notifications and prevent API bans and alert fatigue."""

from collections import deque
import time
from typing import Deque, Optional
from src.alert_dispatcher.models import AlertPayload


class AlertRateLimiter:
    """
    Sliding window rate limiter with an in-memory queue buffer.
    Target: Max N messages per minute (default: 10 msgs / 60 seconds).
    """

    def __init__(self, max_messages: int = 10, window_seconds: float = 60.0, max_queue_size: int = 100):
        self.max_messages = max_messages
        self.window_seconds = window_seconds
        self.max_queue_size = max_queue_size
        self._timestamps: Deque[float] = deque()
        self._buffer_queue: Deque[AlertPayload] = deque()

    def allow_send(self, now: Optional[float] = None) -> bool:
        """
        Check if an alert is allowed to be dispatched immediately.
        Prunes timestamps outside the current sliding window.
        """
        current_time = now if now is not None else time.time()
        cutoff = current_time - self.window_seconds

        # Evict timestamps older than window
        while self._timestamps and self._timestamps[0] <= cutoff:
            self._timestamps.popleft()

        return len(self._timestamps) < self.max_messages

    def record_send(self, now: Optional[float] = None) -> None:
        """Record the timestamp of a sent message."""
        current_time = now if now is not None else time.time()
        self._timestamps.append(current_time)

    def enqueue_delayed(self, alert: AlertPayload) -> bool:
        """
        Place alert into the delayed buffer queue when rate limit is exceeded.
        Returns False if buffer is full (overflow drop).
        """
        if len(self._buffer_queue) >= self.max_queue_size:
            return False
        self._buffer_queue.append(alert)
        return True

    def dequeue_next(self) -> Optional[AlertPayload]:
        """Retrieve next pending alert from buffer queue if available."""
        if self._buffer_queue:
            return self._buffer_queue.popleft()
        return None

    def queue_size(self) -> int:
        """Return number of queued alerts waiting in buffer."""
        return len(self._buffer_queue)

    def clear(self) -> None:
        """Clear timestamps and queue buffer."""
        self._timestamps.clear()
        self._buffer_queue.clear()
