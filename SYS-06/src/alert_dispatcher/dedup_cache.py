"""In-memory Deduplication Cache for suppressing repeated alerts within a time window."""

import time
from typing import Dict, Optional


class DeduplicationCache:
    """
    Maintains a record of recently seen alert keys with timestamps.
    Suppresses identical alerts if received within the TTL window (default 300s / 5 mins).
    """

    def __init__(self, ttl_seconds: float = 300.0):
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, float] = {}

    def is_duplicate(self, dedup_key: str, now: Optional[float] = None) -> bool:
        """
        Check if the key was seen within the last `ttl_seconds`.
        Returns True if duplicate (should be suppressed), False if fresh.
        """
        if not dedup_key:
            return False

        current_time = now if now is not None else time.time()
        last_seen = self._cache.get(dedup_key)

        if last_seen is not None:
            if current_time - last_seen < self.ttl_seconds:
                return True

        return False

    def record_alert(self, dedup_key: str, now: Optional[float] = None) -> None:
        """Record the timestamp for the deduplication key."""
        if not dedup_key:
            return
        current_time = now if now is not None else time.time()
        self._cache[dedup_key] = current_time

    def cleanup_expired(self, now: Optional[float] = None) -> int:
        """Remove keys older than ttl_seconds. Returns count of purged keys."""
        current_time = now if now is not None else time.time()
        expired_keys = [
            k for k, timestamp in self._cache.items()
            if current_time - timestamp >= self.ttl_seconds
        ]
        for k in expired_keys:
            del self._cache[k]
        return len(expired_keys)

    def size(self) -> int:
        """Return current cache size."""
        return len(self._cache)

    def clear(self) -> None:
        """Clear all entries."""
        self._cache.clear()
