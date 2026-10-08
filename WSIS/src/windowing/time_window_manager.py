"""Time Window Management for 15-Minute Clock Alignment with Airflow."""
import time
from datetime import datetime, timezone

class TimeWindowManager:
    def __init__(self, window_duration_ms: int = 900000, grace_period_seconds: int = 10):
        self.window_duration_ms = window_duration_ms
        self.grace_period_seconds = grace_period_seconds

    @staticmethod
    def get_window_start(timestamp_ms: int, window_duration_ms: int = 900000) -> int:
        """Returns the floor timestamp of the 15-minute clock boundary."""
        return timestamp_ms - (timestamp_ms % window_duration_ms)

    @staticmethod
    def get_window_run_key(window_start_ms: int) -> str:
        """Generates exact same run_key as Airflow: YYYYMMDDTHHMMSSZ."""
        dt = datetime.fromtimestamp(window_start_ms / 1000, timezone.utc)
        return dt.strftime("%Y%m%dT%H%M%SZ")

    def current_window_bounds(self) -> tuple[int, int, str]:
        """Returns (window_start_ms, window_end_ms, run_key) for the current moment."""
        now_ms = int(time.time() * 1000)
        start_ms = self.get_window_start(now_ms, self.window_duration_ms)
        end_ms = start_ms + self.window_duration_ms
        return start_ms, end_ms, self.get_window_run_key(start_ms)

    def seconds_until_next_flush(self) -> float:
        """Calculates seconds to wait until current 15m window finishes + grace period."""
        now_ms = int(time.time() * 1000)
        current_start = self.get_window_start(now_ms, self.window_duration_ms)
        next_boundary_ms = current_start + self.window_duration_ms
        flush_target_ms = next_boundary_ms + (self.grace_period_seconds * 1000)
        remaining_sec = (flush_target_ms - now_ms) / 1000.0
        return max(0.1, remaining_sec)
