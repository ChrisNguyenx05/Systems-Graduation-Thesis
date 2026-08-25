"""Unit tests for Rate Limiter and buffer queue."""

from src.alert_dispatcher.models import AlertPayload
from src.alert_dispatcher.rate_limiter import AlertRateLimiter


def test_rate_limiter_threshold():
    limiter = AlertRateLimiter(max_messages=3, window_seconds=60.0)

    t0 = 1000.0
    # First 3 allowed
    for _ in range(3):
        assert limiter.allow_send(now=t0)
        limiter.record_send(now=t0)

    # 4th disallowed within same window
    assert not limiter.allow_send(now=t0 + 10.0)

    # After window expiration (61s later), allowed again
    assert limiter.allow_send(now=t0 + 61.0)


def test_rate_limiter_buffer_queue():
    limiter = AlertRateLimiter(max_messages=1, window_seconds=60.0, max_queue_size=2)
    alert = AlertPayload(
        alert_type="SYSTEM_HEALTH",
        level="INFO",
        source_system="SYS-06",
        event="HEALTH_CHECK_OK",
        deduplicate_key="health_check_01",
        message="System healthy",
    )

    assert limiter.enqueue_delayed(alert)
    assert limiter.enqueue_delayed(alert)
    # Queue overflow
    assert not limiter.enqueue_delayed(alert)
    assert limiter.queue_size() == 2

    popped = limiter.dequeue_next()
    assert popped is not None
    assert limiter.queue_size() == 1
