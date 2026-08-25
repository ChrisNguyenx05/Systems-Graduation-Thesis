"""Unit tests for Deduplication Cache."""

from src.alert_dispatcher.dedup_cache import DeduplicationCache


def test_deduplication_suppression():
    cache = DeduplicationCache(ttl_seconds=300.0)
    key = "infra:kafka:down"
    
    # First time: not duplicate
    assert not cache.is_duplicate(key, now=1000.0)
    cache.record_alert(key, now=1000.0)

    # 100 seconds later (within 300s): is duplicate
    assert cache.is_duplicate(key, now=1100.0)

    # 299 seconds later: still duplicate
    assert cache.is_duplicate(key, now=1299.0)

    # 301 seconds later: expired, not duplicate
    assert not cache.is_duplicate(key, now=1301.0)


def test_deduplication_cleanup():
    cache = DeduplicationCache(ttl_seconds=300.0)
    cache.record_alert("key1", now=1000.0)
    cache.record_alert("key2", now=1100.0)
    cache.record_alert("key3", now=1400.0)

    # At t=1350.0: key1 (age 350s) is expired, key2 (age 250s) & key3 (future) remain
    purged = cache.cleanup_expired(now=1350.0)
    assert purged == 1
    assert cache.size() == 2
