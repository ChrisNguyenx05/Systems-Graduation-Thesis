"""Integration tests for Alert Dispatcher Engine."""

import asyncio
from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload
from src.alert_dispatcher.rate_limiter import AlertRateLimiter


def test_dispatcher_dry_run_flow():
    async def _test():
        dispatcher = AlertDispatcherEngine(dry_run=True)

        alert = AlertPayload(
            alert_type="MARKET_ANOMALY",
            level="CRITICAL",
            source_system="SYS-01",
            event="PUMP_AND_DUMP_DETECTED",
            deduplicate_key="test_pump_btc",
            message="Simulated pump & dump detection.",
            metadata={"symbol": "BTCUSDT"},
        )

        # 1. First dispatch -> success
        result1 = await dispatcher.dispatch(alert)
        assert result1.success is True
        assert result1.suppressed is False
        assert result1.rate_limited is False
        assert "console" in result1.channel_results or "telegram" in result1.channel_results

        # 2. Immediate duplicate dispatch -> suppressed by dedup cache
        result2 = await dispatcher.dispatch(alert)
        assert result2.success is True
        assert result2.suppressed is True

    asyncio.run(_test())


def test_dispatcher_rate_limiting():
    async def _test():
        limiter = AlertRateLimiter(max_messages=2, window_seconds=60.0)
        dispatcher = AlertDispatcherEngine(dry_run=True, rate_limiter=limiter)

        alert1 = AlertPayload(
            alert_type="INFRASTRUCTURE_FAILURE",
            level="CRITICAL",
            source_system="SYS-03",
            event="CONTAINER_DOWN_1",
            deduplicate_key="key_1",
            message="Container 1 down",
        )
        alert2 = AlertPayload(
            alert_type="INFRASTRUCTURE_FAILURE",
            level="CRITICAL",
            source_system="SYS-03",
            event="CONTAINER_DOWN_2",
            deduplicate_key="key_2",
            message="Container 2 down",
        )
        alert3 = AlertPayload(
            alert_type="INFRASTRUCTURE_FAILURE",
            level="CRITICAL",
            source_system="SYS-03",
            event="CONTAINER_DOWN_3",
            deduplicate_key="key_3",
            message="Container 3 down",
        )

        res1 = await dispatcher.dispatch(alert1)
        res2 = await dispatcher.dispatch(alert2)
        res3 = await dispatcher.dispatch(alert3)

        assert res1.success is True
        assert res2.success is True
        assert res3.rate_limited is True

    asyncio.run(_test())
