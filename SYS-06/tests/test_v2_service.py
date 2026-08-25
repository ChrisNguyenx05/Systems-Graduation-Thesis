import asyncio

from src.alert_dispatcher.channels.dry_run import DryRunChannel
from src.alert_dispatcher.models import AlertPayload
from src.alert_dispatcher.v2_service import AlertDispatcherV2


def _alert(key: str, event: str = "CONTAINER_DOWN") -> AlertPayload:
    return AlertPayload(
        alert_type="INFRASTRUCTURE_FAILURE", level="CRITICAL", source_system="SYS-03",
        event=event, deduplicate_key=key, message="Container is unavailable",
        channels=["telegram", "slack"],
    )


def test_v2_counts_each_channel_delivery_and_queues_next_alert():
    async def scenario():
        channels = {"telegram": DryRunChannel(), "slack": DryRunChannel()}
        dispatcher = AlertDispatcherV2(channels, max_messages=3)
        first = await dispatcher.dispatch(_alert("one"))
        second = await dispatcher.dispatch(_alert("two", "KAFKA_DOWN"))
        assert first.success and not first.rate_limited
        assert second.success and second.rate_limited
        assert dispatcher.health()["queue_depth"] == 1
    asyncio.run(scenario())


def test_v2_suppresses_duplicate_while_alert_is_waiting():
    async def scenario():
        channels = {"telegram": DryRunChannel(), "slack": DryRunChannel()}
        dispatcher = AlertDispatcherV2(channels, max_messages=1)
        await dispatcher.dispatch(_alert("waiting"))
        duplicate = await dispatcher.dispatch(_alert("waiting"))
        assert duplicate.success and duplicate.suppressed
    asyncio.run(scenario())
