"""Example: SYS-03 (Infra Monitor) emitting a container crash alert."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload


def trigger_infra_alert():
    print("[SYS-03] Triggering Infrastructure Crash Alert...")
    payload = AlertPayload(
        alert_type="INFRASTRUCTURE_FAILURE",
        level="CRITICAL",
        source_system="SYS-03",
        event="DOCKER_CONTAINER_DOWN",
        deduplicate_key="infra:container:redis_cache_01",
        message="Container 'redis-cache-01' đã tắt bất thường. Tiến trình cache bị ngắt quãng.",
        receiver_roles=["devops_sre"],
        channels=["telegram", "slack"],
        action_url="http://localhost:3000/d/infra-monitor",
        suggested_action="Kiểm tra `docker logs redis-cache-01` và chạy `docker-compose up -d redis-cache-01`.",
        metadata={
            "container_name": "redis-cache-01",
            "image": "redis:7.2-alpine",
            "exit_code": 1,
            "restart_count": 5
        }
    )

    import asyncio
    engine = AlertDispatcherEngine()
    result = asyncio.run(engine.dispatch(payload))
    print(f"Infra Alert Result: {result.model_dump()}")


if __name__ == "__main__":
    trigger_infra_alert()
