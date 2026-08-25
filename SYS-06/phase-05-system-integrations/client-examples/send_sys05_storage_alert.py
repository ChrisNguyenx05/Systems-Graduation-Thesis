"""Example: SYS-05 (Lakehouse) emitting storage health and data freshness alert."""

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


def trigger_lakehouse_alert():
    print("[SYS-05 Lakehouse] Triggering Storage / Data Freshness Alert...")
    payload = AlertPayload(
        alert_type="STORAGE_FAILURE",
        level="WARNING",
        source_system="SYS-05",
        event="DATASET_FRESHNESS_STALE",
        deduplicate_key="lakehouse:freshness:fact_order_book",
        message="Bảng dữ liệu 'fact_order_book' trong ClickHouse đã quá 20 phút không có bản ghi mới.",
        receiver_roles=["data_engineer"],
        channels=["slack"],
        action_url="http://localhost:8123/play?query=SELECT+max(event_time)+FROM+fact_order_book",
        suggested_action="Kiểm tra lại service Kafka-to-ClickHouse consumer connector.",
        metadata={
            "database": "crypto_lakehouse",
            "table": "fact_order_book",
            "last_record_timestamp": "2026-08-23T11:20:00Z",
            "delay_minutes": 20
        }
    )

    import asyncio
    engine = AlertDispatcherEngine()
    result = asyncio.run(engine.dispatch(payload))
    print(f"Lakehouse Alert Result: {result.model_dump()}")


if __name__ == "__main__":
    trigger_lakehouse_alert()
