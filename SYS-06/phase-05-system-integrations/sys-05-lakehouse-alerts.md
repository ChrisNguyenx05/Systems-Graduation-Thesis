# 🗄️ HƯỚNG DẪN TÍCH HỢP CẢNH BÁO TỪ SYS-05 (LAKEHOUSE & STORAGE)

Tài liệu hướng dẫn kết nối cảnh báo khả dụng lưu trữ (MinIO / ClickHouse / PostgreSQL) và độ tươi mới dữ liệu (Data Freshness) từ **`SYS-05`** về **`SYS-06`**.

---

## 1. Các Loại Sự Kiện Cần Gửi

* `CLICKHOUSE_CLUSTER_UNREACHABLE` (CRITICAL): Mất kết nối tới máy chủ ClickHouse.
* `MINIO_STORAGE_UNAVAILABLE` (CRITICAL): Dịch vụ lưu trữ đối tượng MinIO bị ngắt.
* `DATASET_FRESHNESS_STALE` (WARNING): Bảng dữ liệu quan trọng không nhận thêm bản ghi mới vượt quá ngưỡng cho phép (15 phút).

---

## 2. Mã Mẫu Kiểm Tra Sức Khỏe Kho Dữ Liệu

```python
import asyncio
from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload

async def notify_lakehouse_outage(service_name: str, port: int, error_details: str):
    alert = AlertPayload(
        alert_type="STORAGE_FAILURE",
        level="CRITICAL",
        source_system="SYS-05",
        event=f"{service_name.upper()}_UNREACHABLE",
        deduplicate_key=f"storage_failure:{service_name}:outage",
        message=f"Không thể kết nối tới dịch vụ lưu trữ {service_name} trên cổng {port}: {error_details}.",
        receiver_roles=["data_engineer", "devops_sre"],
        channels=["telegram", "slack"],
        action_url=f"http://localhost:{port}/health",
        suggested_action=f"Kiểm tra trạng thái container {service_name} và log mạng.",
        metadata={
            "service": service_name,
            "port": port,
            "error": error_details
        }
    )
    engine = AlertDispatcherEngine()
    return await engine.dispatch(alert)
```
