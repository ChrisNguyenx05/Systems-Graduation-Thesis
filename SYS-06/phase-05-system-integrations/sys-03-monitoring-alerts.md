# 🖥️ HƯỚNG DẪN TÍCH HỢP CẢNH BÁO TỪ SYS-03 (INFRASTRUCTURE OPERATIONS)

Tài liệu hướng dẫn cách kết nối hệ thống cảnh báo từ Prometheus Alertmanager / Docker Health Check trong **`SYS-03`** về **`SYS-06`**.

---

## 1. Các Loại Sự Kiện Cần Gửi

* `DOCKER_CONTAINER_DOWN` (CRITICAL): Container dịch vụ (Kafka, MinIO, ClickHouse) bị sập.
* `DISK_USAGE_CRITICAL` (CRITICAL): Bộ nhớ ổ đĩa vượt quá 85%.
* `KAFKA_CONSUMER_LAG_HIGH` (WARNING): Độ trễ đọc thông điệp từ Kafka vượt ngưỡng 10,000 bản ghi.

---

## 2. Mã Mẫu Gửi Cảnh Báo Hạ Tầng

```python
import asyncio
from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload

async def notify_container_down(container_name: str, exit_code: int):
    alert = AlertPayload(
        alert_type="INFRASTRUCTURE_FAILURE",
        level="CRITICAL",
        source_system="SYS-03",
        event="DOCKER_CONTAINER_DOWN",
        deduplicate_key=f"infra_failure:{container_name}:down",
        message=f"Container '{container_name}' đã dừng đột ngột với Exit Code {exit_code}.",
        receiver_roles=["devops_sre"],
        channels=["telegram", "slack"],
        action_url="http://localhost:3000/d/infra-monitor",
        suggested_action=f"Khởi động lại dịch vụ: `docker-compose up -d {container_name}`",
        metadata={
            "container_name": container_name,
            "exit_code": exit_code
        }
    )
    engine = AlertDispatcherEngine()
    return await engine.dispatch(alert)
```
