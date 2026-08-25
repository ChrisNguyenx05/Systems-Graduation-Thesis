# ⚙️ HƯỚNG DẪN TÍCH HỢP CẢNH BÁO TỪ SYS-02 (AIRFLOW PIPELINES)

Tài liệu hướng dẫn cách cấu hình Airflow Callbacks (`on_failure_callback`, `sla_miss_callback`) trong **`SYS-02`** để tự động gửi thông báo lỗi pipeline về **`SYS-06`**.

---

## 1. Các Loại Sự Kiện Cần Gửi

* `DAG_RUN_FAILED` (CRITICAL): Task hoặc DAG ETL gặp ngoại lệ không phục hồi được.
* `SLA_MISSED` (WARNING): Luồng dữ liệu chạy vượt thời gian cam kết SLA.
* `DATA_QUALITY_CHECK_FAILED` (CRITICAL): Dữ liệu không đạt tiêu chuẩn schema / null checks.

---

## 2. Mã Cấu Hình Mẫu Cho Airflow DAG

Đặt hàm callback vào thư mục utils của Airflow hoặc định nghĩa trực tiếp trong DAG:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload

def sys06_airflow_failure_alert(context):
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = str(context.get("execution_date"))
    exception = str(context.get("exception"))

    alert = AlertPayload(
        alert_type="PIPELINE_FAILURE",
        level="CRITICAL",
        source_system="SYS-02",
        event="DAG_RUN_FAILED",
        deduplicate_key=f"pipeline_failure:{dag_id}:{task_id}",
        message=f"Task '{task_id}' trong DAG '{dag_id}' bị lỗi: {exception[:200]}",
        receiver_roles=["data_engineer", "devops_sre"],
        channels=["telegram", "slack"],
        action_url=f"http://localhost:8080/dags/{dag_id}/grid",
        suggested_action="Kiểm tra Airflow Task Logs và retry task.",
        metadata={
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date
        }
    )

    import asyncio
    engine = AlertDispatcherEngine()
    asyncio.run(engine.dispatch(alert))

default_args = {
    "owner": "data_engineer",
    "on_failure_callback": sys06_airflow_failure_alert,
}
```
