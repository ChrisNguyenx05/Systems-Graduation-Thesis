# 🤖 HƯỚNG DẪN TÍCH HỢP CẢNH BÁO TỪ SYS-04 (MLOPS & FRAUD CENTER)

Tài liệu hướng dẫn cách kết nối cảnh báo gian lận và giám sát chất lượng mô hình (Model Drift / Latency) từ **`SYS-04`** về **`SYS-06`**.

---

## 1. Các Loại Sự Kiện Cần Gửi

* `HIGH_FRAUD_TRANSACTION_FLAGGED` (CRITICAL): Giao dịch nghi vấn gian lận với Fraud Score > 0.85.
* `DATA_DRIFT_DETECTED` (WARNING): Dữ liệu thực tế bị trôi dạt so với phân phối dữ liệu huấn luyện.
* `MODEL_SERVING_LATENCY_HIGH` (WARNING): Thời gian phản hồi dự đoán API vượt ngưỡng 500ms.

---

## 2. Mã Mẫu Bắn Cảnh Báo Khi Phát Hiện Gian Lận

```python
import asyncio
from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload

async def notify_fraud_detected(tx_id: str, amount: float, fraud_score: float, flags: list):
    alert = AlertPayload(
        alert_type="FRAUD_TRANSACTION",
        level="CRITICAL",
        source_system="SYS-04",
        event="HIGH_FRAUD_TRANSACTION_FLAGGED",
        deduplicate_key=f"fraud_tx:{tx_id}:high_risk",
        message=f"Phát hiện giao dịch nghi vấn gian lận ID '{tx_id}' (${amount:,.2f}) với Fraud Score {fraud_score:.2f}.",
        receiver_roles=["risk_manager", "compliance"],
        channels=["telegram", "slack"],
        action_url=f"http://localhost:5000/fraud/tx/{tx_id}",
        suggested_action="Tạm giữ giao dịch và kích hoạt quy trình xác thực bổ sung.",
        metadata={
            "transaction_id": tx_id,
            "amount_usd": amount,
            "fraud_score": fraud_score,
            "violations": flags
        }
    )
    engine = AlertDispatcherEngine()
    return await engine.dispatch(alert)
```
