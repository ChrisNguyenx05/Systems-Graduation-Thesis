# 📈 HƯỚNG DẪN TÍCH HỢP CẢNH BÁO TỪ SYS-01 (BUSINESS DASHBOARD)

Tài liệu hướng dẫn cách kết nối và gửi cảnh báo biến động thị trường, rủi ro luồng tiền từ **`SYS-01`** về **`SYS-06`**.

---

## 1. Các Loại Sự Kiện Cần Gửi

* `PUMP_AND_DUMP_DETECTED` (CRITICAL): Khối lượng khớp lệnh tăng đột biến > 300% trong thời gian ngắn kèm biến động giá lớn.
* `MARKET_VOLATILITY_SPIKE` (WARNING): Độ lệch chuẩn biến động giá vượt ngưỡng an toàn.
* `SUSPICIOUS_ORDER_FLOW` (WARNING): Dòng tiền gom/xả bất thường từ cụm ví/IP.

---

## 2. Cách Tích Hợp Vào Mã Nguồn SYS-01

### Sử dụng Python Alert Client hoặc Gọi Trực Tiếp Dispatcher Engine:

```python
from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload

async def notify_market_anomaly(symbol: str, volume_pct: float, score: float):
    alert = AlertPayload(
        alert_type="MARKET_ANOMALY",
        level="CRITICAL",
        source_system="SYS-01",
        event="PUMP_AND_DUMP_DETECTED",
        deduplicate_key=f"market_anomaly:{symbol}:pump_dump",
        message=f"Phát hiện dấu hiệu thao túng giá trên cặp {symbol}. Volume tăng {volume_pct}%.",
        receiver_roles=["trader", "risk_manager"],
        channels=["telegram", "slack"],
        action_url=f"http://localhost:8501/analytics?symbol={symbol}",
        suggested_action="Tạm dừng auto-trading bot và kiểm tra order book.",
        metadata={
            "symbol": symbol,
            "volume_spike_pct": volume_pct,
            "anomaly_score": score
        }
    )
    engine = AlertDispatcherEngine()
    return await engine.dispatch(alert)
```
