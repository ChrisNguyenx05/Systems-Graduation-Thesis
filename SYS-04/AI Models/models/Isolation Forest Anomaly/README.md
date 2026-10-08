# 📦 Isolation Forest Anomaly - Model Artifacts & Serving Registry

> **Phân hệ**: `SYS-04` - MLOps & Model Governance Center  
> **Vị trí lưu trữ**: `Systems-Graduation-Thesis/SYS-04/AI Models/models/Isolation Forest Anomaly/`  
> **Vai trò**: Lưu trữ các file artifacts mô hình Scikit-Learn đã được calibrate, RobustScaler và cấu hình tham số phục vụ cho FastAPI Real-Time Risk Engine.

---

## 📂 1. Cấu Trúc Thư Mục Lưu Trữ

```
models/Isolation Forest Anomaly/
├── preprocessors/                    # Bộ chuẩn hóa dữ liệu đầu vào RobustScaler
│   ├── scaler_BTCUSDT.joblib
│   ├── scaler_ETHUSDT.joblib
│   ├── scaler_BNBUSDT.joblib
│   ├── scaler_AAPLBUSDT.joblib
│   ├── scaler_AMZNBUSDT.joblib
│   └── scaler_DELLBUSDT.joblib
├── production/                       # Mô hình sẵn sàng phục vụ suy luận trực tuyến
│   ├── isolation_forest_{symbol}.joblib       # Serialized Model Artifact kèm calibration parameters
│   ├── anomaly_report_{symbol}.json           # Báo cáo đánh giá chi tiết phân phối điểm số và top dị thường
│   └── benchmark_summary_all_symbols.json     # Báo cáo đối sánh tổng hợp cả 6 cặp
└── README.md                         # Tài liệu hướng dẫn nạp và tích hợp này
```

---

## ⚡ 2. Hướng Dẫn Tích Hợp Trực Tiếp Với Database (Inference Code)

Ví dụ mã nguồn Python minh họa cách một Risk Engine Service hoặc FastAPI Server nạp mô hình và suy luận trực tiếp từ 20 nến thô truy vấn từ ClickHouse/PostgreSQL:

```python
import joblib
import pandas as pd
from model import MarketAnomalyDetector

# 1. Nạp mô hình đã lưu
detector = MarketAnomalyDetector.load("models/Isolation Forest Anomaly/production/isolation_forest_BTCUSDT.joblib")

# 2. Giả lập 20 nến thô vừa SELECT từ ClickHouse (crypto_klines_gold)
# Chỉ cần chứa các trường thô cơ bản: open, high, low, close, volume, close_time
df_db_candles = pd.DataFrame([
    {
        "open": 83450.0, "high": 84900.0, "low": 83400.0, "close": 84828.0,
        "volume": 2500.0, "number_of_trades": 18500, "close_time": 1790793899999
    },
    # ... 19 nến trước đó ...
])

# 3. Chạy toàn bộ pipeline (Feature Extraction -> Scaling -> Model -> Calibrated Risk)
result = detector.predict_from_raw_candles(df_db_candles)

print("Symbol      :", result["symbol"])
print("Close Price :", f"${result['close_price']:,.2f}")
print("Is Anomaly  :", result["is_anomaly"])
print("Risk Score  :", result["anomaly_score"], f"({result['risk_level']})")
print("Features    :", result["microstructure_features"])
```

### Kết quả trả về mẫu:
```json
{
  "symbol": "BTCUSDT",
  "timestamp": 1790793899999,
  "close_price": 84828.16,
  "is_anomaly": true,
  "anomaly_score": 0.892,
  "risk_level": "CRITICAL_ANOMALY",
  "microstructure_features": {
    "price_velocity": 1.45,
    "price_velocity_1h": 3.82,
    "volume_surge_ratio": 8.55,
    "trades_surge_ratio": 5.03,
    "volatility_parkinson": 0.0085,
    "bar_range_bps": 141.23,
    "taker_buy_ratio": 0.5,
    "quote_net_flow_ratio": 0.0
  }
}
```

---

## 🎯 3. Quy Chuẩn Đánh Giá & SLA Vận Hành (Promotion Gate)

* **Thời gian suy luận (Inference Latency)**: $P_{99} < 10\text{ms}$ / nến trên CPU.
* **Tỷ lệ báo động (Flagged Rate)**: Phải nằm trong khoảng $[1.0\%, 3.0\%]$ trên tập dữ liệu ngoài mẫu (Out-Of-Time).
* **Độ nhạy phân loại (Contrastive Separation)**: Tỷ số `volume_surge_ratio` và `bar_range_bps` của nhóm nến dị thường so với nhóm bình thường phải $> 3.0\times$ (thực tế mô hình đạt $3.4\times - 4.5\times$).
