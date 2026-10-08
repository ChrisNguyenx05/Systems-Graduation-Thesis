# 📦 LSTM Price Forecaster - Model Artifacts & Serving Registry

> **Phân hệ**: `SYS-04` - MLOps & Model Governance Center  
> **Vị trí lưu trữ**: `Systems-Graduation-Thesis/SYS-04/AI Models/models/LSTM Price Forecaster/`  
> **Vai trò**: Lưu trữ các file artifacts, trọng số nhị phân và đối tượng tiền xử lý sẵn sàng phục vụ cho FastAPI Real-Time Serving.

---

## 📂 1. Cấu trúc Thư mục Lưu trữ

```
models/LSTM Price Forecaster/
├── preprocessors/                    # Bộ chuẩn hóa dữ liệu đầu vào cho từng mã
│   ├── price_scaler_BTCUSDT.joblib
│   ├── price_scaler_ETHUSDT.joblib
│   ├── price_scaler_BNBUSDT.joblib
│   ├── price_scaler_AAPLBUSDT.joblib
│   ├── price_scaler_AMZNBUSDT.joblib
│   └── price_scaler_DELLBUSDT.joblib
├── production/                       # Mô hình sẵn sàng phục vụ suy luận
│   ├── lstm_price_model_{symbol}.pt           # PyTorch state dict weights (6 cặp)
│   ├── lstm_price_model_{symbol}.torchscript  # TorchScript JIT compiled model (6 cặp, < 35ms inference)
│   ├── training_report_{symbol}.json          # Báo cáo đánh giá chất lượng chi tiết từng mã
│   └── benchmark_summary_all_symbols.json     # Báo cáo tổng hợp đối sánh 6 mã
└── README.md                         # Tài liệu thuyết minh này
```

---

## ⚡ 2. Hướng dẫn Nạp Mô hình phục vụ Suy luận (Inference Example)

Mã nguồn Python minh họa cách FastAPI Server hoặc Client nạp mô hình TorchScript và dự đoán:

```python
import torch
import joblib
import numpy as np

# 1. Nạp preprocessor và model TorchScript
scaler = joblib.load("models/LSTM Price Forecaster/preprocessors/price_scaler_BTCUSDT.joblib")
model = torch.jit.load("models/LSTM Price Forecaster/production/lstm_price_model_BTCUSDT.torchscript")
model.eval()

# 2. Đầu vào: 60 nến OHLCV gần nhất dạng numpy array (60, 5)
recent_candles_raw = np.array([...]) # shape (60, 5)

# 3. Chuẩn hóa qua Scaler
recent_candles_scaled = scaler.transform(recent_candles_raw)
input_tensor = torch.tensor(recent_candles_scaled, dtype=torch.float32).unsqueeze(0) # shape (1, 60, 5)

# 4. Dự báo siêu tốc (< 35ms)
with torch.no_grad():
    preds_scaled = model(input_tensor).numpy()[0] # [pred_15m_scaled, pred_60m_scaled]

# 5. Khôi phục giá trị thực USD
close_min = scaler.data_min_[3]
close_max = scaler.data_max_[3]
close_range = close_max - close_min

pred_15m_usd = preds_scaled[0] * close_range + close_min
pred_60m_usd = preds_scaled[1] * close_range + close_min

current_close = recent_candles_raw[-1, 3]
trend = "BULLISH" if pred_15m_usd > current_close else "BEARISH"

print(f"Current Price : ${current_close:,.2f}")
print(f"Predicted 15m : ${pred_15m_usd:,.2f} ({trend})")
print(f"Predicted 60m : ${pred_60m_usd:,.2f}")
```

---

## 📋 3. Quy chuẩn Đánh giá Chất lượng (Promotion SLA Gate)
* **MAPE (15m)**: Phải $< 1.2\%$ trên tập Test.
* **Directional Accuracy (Xu hướng)**: Phải $> 55.0\%$ (vượt trội so với ngưỡng ngẫu nhiên 50%).
* **Độ trễ suy luận TorchScript**: $P_{95} < 35\text{ms}$ khi chạy trên CPU.
