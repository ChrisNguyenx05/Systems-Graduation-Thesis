# 📈 LSTM Price Forecaster - Training & Source Code Documentation

> **Phân hệ**: `SYS-04` - MLOps & Model Governance Center  
> **Vị trí module**: `Systems-Graduation-Thesis/SYS-04/AI Models/src/LSTM Price Forecaster/`  
> **Mục tiêu**: Huấn luyện mạng nơ-ron hồi quy LSTM dự báo chuỗi thời gian giá nến tại $t+15m$ và $t+60m$.

---

## 📁 1. Cấu trúc Module Mã nguồn

```
src/LSTM Price Forecaster/
├── model.py         # Định nghĩa kiến trúc PyTorch LSTM 2 tầng + Dense Head
├── dataset.py       # Cửa sổ trượt 60 nến, phân chia Train/Val/Test theo thời gian, MinMaxScaler
├── evaluate.py      # Bộ đo lường tài chính: RMSE, MAE, MAPE, Directional Accuracy (DA)
├── train.py         # Pipeline huấn luyện tự động (AdamW, HuberLoss, ReduceLROnPlateau, Early Stopping)
└── README.md        # Tài liệu hướng dẫn sử dụng và thông số kỹ thuật (file này)
```

---

## ⚙️ 2. Thông số Kỹ thuật & Thiết kế Toán học

### 2.1. Không gian Đầu vào (Input Tensor)
* **Kích thước đầu vào**: $\mathbf{X} \in \mathbb{R}^{B \times 60 \times 5}$
* **5 Đặc trưng cốt lõi**: `[open, high, low, close, volume]`
* **Độ dài chuỗi ($L$)**: 60 bước thời gian (tương đương 15 giờ liên tục với nến 15m).
* **Chuẩn hóa**: `MinMaxScaler(0, 1)` fit **duy nhất** trên tập Train, lưu tại `models/LSTM Price Forecaster/preprocessors/`.

### 2.2. Mục tiêu Đầu ra (Prediction Targets)
* Vector 2 chiều $\mathbf{y}_t = [P_{t+15m}, P_{t+60m}]$.
* Dự báo đồng thời giá đóng cửa sau 1 nến (15 phút) và 4 nến (1 giờ).
* Xác định xu hướng thị trường:
  $$\text{Trend} = \begin{cases} \text{BULLISH} & \text{nếu } \hat{P}_{t+15m} > P_t \\ \text{BEARISH} & \text{nếu } \hat{P}_{t+15m} \le P_t \end{cases}$$

### 2.3. Kiến trúc Mạng Nơ-ron (`model.py`)
```
Input: (Batch, 60, 5)
  │
  ├── LSTM 1 (input=5, hidden=64, return_seq=True)
  ├── Dropout (p=0.20)
  ├── LSTM 2 (input=64, hidden=32, return_seq=False) -> Vector ngữ cảnh (Batch, 32)
  ├── Dropout (p=0.10)
  ├── Linear (32 -> 16) + ReLU
  └── Linear (16 -> 2) -> Output: [P_15m, P_60m]
```

### 2.4. Hàm Mất mát & Tối ưu hóa
* **Loss Function**: `HuberLoss(delta=1.0)`:
  $$L_\delta(y, \hat{y}) = \begin{cases} \frac{1}{2}(y - \hat{y})^2 & \text{khi } |y - \hat{y}| \le \delta \\ \delta |y - \hat{y}| - \frac{1}{2}\delta^2 & \text{ngược lại} \end{cases}$$
  *(Ưu điểm: Kháng nhiễu cực tốt trước các râu nến quét thanh lý đột ngột)*.
* **Optimizer**: `AdamW` (`lr=0.001`, `weight_decay=0.0001`).
* **LR Scheduler**: `ReduceLROnPlateau(factor=0.5, patience=4)`.
* **Early Stopping**: Tự động ngắt khi validation loss không giảm sau 12 epochs liên tiếp.

---

## 🚀 3. Hướng dẫn Chạy Huấn luyện (CLI Commands)

### Huấn luyện trên cặp chuẩn BTCUSDT (100 Epochs):
```bash
python "Systems-Graduation-Thesis/SYS-04/AI Models/src/LSTM Price Forecaster/train.py" \
  --symbol BTCUSDT \
  --epochs 100 \
  --batch-size 64 \
  --lr 0.001 \
  --patience 12
```

### Huấn luyện toàn bộ 6 cặp tự động (Batch Training):
```bash
python "Systems-Graduation-Thesis/SYS-04/AI Models/src/LSTM Price Forecaster/train_all.py" --skip-existing --epochs 100
```

---

## 📊 4. Đầu ra & Artifacts Sinh ra
Khi quá trình huấn luyện hoàn tất, script tự động xuất:
1. `models/LSTM Price Forecaster/preprocessors/price_scaler_{symbol}.joblib`: Đối tượng chuẩn hóa dùng khi serving online.
2. `models/LSTM Price Forecaster/production/lstm_price_model_{symbol}.pt`: Trọng số PyTorch State Dict.
3. `models/LSTM Price Forecaster/production/lstm_price_model_{symbol}.torchscript`: Mô hình nhị phân biên dịch JIT phục vụ FastAPI với độ trễ $< 35\text{ms}$.
4. `models/LSTM Price Forecaster/production/training_report_{symbol}.json`: Báo cáo chi tiết metrics RMSE, MAE, MAPE, DA trên tập kiểm thử độc lập.
5. `models/LSTM Price Forecaster/production/benchmark_summary_all_symbols.json`: Tổng hợp toàn bộ 6 mô hình.

---

## 📈 5. Bảng So Sánh Hiệu Năng Thực Nghiệm (6 Symbols Benchmark)

| Symbol | Nến Test (OOT) | Epochs Dừng | RMSE ($t+15m$) | RMSE ($t+60m$) | MAPE ($t+15m$) | MAPE ($t+60m$) | DA ($t+60m$) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **BTCUSDT** | 1,259 | 33 | $1,734.08 | $1,845.07 | **1.85%** | **1.99%** | 48.93% |
| **ETHUSDT** | 1,259 | 24 | $98.19 | $99.85 | **3.40%** | **3.48%** | 48.93% |
| **BNBUSDT** | 1,259 | 26 | $31.98 | $28.43 | **3.98%** | **3.50%** | 49.17% |
| **AAPLBUSDT** | 849 | 23 | $1.50 | $1.68 | **0.38%** | **0.41%** | 50.77% |
| **AMZNBUSDT** | 849 | 22 | $1.14 | $1.65 | **0.34%** | **0.53%** | 48.41% |
| **DELLBUSDT** | 849 | 35 | $5.36 | $3.87 | **0.84%** | **0.51%** | 55.71% |

