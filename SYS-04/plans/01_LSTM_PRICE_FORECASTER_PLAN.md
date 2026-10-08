# 📈 KẾ HOẠCH HUẤN LUYỆN: MÔ HÌNH DỰ BÁO GIÁ LSTM (PRICE FORECASTER)
## Detailed Plan: PyTorch LSTM Sequence-to-Point Forecaster

> **Mã mô hình**: `LSTM-PRICE-01`  
> **Thuật toán**: Long Short-Term Memory (PyTorch 2.x) hoặc Time-Series Transformer (PatchTST)  
> **Nhiệm vụ**: Dự báo giá đóng cửa tương lai tại $t+15m$ và $t+60m$, xác định xu hướng (`BULLISH` / `BEARISH`).

---

### 1. Yêu cầu chi tiết về Dữ liệu Đầu vào (Input Data Specifications)

#### 1.1. Cấu trúc và Nguồn gốc Dữ liệu Thực tế
* **Tập dữ liệu chính thức**:
  * File kết hợp 6 cặp: [`AI Models/Dataset/LSTM Price Forecaster/data/klines_15m_all_6pairs_3months.parquet`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/AI%20Models/Dataset/LSTM%20Price%20Forecaster/data/klines_15m_all_6pairs_3months.parquet) (**44,652 nến**, 2.39 MB).
  * 6 cặp quy hoạch: `BTCUSDT`, `ETHUSDT`, `AAPLBUSDT`, `BNBUSDT`, `AMZNBUSDT`, `DELLBUSDT` (mỗi cặp có file Parquet riêng trong `data/`).
* **Định dạng trường dữ liệu**:
  * Các trường bắt buộc: `symbol`, `open_time`, `open_time_utc`, `close_time`, `close_time_utc`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `number_of_trades`, `taker_buy_base_volume`, `taker_buy_quote_volume`.
  * Chu kỳ nến: **15 phút (`15m`)** (đồng bộ với dữ liệu Ingestion của hệ thống).
  * Khung thời gian: **01/07/2026 $\rightarrow$ 30/09/2026 UTC** (92 ngày).
* **Nguồn trích xuất**: Binance Market Data REST API (tập dữ liệu offline) và ClickHouse Silver (`db_silver_market.silver_crypto_klines`).

#### 1.2. Kỹ thuật Tiền xử lý & Cửa sổ Trượt (Sliding Window)
* **Chiều dài cửa sổ (Sequence Length $L$)**: Đúng **60 timesteps** ($t-59$ đến $t$).
* **Số lượng đặc trưng mỗi timestep ($F$)**: Tối thiểu 5 (`[open, high, low, close, volume]`).
* **Định dạng Tensor đầu vào cho mô hình**:
  $$\mathbf{X} \in \mathbb{R}^{B \times 60 \times 5}$$
  *(Trong đó $B$ là Batch Size, ví dụ $B=64$ hoặc $128$ khi train; $B=1$ khi suy luận API).*
* **Chuẩn hóa (Normalization)**:
  * Áp dụng `MinMaxScaler` trên từng feature, fit **duy nhất** trên tập dữ liệu Train:
    $$x_{\text{scaled}} = \frac{x - x_{\min}}{x_{\max} - x_{\min}}$$
  * Lưu đối tượng scaler ra file `models/preprocessors/price_scaler.joblib` để tái sử dụng chính xác khi suy luận online.

#### 1.3. Nhãn mục tiêu (Target Labels)
* Đối với cửa sổ kết thúc tại thời điểm $t$, nhãn đầu ra là một vector 2 chiều:
  $$\mathbf{y}_t = \begin{bmatrix} P_{t+15m} \\ P_{t+60m} \end{bmatrix}$$
* **Quy tắc phân loại xu hướng (Trend Rule)**:
  $$\text{Trend} = \begin{cases} \text{BULLISH} & \text{nếu } P_{t+15m} > P_t \\ \text{BEARISH} & \text{nếu } P_{t+15m} \le P_t \end{cases}$$

#### 1.4. Chiến lược Phân chia Dữ liệu & Kiểm thử Ngoài Thời Gian (Out-of-Time Splitting)
* Để loại bỏ hoàn toàn **Lookahead Bias** và kiểm chứng khả năng chống chịu **Concept Drift** trong lộ trình thực tế:
  * **Tập Huấn luyện Gốc (Base Train Set)**: Dữ liệu **Tháng 7, 8 và 20 ngày đầu Tháng 9 Năm 2026** (80% chuỗi thời gian). Dùng để fit scaler và huấn luyện Base Model v1.0.0.
  * **Tập Validation Nội bộ (In-Time Validation)**: 10 ngày cuối của Tháng 9/2026 (21/09 $\rightarrow$ 30/09/2026) để tinh chỉnh hyperparameter và Early Stopping.
  * **Tập Kiểm thử Độc lập Ngoài Thời Gian (Out-of-Time - OOT Test Set)**: Dữ liệu **Nửa tháng 11 Năm 2026** (15 ngày). Nằm cách tập train 1.5 tháng mà không có dữ liệu nối tiếp, dùng để đo lường mức độ suy giảm tự nhiên (Degradation Rate) của mô hình.
  * **Giai đoạn Vận hành Thực tế 1 (Pilot Serving & Retrain)**: **Full tháng 1 Năm 2027**. Airflow tự động kích hoạt Fine-tune nạp thêm tri thức mới.
  * **Giai đoạn Vận hành Phục vụ Bảo vệ Tốt nghiệp**: **3 tháng liên tục (Tháng 3 – 5 Năm 2027)**. Toàn bộ chu trình Continuous Training và Hot-Reload chạy live 24/7 để phục vụ hội đồng chấm đồ án.

---

### 2. Thiết kế Kiến trúc Mạng Nơ-ron (PyTorch Architecture)

```
Input Tensor: (Batch, 60, 5)
      │
      ▼
┌──────────────────────────────────────────────┐
│  Layer 1: LSTM Layer (hidden_size=64)        │
│  - bidirectional=False                       │
│  - return_sequences=True                     │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│  Layer 2: Dropout (p=0.20)                   │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│  Layer 3: LSTM Layer (hidden_size=32)        │
│  - return_sequences=False (chỉ lấy last step)│
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│  Layer 4: Dense / Linear Layer (32 -> 16)    │
│  - Activation: ReLU                          │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────┐
│  Output Layer: Linear (16 -> 2)              │
│  - Dự báo đồng thời [P_15m, P_60m]           │
└──────────────────────────────────────────────┘
```

---

### 3. Thiết lập Siêu tham số & Quá trình Huấn luyện (Training Hyperparameters)

| Tham số | Giá trị đề xuất | Giải thích kỹ thuật |
| :--- | :--- | :--- |
| **Loss Function** | **Huber Loss / Smooth L1 Loss** | Ít bị ảnh hưởng bởi các râu nến quét thanh lý cực đoan (spikes) hơn MSE thuần túy. |
| **Optimizer** | **AdamW** | Learning rate $\eta = 10^{-3}$, weight_decay = $10^{-4}$. |
| **LR Scheduler** | **ReduceLROnPlateau** | Giảm learning rate đi 50% nếu validation loss không cải thiện sau 5 epochs. |
| **Batch Size** | `64` hoặc `128` | Cân bằng giữa tốc độ cập nhật gradient và độ ổn định của bộ nhớ GPU/CPU. |
| **Epochs** | `50` | Kết hợp **Early Stopping** (patience = 8 epochs theo val_loss). |

---

### 4. Tiêu chuẩn Đánh giá, Kỳ vọng Độ chính xác & So sánh Benchmark

#### 4.1. Tiêu chuẩn Gate Thăng hạng (Promotion Criteria)
Theo quy định trong [`CONTEXT.md`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/CONTEXT.md) và [`BUILD_PLAN.md`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/docs/BUILD_PLAN.md):
1. **RMSE (Root Mean Square Error)**:
   $$\text{RMSE} = \sqrt{\frac{1}{N}\sum_{i=1}^{N} (y_i - \hat{y}_i)^2}$$
   * Tiêu chuẩn Gate: Mô hình Challenger phải đạt **RMSE giảm tối thiểu $\ge 5\%$** so với mô hình baseline cũ.
2. **Directional Accuracy (Độ chính xác đoán xu hướng)**:
   $$\text{DA} = \frac{1}{N} \sum_{i=1}^N \mathbb{I}\left( \text{sgn}(y_{t+15} - y_t) == \text{sgn}(\hat{y}_{t+15} - y_t) \right) \ge 55\%$$
3. **Độ trễ suy luận (Inference Latency SLA)**:
   * Single-batch pass ($B=1$, 60 nến): **Thời gian suy luận $< 45\text{ms}$** (đáp ứng trọn vẹn SLA toàn hệ thống $< 200\text{ms}$).

#### 4.2. Kỳ vọng Độ chính xác Thực tế (Expected Metrics)
* **Directional Accuracy (DA)**: Kỳ vọng đạt **55.5% – 58.0%** (Vượt trội so với mức đoán ngẫu nhiên 50.0%).
* **Sai số tuyệt đối tỷ lệ (MAPE 15m)**: Kỳ vọng **$< 0.8\% – 1.2\%$**.

#### 4.3. Bảng Đối sánh với các Mô hình Hiện có (Benchmark Comparison)
| Thuật toán | Directional Accuracy | Sai số MAPE (15m) | Tốc độ suy luận (CPU) | Đánh giá khả thi cho Hệ thống |
| :--- | :---: | :---: | :---: | :--- |
| **ARIMA / SARIMAX** | 50.8% – 51.5% | 1.8% – 2.5% | ~5ms | ❌ Không học được phi tuyến tính của crypto. |
| **Random Forest / XGBoost** | 52.5% – 53.5% | 1.4% – 1.8% | ~12ms | ❌ Không có cơ chế nhớ thứ tự thời gian. |
| **PyTorch LSTM (Mô hình SYS-04)** | **55.5% – 58.0%** | **0.8% – 1.2%** | **< 35ms** | ✅ **Tối ưu nhất**: Cân bằng xuất sắc giữa độ chính xác và SLA latency < 50ms. |
| **PatchTST / Transformer** | 57.0% – 59.0% | 0.7% – 1.0% | 150ms – 250ms | ⚠️ Rất nặng, tốn tài nguyên và suy luận chậm gấp 5 lần. |

---

### 5. Kế hoạch Đóng gói Trọng số (Artifact Packaging)

* **Bước 1**: Sau khi huấn luyện đạt chỉ số, chuyển đổi mô hình PyTorch sang **TorchScript JIT (Trace mode)** hoặc **ONNX**:
  ```python
  example_input = torch.randn(1, 60, 5)
  traced_model = torch.jit.trace(model, example_input)
  traced_model.save("models/production/lstm_price_model.pt")
  ```
* **Bước 2**: Ghi nhận toàn bộ thông số, metrics đồ thị và artifact vào MLflow:
  * Experiment: `Price_Trend_Forecasting`
  * Model Name: `Price_Forecast_LSTM`
  * Stage: `Production`

---

### 6. Kỹ thuật Học Gia Tăng (Warm-Start Fine-Tuning) trên Dữ liệu Mới

Sau khi hoàn thành đợt huấn luyện ban đầu trên 7 ngày thu thập đầu tiên, mô hình được cấu hình để tự động cập nhật kiến thức khi có dữ liệu ngày thứ 8 trở đi:

```python
# Script cập nhật gia tăng (Incremental Update)
def fine_tune_lstm(model, new_klines_df, base_weights_path="models/production/lstm_price_model.pt"):
    # 1. Nạp lại trọng số mô hình đã được kiểm định
    model.load_state_dict(torch.load(base_weights_path))
    model.train()
    
    # 2. Sử dụng Learning Rate nhỏ hơn để tránh Catastrophic Forgetting (quên kiến thức cũ)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-4, weight_decay=1e-5)
    criterion = torch.nn.SmoothL1Loss()
    
    # 3. Chuẩn bị DataLoader từ 1,440 nến mới nhất (24 giờ qua)
    new_loader = prepare_sliding_windows(new_klines_df, window_size=60, batch_size=32)
    
    # 4. Chỉ cần chạy 3 - 5 epochs (thời gian chạy < 20 giây trên CPU)
    for epoch in range(4):
        for X_batch, y_batch in new_loader:
            optimizer.zero_grad()
            preds = model(X_batch)
            loss = criterion(preds, y_batch)
            loss.backward()
            optimizer.step()
            
    return model
```

* **Ưu điểm vượt trội**:
  * **Tốc độ**: Cập nhật chỉ trong 15 - 30 giây.
  * **Tránh rớt giá trị**: Trọng số của 7 ngày trước đó đóng vai trò là "Kiến thức nền" (Pre-trained Foundation), dữ liệu ngày mới giúp mô hình điều chỉnh nhạy bén với đợt biến động hiện tại.

---

### 7. Khả năng Mở rộng sang Reinforcement Learning (RL Trading Agent)

Mô hình LSTM đóng vai trò là **Feature Extractor & Perception Layer** cho Tác tử Học tăng cường (RL Agent):
1. **Observation Space (Không gian Quan sát)**:
   $$s_t = \left[ \frac{\hat{P}_{t+15m} - P_t}{P_t}, \text{RSI}_{14}, \text{AnomalyScore}, \text{CurrentPosition} \right]$$
2. **Action Space (Không gian Hành động)**:
   $$a_t \in \{ 0: \text{HOLD}, 1: \text{LONG / BUY}, 2: \text{SHORT / SELL} \}$$
3. **Reward Function (Hàm Phần thưởng)**:
   $$R_t = \text{PnL}_t - \text{Fee}_{\text{Binance}} - \lambda \cdot \text{Drawdown}$$
4. **Cơ chế tự thích ứng**: Khi có nến mới, Agent lưu các bước chuyển đổi $(s, a, r, s')$ vào **Experience Replay Buffer** và thực hiện một số bước gradient update định kỳ.
