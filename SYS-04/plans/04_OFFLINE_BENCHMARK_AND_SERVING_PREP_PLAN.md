# ⚡ KẾ HOẠCH KIỂM THỬ OFFLINE, BENCHMARK ĐỘ TRỄ & CHUẨN BỊ CHO SERVING API
## Detailed Plan: Offline Inference Verification, Latency Benchmark & MLflow Registry

> **Mục tiêu**: Kiểm tra tính toàn vẹn của cả 3 mô hình, đo độ trễ suy luận cục bộ (Latency SLA Benchmark), xác nhận định dạng dữ liệu đầu ra và chuẩn bị sẵn các hàm nạp mô hình vào RAM cho FastAPI Serving.

---

### 1. Mục tiêu Kiểm thử trước khi Dựng API (Pre-API Verification Goals)

Trước khi cấu hình web server FastAPI, toàn bộ các mô hình phải được kiểm thử cục bộ nhằm thỏa mãn **4 điều kiện tiên quyết**:
1. **Model Weight Integrity**: Tất cả các file mô hình (`.pt`, `.txt`, `.joblib`) nạp vào bộ nhớ RAM thành công mà không có lỗi thiếu thư viện hay lỗi xung đột phiên bản.
2. **Schema & Shape Conformity**: Đầu ra của các hàm suy luận offline trả về đúng cấu trúc JSON mà [SYS-01 Dashboard](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-01) và [SYS-06 Alert Dispatcher](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-06) yêu cầu.
3. **Latency SLA Compliance**:
   * Mô hình Fraud Detection: $P_{95} < 15\text{ms}$ (Ngưỡng tối đa qua HTTP API là $50\text{ms}$).
   * Mô hình Isolation Forest: $P_{95} < 5\text{ms}$.
   * Mô hình LSTM Price Forecast: $P_{95} < 45\text{ms}$ (Ngưỡng tối đa qua HTTP API là $200\text{ms}$).
4. **Zero-Exception Fallback**: Khi gặp dữ liệu khuyết thiếu hoặc tensor không đúng chiều, hệ thống phải kích hoạt cơ chế fallback an toàn, không bị crash runtime.

---

### 2. Thiết kế Script Kiểm thử Offline (`tests/test_offline_inference.py`)

Kịch bản kiểm thử sẽ mô phỏng toàn bộ quy trình nhận payload và suy luận:

```
┌────────────────────────────────────────────────────────┐
│             KỊCH BẢN KIỂM THỬ SUY LUẬN OFFLINE         │
└──────────────────────────┬─────────────────────────────┘
                           │
       ┌───────────────────┼───────────────────┐
       ▼                   ▼                   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│Test 1: Price │    │Test 2: Fraud │    │Test 3: Forest│
│(60 OHLCV)    │    │(35 Features) │    │(6 Micro feats│
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌──────────────────────────────────────────────────────┐
│ Benchmark 1,000 Vòng lặp: Đo Mean, P50, P95, P99     │
└──────────────────────────────────────────────────────┘
```

#### 2.1. Kiểm thử Test 1: LSTM Price Forecaster
* **Payload đầu vào giả lập**: Chuỗi 60 nến OHLCV:
  ```python
  mock_ohlcv = [
      [64000.0 + i*10, 64050.0 + i*10, 63950.0 + i*10, 64020.0 + i*10, 150.0 + i]
      for i in range(60)
  ]
  ```
* **Quy trình xử lý**:
  1. Nạp `price_scaler.joblib`, chuẩn hóa 60 nến về khoảng $[0, 1]$.
  2. Tạo Tensor PyTorch `torch.tensor(scaled_data, dtype=torch.float32).unsqueeze(0)`.
  3. Chạy `model.forward()`.
  4. Nghịch đảo chuẩn hóa (Inverse transform) để thu được giá thực tế $P_{t+15m}, P_{t+60m}$.
* **Kiểm tra đầu ra**:
  * Giá dự báo $P_{15m}, P_{60m} > 0$.
  * Xác định đúng trend: `BULLISH` nếu $P_{15m} > P_{\text{close\_current}}$, ngược lại `BEARISH`.

#### 2.2. Kiểm thử Test 2: LightGBM Fraud Detector
* **Payload đầu vào giả lập**: Bản ghi giao dịch với 35 thuộc tính nén:
  ```python
  mock_tx = {
      "transaction_id": "TX_TEST_001",
      "amount": 2500.0,
      "features": [2500.0, 1, 10001, 150.0, 1, 2, 0, 1, 15.0, ...] # 35 elements
  }
  ```
* **Quy trình xử lý**:
  1. Tạo `pandas.DataFrame([mock_tx['features']], columns=feature_names)`.
  2. Chạy `booster.predict()`.
* **Kiểm tra đầu ra**:
  * `0.0 <= fraud_probability <= 1.0`.
  * Gán nhãn `risk_level` và `action` chính xác theo các mốc threshold.

#### 2.3. Kiểm thử Test 3: Isolation Forest Bar Anomaly
* **Payload đầu vào giả lập**: Vector 8 đặc trưng nến vi cấu trúc:
  ```python
  # [price_velocity, price_velocity_1h, volume_surge_ratio, trades_surge_ratio,
  #  volatility_parkinson, bar_range_bps, taker_buy_ratio, quote_net_flow_ratio]
  mock_market = [2.5, 4.8, 8.5, 6.2, 0.045, 185.0, 0.85, 0.72] # Bùng nổ volume x8.5 lần + nến giật mạnh 185 bps
  ```
* **Kiểm tra đầu ra**:
  * Tính toán `anomaly_score` $\in [0.0, 1.0]$.
  * Với volume x8.5 lần và nến giật 185 bps, `anomaly_score` phải $\ge 0.85$ và `is_anomaly = True`.

#### 2.4. Kiểm thử Đặc biệt: Out-of-Time Drift Benchmark (Kiểm định lệch thời gian T7-T9 vs T11/2026)
* **Mục tiêu**: Đánh giá khả năng suy luận khi có khoảng hở thời gian (Time Lag ~1.5 đến 2 tháng).
* **Quy trình chạy test**:
  1. Nạp Base Model v1.0.0 (được train trên T7-T9/2026).
  2. Nạp dữ liệu thực tế của **Nửa tháng 11 Năm 2026** (~21,600 nến).
  3. Đo lường chỉ số suy thoái RMSE và Directional Accuracy:
     * Tiêu chuẩn chấp nhận: $\text{Directional Accuracy} \ge 52\%$ (vẫn giữ được ưu thế thống kê trên thị trường mới).
     * Nếu $\text{DA} < 52\%$, hệ thống tự động gắn cờ `DRIFT_TRIGGERED` gửi tín hiệu sang SYS-06 để yêu cầu kích hoạt pipeline Fine-tuning.

---

### 3. Kịch bản Đo Độ trễ (Benchmark Latency Runner)

Mỗi mô hình được chạy suy luận lặp lại **1,000 lần** liên tục để thu thập phân phối thời gian phản hồi:
* **Chỉ số đo lường**:
  * $\text{Mean Latency}$: Thời gian phản hồi trung bình.
  * $P_{50}$ (Median Latency): Thời gian phản hồi của 50% số lượt.
  * $P_{95}$ Latency: Thời gian phản hồi của 95% số lượt (Chỉ số cam kết SLA).
  * $P_{99}$ Latency: Thời gian phản hồi trong trường hợp tải đột biến.
* **Tiêu chuẩn vượt qua (Pass Criteria)**:
  * Không có bất kỳ ngoại lệ (Unhandled Exception) nào trong suốt 1,000 lượt.
  * $P_{95}$ của cả 3 mô hình nằm hoàn toàn trong ngưỡng xanh (dưới 50% ngân sách thời gian của REST API).

---

### 4. Đăng ký Mô hình vào MLflow Registry Cục bộ (Local Registry Promotion)

Sau khi pass kiểm thử benchmark, các model artifacts sẽ được đăng ký chính thức vào MLflow:

1. **Khởi tạo MLflow Experiment**:
   ```bash
   mlflow experiments create --experiment-name MLOps_Production_Suite
   ```
2. **Ghi nhận Run & Log Model**:
   * Log các thông số kiến trúc, hyperparameters và đồ thị loss.
   * Log binary artifacts:
     * `models/production/lstm_price_model.pt`
     * `models/production/lgbm_fraud_model.txt`
     * `models/production/iforest_anomaly.joblib`
     * `models/preprocessors/*.joblib`
3. **Thăng hạng (Promotion to Production)**:
   * Chuyển trạng thái stage sang **`Production`** trên MLflow Model Registry UI (`http://localhost:5000`).

---

### 5. Sẵn sàng Tích hợp vào Tầng FastAPI Serving (Next Step Transition)

Sau khi hoàn thành kế hoạch này:
1. Thư mục `models/production/` và `models/preprocessors/` đã có đầy đủ toàn bộ file trọng số mô hình.
2. Quá trình viết FastAPI serving chỉ đơn giản là:
   * Viết class `ModelManager` để nạp sẵn 3 file trọng số này vào bộ nhớ RAM khi server startup.
   * Viết 3 API routers map trực tiếp payload từ client vào logic suy luận đã kiểm thử thành công.
   * Cung cấp endpoint hot-reload để nạp lại các file này khi có phiên bản mới từ Airflow.

---

### 6. Kịch bản Kiểm thử Tái nạp Trọng số Tức thời (Hot-Reload Simulation Test)

Để kiểm chứng năng lực cập nhật liên tục (Continuous Training) mà không làm rớt kết nối của người dùng:
* **Mô phỏng chu trình nạp lại**:
  ```python
  def test_simulated_hot_reload():
      # 1. Khởi tạo đối tượng ModelManager và load Model Version 1
      manager = ModelManager()
      manager.load_all()
      res1 = manager.predict_price(mock_60_candles)
      assert res1['model_version'] == 'v1.0.0'
      
      # 2. Giả lập quá trình Fine-tuning xong tạo ra file weights mới v1.1.0
      fine_tune_lstm(...) # Ghi đè trọng số mới
      
      # 3. Kích hoạt hàm reload_weights()
      reload_success = manager.reload_weights("price_lstm")
      assert reload_success is True
      
      # 4. Kiểm tra suy luận ngay lập tức mà không restart process
      res2 = manager.predict_price(mock_60_candles)
      assert res2['model_version'] == 'v1.1.0'
  ```
* **Tiêu chí nghiệm thu**: Thời gian thực hiện hàm `reload_weights()` phải **$< 1.5\text{ giây}$**, bộ nhớ RAM không bị rò rỉ (Memory Leak) và con trỏ mô hình trong bộ nhớ được hoán đổi nguyên tử (Atomic Pointer Swap).
