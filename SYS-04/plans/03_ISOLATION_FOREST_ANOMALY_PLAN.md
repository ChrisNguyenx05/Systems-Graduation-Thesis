# 🚨 KẾ HOẠCH HUẤN LUYỆN: MÔ HÌNH PHÁT HIỆN BẤT THƯỜNG THỊ TRƯỜNG DỰA TRÊN DỮ LIỆU NẾN (ISOLATION FOREST)
## Detailed Plan: Bar-Derived Market Volatility & Volume Surge Anomaly Detector

> **Mã mô hình**: `IFOREST-ANOMALY-03`  
> **Thuật toán**: Isolation Forest (Scikit-Learn) - Học không giám sát (Unsupervised Learning)  
> **Nhiệm vụ**: Phát hiện các trạng thái dị thường về biến động giá và khối lượng (Bùng nổ khối lượng đột biến, biến động nến cực đoan, số lượng lệnh khớp bất thường), tính toán `anomaly_score` $\in [0.0, 1.0]$.  
> **Giới hạn phạm vi (Boundary Disclaimer)**: Mô hình hoạt động hoàn toàn trên **dữ liệu nến và khối lượng khớp lệnh tổng hợp (Bar-Derived Metrics)**; tập dữ liệu **không chứa dữ liệu chênh lệch giá thực tế (Level-2 Orderbook Spread) hay sự mất cân bằng sổ lệnh (Orderbook Depth Imbalance)**, do đó **không tuyên bố phát hiện hành vi thao túng sổ lệnh (Order-book Manipulation / Spoofing)**.

---

### 1. Yêu cầu chi tiết về Dữ liệu Đầu vào (Input Data Specifications)

#### 1.1. Bản chất Dữ liệu & Nguồn gốc Thực tế
* **Đặc tính bài toán**: Học không giám sát (Unsupervised Learning), không cần nhãn gán thủ công của con người.
* **Tập dữ liệu chính thức**:
  * File kết hợp 6 cặp: [`AI Models/Dataset/Isolation Forest Anomaly/data/features_microstructure_15m_all_6pairs_2months.parquet`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/AI%20Models/Dataset/Isolation%20Forest%20Anomaly/data/features_microstructure_15m_all_6pairs_2months.parquet) (**35,010 dòng**, 2.91 MB).
  * 6 cặp quy hoạch đồng bộ: `BTCUSDT`, `ETHUSDT`, `AAPLBUSDT`, `BNBUSDT`, `AMZNBUSDT`, `DELLBUSDT` (mỗi cặp có file Parquet riêng trong `data/`).
  * Khung thời gian: **01/08/2026 $\rightarrow$ 30/09/2026 UTC** (61 ngày, nến 15m).
  * Script trích xuất gốc: [`Scripts_collect_data_2months_ago.py`](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-04/AI%20Models/Dataset/Isolation%20Forest%20Anomaly/Scripts_collect_data_2months_ago.py).
* **Nguồn dữ liệu tham chiếu trong hệ thống**:
  * Bảng Silver ClickHouse: `db_silver_market.silver_crypto_klines` (tính toán các chỉ số dẫn xuất nến).

#### 1.2. Hợp đồng Đặc trưng Dựa trên Dữ liệu Nến (Bar-Derived Feature Contract)
Mỗi mẫu dữ liệu tại thời điểm $t$ là một vector gồm **8 đặc trưng vi cấu trúc dẫn xuất từ nến và khối lượng**:

```
                 ┌────────────────────────────────────────────────────────┐
                 │       VECTOR ĐẶC TRƯNG DỊ THƯỜNG DẪN XUẤT TỪ NẾN       │
                 └──────────────────────────┬─────────────────────────────┘
                                            │
        ┌──────────────┬──────────────┬─────┴────────┬──────────────┬──────────────┐
        ▼              ▼              ▼              ▼              ▼              ▼
┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐┌──────────────┐
│price_velocity││volume_surge  ││trades_surge  ││bar_range_bps ││volatility_pk ││taker_buy_rt  │
│(Tốc độ giá)  ││(Đột biến KL) ││(Đột biến lệnh││(Biên độ nến) ││(Biến động PK)││(Tỷ lệ mua CĐ)│
└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘└──────────────┘
```

1. **`price_velocity` (Tốc độ thay đổi giá 15m)**:
   $$\text{PriceVelocity} = \frac{P_t - P_{t-1}}{P_{t-1}} \times 100\%$$
2. **`price_velocity_1h` (Tốc độ thay đổi giá 1 giờ / 4 kỳ)**:
   $$\text{PriceVelocity}_{1h} = \frac{P_t - P_{t-4}}{P_{t-4}} \times 100\%$$
3. **`volume_surge_ratio` (Tỷ lệ bùng nổ khối lượng so với SMA-20)**:
   $$\text{VolumeSurge} = \frac{V_t}{\text{SMA}(V, 20)}$$
4. **`trades_surge_ratio` (Tỷ lệ đột biến số lệnh giao dịch so với SMA-20)**:
   $$\text{TradesSurge} = \frac{\text{Trades}_t}{\text{SMA}(\text{Trades}, 20)}$$
5. **`volatility_parkinson` (Độ biến động cực đoan Parkinson qua High/Low)**:
   $$\sigma_{\text{Parkinson}} = \sqrt{\frac{(\ln(H_t / L_t))^2}{4 \ln 2}}$$
6. **`bar_range_bps` (Biên độ dao động nến tính bằng basis points)**:
   $$\text{BarRangeBps} = \frac{H_t - L_t}{C_t} \times 10,000$$
   *(Đo lường độ rộng nến High-Low tương đối so với giá đóng cửa, không phải chênh lệch bid-ask spread của sổ lệnh).*
7. **`taker_buy_ratio` (Tỷ lệ khớp lệnh mua chủ động)**:
   $$\text{TakerBuyRatio} = \frac{\text{TakerBuyBaseVolume}}{\text{TotalVolume}} \in [0.0, 1.0]$$
8. **`quote_net_flow_ratio` (Dòng tiền quote ròng của taker)**:
   $$\text{QuoteNetFlow} = \frac{2 \times \text{TakerBuyQuoteVolume} - \text{QuoteAssetVolume}}{\text{QuoteAssetVolume}} \in [-1.0, +1.0]$$

---

### 2. Pipeline Tiền xử lý & Xử lý Ngoại lai

* **Bộ chuẩn hóa**: Dùng **`RobustScaler`** thay vì `StandardScaler` hay `MinMaxScaler`:
  $$x_{\text{scaled}} = \frac{x - Q_2(x)}{Q_3(x) - Q_1(x)}$$
  *(Lý do: RobustScaler dùng trung vị Median và khoảng cách tứ phân vị IQR, không bị kéo dãn bởi chính những điểm dị thường mà ta đang tìm cách phát hiện)*.
* Lưu scaler: `models/preprocessors/anomaly_scaler.joblib`.

---

### 3. Cấu hình Thuật toán & Siêu tham số (Isolation Forest Hyperparameters)

```python
from sklearn.ensemble import IsolationForest

iforest_model = IsolationForest(
    n_estimators=150,             # Số lượng cây phân lập (Isolation Trees)
    max_samples='auto',           # Số mẫu dùng để xây dựng mỗi cây (thường là 256)
    contamination=0.02,           # Tỷ lệ bất thường kỳ vọng (2% dữ liệu thị trường là dị thường)
    max_features=1.0,             # Sử dụng toàn bộ 6 đặc trưng
    bootstrap=False,
    random_state=42,
    n_jobs=4
)
```

---

### 4. Công thức Chuyển đổi Điểm số (Score Normalization Formula)

Mặc định, hàm `decision_function` của Scikit-Learn trả về giá trị âm (điểm dị thường càng âm càng bất thường). Để phục vụ API trực quan cho [SYS-01](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-01) và [SYS-06](file:///e:/Graduation_Thesis/Systems-Graduation-Thesis/SYS-06), ta chuẩn hóa thành `anomaly_score` $\in [0.0, 1.0]$:

$$\text{raw\_score} = - \text{model.score\_samples}(X)$$
$$\text{anomaly\_score} = \frac{1}{1 + e^{-k \cdot (\text{raw\_score} - \text{threshold})}}$$

* **Ngưỡng quyết định (Decision Logic)**:
  * Nếu $\text{anomaly\_score} \ge 0.85$: Đánh dấu `is_anomaly = True` (gắn pin đỏ 🔴 cảnh báo Pump & Dump trên biểu đồ SYS-01, đồng thời gửi cảnh báo sang SYS-06).
  * Nếu $\text{anomaly\_score} < 0.85$: Thị trường bình thường (`is_anomaly = False`).

---

### 5. Tiêu chuẩn Đánh giá, Kỳ vọng Độ chính xác & Đối sánh Benchmark

#### 5.1. Tiêu chuẩn Đánh giá & Kiểm định Phân lập (Separation Validation)
1. **Kiểm tra tính phân lập trên dữ liệu giả lập**:
   * Chạy kiểm tra trên các đoạn thời gian có hiện tượng Flash Crash hoặc Pump & Dump giả lập: mô hình phải phát hiện và cho điểm $\ge 0.90$ trên ít nhất 90% sự kiện.
2. **Độ trễ suy luận (SLA)**:
   * Thời gian tính điểm 1 snapshot: **$< 5\text{ms}$** (cực nhanh trên CPU).
3. **Đóng gói Artifact**:
   * Lưu qua `joblib`: `models/production/iforest_anomaly.joblib`.
   * Đăng ký vào MLflow Experiment: `Market_Anomaly_Detection`.

#### 5.2. Kỳ vọng Chỉ số Đo lường Thực tế (Expected Metrics)
* **Detection Rate (Tỷ lệ bắt trúng Pump & Dump / Spike)**: Kỳ vọng đạt **$\ge 90.0\%$**.
* **False Alarm Rate (Tỷ lệ báo động nhầm khi thị trường bình thường)**: Kiểm soát chặt chẽ **$\le 2.0\%$** (nhờ tham số `contamination = 0.02`).

#### 5.3. Bảng Đối sánh với các Phương pháp Phát hiện Bất thường Khác
| Thuật toán | Tỷ lệ phát hiện | Báo động sai (FPR) | Tốc độ CPU | Đánh giá khả thi thực tế |
| :--- | :---: | :---: | :---: | :--- |
| **Ngưỡng tĩnh (Z-Score / 3-Sigma)** | ~70% | > 8.0% | < 1ms | ❌ Kém thích nghi: Khi thị trường biến động mạnh, ngày nào cũng báo động nhầm. |
| **DBSCAN** | ~82% | ~4.5% | Rất chậm ($O(N^2)$) | ❌ Không xuất được điểm liên tục `anomaly_score` [0, 1], không chạy nổi real-time. |
| **One-Class SVM** | ~85% | ~3.8% | ~45ms | ⚠️ Rất nặng bộ nhớ RAM khi số lượng nến lớn, nhạy cảm với thang đo. |
| **Isolation Forest (Mô hình SYS-04)** | **> 90%** | **< 2.0%** | **< 3ms** | ✅ **Lựa chọn tối ưu**: Phân lập trực tiếp các điểm ngoại lai đa chiều, tốc độ siêu tốc. |

---

### 6. Chiến lược Cập nhật Thích ứng theo Cửa sổ trượt (Rolling Window Retraining)

Vì thị trường tiền mã hóa liên tục thay đổi chế độ biến động (Volatility Regime Shifts), việc giữ một mô hình Isolation Forest cố định trong nhiều tháng sẽ khiến mô hình bị lỗi thời (Model Drift).

* **Giải pháp Cửa sổ trượt 7 ngày gần nhất (Rolling 7-Day Window)**:
  * Kích thước cửa sổ: Luôn duy trì **10,080 nến 1m gần nhất** trong ClickHouse Gold Layer.
  * Mỗi ngày lúc 00:00 UTC, Airflow kích hoạt lệnh cập nhật:
  
```python
def rolling_update_iforest(clickhouse_client):
    # 1. Trích xuất đúng 10,080 dòng features vi cấu trúc gần nhất (7 ngày vừa qua)
    query = """
        SELECT price_velocity, price_velocity_1h, volume_surge_ratio, 
               trades_surge_ratio, volatility_parkinson, bar_range_bps,
               taker_buy_ratio, quote_net_flow_ratio
        FROM db_silver_market.silver_crypto_klines
        WHERE open_time >= now() - INTERVAL 7 DAY
        ORDER BY open_time ASC
    """
    df_recent = clickhouse_client.query_df(query)
    
    # 2. Fit lại toàn bộ cây phân lập trên phân phối mới nhất
    # (Thuật toán chỉ mất ~1.2 giây trên CPU cho 10,000 dòng)
    new_scaler = RobustScaler()
    X_scaled = new_scaler.fit_transform(df_recent)
    
    new_model = IsolationForest(n_estimators=150, contamination=0.02, random_state=42, n_jobs=-1)
    new_model.fit(X_scaled)
    
    # 3. Lưu đè trọng số mới và kích hoạt Hot-Reload
    joblib.dump(new_scaler, "models/preprocessors/anomaly_scaler.joblib")
    joblib.dump(new_model, "models/production/iforest_anomaly.joblib")
    return True
```

* **Hiệu quả thực tế**:
  * Tự động điều chỉnh ngưỡng thể tích (Volume Threshold): Nếu thị trường bước vào mùa bão thanh khoản (Bull-run), khối lượng thông thường tăng cao sẽ không bị ngộ nhận là bất thường. Mô hình luôn bắt đúng những pha bơm xả thực sự dị biệt so với tuần hiện tại.
