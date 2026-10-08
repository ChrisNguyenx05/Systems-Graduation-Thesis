# 🌲 Isolation Forest Market Anomaly Detector

> **Phân hệ**: `SYS-04` - MLOps & Model Governance Center  
> **Thư mục**: `AI Models/src/Isolation Forest Anomaly/`  
> **Mục tiêu**: Nhận diện dị thường vi cấu trúc thị trường không giám sát (Unsupervised Anomaly Detection) cho 6 cặp tài sản số và RWA trên khung nến 15 phút.

---

## 📐 1. Nguyên Lý Toán Học & Thuật Toán

Mô hình triển khai thuật toán **Isolation Forest** (Liu, Ting, Zhou - 2008), dựa trên hai đặc tính nền tảng của các điểm dị thường trên thị trường tài chính:
1. Số lượng rất ít (thiểu số - minority).
2. Mang giá trị thuộc tính khác biệt xa so với các điểm dữ liệu chuẩn (outlying attribute values).

### 1.1. Cấu Trúc Cây Cách Ly (iTree)
Với tập dữ liệu con kích thước $\psi = 256$ chọn ngẫu nhiên không hoàn lại, một iTree được sinh bằng cách chia đệ quy một thuộc tính $q$ ngẫu nhiên theo một điểm chia ngẫu nhiên $p \in [\min(q), \max(q)]$ cho đến khi:
- Cây đạt độ sâu giới hạn $h_{\max} = \lceil \log_2(\psi) \rceil = 8$.
- Hoặc nút chỉ còn 1 điểm dữ liệu.

Độ dài đường đi $h(x)$ của điểm $x$ qua $t$ cây iTrees được tính bằng số cạnh $x$ đi qua từ gốc đến lá:
$$E(h(x)) = \frac{1}{t} \sum_{i=1}^t h_i(x)$$

### 1.2. Điểm Số Dị Thường Gốc (Raw Score)
$$s(x, \psi) = 2^{-\frac{E(h(x))}{c(\psi)}}$$
Trong đó $c(\psi) = 2 \ln(\psi - 1) + 0.5772156649 - \frac{2(\psi - 1)}{\psi}$ là độ dài đường đi trung bình của phép phân chia nhị phân.

### 1.3. Chuẩn Hóa Điểm Rủi Ro Vận Hành (Calibrated Risk Score $\in [0.0, 1.0]$)
Để tích hợp mượt mà vào **Risk Engine** và **Circuit Breaker** của sàn giao dịch, hàm `decision_function(x)` của Scikit-Learn (ngưỡng dị thường tại $0.0$) được chuẩn hóa phi tuyến tính:
- $df \ge 0.0$ (Điểm bình thường): Điểm số ánh xạ tuyến tính trong dải $[0.05, 0.65)$.
- $df < 0.0$ (Điểm dị thường): Điểm số ánh xạ từ $[0.65, 1.0]$.

### 1.4. Phân Cấp Mức Độ Rủi Ro (Risk Levels)
| Điểm số ($S$) | Cấp độ rủi ro | Hành động hệ thống |
| :---: | :---: | :--- |
| $S < 0.45$ | **NORMAL** | Giao dịch bình thường, tính thanh khoản ổn định |
| $0.45 \le S < 0.65$ | **ELEVATED** | Thị trường có dấu hiệu biến động tăng, giám sát chủ động |
| $0.65 \le S < 0.85$ | **ANOMALY_WARNING** | Cảnh báo trượt giá, giãn cách margin an toàn |
| $S \ge 0.85$ | **CRITICAL_ANOMALY** | Đột biến cực đoan (flash crash / volume dump), kích hoạt Circuit Breaker |

---

## 🧬 2. Pipeline Trích Xuất Đặc Trưng Từ Database (`feature_extractor.py`)

Do dữ liệu lưu trữ tại ClickHouse Gold (`crypto_klines_gold`) chỉ là các nến thô OHLCV, module `MarketFeatureExtractor` đảm nhiệm việc tính toán động 8 đặc trưng vi cấu trúc:

```
[Raw Candles: Open, High, Low, Close, Volume, Trades, TakerVol]
                              │
     ┌────────────────────────┴────────────────────────┐
     ▼                                                 ▼
[Biên độ & Biến động]                         [Khối lượng & Dòng tiền]
1. bar_range_bps = (H - L)/C * 10,000         3. volume_surge_ratio = V / SMA20(V)
2. volatility_parkinson                        4. trades_surge_ratio = Trades / SMA20(Trades)
5. price_velocity = (C_t - C_{t-1})/C_{t-1}   7. taker_buy_ratio = Taker_Buy / Total_Vol
6. price_velocity_1h = (C_t - C_{t-4})/C_{t-4} 8. quote_net_flow_ratio = 2 * TakerRatio - 1
```

> **Khả năng chịu lỗi (Fault Tolerance)**:
> - Nếu cơ sở dữ liệu thiếu trường `number_of_trades`, module tự động gán `trades_surge_ratio = 1.0`.
> - Nếu thiếu `taker_buy_base_asset_volume`, tự động gán `taker_buy_ratio = 0.5` (trung lập).
> - Chuẩn hóa thông qua `RobustScaler` (Median & Interquartile Range) nhằm kháng biến dạng ngoại lai.

---

## 💻 3. Hướng Dẫn Huấn Luyện (CLI Commands)

### 3.1. Huấn luyện đơn mã (Single Pair):
```bash
python "Systems-Graduation-Thesis/SYS-04/AI Models/src/Isolation Forest Anomaly/train.py" \
  --symbol BTCUSDT \
  --contamination 0.02 \
  --n-estimators 200
```

### 3.2. Huấn luyện toàn bộ 6 mã tự động (Batch Training):
```bash
python "Systems-Graduation-Thesis/SYS-04/AI Models/src/Isolation Forest Anomaly/train_all.py" \
  --contamination 0.02 \
  --n-estimators 200
```

---

## 📈 4. Bảng Kết Quả Thực Nghiệm Trên Tập Test Độc Lập (Out-Of-Time Benchmark)

| Mã Giao Dịch | Nến Test | Số Nến Dị Thường | Tỷ Lệ Phát Hiện | Điểm Anomaly TB | NORMAL | ELEVATED | ANOMALY_WARNING | CRITICAL_ANOMALY |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`BTCUSDT`** | 1,751 | 35 | **1.999%** | 0.2026 | 1,628 | 88 | 14 | 21 |
| **`ETHUSDT`** | 1,751 | 37 | **2.113%** | 0.2074 | 1,621 | 93 | 18 | 19 |
| **`BNBUSDT`** | 1,751 | 34 | **1.942%** | 0.2090 | 1,628 | 89 | 21 | 13 |
| **`AAPLBUSDT`**| 1,751 | 21 | **1.199%** | 0.1775 | 1,675 | 55 | 11 | 10 |
| **`AMZNBUSDT`**| 1,751 | 32 | **1.828%** | 0.1699 | 1,654 | 65 | 19 | 13 |
| **`DELLBUSDT`** | 1,751 | 24 | **1.371%** | 0.1675 | 1,685 | 42 | 14 | 10 |
