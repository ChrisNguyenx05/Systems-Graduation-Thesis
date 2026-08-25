# 🗺️ BẢN ĐỒ ÁNH XẠ NGUỒN CẢNH BÁO (ALERT SOURCE MAP)

Tài liệu này xác định các nguồn phát sinh sự kiện cảnh báo từ 5 phân hệ trong nền tảng kiến trúc tổng thể (`SYS-01` đến `SYS-05`) gửi về cho **`SYS-06` (Operational Alerting & Notification System)**.

---

## 1. Tổng quan Kiến trúc Tích hợp Nguồn Cảnh báo

```
+-------------------------------------------------------------------------+
|                              Nền Tảng 6 Phân Hệ                         |
+-------------------------------------------------------------------------+
| [SYS-01: Dashboard / Analytics]     --> Market Anomaly, Business Fraud  |
| [SYS-02: Airflow Data Pipelines]    --> DAG Failure, SLA Miss, DQ Error |
| [SYS-03: Infrastructure Monitoring] --> Container Down, High Disk/RAM   |
| [SYS-04: MLOps & Fraud Center]      --> Model Drift, High Fraud Score   |
| [SYS-05: Lakehouse & Storage]       --> MinIO/ClickHouse Down, Late Data|
+-------------------------------------------------------------------------+
                                    │ (JSON Alert Payload)
                                    ▼
+-------------------------------------------------------------------------+
|               SYS-06: Dispatcher & Notification Hub                     |
|  - JSON Schema Validation                                               |
|  - 300s Deduplication Filtering                                        |
|  - 10 msgs/min Rate Limiting Buffer                                     |
|  - Markdown Formatting & Emojis                                         |
|  - Role-based Channel Routing                                           |
+-------------------------------------------------------------------------+
             │                                              │
             ▼                                              ▼
     [📱 Telegram Bot]                              [💬 Slack Webhook]
   (DevOps, Traders, SRE)                         (Enterprise Channels)
```

---

## 2. Chi tiết các Nguồn Phát Cảnh báo (Source Systems)

### 2.1. `SYS-01`: Business & Predictive Analytics Dashboard
* **Mô tả**: Phân hệ hiển thị dữ liệu giao dịch thời gian thực và phát hiện biến động tài chính.
* **Các sự kiện cảnh báo**:
  1. `PUMP_AND_DUMP_DETECTED`: Khối lượng giao dịch (Volume) tăng đột biến > 300% trong 3 phút kèm biến động giá bất thường.
  2. `MARKET_VOLATILITY_SPIKE`: Biến động giá vượt ngưỡng 3 độ lệch chuẩn (\(3\sigma\)).
  3. `SUSPICIOUS_ORDER_FLOW`: Phát hiện dòng lệnh gom/xả đáng ngờ từ cùng một cụm IP/ví.

### 2.2. `SYS-02`: Data Pipeline Orchestration System (Airflow)
* **Mô tả**: Phân hệ điều phối các luồng ETL/ELT xử lý dữ liệu theo lịch trình và streaming.
* **Các sự kiện cảnh báo**:
  1. `DAG_RUN_FAILED`: Một pipeline trích xuất hoặc xử lý dữ liệu bị lỗi thực thi.
  2. `SLA_MISSED`: Luồng dữ liệu hoàn thành muộn hơn thời hạn cam kết SLA.
  3. `DATA_QUALITY_CHECK_FAILED`: Dữ liệu kiểm tra (Great Expectations / SQL Check) bị lỗi schema hoặc tỷ lệ null vượt ngưỡng.
  4. `UPSTREAM_API_UNAVAILABLE`: Không thể kết nối API nguồn sàn giao dịch (Binance, OKX, ...).

### 2.3. `SYS-03`: Infrastructure Operations Center (Prometheus / Grafana / Docker)
* **Mô tả**: Phân hệ giám sát tài nguyên máy chủ, cụm container và hàng đợi thông điệp.
* **Các sự kiện cảnh báo**:
  1. `DOCKER_CONTAINER_DOWN`: Container trọng yếu (Kafka, MinIO, ClickHouse, Airflow) bị dừng đột ngột (Exit Code != 0).
  2. `DISK_USAGE_CRITICAL`: Dung lượng ổ đĩa vượt quá 85% ngưỡng dung lượng cho phép.
  3. `MEMORY_EXHAUSTION`: Tỷ lệ sử dụng RAM vượt 90% liên tục trong 5 phút.
  4. `KAFKA_CONSUMER_LAG_HIGH`: Hàng đợi Kafka có độ trễ tiêu thụ (Lag) vượt ngưỡng 10,000 tin nhắn.

### 2.4. `SYS-04`: MLOps & Model Governance Center (MLflow / Serving)
* **Mô tả**: Phân hệ quản trị vòng đời mô hình học máy (Fraud Detection, Anomaly Detection).
* **Các sự kiện cảnh báo**:
  1. `HIGH_FRAUD_TRANSACTION_FLAGGED`: Mô hình LightGBM phát hiện giao dịch có điểm rủi ro `fraud_score > 0.85`.
  2. `DATA_DRIFT_DETECTED`: Phân phối đặc trưng đầu vào bị trôi dạt so với tập dữ liệu huấn luyện (Evidently AI Drift Score > 0.3).
  3. `MODEL_SERVING_LATENCY_HIGH`: Thời gian phản hồi dự đoán API vượt quá 500ms.
  4. `MODEL_SERVING_CRASH`: Container phục vụ model inferencing không phản hồi health check.

### 2.5. `SYS-05`: Lakehouse & Database Management Portal (MinIO, ClickHouse, PostgreSQL)
* **Mô tả**: Phân hệ quản trị kho dữ liệu phân tích và lưu trữ đối tượng.
* **Các sự kiện cảnh báo**:
  1. `CLICKHOUSE_CLUSTER_UNREACHABLE`: Không thể kết nối hoặc thực thi truy vấn tới ClickHouse.
  2. `MINIO_STORAGE_UNAVAILABLE`: Dịch vụ lưu trữ đối tượng MinIO bị ngắt kết nối.
  3. `DATASET_FRESHNESS_STALE`: Bảng dữ liệu giao dịch quá 15 phút chưa được cập nhật bản ghi mới.
  4. `DATABASE_CONNECTION_POOL_EXHAUSTED`: Hết kết nối khả dụng trong connection pool.
