# 📊 MA TRẬN PHÂN LOẠI CẢNH BÁO HỆ THỐNG (SYSTEM ALERT MATRIX)

Tài liệu này chuẩn hóa ma trận phân loại mức độ nghiêm trọng (`Severity Level`), kênh phát tín hiệu mặc định và cam kết thời gian phản hồi (Response SLA) cho toàn bộ các sự kiện cảnh báo trong nền tảng.

---

## 1. Bảng Ma Trận Tổng Hợp Cảnh Báo

| Hệ thống nguồn | Mã sự kiện (Event Name) | Loại cảnh báo (Alert Type) | Mức độ (Level) | Kênh mặc định (Default Channels) | SLA Phản hồi | Nhóm tiếp nhận (Target Roles) |
| :--- | :--- | :--- | :---: | :--- | :---: | :--- |
| **`SYS-01`** | `PUMP_AND_DUMP_DETECTED` | `MARKET_ANOMALY` | `CRITICAL` 🔴 | Telegram, Slack | < 1 phút | Trader, Risk Manager |
| **`SYS-01`** | `MARKET_VOLATILITY_SPIKE` | `MARKET_ANOMALY` | `WARNING` ⚠️ | Telegram, Slack | < 5 phút | Trader, Risk Manager |
| **`SYS-01`** | `SUSPICIOUS_ORDER_FLOW` | `BUSINESS_RISK` | `WARNING` ⚠️ | Slack | < 15 phút | Risk Manager, Compliance |
| **`SYS-02`** | `DAG_RUN_FAILED` | `PIPELINE_FAILURE` | `CRITICAL` 🔴 | Telegram, Slack | < 5 phút | Data Engineer, DevOps |
| **`SYS-02`** | `SLA_MISSED` | `PIPELINE_FAILURE` | `WARNING` ⚠️ | Telegram, Slack | < 15 phút | Data Engineer |
| **`SYS-02`** | `DATA_QUALITY_CHECK_FAILED` | `DATA_QUALITY` | `CRITICAL` 🔴 | Telegram, Slack | < 10 phút | Data Engineer, Risk Manager |
| **`SYS-02`** | `UPSTREAM_API_UNAVAILABLE` | `PIPELINE_FAILURE` | `CRITICAL` 🔴 | Telegram | < 2 phút | Data Engineer, DevOps |
| **`SYS-03`** | `DOCKER_CONTAINER_DOWN` | `INFRASTRUCTURE_FAILURE`| `CRITICAL` 🔴 | Telegram, Slack | < 1 phút | DevOps, SRE |
| **`SYS-03`** | `DISK_USAGE_CRITICAL` | `INFRASTRUCTURE_FAILURE`| `CRITICAL` 🔴 | Telegram, Slack | < 5 phút | DevOps, SRE |
| **`SYS-03`** | `MEMORY_EXHAUSTION` | `INFRASTRUCTURE_FAILURE`| `WARNING` ⚠️ | Telegram | < 10 phút | DevOps, SRE |
| **`SYS-03`** | `KAFKA_CONSUMER_LAG_HIGH` | `INFRASTRUCTURE_FAILURE`| `WARNING` ⚠️ | Telegram, Slack | < 5 phút | DevOps, Data Engineer |
| **`SYS-04`** | `HIGH_FRAUD_TRANSACTION_FLAGGED`| `FRAUD_TRANSACTION` | `CRITICAL` 🔴 | Telegram, Slack | < 1 phút | Risk Manager, Trader |
| **`SYS-04`** | `DATA_DRIFT_DETECTED` | `MODEL_GOVERNANCE` | `WARNING` ⚠️ | Slack | < 1 giờ | ML Engineer |
| **`SYS-04`** | `MODEL_SERVING_LATENCY_HIGH` | `MODEL_GOVERNANCE` | `WARNING` ⚠️ | Slack | < 15 phút | ML Engineer, DevOps |
| **`SYS-04`** | `MODEL_SERVING_CRASH` | `MODEL_GOVERNANCE` | `CRITICAL` 🔴 | Telegram, Slack | < 2 phút | ML Engineer, DevOps, SRE |
| **`SYS-05`** | `CLICKHOUSE_CLUSTER_UNREACHABLE`| `STORAGE_FAILURE` | `CRITICAL` 🔴 | Telegram, Slack | < 2 phút | Data Engineer, DevOps, SRE |
| **`SYS-05`** | `MINIO_STORAGE_UNAVAILABLE` | `STORAGE_FAILURE` | `CRITICAL` 🔴 | Telegram, Slack | < 2 phút | Data Engineer, DevOps, SRE |
| **`SYS-05`** | `DATASET_FRESHNESS_STALE` | `DATA_QUALITY` | `WARNING` ⚠️ | Slack | < 15 phút | Data Engineer |
| **`SYS-05`** | `DATABASE_CONNECTION_POOL_EXHAUSTED`| `STORAGE_FAILURE`| `CRITICAL` 🔴| Telegram | < 5 phút | Data Engineer, DevOps |

---

## 2. Tiêu Chuẩn Phân Cấp Mức Độ (Severity Standard)

1. **🔴 CRITICAL (Cực kỳ nghiêm trọng)**:
   * Sự cố làm tê liệt hạ tầng cốt lõi, thất thoát dữ liệu, hoặc phát hiện rủi ro gian lận/thao túng thị trường quy mô lớn.
   * Yêu cầu thông báo push tức thì tới Telegram và Slack, kích hoạt On-call Engineer can thiệp ngay lập tức.
2. **⚠️ WARNING (Cảnh báo tiềm ẩn)**:
   * Tài nguyên sắp vượt ngưỡng, mô hình suy giảm hiệu năng (Drift), pipeline trễ tiến độ nhưng chưa sập hoàn toàn.
   * Gửi qua Telegram/Slack để đội ngũ kiểm tra trong ca làm việc.
3. **🟢 INFO (Thông tin vận hành)**:
   * Trạng thái phục hồi thành công (`RESOLVED`), hoàn tất backup định kỳ, deploy phiên bản mới.
   * Gửi vào kênh Slack log chung để ghi vết, không phát chuông push điện thoại.
