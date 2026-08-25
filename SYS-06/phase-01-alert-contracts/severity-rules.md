# 🚨 QUY TẮC PHÂN CẤP MỨC ĐỘ & LEO THANG (SEVERITY & ESCALATION RULES)

Tài liệu này xác lập tiêu chuẩn phân loại cấp độ nghiêm trọng của sự kiện (`Severity Level`), chính sách ứng phó và quy trình leo thang sự cố (Incident Escalation).

---

## 1. Bảng Tiêu Chuẩn Mức Độ Nghiêm Trọng

### 🔴 Cấp độ 1: `CRITICAL` (Nghiêm trọng cấp cao)
* **Định nghĩa**: Sự cố làm tê liệt hoặc ảnh hưởng trực tiếp đến tính khả dụng của nền tảng, sai lệch dữ liệu tài chính hoặc phát hiện gian lận/thao túng quy mô lớn.
* **Thời gian giao tin**: `< 3 giây`.
* **Kênh gửi bắt buộc**: `Telegram` (Push tức thì) + `Slack`.
* **Hành động phản hồi**: Yêu cầu On-call Engineer hoặc Risk Manager phản hồi trong vòng **5 phút**.
* **Ví dụ**:
  - `DOCKER_CONTAINER_DOWN` (Container Kafka, ClickHouse, Airflow sập).
  - `PUMP_AND_DUMP_DETECTED` (Thao túng thị trường tài chính).
  - `HIGH_FRAUD_TRANSACTION_FLAGGED` (Giao dịch gian lận giá trị cao).
  - `CLICKHOUSE_CLUSTER_UNREACHABLE` (Kho dữ liệu tê liệt).

---

### ⚠️ Cấp độ 2: `WARNING` (Cảnh báo tiềm ẩn)
* **Định nghĩa**: Sự cố hoặc dấu hiệu bất thường có khả năng dẫn đến gián đoạn dịch vụ nếu không được xử lý kịp thời, nhưng hệ thống vẫn đang hoạt động.
* **Thời gian giao tin**: `< 10 giây`.
* **Kênh gửi**: `Telegram` (tùy chọn) + `Slack`.
* **Hành động phản hồi**: Kiểm tra và khắc phục trong ca trực (trong vòng **30 - 60 phút**).
* **Ví dụ**:
  - `DISK_USAGE_WARNING` (Dung lượng ổ cứng > 80%).
  - `DATA_DRIFT_DETECTED` (Đặc trưng đầu vào mô hình AI bị trôi dạt).
  - `KAFKA_CONSUMER_LAG_HIGH` (Lag tăng cao tạm thời).
  - `SLA_MISSED` (Pipeline hoàn thành trễ hạn).

---

### 🟢 Cấp độ 3: `INFO` (Thông tin & Phục hồi)
* **Định nghĩa**: Thông báo trạng thái vận hành bình thường, xác nhận sự cố đã được khắc phục (`RESOLVED`) hoặc thông báo triển khai thành công.
* **Kênh gửi**: `Slack` (Kênh nhật ký/Audit log) hoặc `Console`.
* **Hành động phản hồi**: Không yêu cầu hành động can thiệp tức thì.
* **Ví dụ**:
  - `CONTAINER_RESTARTED_OK` (Container đã phục hồi hoạt động).
  - `BACKUP_COMPLETED_SUCCESS` (Sao lưu dữ liệu hoàn tất).
  - `DAILY_SYSTEM_HEALTH_REPORT` (Báo cáo sức khỏe định kỳ).

---

## 2. Quy Tắc Leo Thang Sự Cố (Escalation Rules)

1. **Khử trùng lặp (Deduplication)**:
   * Nếu sự cố lặp lại liên tục trong vòng 300 giây, Dispatcher sẽ bỏ qua các thông báo trùng lặp để tránh làm ngập kênh liên lạc.
2. **Nâng cấp độ (Severity Escalation)**:
   * Nếu một cảnh báo `WARNING` (ví dụ: `DISK_USAGE_WARNING` ở mức 80%) tiếp tục tăng lên 90% sau 5 phút, sự kiện mới với `CRITICAL` sẽ lập tức được phát ra và bỏ qua cửa sổ deduplication của warning trước đó.
