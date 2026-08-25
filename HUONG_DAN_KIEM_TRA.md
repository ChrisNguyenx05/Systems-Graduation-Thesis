# 📖 SỔ TAY HƯỚNG DẪN KIỂM TRA & DEMO TOÀN BỘ HỆ SINH THÁI (LOCAL ALL-IN-ONE)

> **Đồ án Tốt nghiệp / Enterprise Platform**: Financial Analytics & Fraud Detection System  
> **Kiến trúc**: Microservices Data & AI Ecosystem (SYS-01 $\rightarrow$ SYS-06)  
> **Phương pháp triển khai**: Phương án 1 (Local All-in-One + Unified Web Portal + Tunneling)

> **Kế hoạch cloud/free-tier cho giai đoạn bảo vệ**: xem [Plan/FREE_TIER_CLOUD_DEPLOYMENT_PLAN.md](Plan/FREE_TIER_CLOUD_DEPLOYMENT_PLAN.md). Khuyến nghị dùng cloud VM trong 3 tháng cuối trước ngày bảo vệ để không phụ thuộc máy cá nhân.

---

## 📌 1. Bảng Tra cứu Cổng Mạng & Tài khoản Đăng nhập

Tất cả các dịch vụ nội bộ đã được cấu hình tài khoản đồng bộ để dễ dàng kiểm tra:

| Phân hệ (System) | Dịch vụ cốt lõi | URL / Cổng Port Local | Tài khoản (User) | Mật khẩu (Password) | Chức năng chính |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **PORTAL** | **Cổng Điều Hướng Trung Tâm** | [portal.html](file:///e:/Graduation_Thesis/portal.html) | *Không cần* | *Không cần* | Bảng điều khiển kiểm tra live status 6 system |
| **SYS-01** | **Predictive Analytics Web** | `http://localhost:8501` | *Không cần* | *Không cần* | Streamlit Dashboard phân tích nến, tín hiệu AI |
| **SYS-02** | **Airflow Webserver** | `http://localhost:8085` | `admin123` | `admin123` | Quản lý ETL DAGs Binance $\rightarrow$ MinIO $\rightarrow$ ClickHouse |
| **SYS-02** | **Airflow Metadata DB** | `localhost:5433` | `airflow` | `airflow_secret` | PostgreSQL nội bộ lưu metadata Airflow |
| **SYS-03** | **Grafana Dashboard** | `http://localhost:3000` | `admin` | `admin123` | Giám sát tài nguyên máy chủ & Kafka Lag |
| **SYS-03** | **Prometheus Server** | `http://localhost:9090` | *Không cần* | *Không cần* | Động cơ thu thập Time-Series Metrics |
| **SYS-04** | **FastAPI Swagger API** | `http://localhost:8000/docs` | *Không cần* | *Không cần* | REST API suy luận rủi ro tài chính |
| **SYS-04** | **MLflow Tracking UI** | `http://localhost:5000` | *Không cần* | *Không cần* | Quản trị thử nghiệm và phiên bản mô hình AI |
| **SYS-05** | **MinIO S3 Console (Web)** | `http://localhost:9001` | `admin123` | `admin123` | Giao diện Web duyệt file Bronze, Silver, Gold |
| **SYS-05** | **MinIO S3 API Endpoint** | `http://localhost:9000` | `admin123` | `admin123` | API S3 cho Airflow/Spark đọc ghi dữ liệu |
| **SYS-05** | **ClickHouse Web Client** | `http://localhost:8123/play` | `admin123` | `admin123` | Truy vấn SQL thời gian thực (< 10ms) |
| **SYS-05** | **Enterprise PostgreSQL** | `localhost:5432` | `admin123` | `admin123` | Database lưu trữ metadata nghiệp vụ |
| **SYS-06** | **Telegram Bot Dispatcher** | Telegram Web / App | - | - | Kênh nhận cảnh báo biến động thị trường tức thì |

---

## 🚀 2. Quy trình 3 bước Khởi động & Kiểm tra Hệ thống

### Bước 1: Mở Docker Desktop
Đảm bảo ứng dụng **Docker Desktop** trên máy Windows đã được bật và hiển thị trạng thái `Engine Running` (icon cá voi màu xanh).

### Bước 2: Chạy Script Khởi động 1-Chạm
Mở PowerShell tại thư mục gốc `e:\Graduation_Thesis` và chạy:
```powershell
.\start_ecosystem.ps1
```
*Script sẽ tự động khởi động các container theo thứ tự: SYS-05 (Storage/Lakehouse) $\rightarrow$ SYS-02 (Airflow Pipelines) và tự động mở file [portal.html](file:///e:/Graduation_Thesis/portal.html) trên trình duyệt.*

### Bước 3: Kiểm tra Trạng thái trên Web Portal
* Trên giao diện [portal.html](file:///e:/Graduation_Thesis/portal.html), bấm nút **"Kiểm tra Trạng thái"**.
* Các badge sẽ chuyển sang màu xanh lá **"Online"**.
* Bấm trực tiếp vào các nút:
  * **Mở Airflow UI** $\rightarrow$ đăng nhập với `admin123` / `admin123`.
  * **Mở MinIO S3 Console** $\rightarrow$ kiểm tra 3 bucket `crypto-bronze`, `crypto-silver`, `crypto-gold`.
  * **Mở ClickHouse Web Client** $\rightarrow$ chạy thử câu lệnh: `SHOW TABLES;`.

---

## 🩺 3. Quét nhanh Sức khỏe Cổng mạng (Health Check CLI)

Khi muốn kiểm tra nhanh trạng thái các cổng mạng bằng dòng lệnh PowerShell mà không cần mở trình duyệt:
```powershell
.\check_health.ps1
```

---

## 🌐 4. Mở Đường hầm Demo Trực tuyến từ xa (Cloudflare Tunnel / Ngrok)

Khi muốn gửi đường link cho Thầy/Cô hoặc Hội đồng phản biện truy cập trực tiếp từ xa:

1. Chạy script:
   ```powershell
   .\start_tunnel.ps1
   ```
2. Chọn phân hệ muốn public (Ví dụ: `1` cho Airflow, `2` cho MinIO, `3` cho Streamlit Dashboard).
3. Script sẽ cung cấp 1 đường link HTTPS công khai (ví dụ: `https://xxxx.trycloudflare.com`). Bạn chỉ cần copy link này gửi cho người chấm bài.

---

## 🛑 5. Dừng Hệ thống & Giải phóng Bộ nhớ

Sau khi hoàn thành buổi demo hoặc kiểm tra, chạy lệnh sau để dừng tất cả container và giải phóng RAM:
```powershell
.\stop_ecosystem.ps1
```

---

## 💡 6. Tài liệu Chi tiết Kiến trúc Hệ thống
* [Tổng quan Hệ sinh thái Ecosystem Master Guide](file:///e:/Graduation_Thesis/Plan/systems_docs/00_System_Ecosystem_Master_Index.html)
* [SYS-01: Business Predictive Analytics Dashboard](file:///e:/Graduation_Thesis/Plan/systems_docs/01_Business_Predictive_Analytics_Dashboard.html)
* [SYS-02: Data Pipeline & Orchestration System](file:///e:/Graduation_Thesis/Plan/systems_docs/02_Data_Pipeline_Orchestration_System.html)
* [SYS-03: Infrastructure Operations Center](file:///e:/Graduation_Thesis/Plan/systems_docs/03_Infrastructure_Operations_Center.html)
* [SYS-04: MLOps Model Governance Center](file:///e:/Graduation_Thesis/Plan/systems_docs/04_MLOps_Model_Governance_Center.html)
* [SYS-05: Lakehouse & Database Management Portal](file:///e:/Graduation_Thesis/Plan/systems_docs/05_Lakehouse_Database_Management_Portal.html)
* [SYS-06: Operational Alerting & Notification System](file:///e:/Graduation_Thesis/Plan/systems_docs/06_Operational_Alerting_Notification_System.html)
