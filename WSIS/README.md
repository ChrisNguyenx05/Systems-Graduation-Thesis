# ⚡ WSIS: WebSocket Ingestion Service
> **Hệ thống thu thập luồng dữ liệu thời gian thực Binance đồng bộ chu kỳ 15 phút với Apache Airflow**

Dịch vụ `WSIS` thay thế hoàn toàn Chrome Extension client-side, hoạt động dưới dạng một Daemon/Docker container 24/7 độc lập, kết nối trực tiếp đến các WebSocket Streams của Binance, gom dữ liệu theo cửa sổ thời gian 15 phút đồng bộ tuyệt đối với Airflow (`SYS-02`) và nạp vào MinIO Bronze S3 (`SYS-05`).

---

## 🌟 Tính năng nổi bật

1. **Đồng bộ tuyệt đối chu kỳ 15 phút (Clock-Aligned Tumbling Windows):**
   - Tự động gom các sự kiện vào cửa sổ: `window_start = t - (t % 900000)` (trùng khớp mốc `:00`, `:15`, `:30`, `:45`).
   - Gắn nhãn `run_key` định dạng `YYYYMMDDTHHMMSSZ` đồng nhất 100% với Airflow DAG `04_binance_market_metrics_15m_dag`.
   - Giúp đối soát kiểm định chất lượng dữ liệu (Data Quality Control - QC) giữa REST API và WebSocket cực kỳ chuẩn xác.
2. **Tiết kiệm tài nguyên kết nối (Combined Streams):**
   - Chỉ mở 1 kết nối Spot và 1 kết nối Futures lắng nghe đồng thời 6 cặp giao dịch (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `AAPLBUSDT`, `AMZNBUSDT`, `DELLBUSDT`).
3. **Cơ chế tự phục hồi (Self-Healing):**
   - Tự động phản hồi Ping/Pong và Reconnect với Exponential Backoff khi mạng chập chờn hoặc sàn ngắt sau 24h.
4. **Nạp trực tiếp MinIO Bronze Layer:**
   - Dữ liệu được nén dạng `JSONL.GZ` và upload theo đường dẫn phân vùng:
     `s3://crypto-bronze/wsis/{dataset}/year=YYYY/month=MM/day=DD/{run_key}.jsonl.gz`.
5. **Đối soát REST API và WebSocket cho nến đã đóng:**
   - Khi một nến WebSocket 15 phút đóng, WSIS gọi Binance REST API cho đúng `symbol` và `open_time`.
   - Lưu riêng dữ liệu REST vào `api_klines` và kết quả vào `reconciliation`.
   - `reconciliation.status` là `matched`, `mismatch`, hoặc `api_missing`; không tự động thay thế dữ liệu WebSocket khi REST API thiếu hoặc khác dữ liệu.

## 📦 Bronze datasets for comparison

| Dataset | Purpose |
| --- | --- |
| `klines` | Klines 15 phút từ WebSocket, bao gồm cả nến đang mở và nến đã đóng. |
| `api_klines` | Snapshot REST API tương ứng với nến WebSocket đã đóng. |
| `reconciliation` | Bản ghi so sánh theo `symbol + open_time`, gồm `matched` và `mismatched_fields`. |

Only finalized (`is_closed=true`) bars that are reconciled successfully should be promoted to the SYS-04 Gold 15-minute training contract.

---

## 🚀 Hướng dẫn khởi chạy

### Cách 1: Chạy trực tiếp bằng Python trên máy chủ
```powershell
cd E:\Graduation_Thesis\Systems-Graduation-Thesis\WSIS
pip install -r requirements.txt
python -m src.main
```

### Cách 2: Chạy độc lập bằng Docker
```powershell
cd E:\Graduation_Thesis\Systems-Graduation-Thesis\WSIS
docker compose up -d --build
```
Kiểm tra logs thời gian thực:
```powershell
docker logs -f wsis_websocket_collector
```
