# 📖 SỔ TAY VẬN HÀNH & TRIỂN KHAI SYS-06 (OPERATIONAL RUNBOOK)

Tài liệu này hướng dẫn chi tiết cách cấu hình, khởi chạy, kiểm thử và xử lý sự cố cho phân hệ **`SYS-06: Operational Alerting & Notification System`**.

---

## 1. Hướng Dẫn Thiết Lập Kênh Telegram Thật (Live Telegram Setup)

Khi muốn nhận thông báo đẩy trực tiếp trên điện thoại:

1. **Tạo Bot Telegram**:
   - Mở Telegram và tìm kiếm bot `@BotFather`.
   - Gửi lệnh `/newbot`, đặt tên và username cho bot.
   - Nhận chuỗi `TELEGRAM_BOT_TOKEN` (Ví dụ: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`).
2. **Lấy Chat ID**:
   - Tạo một Group Telegram mới trên điện thoại và add Bot vừa tạo vào group.
   - Gửi bất kỳ một tin nhắn vào group (ví dụ `/start`).
   - Mở trình duyệt và truy cập: `https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getUpdates`.
   - Tìm giá trị `"chat":{"id": -100xxxxxxxx}` — đây chính là `TELEGRAM_CHAT_ID`.
3. **Cấu hình file `.env`**:
   - Sao chép file `.env.example` thành `.env`:
     ```bash
     cp .env.example .env
     ```
   - Điền các thông tin vừa lấy được và đổi `ALERT_DRY_RUN=false`:
     ```env
     ALERT_DRY_RUN=false
     TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
     TELEGRAM_CHAT_ID=-100xxxxxxxx
     ```

---

## 2. Khởi Chạy Local (Chế độ phát triển & kiểm thử)

```bash
# 1. Kích hoạt môi trường ảo Python
source venv/bin/activate  # Hoặc venv\Scripts\activate trên Windows

# 2. Cài đặt các thư viện cần thiết
pip install -r requirements.txt

# 3. Chạy toàn bộ Test Suite
pytest tests/ -v

# 4. Chạy mô phỏng Dispatcher Engine
python -m src.alert_dispatcher.main
```

---

## 3. Khởi Chạy Bằng Docker & Docker Compose

```bash
# 1. Tạo mạng Docker chung (nếu chưa có)
docker network create thesis-network || true

# 2. Build và khởi chạy container trong background
docker-compose up --build -d

# 3. Xem log thời gian thực của Dispatcher
docker-compose logs -f sys06-alert-dispatcher

# 4. Dừng container
docker-compose down
```

---

## 4. Xử Lý Sự Cố Thường Gặp (Troubleshooting)

| Vấn đề / Lỗi | Nguyên nhân khả dĩ | Cách khắc phục |
| :--- | :--- | :--- |
| **Không nhận được tin nhắn Telegram** | Chưa set `ALERT_DRY_RUN=false` hoặc Token/Chat ID sai | Kiểm tra lại `.env`, đảm bảo bot đã được add vào group Telegram và có quyền gửi tin. |
| **Cảnh báo không phát ra liên tục** | Cơ chế Deduplication đang chặn tin lặp trong 300s | Đây là tính năng dự kiến chống Spam. Nếu cần test, đổi giá trị `deduplicate_key` khác nhau. |
| **Tin nhắn bị chậm hoặc rơi vào hàng đợi** | Tần suất gửi vượt quá 10 tin / phút | Rate limiter tự động đệm tin nhắn để bảo vệ bot không bị Telegram API khóa. |
| **Lỗi Unicode / Ký tự Emoji trên Console** | Môi trường terminal Windows dùng bảng mã cp1252 cũ | Dispatcher đã tự động xử lý buffer encode UTF-8; hãy sử dụng Windows Terminal hoặc PowerShell UTF-8. |
