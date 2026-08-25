# SYS-06: Kế Hoạch Build Hệ Thống Cảnh Báo Vận Hành, Thông Báo & Tích Hợp Alert

SYS-06 tuân theo tài liệu master chính thức:

- `Plan/systems_docs/06_Operational_Alerting_Notification_System.md`

Thư mục `SYS-06` là phiên bản kết hợp mạnh hơn giữa kế hoạch chính thức và phần cải tiến tích hợp alert.

## Vai Trò Chính Thức

SYS-06 chịu trách nhiệm gửi cảnh báo thời gian thực. Hệ thống nhận các event quan trọng từ những hệ thống khác, loại bỏ cảnh báo trùng lặp, áp dụng rate limit, format message, và gửi thông báo qua Telegram, Slack, hoặc sau này là SMS/email.

## Vai Trò Cải Tiến

SYS-06 cũng định nghĩa chuẩn tích hợp alert cho kiến trúc kiểu công ty:

- alert payload schema dùng chung.
- source-system alert map.
- role-based receiver rules.
- severity-based routing rules.
- health và data quality alert input design.
- containerized dispatcher plan.

## Thứ Tự Build Ưu Tiên

1. Định nghĩa alert payload contract.
2. Định nghĩa role/channel routing rules.
3. Build Python dispatcher engine.
4. Thêm Telegram dry-run và real-send support.
5. Thêm deduplication trong 300 giây.
6. Thêm rate limiting 10 tin nhắn/phút.
7. Thêm Slack webhook support.
8. Thêm Docker Compose và `.env.example`.
9. Kết nối alert source đầu tiên từ `SYS-02`.
10. Thêm alert examples cho `SYS-01`, `SYS-03`, `SYS-04`, và `SYS-05`.

## Ranh Giới

Không tạo thêm số hệ thống mới. SYS-06 kết hợp alerting và alert integration bên trong master plan 6 hệ thống ban đầu.
