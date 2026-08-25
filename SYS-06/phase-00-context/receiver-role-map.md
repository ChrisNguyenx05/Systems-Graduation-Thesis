# 👥 ÁNH XẠ VAI TRÒ VÀ NHÓM TIẾP NHẬN (RECEIVER ROLE MAP)

Tài liệu này xác định các vai trò người dùng (Roles), quyền hạn nhận tin (RBAC) và chính sách phân phối cảnh báo theo vai trò nghiệp vụ.

---

## 1. Định nghĩa các Nhóm Vai Trò (Target Roles)

| Role Code | Tên vai trò | Trách nhiệm chính | Loại cảnh báo quan tâm |
| :--- | :--- | :--- | :--- |
| `devops_sre` | DevOps & SRE Engineers | Đảm bảo hệ thống 24/7, duy trì container, máy chủ, Kafka, storage | `INFRASTRUCTURE_FAILURE`, `STORAGE_FAILURE` |
| `data_engineer` | Data Engineers | Đảm bảo tính liên tục của luồng ETL/ELT, chất lượng dữ liệu, freshness | `PIPELINE_FAILURE`, `DATA_QUALITY`, `STORAGE_FAILURE` |
| `ml_engineer` | MLOps & ML Engineers | Giám sát mô hình dự đoán, model drift, inference latency | `MODEL_GOVERNANCE`, `FRAUD_TRANSACTION` |
| `risk_manager` | Risk & Compliance Managers | Kiểm soát giao dịch gian lận, rủi ro tài khoản, vi phạm quy tắc | `FRAUD_TRANSACTION`, `MARKET_ANOMALY`, `BUSINESS_RISK` |
| `trader` | Quantitative Traders | Theo dõi biến động thị trường bất thường, thanh khoản và volume spike | `MARKET_ANOMALY` |

---

## 2. Quy Tắc Định Tuyến Theo Kênh (Channel Routing Rules)

```
                       [Event Payload]
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
           [Receiver Roles]     [Severity Level]
                    │                   │
      ┌─────────────┼─────────────┐     │
      ▼             ▼             ▼     ▼
 [devops_sre] [risk_manager]  [trader] ...
      │             │             │
      ▼             ▼             ▼
   Telegram      Telegram      Telegram
   (On-call)     (Risk-Bot)   (Trading-Bot)
      &             &             &
   Slack         Slack         Slack
  (#infra)      (#fraud)     (#market)
```

1. **Telegram Channels/Chats**:
   * Kênh chính `TELEGRAM_CHAT_ID`: Nhận các cảnh báo `CRITICAL` và `WARNING` phục vụ On-call xử lý tức thì.
   * Các cảnh báo `CRITICAL` luôn được gắn kèm icon 🔴 và Markdown in đậm để tạo độ tương phản cao trên màn hình điện thoại.
2. **Slack Channels (Webhooks)**:
   * Kênh `#alerts-ops`: Tiếp nhận cảnh báo hạ tầng và luồng dữ liệu (`devops_sre`, `data_engineer`).
   * Kênh `#alerts-risk-fraud`: Tiếp nhận cảnh báo giao dịch gian lận và model drift (`risk_manager`, `ml_engineer`).
   * Kênh `#alerts-market`: Tiếp nhận cảnh báo biến động thanh khoản (`trader`, `risk_manager`).
