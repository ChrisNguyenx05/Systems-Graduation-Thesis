# 🤖 SYS-04: AI Models & Quantitative Intelligence Center

Chào mừng bạn đến với trung tâm phát triển mô hình trí tuệ nhân tạo (AI Models) thuộc phân hệ **SYS-04: MLOps & Model Governance Center**.

---

## 📌 Báo Cáo Tổng Hợp & Tiến Độ
Để xem thống kê toàn bộ tiến độ, quy chuẩn dữ liệu, kết quả thực nghiệm và benchmark của các mô hình, vui lòng tham khảo file:
👉 **[AI_MODELS_SUMMARY_REPORT.md](AI_MODELS_SUMMARY_REPORT.md)**

---

## 📂 Cấu Trúc Phân Hệ Đồng Bộ

```
AI Models/
├── AI_MODELS_SUMMARY_REPORT.md     # 📑 Báo cáo tổng thể toàn bộ phân hệ
├── Dataset/                        # 🗄️ Dữ liệu huấn luyện & kiểm thử
│   ├── LSTM Price Forecaster/      # Dữ liệu nến 15m (3 tháng, 6 mã)
│   ├── Isolation Forest Anomaly/   # 8 đặc trưng vi cấu trúc nến 15m (2 tháng, 6 mã)
│   └── LightGBM Fraud Detector/    # IEEE-CIS Fraud Detection dataset (590k train, 506k test)
├── src/                            # 🛠️ Mã nguồn huấn luyện & tiền xử lý
│   ├── LSTM Price Forecaster/      # Pipeline PyTorch LSTM, evaluation & batch training
│   ├── Isolation Forest Anomaly/   # Pipeline Unsupervised Anomaly Detection
│   └── LightGBM Fraud Detector/    # Pipeline Phân loại Gian lận Giao dịch
└── models/                         # 📦 Model Registry phục vụ Production Inference
    ├── LSTM Price Forecaster/      # 6x Scaler (.joblib), 6x Weights (.pt), 6x TorchScript (.torchscript)
    ├── Isolation Forest Anomaly/   # Production Anomaly models & scalers
    └── LightGBM Fraud Detector/    # Production GBDT trees & preprocessors
```

---

## 📊 Tóm Tắt Hiện Trạng & Phân Định Vai Trò Production (Governance Roles)

| Mô Hình | Loại Mô Hình | Dữ Liệu | Trạng Thái & Vai Trò Production | Hiệu Năng Thực Nghiệm (1.000 Run Benchmark) |
| :--- | :---: | :---: | :---: | :--- |
| **LSTM Price Forecaster** | Deep Learning (PyTorch TorchScript) | 6 cặp tài sản (Crypto & RWA) | ⚠️ **Advisory Mode Only (Hỗ trợ phân tích)**<br>*Không dùng cho tự động đặt lệnh (unattended execution)* | • MAE: **$107.81** (BTC), **$0.19** (AAPL), **$0.20** (AMZN)<br>• MAPE: **0.056% - 0.167%** (Ngang bằng Naïve Persistence)<br>• Hướng DA: **54.65%** (AMZN), **51.83%** (AAPL)<br>• Độ trễ P99 CPU: **1.30ms - 3.47ms** (vượt chuẩn < 10ms) |
| **Isolation Forest Anomaly**| Machine Learning (Unsupervised Scikit-Learn)| 8 đặc trưng vi cấu trúc nến 15m | ✅ **Risk Engine Guard (Lọc rủi ro thị trường)**<br>*Kích hoạt giãn spread, giảm đòn bẩy khi thị trường giật* | • Tỷ lệ cảnh báo ngoại lai: **1.2% - 2.1%** (Contamination $\nu=0.02$)<br>• Feature Parity Max Diff: **0.000000** (Zero skew)<br>• Data Quality Telemetry: **Kháng mất dữ liệu**<br>• Độ trễ CPU: Mean **39ms**, P99 **69ms** (chạy Async bar-close) |
| **LightGBM Fraud Detector** | Gradient Boosted Decision Trees | IEEE-CIS Dataset (1.1M rows) | ⏳ **Chờ triển khai** | • Target ROC-AUC $\ge 0.90$<br>• Target PR-AUC & Precision/Recall Cost Optimization<br>• Target Inference: **< 15ms** |

