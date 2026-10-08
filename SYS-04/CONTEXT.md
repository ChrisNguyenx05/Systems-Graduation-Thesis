# 📖 CONTEXT: SYS-04 MLOps & Model Governance Center Domain Model

This document defines the **Ubiquitous Language** and operational boundaries for **SYS-04 (MLOps & Model Governance Center)** within the Financial Analytics & Fraud Detection Enterprise Platform.

---

## 📚 Ubiquitous Language & Terminology

### 1. MLOps Lifecycle & Governance Concepts
- **Experiment Run**: A single execution of a machine learning training pipeline logging hyper-parameters, code version (Git commit), validation metrics, and model binary artifacts.
- **Model Registry**: Centralized repository providing model lineage, semantic versioning (`v1.0.0`, `v1.1.0`), and lifecycle stage management (`Staging`, `Production`, `Archived`).
- **Stage Promotion Gate**: Automated or human-in-the-loop governance rule requiring a challenger model to outperform the champion model (e.g., RMSE reduction >= 5% or AUC-ROC > 0.95) before transitioning to `Production`.
- **Hot-Reload Weights**: Dynamic reloading of model weight tensors in the FastAPI memory space upon receiving an administrative webhook or polling event, achieving zero-downtime model updates.

### 2. Machine Learning Models & Inference Tasks
- **Time-Series Price Predictor (LSTM / PatchTST)**: Sequence-to-point neural network taking a tensor of 60 historical OHLCV bars `(Batch, 60, 5)` to forecast short-term prices $P_{t+15m}$ and $P_{t+60m}$.
- **Fraud Classification Classifier (LightGBM / XGBoost)**: Gradient-boosted decision tree evaluating tabular transactional features (amount, card attributes, velocity flags) to compute `fraud_probability`.
- **Bar Anomaly Detector (Isolation Forest)**: Unsupervised ensemble algorithm evaluating bar-derived metrics (extreme volume surges, rapid price velocity, abnormal high-low candle range, and Parkinson volatility) to detect bar-level anomaly patterns without claiming orderbook manipulation detection.
- **Model Drift & Concept Drift**: The degradation of prediction performance over time caused by shifting cryptocurrency market regimes or changing fraud patterns.

### 3. Serving & Performance SLAs
- **Inference Latency SLA**: Strict response deadline: `< 50ms` for single-record fraud classification and `< 200ms` for 60-candle sequence deep learning prediction.
- **Async Prediction Workers**: Uvicorn ASGI workers leveraging asynchronous non-blocking event loops to maximize concurrency under burst trading traffic.

---

## ⚙️ Operational Port Mapping & Isolation
- **MLflow Tracking & Registry UI**: Port `5000` (External Host Binding: `http://localhost:5000`)
- **FastAPI Real-Time Serving**: Port `8000` (External Host Binding: `http://localhost:8000`, Swagger at `/docs`)
- **Internal Storage Dependencies**:
  - SQLite / PostgreSQL metadata backend for MLflow tracking
  - MinIO S3 bucket `mlflow-artifacts` for saving model binaries (.pt, .txt, .pkl)
- **Integration Endpoints**:
  - `/api/v1/predict/price` (Invoked by SYS-01)
  - `/api/v1/predict/fraud` (Invoked by SYS-01 & Payment stream)
  - `/api/v1/admin/reload-model` (Invoked by SYS-02 upon DAG training completion)
  - `/metrics` (Scraped by SYS-03 Prometheus)
