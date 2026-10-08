# SYS-04: MLOps & Model Governance Center

SYS-04 is the **Artificial Intelligence & Machine Learning Operations (MLOps) Engine** of the Financial Analytics & Fraud Detection Enterprise Platform.

It governs the full machine learning lifecycle from experiment tracking and automated model evaluation to production model registry, dynamic zero-downtime serving, and sub-50ms REST inference APIs.

## 📌 Architecture Reference
- Official Master Specification: `Plan/systems_docs/04_MLOps_Model_Governance_Center.md`
- Master Plan v2.0: `Plan/master_plan(update_2026_08_07).html`

## 🚀 Key Responsibilities
1. **Experiment Tracking & Artifact Versioning (MLflow)**: Recording all training runs, hyper-parameters, loss curves, RMSE/AUC metrics, and binary model artifacts into MinIO S3 (`mlflow-artifacts`).
2. **Model Registry & Governance**: Managing model lifecycle stages (`Staging` vs `Production`) with automated promotion criteria.
3. **High-Performance Real-Time Model Serving (FastAPI)**: Serving 3 production models with ultra-low latency:
   - **LSTM Price Forecaster**: 15m/60m price prediction for crypto assets.
   - **LightGBM Fraud Detector**: Transaction fraud classification on IEEE-CIS features.
   - **Isolation Forest Anomaly Detector**: Unsupervised bar-derived volume surge & extreme volatility scoring.
4. **Zero-Downtime Hot-Reload**: Updating active model weights dynamically in memory without restarting the FastAPI service.
5. **Multi-System Integration**: Consuming Gold features from SYS-05, orchestrating retrain runs with SYS-02, providing inference to SYS-01, exporting telemetry to SYS-03, and dispatching drift alerts to SYS-06.

## 🧭 Directory Structure & Documentation
- `CONTEXT.md`: Ubiquitous domain language, MLOps stages, and model contracts.
- `docs/BUILD_PLAN.md`: Comprehensive engineering plan covering ML pipelines, serving architecture, system connections, tech stack evaluation, and test suites.

Start with [docs/BUILD_PLAN.md](docs/BUILD_PLAN.md).
