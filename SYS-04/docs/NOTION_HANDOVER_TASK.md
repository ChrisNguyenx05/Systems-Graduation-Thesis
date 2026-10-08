# Task: SYS-04 MLOps & Model Governance Center Implementation

> **Target Subsystem**: `SYS-04` (MLOps & Model Governance Center)  
> **Master Architecture Document**: `SYS-04/docs/PLANNING_SYS04_MLOPS.md` (Version 3.2 Production-Ready)  
> **Environment Template**: `SYS-04/.env.example`  
> **Primary URL**: `http://localhost:8000/` | **MLflow URL**: `http://localhost:5000`

---

## ▶ Context
• **Project Ecosystem**: NHFinance Enterprise Data Platform & Financial Analytics Ecosystem (Covers subsystems SYS-01 through SYS-06).  
• **Subsystem Role**: SYS-04 functions as the central Artificial Intelligence Intelligence Engine & MLOps Model Governance Center.  
• **Current Production Baseline (100% Completed Assets)**:
  - **Data Engineering**: Parquet datasets for 3 core models are validated and stored:
    * *LSTM Price Forecaster*: 44,652 15m closed bars (3 months across 6 crypto and tokenized equity pairs: BTC, ETH, BNB, AAPLB, AMZNB, DELLB).
    * *Isolation Forest Anomaly*: 35,010 15m microstructure bars (8 engineered volatility, volume, and orderflow features).
    * *LightGBM Fraud Detector*: 590,540 IEEE-CIS transaction records (434 features).
  - **Model Artifacts**: Pre-trained `.torchscript`, `.pt`, and `.joblib` serialized weights are stored and bench-tested.
  - **Serving Prototype**: A baseline monolithic script (`server.py`) with 3 verified inference endpoints is running.  
• **Strategic Objective**:
  - Graduate SYS-04 from isolated scripts into an enterprise-grade, decoupled Full-Stack Command Center named:
    **"NHFinance MLOps & Model Governance Center"**
  - The Center operates with its own standalone UI (`http://localhost:8000/`), a dedicated MLflow container (`:5000`), and a synchronized Operator Profile & Granular RBAC system integrated with Admin Portal (`:8090`).

---

## ▶ Purpose
1. **Build the "NHFinance MLOps & Model Governance Center"**:
   - Establish an independent Command Center Portal hosted natively at `http://localhost:8000/` (styled after Admin Operations Gateway and SOC Hub).
2. **Modularize Backend Serving Architecture**:
   - Deconstruct monolithic `server.py` into clean enterprise packages: `src/serving/routers/`, `schemas/`, `services/`, `governance/`, and `integrations/`.
3. **Deploy Centralized MLflow Registry**:
   - Run an isolated Docker container on port `5000` with SQLite metadata and MinIO S3 (`s3://mlflow-artifacts`) for versioning (`Staging` ➔ `Production` ➔ `Archived`).
4. **Decouple UI & Synchronize Operator Profile**:
   - Extract all AI Model views out of Admin Portal into `SYS-04/frontend/`.
   - Implement Single Sign-On (SSO) session sharing and RBAC synchronization with Admin Portal (`:8090`).
5. **Enforce Production SLAs & Zero-Downtime Guarantees**:
   - Sub-15ms latency for Fraud Scoring.
   - Sub-50ms latency for Price Forecasting.
   - In-memory hot-reload of active model weights in <1.5s with zero socket disconnects under concurrent traffic.
6. **Automate Continuous MLOps Lifecycle**:
   - Automated Champion vs. Challenger promotion gate (RMSE reduction >= 5%, AUC >= 0.94).
   - Real-time Data & Model Drift Observatory (Kolmogorov-Smirnov test for candles, Population Stability Index for fraud scores).
   - Event-driven webhook alerting to SYS-06 and automated retraining triggers from SYS-02 (Airflow DAG).

---

## ▶ Scope
• **Target Boundary**: Strictly confined to `Systems-Graduation-Thesis/SYS-04/`.  
• **Reference Architecture File**: `SYS-04/docs/PLANNING_SYS04_MLOPS.md` (Single Source of Truth).  
• **Environment Config**: `SYS-04/.env.example`.

• **In Scope (Core Deliverables)**:
  - [ ] **Frontend Command Portal (`SYS-04/frontend/`)**:
      - Served directly by FastAPI at `http://localhost:8000/` (Dark/Light mode, JetBrains Mono font, ApexCharts).
      - **View 1 - Executive Cockpit & Governance Invariants**: Live status of 3 models (`3/3 PRODUCTION READY`), satellite service health matrix, and live KPI gauges.
      - **View 2 - Interactive AI Live Inference Labs**: 4 interactive labs (1. LSTM Price Forecast, 2. IsoForest Anomaly Guard, 3. LightGBM Fraud Gate, 4. Historical 1h Reconciliation).
      - **View 3 - Model Registry & Lifecycle Management**: Live MLflow stage inspection, instant RAM hot-reload, Challenger promotion, and Rollback controls.
      - **View 4 - Data & Model Drift Observatory**: KS-Test distributions on 15m bars, PSI calculations on fraud score distributions, and SYS-06 alert logs.
      - **View 5 - Continuous Retraining & Feature Store**: ClickHouse Gold feature monitor (`crypto_klines_gold`), and Airflow retrain job tracker.
      - **View 6 - Telemetry, SLAs & Prometheus Metrics**: Real-time ApexCharts tracking P50/P95/P99 latency histograms, throughput, and error rates.
      - **View 7 - Operator Profile & Synchronized RBAC Security**: Profile identity card, shared JWT SSO session, and active permissions matrix.
  - [ ] **Operator Profile & RBAC Synchronization**:
      - Shared JWT authentication session with Admin Portal (`:8090`).
      - Synchronized operator identity (Avatar, Full Name, Email, and System Role).
      - Granular RBAC permissions enforcement:
        * `view_ai_models`: Access dashboards, telemetry, and forecasts.
        * `trigger_ai_inference`: Execute the 4 interactive testing labs.
        * `manage_ai_governance`: Execute weights Hot-Reload, Model Promotion, and policy threshold overrides.
  - [ ] **Backend Serving Modularization (`src/serving/`)**:
      - Pydantic v2 schemas for Request/Response validation.
      - 6 Grouped APIRouters: Inference, Registry/Hot-Reload, Governance/Drift, Retrain Trigger, Telemetry, and Profile Auth.
      - `ModelRuntimeManager` providing thread-safe in-memory caching and atomic model swapping.
  - [ ] **Centralized MLflow Tracking Container**:
      - Dedicated Docker container on port `5000` (`sys04-mlflow-server`).
      - SQLite metadata tracking store (`mlflow.db`) + MinIO S3 artifact repository.
      - 3 Registered Models officially tagged as `Production`.
  - [ ] **Ecosystem Integrations**:
      - Prometheus `/metrics` endpoint instrumented for SYS-03 (Prometheus :9090 / Grafana :3000).
      - Non-blocking HTTP POST webhook dispatcher to SYS-06 Alert Dispatcher (`:8080`).
      - ClickHouse Gold table reader (`crypto_klines_gold`) from SYS-05.
      - Airflow retrain callback trigger endpoint for SYS-02.
  - [ ] **Multi-Container Deployment & Test Suites**:
      - `docker-compose.yml` orchestrating `sys04-ai-models` (:8000) and `sys04-mlflow-server` (:5000).
      - Full automated PyTest suite (`tests/`) covering inference parity, hot-reload concurrency, drift detection, and latency SLA.

• **Out of Scope**:
  - Modifying external subsystem repositories (SYS-01, SYS-02, SYS-03, SYS-05, SYS-06).
  - Retraining neural network architectures from scratch (existing model weights are final).

---

## ▶ Expected Outcome
• **Operational Endpoints & Web Portals**:
  1. NHFinance MLOps & Model Governance Center: `http://localhost:8000/` (or `/dashboard`)
  2. MLflow Tracking & Model Registry UI: `http://localhost:5000`
  3. FastAPI Interactive OpenAPI Swagger Docs: `http://localhost:8000/docs`
  4. Prometheus Telemetry Exporter: `http://localhost:8000/metrics`
• **Containerized Services**:
  - Container `nhfinance_sys04_ai_models` (FastAPI Serving + Center UI on port 8000).
  - Container `nhfinance_sys04_mlflow` (MLflow Server on port 5000).
• **Production SLAs & Governance Invariants**:
  - Fraud Scoring P95 Latency: < 15ms.
  - Price Forecasting P95 Latency: < 50ms.
  - Zero-Downtime Hot-Reload: < 1.5s with zero socket disconnects under 100 concurrent requests.
  - Governance Invariant: Zero Automatic User Account Bans triggered by market volatility.
• **Verification**:
  - All test suites in `tests/` pass with 100% success rate.
  - Health check script `check_health.ps1` confirms ports `8000` and `5000` are `[ONLINE]`.

---

## ▶ Summary Result
*(To be completed by the assignee upon milestone delivery)*

| Phase | Milestone | Assignee | Status | Latency / Metric | Note |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Phase 01 | Modular Codebase (`src/serving/`) | [Assignee] | [Pending / Done] | - | Routers, Schemas & Services separated |
| Phase 02 | MLflow Container (:5000) | [Assignee] | [Pending / Done] | 3 Models Tagged | SQLite + MinIO S3 connected |
| Phase 03 | Hot-Reload & Promotion Gate | [Assignee] | [Pending / Done] | < 1.5s Reload | Champion vs Challenger rule active |
| Phase 04 | Drift Engine & SYS-06 Alert | [Assignee] | [Pending / Done] | KS-Test & PSI OK | Automated Webhook alert active |
| Phase 05 | Prometheus Metrics & Retrain API | [Assignee] | [Pending / Done] | `/metrics` Active | ClickHouse Gold adapter ready |
| Phase 06 | Docker Compose & SLA Benchmark | [Assignee] | [Pending / Done] | P95 < 20ms | 2 containers reporting ONLINE |
| Phase 07 | NHFinance MLOps & Governance Center | [Assignee] | [Pending / Done] | Port 8000 Live | 7 Views + Profile & RBAC synced |

---

## ▶ Feedback
• **Code Reviewer**: [Reviewer Name]  
• **Handover Date**: [YYYY-MM-DD]  
• **Review Notes**: [Feedback & Observations]  
• **Sign-off Status**: [Approved / Revisions Requested]
