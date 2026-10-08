# 🛠️ NOTION ENGINEERING & IMPLEMENTATION TASK TRACKER
## System: SYS-03 (NHFinance SOC Hub & Infrastructure Operations Center)
### Mode: Technical Task Execution & Development Runbook

---

### ▶ Context
* **Target Subsystem**: `SYS-03` (`Systems-Graduation-Thesis/SYS-03/`)
* **Operational Role**: Enterprise Operations Center (SOC Hub) & Full-Stack Telemetry Harvester.
* **Architecture Topology**: Decoupled, Single Pane of Glass architecture running Prometheus 2.45+ TSDB (`:9090`) and Grafana 10.x UI (`:3000`) over shared Docker bridge network `fintech_net`.
* **Monitored Perimeter**: 11+ containerized services across data ingestion (WSIS), lakehouse storage (MinIO `:9000`, ClickHouse `:8123`), workflow orchestration (Airflow `:8085`), model serving (FastAPI `:8000`), analytics (Streamlit `:8501`), and physical host hardware.
* **Underlying Problem to Solve**: High-frequency financial tick pipelines are vulnerable to silent backpressure (Kafka consumer lag), runaway memory consumption (OOM kills on ClickHouse/Spark), and storage saturation. Telemetry and alerting must be implemented as automated Infrastructure-as-Code (IaC) without manual UI click-ops.

---

### ▶ Purpose
1. **Infrastructure as Code (IaC) Provisioning**: Deploy Prometheus, Grafana, cAdvisor, Node Exporter, and Kafka Exporter via a declarative, self-contained `docker-compose.yml` strictly within `SYS-03/`.
2. **Deterministic Scrape Engine Implementation**: Configure `prometheus.yml` to pull OpenMetrics every **15 seconds** from all 11 services and exporters, enforcing a 15-day TSDB retention boundary.
3. **Automated Incident Alerting Engine**: Author 5 production-grade PromQL rules in `alert_rules.yml` and wire Alertmanager to dispatch structured JSON payloads directly to `http://sys06_alert_dispatcher:8006/api/v1/alerts` (< 30s SLA).
4. **Zero-Touch Dashboard & Identity Provisioning**: Implement declarative YAML provisioning for Grafana datasources, pre-compile 3 production JSON dashboards, and synchronize operator identity with Admin Portal's `profiles.json` (Tab 5 Profile & RBAC).
5. **CI/CD Validation & Chaos Engineering**: Build automated `pytest` test suites to validate PromQL syntax (`promtool`) and simulate cluster failure scenarios (e.g., stopping ClickHouse to verify Telegram dispatch).

---

### ▶ Scope
* **Phase-by-Phase Implementation Deliverables (Strictly inside `SYS-03/`)**:
  * **Phase 01: Exporters & Compose Topology**:
    * Create `.env` and `.env.example` (ports `3000`, `9090`, `8085`, `9100`, `9308`, credentials).
    * Author `docker-compose.yml` defining services on `fintech_net`:
      * `sys03_prometheus` (`prom/prometheus:v2.45.0`)
      * `sys03_grafana` (`grafana/grafana:10.0.3`)
      * `sys03_cadvisor` (`gcr.io/cadvisor/cadvisor:v0.47.2`)
      * `sys03_node_exporter` (`prom/node-exporter:v1.6.1`)
      * `sys03_kafka_exporter` (`danielqsj/kafka-exporter:v1.7.0`)
  * **Phase 02: Prometheus Core Engine & Scraping Config**:
    * Author `prometheus/prometheus.yml` with global `scrape_interval: 15s`, `evaluation_interval: 15s`.
    * Define scrape jobs: `prometheus`, `cadvisor`, `node_exporter`, `kafka_exporter`, `fastapi_serving`, and `clickhouse`.
  * **Phase 03: Alert Rules & Webhook Dispatching**:
    * Author `prometheus/alert_rules.yml` containing:
      * `ServiceDown`: `up == 0` for 1m (Severity: Critical)
      * `HighKafkaConsumerLag`: `sum(kafka_consumergroup_lag) > 5000` for 2m (Severity: Critical)
      * `ContainerHighMemory`: `(container_memory_usage_bytes / container_spec_memory_limit_bytes) > 0.85` for 3m (Severity: Warning)
      * `HostDiskSpaceLow`: `(node_filesystem_free_bytes / node_filesystem_size_bytes) < 0.15` for 5m (Severity: Warning)
      * `HighModelServingLatency`: `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le)) > 0.05` for 2m (Severity: Warning)
  * **Phase 04: Zero-Touch Dashboard & Profile Provisioning**:
    * Author `grafana/provisioning/datasources/prometheus_datasource.yml` (auto-target `http://prometheus:9090`).
    * Author `grafana/provisioning/dashboards/dashboards.yml` (auto-mount folder `/var/lib/grafana/dashboards`).
    * Generate 3 JSON Dashboards + SOC Hub Master Layout:
      * `01_system_overview.json`: 11 containers CPU/RAM matrix & host storage.
      * `02_kafka_streaming_lag.json`: Consumer lag area chart, topic offset progress, backpressure rates.
      * `03_fastapi_serving_perf.json`: P50/P95/P99 latency, RPS, HTTP 5xx errors.
      * Tab 5 Profile & RBAC View: Operator identity parity linked to `Admin_Portal/backend/app/data/profiles.json`.
  * **Phase 05: Automated Test Suite & Chaos Verification**:
    * `tests/test_prometheus_syntax.py`: Execute `promtool check config` and `promtool check rules`.
    * `tests/test_exporters_connectivity.py`: Python socket and HTTP health probe tests.
    * Chaos Test: Validate Telegram alert triggers under simulated node termination.
  * **Phase 06: Runbook & Ecosystem Documentation**:
    * Author `docs/SYS03_Operations_Guide_A_to_Z.md`.
    * Finalize `README.md`.
* **Out-of-Scope**: Modifications outside `SYS-03/` (e.g., Admin Portal backend, Airflow DAG code, MinIO bucket policies).

---

### ▶ Expected Outcome
1. **Single Entrypoint SOC Hub UI**: `http://localhost:3000` serves the complete unified NHFinance SOC Hub with 5-tab zero-reload navigation.
2. **100% Declarative Zero-Touch Setup**: Executing `docker compose up -d` completely provisions datasources and dashboards without requiring manual UI configuration or credentials prompts.
3. **Automated Incident Escalation (< 30s SLA)**: Alertmanager evaluates rules every 15s and immediately posts valid JSON webhook payloads to SYS-06 upon breach.
4. **Strict SLA Telemetry Compliance**:
   * Scrape cycle latency < 1.5s per batch.
   * PromQL query render time < 25ms in Grafana.
   * Total memory footprint across all SYS-03 containers strictly `< 500MB RAM`.
5. **Verified Test Pass Rate**: 100% green test passes across `promtool` syntax checks, exporter health probes, and Chaos kill simulations.

---

### ▶ Summary Result
* **Technical Plan Status**: Master Engineering Specification v3.0 approved and ready for immediate execution.
* **File Architecture Defined**:
  ```
  SYS-03/
  ├── .env & .env.example
  ├── docker-compose.yml
  ├── prometheus/ (prometheus.yml, alert_rules.yml)
  ├── grafana/provisioning/ (datasources, dashboards)
  ├── grafana/dashboards/ (3 JSON files)
  ├── tests/ (promtool syntax, connectivity, chaos scripts)
  └── docs/ (Runbooks, Master Plans in VN and EN)
  ```
* **Execution Milestones**:
  * [x] Phase 00: Architecture Specification & Single Pane of Glass Design (Completed)
  * [ ] Phase 01: Compose Topology & Exporter Infrastructure
  * [ ] Phase 02: Prometheus Engine & Scrape Target Mapping
  * [ ] Phase 03: Alert Rules & SYS-06 Webhook Dispatcher
  * [ ] Phase 04: Grafana Provisioning & 3 Production Dashboards + Profile Tab
  * [ ] Phase 05: Automated Test Suite & Chaos Simulation
  * [ ] Phase 06: Runbook & Final Production Sign-off

---

### ▶ Feedback
* **Implementation Recommendation**: Use explicit volume mounts with read-only flags (`:ro`) for `/var/run/docker.sock` and Linux cgroups on cAdvisor and Node Exporter to maintain Windows/WSL2 host security.
* **Embedding Optimization**: Set `GF_SECURITY_ALLOW_EMBEDDING=true` and `GF_AUTH_ANONYMOUS_ENABLED=true` in Grafana environment configurations to ensure immediate, zero-friction iframe embedding in NHFinance Admin Portal (`:8090`).
* **Memory Tuning**: Limit Prometheus TSDB cache allocation via `--storage.tsdb.max-block-duration=2h` to maintain the strict `< 500MB RAM` SLA requirement.
* **Ready for Execution**: Trigger Phase 01 implementation immediately.
