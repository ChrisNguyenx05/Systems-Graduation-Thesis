# SYS-03: Infrastructure & Operations Center (SYS-OPS)

SYS-03 is the centralized **Systems Operations Center (SOC) & Infrastructure Observability Hub** of the Financial Analytics & Fraud Detection Enterprise Platform.

It provides DevOps engineers, Site Reliability Engineers (SREs), and system administrators with complete 360-degree observability into host hardware, Docker container lifecycles, Kafka streaming pipeline backpressure, and database health.

## 📌 Architecture Reference
- Official Master Specification: `Plan/systems_docs/03_Infrastructure_Operations_Center.md`
- Master Plan v2.0: `Plan/master_plan(update_2026_08_07).html`

## 🚀 Key Responsibilities
1. **Host & Container Resource Telemetry**: Real-time tracking of CPU utilization, RAM pressure, Disk I/O saturation, and Network traffic across all 11 Docker services via Prometheus, Node Exporter, and cAdvisor.
2. **Kafka Streaming Consumer Lag Observability**: Continuous monitoring of message offset lag between Binance Ingestion and Spark Streaming workers via Kafka Exporter to guarantee zero data bottleneck.
3. **Automated Incident Alerting (SYS-06 Integration)**: Evaluating PromQL alert rules every 15s and dispatching structured alert payloads to the SYS-06 Notification Dispatcher when thresholds are breached.
4. **Interactive Grafana Dashboards**: Pre-provisioned dashboards for system health, streaming lag, and container performance.

## 🧭 Directory Structure & Documentation
- `CONTEXT.md`: Ubiquitous domain language, exporter definitions, and telemetry concepts.
- `docs/PLANNING_SYS03_SOC_HUB.md`: **Master Planning Specification (v3.0 - Tiếng Việt)** - NHFinance SOC Hub Architecture (Single Pane of Glass, 3 Dashboards, Alert SLAs).
- `docs/PLANNING_SYS03_SOC_HUB_EN.md`: **Master Engineering Plan (v3.0 - English)** - Full English specification.
- `docs/BUILD_PLAN.md`: Technical specification covering telemetry architecture and scraping workflows.

👉 **Ready to Build**: Follow [docs/PLANNING_SYS03_SOC_HUB.md](docs/PLANNING_SYS03_SOC_HUB.md) (or [English Version](docs/PLANNING_SYS03_SOC_HUB_EN.md)) to implement Phase 01 to Phase 06.

