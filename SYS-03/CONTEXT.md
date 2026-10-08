# 📖 CONTEXT: SYS-03 Infrastructure & Operations Center Domain Model

This document defines the **Ubiquitous Language** and operational boundaries for **SYS-03 (Infrastructure & Operations Center - SOC)** within the Financial Analytics & Fraud Detection Enterprise Platform.

---

## 📚 Ubiquitous Language & Terminology

### 1. Monitoring & Telemetry Concepts
- **Time-Series Metric**: A timestamped numerical measurement recorded sequentially (e.g., `cpu_usage_percent`, `memory_bytes_used`).
- **Scrape Interval**: The scheduled periodicity (default `15s`) with which Prometheus queries HTTP `/metrics` endpoints across target exporters.
- **PromQL (Prometheus Query Language)**: The functional expression language used to filter, aggregate, and rate-calculate time-series data vectors.
- **Metric Vector**: An instant vector (current value per time series) or range vector (buffer of values over a time duration, e.g., `[5m]`).

### 2. Exporter Infrastructure Concepts
- **cAdvisor (Container Advisor)**: Daemon collecting resource usage and performance characteristics of running Docker containers directly from Linux kernel Cgroups.
- **Node Exporter**: Host-level hardware and OS telemetry collector (CPU idle/iowait, RAM buffers/cache, filesystem disk fullness, network duplex bandwidth).
- **Kafka Exporter**: Specialized exporter extracting consumer group offsets, partition lag, and broker throughput from the Apache Kafka cluster.
- **Application Instrumentation**: Prometheus client libraries integrated into Python services (FastAPI Serving, Airflow StatsD) exposing custom application-level metrics.

### 3. Kafka Streaming & Backpressure Concepts
- **Consumer Lag**: The delta between the latest produced message offset in a Kafka partition and the current committed offset of a consumer group (e.g., Spark Streaming). A growing lag indicates processing bottlenecks.
- **Zero-Latency SLA**: Operating guarantee requiring Kafka Consumer Lag to remain strictly below 5,000 records under peak market volatility.
- **Backpressure**: The adverse condition where downstream database writes or stream transformations cannot keep pace with high-frequency Binance ingestion.

### 4. Alerting & Threshold Concepts
- **Alert Rule**: A PromQL expression coupled with a duration threshold (`for: 2m`) and severity label (`critical`, `warning`, `info`).
- **Firing State**: The active condition when an alert expression evaluates to true continuously past its configured hold duration.
- **Alert Dispatch Webhook**: The outbound HTTP POST notification sent from Prometheus/Alertmanager to the SYS-06 alert integration point.

---

## ⚙️ Operational Port Mapping & Isolation
- **Grafana Visualization UI**: Port `3000` (External Host Binding: `http://localhost:3000`, default credentials `admin:admin`)
- **Prometheus Time-Series Engine**: Port `9090` (External Host Binding: `http://localhost:9090`)
- **Exporters Network**:
  - `cadvisor`: Port `8085` (host) / `8080` (internal)
  - `node_exporter`: Port `9100` (internal)
  - `kafka_exporter`: Port `9308` (internal)
- **Integration Destination**: Forwards firing alert payloads to `sys06_alert_dispatcher:8006` (SYS-06).
