# SYS-06 Operational Alerting & Notification System

SYS-06 follows the official master-plan document:

- `Plan/systems_docs/06_Operational_Alerting_Notification_System.md`

This folder combines two ideas into one stronger version:

1. **Your official plan**: SYS-06 is the Operational Alerting & Notification System.
2. **The improved architecture plan**: SYS-06 also becomes the alert integration point that links the other systems through alert contracts, source mapping, health inputs, and notification routing.

SYS-06 is still one of the original 6 systems. It is not a new seventh system and it is not replacing the other systems.

## Role In The 6-System Architecture

- `SYS-01`: Business dashboard can show alert state and business anomaly alerts.
- `SYS-02`: Airflow and pipeline jobs can trigger pipeline failure, SLA miss, and data quality alerts.
- `SYS-03`: Infrastructure monitoring can trigger CPU, memory, disk, Kafka lag, and container failure alerts.
- `SYS-04`: MLOps can trigger model drift, fraud score, prediction, and serving failure alerts.
- `SYS-05`: Lakehouse and database systems can trigger storage, ClickHouse, MinIO, and dataset freshness alerts.
- `SYS-06`: Receives, validates, deduplicates, rate-limits, formats, routes, and dispatches alerts.

## Combined Scope

Official SYS-06 scope:

- Telegram alert dispatching
- Slack alert dispatching
- Critical/warning/info message formatting
- Deduplication to reduce repeated alerts
- Rate limiting to prevent alert fatigue

Improved integration scope:

- Shared alert JSON contract for all systems
- Source-system alert mapping
- Severity and escalation rules
- Health/status alert input from infrastructure and data systems
- Dispatch routing by role, severity, and channel
- Build-ready Docker and test plan

## Build Phases

- `phase-00-context`: system inventory and alert source mapping
- `phase-01-alert-contracts`: alert payload schema and severity rules
- `phase-02-dispatcher-engine`: core Python alert dispatcher
- `phase-03-notification-channels`: Telegram, Slack, and future channel adapters
- `phase-04-dedup-rate-limit`: duplicate suppression and rate limiting
- `phase-05-system-integrations`: alert links from SYS-01 through SYS-05
- `phase-06-container-build`: Docker Compose, environment variables, and runtime checks
- `docs`: build plan, runbook, and architecture notes

Start with `docs/BUILD_PLAN.md`.
