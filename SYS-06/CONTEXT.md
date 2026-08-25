# CONTEXT: SYS-06 Operational Alerting & Notification System

This document defines the ubiquitous language for SYS-06, the operational alerting and notification system in the 6-system thesis architecture.

## Official Source

SYS-06 follows:

- `Plan/systems_docs/06_Operational_Alerting_Notification_System.md`

This folder improves the official document into an implementation plan and build structure.

## Purpose

SYS-06 is the real-time operational alerting channel for the platform. It receives alert events from data pipelines, infrastructure monitoring, MLOps services, lakehouse/database systems, and business anomaly detection. It formats, filters, rate-limits, routes, and dispatches those alerts to the correct user group.

## Core Terms

- **Alert Event**: JSON payload describing a problem or important condition.
- **Alert Type**: Category of alert, such as `INFRASTRUCTURE_FAILURE`, `MARKET_ANOMALY`, `FRAUD_TRANSACTION`, `DATA_QUALITY`, or `PIPELINE_FAILURE`.
- **Severity Level**: Operational urgency level: `CRITICAL`, `WARNING`, or `INFO`.
- **Deduplication Key**: Stable key used to suppress repeated alerts for the same event during a time window.
- **Deduplication Window**: Time period where repeated alerts are suppressed. Initial target: 300 seconds.
- **Rate Limit**: Maximum number of notifications sent per minute. Initial target: 10 messages per minute.
- **Dispatcher Engine**: Core service that receives an alert, validates it, filters it, formats it, and sends it.
- **Notification Channel**: Destination adapter such as Telegram Bot API, Slack Webhook, SMS, or email.
- **Alert Fatigue**: Problem where too many repeated messages cause users to ignore important alerts.

## Initial Alert Sources

- `SYS-01`: market anomaly or dashboard-triggered business alert.
- `SYS-02`: Airflow DAG failure, SLA miss, pipeline run failure, data quality failure.
- `SYS-03`: Docker container down, disk usage high, CPU/RAM high, Kafka consumer lag high.
- `SYS-04`: model drift, high fraud score, model serving failure, MLflow registry issue.
- `SYS-05`: MinIO unavailable, ClickHouse unavailable, dataset freshness late, storage threshold exceeded.

## Initial Notification Targets

- Telegram group for thesis/local operation.
- Slack webhook as enterprise-style extension.
- SMS/email can be later expansion after the first version is stable.

## SLA Targets

- Alert delivery latency: less than 3 seconds for normal successful dispatch.
- Deduplication window: 5 minutes.
- Rate limit: 10 messages per minute.
- Dispatcher must continue running even when one notification channel fails.
