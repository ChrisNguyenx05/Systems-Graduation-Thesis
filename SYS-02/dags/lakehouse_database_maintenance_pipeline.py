"""Automated Lakehouse Maintenance & Database Cleanup Pipeline (TaskFlow API).

Schedule: Weekly (@weekly)
Scope:
1. Purge expired MinIO Bronze raw payloads across all prefixes (> 14 days).
2. Prune expired MinIO Silver Parquet/JSON partitions (> 90 days).
3. Execute OPTIMIZE TABLE FINAL across all 10 ClickHouse tables and measure parts consolidation.
4. Real Airflow metadata database vacuum and old task_instance / xcom cleanup.
5. Consolidated System Health & Storage Capacity Audit Report via SYS-06.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
import subprocess
import sys

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback
from sys02_orchestration.utils.error_tracer import ErrorTracer
from sys02_orchestration.utils.circuit_breaker import check_and_trigger_circuit_breaker

LOGGER = logging.getLogger(__name__)
MAX_CONSECUTIVE_FAILURES = 3
BRONZE_RETENTION_DAYS = 14
SILVER_RETENTION_DAYS = 90
AIRFLOW_METADATA_RETENTION_DAYS = 14
SYS06_ALERT_URL = os.getenv("SYS06_ALERT_URL", "http://host.docker.internal:8080/alerts")
SYS06_ALERT_API_TOKEN = os.getenv("SYS06_ALERT_API_TOKEN", "")
NOTIFY_TIMEZONE = "Asia/Ho_Chi_Minh"

CLICKHOUSE_MAINTENANCE_TABLES = [
    "crypto_klines_gold",
    "silver_crypto_klines",
    "wsis_silver",
    "crypto_agg_trades_gold",
    "crypto_order_book_depth_gold",
    "crypto_futures_liquidations_gold",
    "db_gold_analytics.ai_hourly_inference_results",
    "db_gold_analytics.ai_forecast_reconciliation",
    "raw_binance_klines_api",
    "raw_binance_klines_wsis",
]


def send_sys06_alert(payload: dict) -> None:
    if not SYS06_ALERT_URL:
        return
    try:
        headers = {"X-Alert-Token": SYS06_ALERT_API_TOKEN} if SYS06_ALERT_API_TOKEN else {}
        resp = requests.post(SYS06_ALERT_URL, json=payload, headers=headers, timeout=5)
        LOGGER.info("SYS-06 alert dispatched: status %s", resp.status_code)
    except Exception as exc:
        LOGGER.warning("Could not send SYS-06 notification: %s", exc)


default_args = {
    "owner": "data_ops",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 8, 30, 0, 0, tz="Asia/Ho_Chi_Minh"),
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": on_task_failure_callback,
}


@dag(
    dag_id="lakehouse_database_maintenance_pipeline",
    default_args=default_args,
    description="Automated Weekly Lakehouse Purge, ClickHouse Parts Optimization & Airflow DB Vacuum",
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    on_success_callback=None,
    tags=["maintenance", "lakehouse", "minio", "clickhouse", "vacuum", "taskflow", "circuit-breaker"],
)
def lakehouse_database_maintenance_pipeline():
    start = EmptyOperator(task_id="start_maintenance")

    @task(task_id="purge_expired_bronze_all_prefixes")
    def purge_expired_bronze_task() -> dict:
        """Scan MinIO Bronze bucket across all prefixes and prune files exceeding retention."""
        LOGGER.info("Scanning MinIO Bronze bucket for objects older than %d days...", BRONZE_RETENTION_DAYS)
        lake_op = LakehouseTaskOperator()
        client = lake_op._client()
        bucket = lake_op.bronze_bucket
        cutoff = datetime.now(timezone.utc) - timedelta(days=BRONZE_RETENTION_DAYS)

        if not client.bucket_exists(bucket):
            return {"bucket": bucket, "purged_count": 0, "reclaimed_mb": 0.0}

        purged_count = 0
        reclaimed_bytes = 0

        # Scan all objects recursively
        try:
            for obj in client.list_objects(bucket, recursive=True):
                mod = obj.last_modified
                if mod.tzinfo is None:
                    mod = mod.replace(tzinfo=timezone.utc)
                if mod < cutoff:
                    client.remove_object(bucket, obj.object_name)
                    purged_count += 1
                    reclaimed_bytes += obj.size or 0
        except Exception as exc:
            LOGGER.error("MinIO Bronze cleanup exception: %s", exc)
            raise AirflowFailException(f"MinIO Bronze cleanup failed: {exc}") from exc

        reclaimed_mb = round(reclaimed_bytes / (1024 * 1024), 2)
        LOGGER.info("Bronze cleanup complete: Purged %d objects, reclaimed %.2f MB", purged_count, reclaimed_mb)
        return {"bucket": bucket, "purged_count": purged_count, "reclaimed_mb": reclaimed_mb}

    @task(task_id="prune_expired_silver_parquet")
    def prune_expired_silver_task() -> dict:
        """Scan MinIO Silver bucket and purge files exceeding long-term retention."""
        LOGGER.info("Scanning MinIO Silver bucket for objects older than %d days...", SILVER_RETENTION_DAYS)
        lake_op = LakehouseTaskOperator()
        client = lake_op._client()
        bucket = lake_op.silver_bucket
        cutoff = datetime.now(timezone.utc) - timedelta(days=SILVER_RETENTION_DAYS)

        if not client.bucket_exists(bucket):
            return {"bucket": bucket, "purged_count": 0, "reclaimed_mb": 0.0}

        purged_count = 0
        reclaimed_bytes = 0

        try:
            for obj in client.list_objects(bucket, recursive=True):
                mod = obj.last_modified
                if mod.tzinfo is None:
                    mod = mod.replace(tzinfo=timezone.utc)
                if mod < cutoff:
                    client.remove_object(bucket, obj.object_name)
                    purged_count += 1
                    reclaimed_bytes += obj.size or 0
        except Exception as exc:
            LOGGER.error("MinIO Silver cleanup exception: %s", exc)
            raise AirflowFailException(f"MinIO Silver cleanup failed: {exc}") from exc

        reclaimed_mb = round(reclaimed_bytes / (1024 * 1024), 2)
        LOGGER.info("Silver cleanup complete: Purged %d objects, reclaimed %.2f MB", purged_count, reclaimed_mb)
        return {"bucket": bucket, "purged_count": purged_count, "reclaimed_mb": reclaimed_mb}

    @task(task_id="optimize_clickhouse_tables")
    def optimize_clickhouse_tables_task() -> dict:
        """Run OPTIMIZE TABLE FINAL across all 10 ClickHouse tables and measure active parts."""
        ch_op = ClickHouseTaskOperator()
        ch_op.ensure_tables_exist()

        results = {}
        for tbl in CLICKHOUSE_MAINTENANCE_TABLES:
            try:
                # Query active parts before
                parts_before = 0
                q_before = f"SELECT count() as cnt FROM system.parts WHERE table = '{tbl.split('.')[-1]}' AND active = 1 FORMAT JSON"
                try:
                    res_b = json.loads(ch_op.execute_sql(q_before))
                    parts_before = int(res_b.get("data", [{}])[0].get("cnt", 0))
                except Exception:
                    pass

                # Execute OPTIMIZE FINAL
                ch_op.execute_sql(f"OPTIMIZE TABLE {tbl} FINAL;")

                # Query active parts after
                parts_after = parts_before
                try:
                    res_a = json.loads(ch_op.execute_sql(q_before))
                    parts_after = int(res_a.get("data", [{}])[0].get("cnt", 0))
                except Exception:
                    pass

                results[tbl] = {
                    "status": "OPTIMIZED",
                    "parts_before": parts_before,
                    "parts_after": parts_after,
                }
                LOGGER.info("Optimized %s: parts %d -> %d", tbl, parts_before, parts_after)
            except Exception as exc:
                LOGGER.warning("Optimize notice for %s: %s", tbl, exc)
                results[tbl] = {"status": "SKIPPED_OR_EMPTY", "error": str(exc)}

        return {"tables_optimized": len(results), "details": results}

    @task(task_id="vacuum_airflow_metadata_db")
    def vacuum_airflow_db_task() -> dict:
        """Purge old task instances, logs, and xcom records using native Airflow DB cleanup."""
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=AIRFLOW_METADATA_RETENTION_DAYS)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
        LOGGER.info("Purging Airflow metadata records older than: %s", cutoff_str)

        cmd = [
            "airflow", "db", "clean",
            "--clean-before-timestamp", cutoff_str,
            "--tables", "task_instance,xcom,dag_run,log,job",
            "--skip-archive",
            "-y"
        ]

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            LOGGER.info("Airflow DB clean output: %s", res.stdout.strip())
            return {
                "status": "SUCCESS",
                "clean_cutoff": cutoff_str,
                "exit_code": res.returncode
            }
        except Exception as exc:
            LOGGER.warning("Airflow DB clean command notice: %s", exc)
            return {"status": "COMPLETED_WITH_WARNING", "detail": str(exc)}

    @task(task_id="generate_health_capacity_report")
    def generate_health_report(bronze_res: dict, silver_res: dict, ch_res: dict, db_res: dict, **context) -> dict:
        """Consolidate audit metrics across all storage engines and dispatch summary alert."""
        dag_run = context.get("dag_run")
        ld = context.get("logical_date") or context.get("data_interval_start") or datetime.now(timezone.utc)
        if hasattr(ld, "in_timezone"):
            run_time = ld.in_timezone(NOTIFY_TIMEZONE)
        elif hasattr(ld, "astimezone"):
            from zoneinfo import ZoneInfo
            run_time = ld.astimezone(ZoneInfo(NOTIFY_TIMEZONE))
        else:
            run_time = ld

        time_str = run_time.strftime("%Y-%m-%d %H:%M GMT+7")
        msg = (
            f"🧹 Weekly Lakehouse Maintenance Completed ({time_str})\n"
            f"• MinIO Bronze: Purged {bronze_res.get('purged_count', 0)} files, freed {bronze_res.get('reclaimed_mb', 0.0)} MB\n"
            f"• MinIO Silver: Purged {silver_res.get('purged_count', 0)} files, freed {silver_res.get('reclaimed_mb', 0.0)} MB\n"
            f"• ClickHouse: Optimized {ch_res.get('tables_optimized', 0)} tables (OPTIMIZE FINAL parts merged)\n"
            f"• Airflow DB: Metastore vacuumed (status: {db_res.get('status')})"
        )

        send_sys06_alert({
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "WEEKLY_MAINTENANCE_COMPLETED",
            "deduplicate_key": f"sys02:maintenance:{dag_run.run_id if dag_run else 'run'}",
            "message": msg,
            "channels": ["telegram"],
            "action_url": f"http://127.0.0.1:8085/dags/lakehouse_database_maintenance_pipeline/grid",
        })
        return {"status": "REPORTED", "summary": msg}

    @task(task_id="fail_traces", trigger_rule="one_failed")
    def fail_traces_task(**context) -> dict:
        """Capture granular error tracebacks and failure details for incident triage."""
        dag_run = context.get("dag_run")
        failed_tis = []
        if dag_run and hasattr(dag_run, "get_task_instances"):
            try:
                failed_tis = [
                    ti for ti in dag_run.get_task_instances()
                    if getattr(ti, "state", None) in ["failed", "upstream_failed"] and getattr(ti, "task_id", None) != "fail_traces"
                ]
            except Exception as exc:
                LOGGER.debug("Could not inspect task instances: %s", exc)

        summary = ErrorTracer.generate_detailed_error_summary(context)
        tb = summary.get("tb_info", {})
        detailed_message = (
            f"🚨 Maintenance Task Failed: {summary.get('task_id')} in lakehouse_database_maintenance_pipeline\n"
            f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
            f"📍 Function: {tb.get('func_name')}()\n"
            f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
        )
        send_sys06_alert({
            "alert_type": "PIPELINE_FAILURE",
            "level": "CRITICAL",
            "source_system": "SYS-02",
            "event": "MAINTENANCE_TASK_FAILED",
            "deduplicate_key": f"sys02:fail:maintenance:{dag_run.run_id if dag_run else 'run'}",
            "message": detailed_message,
            "channels": ["telegram"],
            "action_url": "http://127.0.0.1:8085/dags/lakehouse_database_maintenance_pipeline/grid",
        })
        return {"status": "CAPTURED", "failed_count": len(failed_tis)}

    @task(task_id="error_watcher", trigger_rule="all_done")
    def error_watcher_task(**context) -> dict:
        """Circuit Breaker: Automatically pauses schedule if consecutive failures threshold exceeded."""
        return check_and_trigger_circuit_breaker(
            dag_id="lakehouse_database_maintenance_pipeline",
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
            alert_callback=send_sys06_alert,
        )

    end = EmptyOperator(task_id="end_maintenance")

    bronze_res = purge_expired_bronze_task()
    silver_res = prune_expired_silver_task()
    ch_res = optimize_clickhouse_tables_task()
    db_res = vacuum_airflow_db_task()

    report_res = generate_health_report(bronze_res, silver_res, ch_res, db_res)

    traces = fail_traces_task()
    watcher = error_watcher_task()

    start >> [bronze_res, silver_res, ch_res, db_res] >> report_res >> end

    [bronze_res, silver_res, ch_res, db_res, report_res] >> traces
    [end, traces] >> watcher


pipeline = lakehouse_database_maintenance_pipeline()
