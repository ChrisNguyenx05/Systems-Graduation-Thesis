"""Collect workbook-defined Binance metrics every 15 minutes (TaskFlow API)."""

from datetime import datetime, timedelta, timezone
import logging
import os
import sys
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.operators.market_metrics_operator import (
    BinanceMarketMetricsOperator,
    MarketMetricsPersistenceOperator,
)
from sys02_orchestration.utils.data_quality import validate_market_metrics_data
from sys02_orchestration.utils.error_tracer import ErrorTracer
from sys02_orchestration.utils.circuit_breaker import check_and_trigger_circuit_breaker
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback

MARKET_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT")
LOGGER = logging.getLogger(__name__)
NOTIFY_TIMEZONE = "Asia/Ho_Chi_Minh"
MAX_CONSECUTIVE_FAILURES = 3


def send_sys06_alert(payload: dict) -> None:
    url = os.getenv("SYS06_ALERT_URL", "").strip()
    token = os.getenv("SYS06_ALERT_API_TOKEN", "").strip()
    if not url or not token:
        LOGGER.warning("SYS-06 notification skipped: dispatcher configuration is missing")
        return
    try:
        response = requests.post(url, json=payload, headers={"X-Alert-Token": token}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("SYS-06 notification could not be delivered: %s", exc)


def notify_hourly_collection_success(context: dict) -> None:
    """Notify Telegram when 15m market metrics collection completes successfully."""
    dag_run = context.get("dag_run")
    if not dag_run:
        return
    run_time = getattr(dag_run, "start_date", None) or pendulum.now(NOTIFY_TIMEZONE)
    if hasattr(run_time, "in_timezone"):
        run_time = run_time.in_timezone(NOTIFY_TIMEZONE)
    send_sys06_alert(
        {
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "MARKET_METRICS_COLLECTION_COMPLETED",
            "deduplicate_key": f"sys02:market-metrics:{dag_run.dag_id}:{dag_run.run_id}",
            "message": f"Airflow DAG 04 (15m Metrics) completed successfully. Ingested into ClickHouse Gold at {run_time.strftime('%Y-%m-%d %H:%M %Z')}.",
            "channels": ["telegram"],
            "action_url": f"http://127.0.0.1:8085/dags/{dag_run.dag_id}/grid",
        }
    )


def send_task_failure_alert(context) -> None:
    """Dispatch critical alert to SYS-06 Telegram when a task in DAG 04 exhausted all 5 retries (Final Failed)."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    run_id = dag_run.run_id if dag_run else "unknown_run"
    try:
        summary = ErrorTracer.generate_detailed_error_summary(context)
        tb = summary.get("tb_info", {})
        detailed_message = (
            f"🚨 [FINAL FAILED] Airflow Task: {task_id} in 04_binance_market_metrics_15m_dag\n"
            f"⚠️ All 5 retries exhausted. Task permanently marked FAILED.\n"
            f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
            f"📍 Function: {tb.get('func_name')}()\n"
            f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
        )
    except Exception as exc:
        detailed_message = f"🚨 [FINAL FAILED] Task: {task_id} in 04_binance_market_metrics_15m_dag (Retries > 5): {exc}"

    send_sys06_alert({
        "alert_type": "PIPELINE_FAILURE",
        "level": "CRITICAL",
        "source_system": "SYS-02",
        "event": "AIRFLOW_TASK_FINAL_FAILED",
        "deduplicate_key": f"sys02:final-failure:04_binance_market_metrics_15m_dag:{task_id}:{run_id}",
        "message": detailed_message,
        "channels": ["telegram"],
        "action_url": f"http://127.0.0.1:8085/dags/04_binance_market_metrics_15m_dag/grid",
    })


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2026, 8, 30, 22, 0, tz="Asia/Ho_Chi_Minh"),
    "on_failure_callback": on_task_failure_callback,
}


@dag(
    dag_id="04_binance_market_metrics_15m_dag",
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    on_success_callback=None,
    tags=["binance", "15m", "market-metrics", "minio", "clickhouse", "taskflow", "circuit-breaker"],
)
def binance_market_metrics_15m_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    @task(task_id="extract_market_metrics", execution_timeout=timedelta(minutes=15))
    def extract_metrics_task() -> dict:
        """Extract multi-symbol spot ticker, orderbook, and liquidations from Binance."""
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        collector = BinanceMarketMetricsOperator(MARKET_SYMBOLS)
        active, unavailable = collector.available_symbols()
        rows = collector.collect_spot(active, now)
        liq_sec = int(os.getenv("LIQUIDATION_COLLECTION_SECONDS", "15"))
        rows["futures_liquidations"] = collector.collect_liquidations(liq_sec, now)
        key = datetime.fromtimestamp(now / 1000, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        persistence = MarketMetricsPersistenceOperator()
        bronze_paths = persistence.write_layer("bronze", key, rows)
        return {
            "run_key": key,
            "available_symbols": active,
            "unavailable_symbols": unavailable,
            "bronze_paths": bronze_paths,
        }

    @task(task_id="transform_market_metrics_silver")
    def transform_metrics_silver_task(extracted: dict) -> dict:
        """Process Bronze market metric payloads into normalized Silver Parquet partitions."""
        persistence = MarketMetricsPersistenceOperator()
        raw_data = persistence.read_layer(extracted["bronze_paths"])
        silver_paths = persistence.write_layer("silver", extracted["run_key"], raw_data)
        return {"silver_paths": silver_paths, "run_key": extracted["run_key"]}

    @task(task_id="validate_market_metrics_quality_gate")
    def validate_quality_gate_task(silver_res: dict) -> dict:
        """Verify API market metrics integrity (trades, orderbook, bounds) prior to ClickHouse Gold load."""
        persistence = MarketMetricsPersistenceOperator()
        silver_data = persistence.read_layer(silver_res["silver_paths"])
        val_res = validate_market_metrics_data(silver_data)
        if not val_res.get("passed", False):
            raise AirflowFailException(f"Market metrics quality gate failed: {val_res.get('errors')}")
        LOGGER.info(
            "Market metrics quality gate passed successfully (%d trades, %d depth snapshots)",
            val_res.get("agg_trades_count", 0),
            val_res.get("order_book_depth_count", 0)
        )
        return {"validation": val_res, "silver_paths": silver_res["silver_paths"]}

    @task(task_id="load_market_metrics_gold")
    def load_metrics_gold_task(quality_res: dict) -> dict:
        """Ingest vetted Silver metrics into ClickHouse Gold analytics tables."""
        persistence = MarketMetricsPersistenceOperator()
        silver_data = persistence.read_layer(quality_res["silver_paths"])
        return persistence.load_gold(silver_data)

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
        for ti in failed_tis:
            task_id = getattr(ti, "task_id", "unknown")
            run_id = getattr(ti, "run_id", "unknown")
            LOGGER.error("fail_traces detected market metrics failure: %s in run: %s", task_id, run_id)
        return {"status": "CAPTURED", "failed_count": len(failed_tis)}

    @task(task_id="error_watcher", trigger_rule="all_done")
    def error_watcher_task(**context) -> dict:
        """Circuit Breaker: Automatically pauses schedule if consecutive failures threshold exceeded."""
        dag_id = context.get("dag").dag_id if context.get("dag") else "04_binance_market_metrics_15m_dag"
        return check_and_trigger_circuit_breaker(
            dag_id=dag_id,
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
            alert_callback=send_sys06_alert
        )

    end = EmptyOperator(task_id="end_pipeline")

    extracted_out = extract_metrics_task()
    silver_out = transform_metrics_silver_task(extracted_out)
    quality_out = validate_quality_gate_task(silver_out)
    gold_out = load_metrics_gold_task(quality_out)

    traces = fail_traces_task()
    watcher = error_watcher_task()

    start >> extracted_out
    gold_out >> end

    [extracted_out, silver_out, quality_out, gold_out] >> traces
    [gold_out, traces] >> watcher


market_metrics_dag = binance_market_metrics_15m_pipeline()
