"""Hourly real Binance -> MinIO Bronze/Silver -> ClickHouse Gold pipeline (TaskFlow API)."""

from datetime import datetime, timedelta, timezone
import logging
import os
import sys

import requests
try:
    from airflow.sdk import Asset
except ImportError:
    try:
        from airflow.assets import Asset
    except ImportError:
        from airflow.datasets import Dataset as Asset

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.config import SYMBOL_TO_PAIR_ID
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
from sys02_orchestration.utils.data_quality import validate_klines_data, reconcile_api_with_wsis_bronze
from sys02_orchestration.utils.code_version_tracker import get_pipeline_source_tag
from sys02_orchestration.utils.error_tracer import ErrorTracer
from sys02_orchestration.utils.circuit_breaker import check_and_trigger_circuit_breaker
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback

MARKET_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT")
BINANCE_KLINES_GOLD_DATASET = Asset("clickhouse://default/binance_klines_gold")
LOGGER = logging.getLogger(__name__)
MAX_CONSECUTIVE_FAILURES = 3


def send_sys06_alert(payload: dict) -> None:
    """Post an alert to SYS-06 without changing the Airflow task/DAG outcome."""
    url = os.getenv("SYS06_ALERT_URL", "").strip()
    token = os.getenv("SYS06_ALERT_API_TOKEN", "").strip()
    if not url or not token:
        LOGGER.warning("SYS-06 alert skipped: dispatcher configuration is missing")
        return
    try:
        response = requests.post(
            url,
            json=payload,
            headers={"X-Alert-Token": token},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("SYS-06 alert could not be delivered: %s", exc)


def send_failure_alert(context) -> None:
    """Notify Telegram when one Airflow task fails with granular root-cause trace."""
    summary = ErrorTracer.generate_detailed_error_summary(context)
    LOGGER.error(summary.get("banner_text", "Task failed"))
    
    tb = summary.get("tb_info", {})
    detailed_message = (
        f"🚨 Airflow Task Failed: {summary.get('task_id')} in DAG {summary.get('dag_id')}\n"
        f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
        f"📍 Function: {tb.get('func_name')}()\n"
        f"📍 Code: {tb.get('code_snippet')}\n"
        f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
    )
    task_instance = context.get("task_instance")
    if task_instance:
        send_sys06_alert(
            {
                "alert_type": "PIPELINE_FAILURE",
                "level": "CRITICAL",
                "source_system": "SYS-02",
                "event": "AIRFLOW_TASK_FAILED",
                "deduplicate_key": f"sys02:failure:{task_instance.dag_id}:{task_instance.task_id}:{task_instance.run_id}",
                "message": detailed_message,
                "channels": ["telegram"],
                "action_url": f"http://localhost:8085/dags/{task_instance.dag_id}/grid",
            }
        )
        ErrorTracer.persist_error_to_metadata(
            summary,
            symbol="ALL_PAIRS",
            source=get_pipeline_source_tag(pipeline_name="api_klines"),
            type_method="API"
        )


def send_dag_success_alert(context) -> None:
    """Send exactly one Telegram message after a complete successful DAG run."""
    dag_run = context.get("dag_run")
    if dag_run:
        send_sys06_alert(
            {
                "alert_type": "SYSTEM_HEALTH",
                "level": "INFO",
                "source_system": "SYS-02",
                "event": "AIRFLOW_DAG_COMPLETED",
                "deduplicate_key": f"sys02:success:{dag_run.dag_id}:{dag_run.run_id}",
                "message": f"Airflow DAG {dag_run.dag_id} completed successfully.",
                "channels": ["telegram"],
                "action_url": f"http://localhost:8085/dags/{dag_run.dag_id}/grid",
            }
        )


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 8, 1),
    "on_failure_callback": on_task_failure_callback,
}


@dag(
    dag_id="01_binance_klines_etl_dag",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    on_success_callback=None,
    tags=["etl", "binance", "minio", "clickhouse", "taskflow", "circuit-breaker"],
)
def binance_klines_etl_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    @task(task_id="prepare_parameters")
    def prepare_parameters(**context) -> dict:
        """Initialize extraction parameters for all market symbols, computing idempotent time windows."""
        source_tag = get_pipeline_source_tag(__file__, component="api", metric_type="klines")
        
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}

        # 1. Custom parameter override via manual trigger or backfill call
        if conf.get("symbols") and isinstance(conf.get("symbols"), (list, tuple)):
            symbols = [s.strip().upper() for s in conf.get("symbols") if s.strip()]
        elif conf.get("symbol"):
            symbols = [conf.get("symbol").strip().upper()]
        else:
            symbols = list(MARKET_SYMBOLS)

        custom_limit = conf.get("limit")
        custom_start = conf.get("start_time") or conf.get("startTime")
        custom_end = conf.get("end_time") or conf.get("endTime")

        start_ms = None
        end_ms = None
        limit = int(custom_limit) if custom_limit else 60

        if custom_start and custom_end:
            try:
                start_ms = int(custom_start) if str(custom_start).isdigit() else int(datetime.fromisoformat(str(custom_start).replace("Z", "+00:00")).timestamp() * 1000)
                end_ms = int(custom_end) if str(custom_end).isdigit() else int(datetime.fromisoformat(str(custom_end).replace("Z", "+00:00")).timestamp() * 1000)
            except Exception as exc:
                LOGGER.warning("Could not parse custom start/end time: %s", exc)

        # 2. Idempotent Data Interval: compute exact start and end based on DAG Run execution window
        if start_ms is None or end_ms is None:
            data_interval_start = context.get("data_interval_start")
            data_interval_end = context.get("data_interval_end")
            if data_interval_start and data_interval_end:
                start_ms = int(data_interval_start.timestamp() * 1000)
                end_ms = int(data_interval_end.timestamp() * 1000)
                limit = 60

        if context.get("data_interval_start"):
            date_str = context.get("data_interval_start").date().isoformat()
        else:
            date_str = datetime.now(timezone.utc).date().isoformat()

        etl_config = {
            "symbols": symbols,
            "interval": "1m",
            "limit": limit,
            "start_time": start_ms,
            "end_time": end_ms,
            "source_tag": source_tag,
            "date_str": date_str
        }
        LOGGER.info("Idempotent ETL parameters prepared for %d symbols: %s (start_ms=%s, end_ms=%s, limit=%s)", len(symbols), symbols, start_ms, end_ms, limit)
        return etl_config

    @task(task_id="extract_bronze_data")
    def extract_bronze_task(etl_config: dict) -> dict:
        """Fetch raw kline candles from Binance API for all symbols and store into MinIO Bronze layer."""
        symbols = etl_config["symbols"]
        source_tag = etl_config["source_tag"]
        date_str = etl_config["date_str"]
        interval = etl_config.get("interval", "1m")
        limit = etl_config.get("limit", 60)
        url = "https://api.binance.com/api/v3/klines"
        lakehouse = LakehouseTaskOperator()

        by_symbol = {}
        for sym in symbols:
            pid = SYMBOL_TO_PAIR_ID.get(sym, 1)
            params = {
                "symbol": sym,
                "interval": interval,
                "limit": limit
            }
            if etl_config.get("start_time"):
                params["startTime"] = etl_config["start_time"]
            if etl_config.get("end_time"):
                params["endTime"] = etl_config["end_time"]

            LOGGER.info("Requesting Binance API %s for %s with params=%s", url, sym, params)
            try:
                response = requests.get(url, params=params, timeout=20)
                response.raise_for_status()
            except requests.RequestException as e:
                raise AirflowFailException(f"Binance API request failed for {sym}: {e}") from e

            records = [
                {
                    "pair_id": pid,
                    "symbol": sym,
                    "timestamp": int(row[0] // 1000),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "source": source_tag,
                    "type_method": "API",
                }
                for row in response.json()
            ]
            LOGGER.info("Extracted %d raw candles for %s from Binance", len(records), sym)
            res = lakehouse.write_bronze_klines(records, sym, date_str, source=source_tag)
            by_symbol[sym] = {
                "status": "SUCCESS",
                "bronze_output_path": res.get("bronze_output_path"),
                "records": records,
                "count": len(records),
            }

        total_count = sum(v["count"] for v in by_symbol.values())
        LOGGER.info("Successfully extracted %d total candles across %d symbols", total_count, len(symbols))
        return {
            "symbols": symbols,
            "date_str": date_str,
            "source_tag": source_tag,
            "by_symbol": by_symbol,
            "total_records": total_count,
        }

    @task(task_id="transform_silver_parquet")
    def transform_silver_task(bronze: dict) -> dict:
        """Deduplicate, clean and transform Bronze JSON to partitioned Silver Parquet for all symbols."""
        date_str = bronze.get("date_str") or datetime.now(timezone.utc).date().isoformat()
        source_tag = bronze.get("source_tag") or get_pipeline_source_tag(__file__, component="api", metric_type="klines")
        lakehouse = LakehouseTaskOperator()

        by_symbol = {}
        all_records = []
        for sym, s_data in bronze["by_symbol"].items():
            records = s_data["records"]
            for r in records:
                r["source"] = source_tag
                r["type_method"] = "API"
            silver_res = lakehouse.process_bronze_to_silver(records, sym, date_str, source=source_tag)
            by_symbol[sym] = {
                "status": "SUCCESS",
                "silver_output_path": silver_res.get("silver_output_path"),
                "cleaned_ticks": silver_res.get("cleaned_ticks", len(records)),
                "records": silver_res.get("records", records),
                "count": len(records),
            }
            all_records.extend(silver_res.get("records", records))

        LOGGER.info("Successfully transformed %d total candles across %d symbols to Silver", len(all_records), len(by_symbol))
        return {
            "symbols": bronze["symbols"],
            "date_str": date_str,
            "source_tag": source_tag,
            "by_symbol": by_symbol,
            "all_records": all_records,
            "total_records": len(all_records),
        }

    @task(task_id="validate_data_quality_gate")
    def data_quality_gate_task(silver: dict) -> dict:
        """
        Two-Level Data Quality Gate across all symbols:
        Level 1: Intra-source schema, null checks, and price boundaries on primary API records per symbol.
        Level 2: Inter-source cross-reconciliation against secondary WSIS Bronze stream per symbol.
        """
        validation_results = {}
        reconciliations = {}
        for sym, s_data in silver["by_symbol"].items():
            records = s_data["records"]
            # Level 1: Intra-Source Integrity Validation
            result = validate_klines_data(records)
            if not result.get("passed", False):
                raise AirflowFailException(f"Data quality gate failed on API records for {sym}: {result}")
            validation_results[sym] = result
            LOGGER.info("Level 1 Quality Gate Passed for %s: %d API records validated", sym, len(records))

            # Level 2: Inter-Source Cross-Reconciliation with WSIS Bronze
            recon = reconcile_api_with_wsis_bronze(records, sym, tolerance_pct=0.5)
            reconciliations[sym] = recon
            LOGGER.info("Level 2 WSIS Cross-Reconciliation for %s: %s (%s)", sym, recon.get("status"), recon.get("details"))

            if recon.get("status") == "DESYNC_WARNING":
                send_sys06_alert({
                    "alert_type": "DATA_QUALITY_WARNING",
                    "level": "WARNING",
                    "source_system": "SYS-02",
                    "event": "WSIS_API_PRICE_DRIFT",
                    "deduplicate_key": f"sys02:wsis-desync:{sym}:{datetime.now(timezone.utc).strftime('%Y%m%d%H')}",
                    "message": f"Data Quality Warning: Price drift between API and WSIS for {sym}: {recon.get('details')}",
                    "channels": ["telegram"],
                    "action_url": "http://localhost:8085/dags/01_binance_klines_etl_dag/grid",
                })

        return {"validations": validation_results, "wsis_reconciliations": reconciliations}

    @task(task_id="load_gold_clickhouse", outlets=[BINANCE_KLINES_GOLD_DATASET])
    def load_gold_task(silver: dict, quality: dict) -> dict:
        """Insert vetted records into ClickHouse Gold analytics table and assert freshness."""
        source_tag = silver.get("source_tag") or get_pipeline_source_tag(__file__, component="api", metric_type="klines")
        operator = ClickHouseTaskOperator()
        all_records = silver["all_records"]
        inserted = operator.insert_gold_klines(all_records, source=source_tag, type_method="API")
        freshness = operator.verify_table_freshness("crypto_klines_gold")
        if not freshness.get("is_fresh", False):
            raise AirflowFailException("ClickHouse Gold freshness verification failed")
        LOGGER.info("Inserted %s rows across %d symbols into ClickHouse crypto_klines_gold", inserted, len(silver["symbols"]))
        return {"inserted_rows": inserted, "symbols": silver["symbols"], "source": source_tag, "type_method": "API", **freshness}

    @task(task_id="fail_traces", trigger_rule="one_failed")
    def fail_traces_task(**context) -> dict:
        """
        Production Error Tracing Task:
        Executes on any upstream task failure to capture granular root-cause tracebacks,
        HTTP status codes, and exact source code file and line numbers.
        """
        dag_run = context.get("dag_run")
        failed_tis = []
        if dag_run and hasattr(dag_run, "get_task_instances"):
            try:
                failed_tis = [
                    ti for ti in dag_run.get_task_instances()
                    if getattr(ti, "state", None) in ["failed", "upstream_failed"] and getattr(ti, "task_id", None) != "fail_traces"
                ]
            except Exception as exc:
                LOGGER.debug("Could not query get_task_instances: %s", exc)
        
        details = []
        for ti in failed_tis:
            task_id = getattr(ti, "task_id", "unknown")
            run_id = getattr(ti, "run_id", "unknown")
            state = getattr(ti, "state", "unknown")
            msg = f"Task [{task_id}] encountered terminal failure in run [{run_id}]"
            LOGGER.error("fail_traces detected: %s", msg)
            details.append({"task_id": task_id, "state": state})

        LOGGER.warning("fail_traces captured %d failed tasks for audit and incident triage", len(failed_tis))
        return {"status": "CAPTURED", "failed_count": len(failed_tis), "tasks": details}

    @task(task_id="error_watcher", trigger_rule="all_done")
    def error_watcher_task(**context) -> dict:
        """
        Production Circuit Breaker & Schedule Watcher (Airflow 3 Compatible):
        Monitors consecutive pipeline run outcomes via Airflow REST API.
        If consecutive failure count exceeds threshold, automatically deactivates schedule.
        """
        dag_id = context.get("dag").dag_id if context.get("dag") else "01_binance_klines_etl_dag"
        return check_and_trigger_circuit_breaker(
            dag_id=dag_id,
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
            alert_callback=send_sys06_alert
        )

    end = EmptyOperator(task_id="end_pipeline")

    # Wire TaskFlow pipeline dependencies
    params_out = prepare_parameters()
    bronze_out = extract_bronze_task(params_out)
    silver_out = transform_silver_task(bronze_out)
    quality_out = data_quality_gate_task(silver_out)
    gold_out = load_gold_task(silver_out, quality_out)

    traces_out = fail_traces_task()
    watcher_out = error_watcher_task()

    start >> params_out
    gold_out >> end

    # Monitor failure traces and error watcher
    [bronze_out, silver_out, quality_out, gold_out] >> traces_out
    [gold_out, traces_out] >> watcher_out


pipeline = binance_klines_etl_pipeline()
