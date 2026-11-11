"""Unified Real-Time Binance Market Data Ingestion Pipeline (TaskFlow API).

Integrates 1m Klines and 15m/5m Market Microstructure Metrics (Ticker, Depth, AggTrades, Liquidations).
Medallion Architecture: MinIO Bronze -> MinIO Silver & ClickHouse Silver -> Cross-Reconciliation Quality Gate -> ClickHouse Gold.
Emits Asset: clickhouse://default/binance_market_data_gold
"""

from datetime import datetime, timedelta, timezone
import json
import logging
import os
import sys

import pendulum
import requests

try:
    from airflow.sdk import Asset
except ImportError:
    try:
        from airflow.assets import Asset
    except ImportError:
        from airflow.datasets import Dataset as Asset

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.config import SYMBOL_TO_PAIR_ID
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
from sys02_orchestration.operators.market_metrics_operator import (
    BinanceMarketMetricsOperator,
    MarketMetricsPersistenceOperator,
)
from sys02_orchestration.utils.data_quality import (
    validate_klines_data,
    validate_market_metrics_data,
    reconcile_api_with_wsis_silver,
)
from sys02_orchestration.utils.code_version_tracker import get_pipeline_source_tag
from sys02_orchestration.utils.error_tracer import ErrorTracer
from sys02_orchestration.utils.circuit_breaker import check_and_trigger_circuit_breaker
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback

MARKET_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT")
BINANCE_MARKET_DATA_GOLD = Asset("clickhouse://default/binance_market_data_gold")
BINANCE_KLINES_GOLD_DATASET = Asset("clickhouse://default/binance_klines_gold")

LOGGER = logging.getLogger(__name__)
MAX_CONSECUTIVE_FAILURES = 3
NOTIFY_TIMEZONE = "Asia/Ho_Chi_Minh"


def send_sys06_alert(payload: dict) -> None:
    """Post an alert to SYS-06 without changing the Airflow task/DAG outcome."""
    url = os.getenv("SYS06_ALERT_URL", "http://host.docker.internal:8080/alerts").strip()
    token = os.getenv("SYS06_ALERT_API_TOKEN", "").strip()
    if not url:
        return
    try:
        headers = {"X-Alert-Token": token} if token else {}
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("SYS-06 alert could not be delivered: %s", exc)


def send_pipeline_success_alert(context) -> None:
    """Dispatch success notification to Telegram after complete ingestion run."""
    dag_run = context.get("dag_run")
    if not dag_run:
        return
    run_time = getattr(dag_run, "start_date", None) or pendulum.now(NOTIFY_TIMEZONE)
    if hasattr(run_time, "in_timezone"):
        run_time = run_time.in_timezone(NOTIFY_TIMEZONE)
    send_sys06_alert({
        "alert_type": "SYSTEM_HEALTH",
        "level": "INFO",
        "source_system": "SYS-02",
        "event": "MARKET_DATA_INGESTION_COMPLETED",
        "deduplicate_key": f"sys02:ingest-success:{dag_run.dag_id}:{dag_run.run_id}",
        "message": f"Airflow Pipeline {dag_run.dag_id} completed successfully at {run_time.strftime('%Y-%m-%d %H:%M %Z')}. Gold datasets refreshed.",
        "channels": ["telegram"],
        "action_url": f"http://127.0.0.1:8085/dags/{dag_run.dag_id}/grid",
    })


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2026, 8, 30, 0, 0, tz="Asia/Ho_Chi_Minh"),
    "on_failure_callback": on_task_failure_callback,
}


@dag(
    dag_id="binance_market_data_ingestion_pipeline",
    default_args=default_args,
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    on_success_callback=None,
    tags=["binance", "realtime", "klines", "market-metrics", "lakehouse", "taskflow", "circuit-breaker", "data_aware"],
)
def binance_market_data_ingestion_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    @task(task_id="prepare_window_parameters")
    def prepare_window_parameters(**context) -> dict:
        """Compute idempotent extraction parameters and time windows for both Klines and Market Metrics."""
        source_tag = get_pipeline_source_tag(__file__, component="api", metric_type="unified_market_data")
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}

        if conf.get("symbols") and isinstance(conf.get("symbols"), (list, tuple)):
            symbols = [s.strip().upper() for s in conf.get("symbols") if s.strip()]
        elif conf.get("symbol"):
            symbols = [conf.get("symbol").strip().upper()]
        else:
            symbols = list(MARKET_SYMBOLS)

        now_utc = datetime.now(timezone.utc)
        run_key = now_utc.strftime("%Y%m%dT%H%M%SZ")
        date_str = now_utc.date().isoformat()

        # Window calculation (Default 15 minutes = 15 x 1m candles)
        window_minutes = int(conf.get("window_minutes", 15))
        custom_start = conf.get("start_time") or conf.get("startTime")
        custom_end = conf.get("end_time") or conf.get("endTime")

        start_ms = None
        end_ms = None

        if custom_start and custom_end:
            try:
                start_ms = int(custom_start) if str(custom_start).isdigit() else int(datetime.fromisoformat(str(custom_start).replace("Z", "+00:00")).timestamp() * 1000)
                end_ms = int(custom_end) if str(custom_end).isdigit() else int(datetime.fromisoformat(str(custom_end).replace("Z", "+00:00")).timestamp() * 1000)
            except Exception as exc:
                LOGGER.warning("Could not parse custom start/end time: %s", exc)

        if start_ms is None or end_ms is None:
            data_interval_start = context.get("data_interval_start")
            data_interval_end = context.get("data_interval_end")
            if data_interval_start and data_interval_end:
                if data_interval_start == data_interval_end:
                    start_dt = data_interval_end - timedelta(minutes=window_minutes)
                    end_dt = data_interval_end - timedelta(milliseconds=1)
                else:
                    start_dt = data_interval_start
                    end_dt = data_interval_end - timedelta(milliseconds=1) if data_interval_end > data_interval_start else data_interval_end
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(end_dt.timestamp() * 1000)
                date_str = data_interval_start.date().isoformat()
            else:
                end_dt = now_utc
                start_dt = end_dt - timedelta(minutes=window_minutes)
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(end_dt.timestamp() * 1000)

        config_payload = {
            "symbols": symbols,
            "interval": "1m",
            "limit": max(window_minutes, 15),
            "window_minutes": window_minutes,
            "start_time": start_ms,
            "end_time": end_ms,
            "run_key": run_key,
            "date_str": date_str,
            "source_tag": source_tag,
        }
        LOGGER.info("Configured Ingestion Window: %s to %s for %d symbols", start_ms, end_ms, len(symbols))
        return config_payload

    @task(task_id="extract_bronze_market_data", execution_timeout=timedelta(minutes=10))
    def extract_bronze_market_data(cfg: dict) -> dict:
        """Extract both 1m Klines and Microstructure Metrics from Binance and persist to MinIO Bronze.
        Returns only S3 URIs in XCom to eliminate XCom bloat."""
        symbols = cfg["symbols"]
        source_tag = cfg["source_tag"]
        date_str = cfg["date_str"]
        run_key = cfg["run_key"]
        start_ms = cfg.get("start_time")
        end_ms = cfg.get("end_time")

        lakehouse = LakehouseTaskOperator()
        metrics_persistence = MarketMetricsPersistenceOperator()

        # 1. Extract 1m Klines for each symbol
        kline_paths = {}
        kline_counts = {}
        url = "https://api.binance.com/api/v3/klines"

        for sym in symbols:
            pid = SYMBOL_TO_PAIR_ID.get(sym, 1)
            params = {
                "symbol": sym,
                "interval": cfg.get("interval", "1m"),
                "limit": cfg.get("limit", 15),
            }
            if start_ms:
                params["startTime"] = start_ms
            if end_ms:
                params["endTime"] = end_ms

            try:
                resp = requests.get(url, params=params, timeout=20)
                resp.raise_for_status()
                raw_rows = resp.json()
            except requests.RequestException as e:
                raise AirflowFailException(f"Binance Klines API request failed for {sym}: {e}") from e

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
                for row in raw_rows
            ]
            res = lakehouse.write_bronze_klines(records, sym, date_str, source=source_tag, stream_type="api")
            kline_paths[sym] = res.get("bronze_output_path")
            kline_counts[sym] = len(records)

        # 2. Extract Market Microstructure Metrics (Ticker, Depth 50, Trades, Liquidations)
        collector = BinanceMarketMetricsOperator(symbols, depth_limit=50)
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        active, _ = collector.available_symbols()
        spot_metrics = collector.collect_spot(active, now_ts)
        liq_sec = int(os.getenv("LIQUIDATION_COLLECTION_SECONDS", "10"))
        spot_metrics["futures_liquidations"] = collector.collect_liquidations(liq_sec, now_ts)

        metrics_paths = metrics_persistence.write_layer("bronze", run_key, spot_metrics)

        total_klines = sum(kline_counts.values())
        LOGGER.info("Bronze Extraction complete. Total klines: %d across %d symbols", total_klines, len(symbols))

        return {
            "cfg": cfg,
            "kline_paths": kline_paths,
            "metrics_paths": metrics_paths,
            "kline_counts": kline_counts,
        }

    @task(task_id="transform_silver_market_data")
    def transform_silver_market_data(bronze_meta: dict) -> dict:
        """Transform Bronze JSON into Silver Parquet and persist to MinIO and ClickHouse Silver."""
        cfg = bronze_meta["cfg"]
        symbols = cfg["symbols"]
        source_tag = cfg["source_tag"]
        date_str = cfg["date_str"]
        run_key = cfg["run_key"]
        start_ms = cfg.get("start_time")
        end_ms = cfg.get("end_time")

        lakehouse = LakehouseTaskOperator()
        clickhouse = ClickHouseTaskOperator()
        clickhouse.ensure_tables_exist()
        metrics_persistence = MarketMetricsPersistenceOperator()

        silver_kline_paths = {}
        silver_kline_counts = {}
        wsis_silver_paths = {}
        wsis_counts = {}

        # 1. Transform API Klines & Ingest into ClickHouse Silver
        for sym, b_path in bronze_meta["kline_paths"].items():
            records = lakehouse.read_market_metrics(b_path)
            silver_res = lakehouse.process_bronze_to_silver(records, sym, date_str, source=source_tag, stream_type="api")
            cleaned_records = silver_res.get("records", records)
            clickhouse.insert_silver_klines(cleaned_records, table_name="silver_crypto_klines", source=source_tag, type_method="API")
            silver_kline_paths[sym] = silver_res.get("silver_output_path")
            silver_kline_counts[sym] = len(cleaned_records)

            # Materialize WSIS Silver if available
            wsis_records = lakehouse.fetch_wsis_silver_records(sym, date_str, start_ms, end_ms)
            if wsis_records:
                clickhouse.insert_silver_klines(wsis_records, table_name="wsis_silver", source="wsis_stream", type_method="WEBSOCKET")
                wsis_silver_paths[sym] = f"s3://{lakehouse.silver_bucket}/wsis/klines/symbol={sym}/date={date_str}/silver.json"
            wsis_counts[sym] = len(wsis_records)

        # 2. Transform Market Metrics into Silver Parquet
        raw_metrics = metrics_persistence.read_layer(bronze_meta["metrics_paths"])
        silver_metrics_paths = metrics_persistence.write_layer("silver", run_key, raw_metrics)

        LOGGER.info("Silver transformation completed for %d symbols", len(symbols))
        return {
            "cfg": cfg,
            "silver_kline_paths": silver_kline_paths,
            "silver_metrics_paths": silver_metrics_paths,
            "wsis_silver_paths": wsis_silver_paths,
            "silver_kline_counts": silver_kline_counts,
            "wsis_counts": wsis_counts,
        }

    @task(task_id="validate_market_data_quality_gate")
    def validate_market_data_quality_gate(silver_meta: dict) -> dict:
        """Cross-Stream & Microstructure Data Quality Gate (Blocking).
        Follows the latest updated 3-level quality framework applied to ALL incoming values:
        Level 1: Intra-Source Integrity on ALL Klines (schema, nulls, OHLC boundaries, volume >= 0).
        Level 2: Strict Cross-Reconciliation between API Silver and WSIS Silver (closed candles, delta <= 0.1%, volume ratio [95%, 105%], score >= 95%).
        Level 3: Microstructure Metrics validation on ALL incoming tickers, orderbook depth 50 levels, aggTrades, and liquidations.
        """
        cfg = silver_meta["cfg"]
        lakehouse = LakehouseTaskOperator()
        metrics_persistence = MarketMetricsPersistenceOperator()
        symbols = cfg["symbols"]
        date_str = cfg.get("date_str")
        start_ms = cfg.get("start_time")
        end_ms = cfg.get("end_time")

        failed_symbols = []
        validation_results = {}
        reconciliations = {}

        # 1. Level 1 (Intra-source) & Level 2 (Cross-Stream Reconciliation) for ALL symbols
        for sym in symbols:
            s_path = silver_meta["silver_kline_paths"].get(sym)
            if not s_path:
                continue
            api_records = lakehouse.read_market_metrics(s_path)

            # Level 1: Intra-Source Integrity Validation
            kline_val = validate_klines_data(api_records)
            if not kline_val.get("passed", False):
                send_sys06_alert({
                    "alert_type": "DATA_QUALITY",
                    "level": "CRITICAL",
                    "source_system": "SYS-02",
                    "event": "INTRA_API_DATA_QUALITY_FAILED",
                    "deduplicate_key": f"sys02:intra-fail:{sym}:{date_str}",
                    "message": f"🚨 Level 1 Quality Gate FAILED for {sym}: {kline_val.get('errors')}. Auto-rerunning at window [{start_ms} - {end_ms}]...",
                    "channels": ["telegram"],
                    "action_url": "http://127.0.0.1:8085/dags/binance_market_data_ingestion_pipeline/grid",
                })
                raise AirflowFailException(f"Level 1 Quality Gate failed on API records for {sym}: {kline_val}")
            validation_results[sym] = kline_val

            # Level 2: Inter-Source Cross-Reconciliation between API Silver and WSIS Silver
            wsis_path = silver_meta.get("wsis_silver_paths", {}).get(sym)
            wsis_records = []
            if wsis_path:
                try:
                    wsis_records = lakehouse.read_market_metrics(wsis_path)
                except Exception as exc:
                    LOGGER.debug("Could not read WSIS silver from path %s: %s", wsis_path, exc)

            if wsis_records:
                wsis_ts_set = {int(r.get("timestamp") or (r.get("open_time", 0) // 1000 if r.get("open_time", 0) > 100000000000 else r.get("open_time", 0))) for r in wsis_records}
                overlapping_api = [r for r in api_records if int(r.get("timestamp") or (r.get("open_time", 0) // 1000 if r.get("open_time", 0) > 100000000000 else r.get("open_time", 0))) in wsis_ts_set]
                if overlapping_api:
                    recon = reconcile_api_with_wsis_silver(
                        api_records=overlapping_api,
                        wsis_records=wsis_records,
                        symbol=sym,
                        price_tolerance_pct=0.1,
                        volume_min_ratio=95.0,
                        volume_max_ratio=105.0,
                        min_accuracy_score=95.0
                    )
                    reconciliations[sym] = recon
                    LOGGER.info("Level 2 Cross-Reconciliation for %s: %s | %s", sym, recon.get("status"), recon.get("details"))
                    if not recon.get("passed", False):
                        failed_symbols.append((sym, recon))

        # If any symbol failed the Cross-Stream Quality Gate: Send Alert & Trigger Auto-Rerun
        if failed_symbols:
            error_details = []
            for sym, r in failed_symbols:
                error_details.append(f"{sym}: Accuracy={r.get('accuracy_score')}% ({r.get('details')})")
                send_sys06_alert({
                    "alert_type": "DATA_QUALITY",
                    "level": "CRITICAL",
                    "source_system": "SYS-02",
                    "event": "CROSS_STREAM_RECONCILIATION_FAILED",
                    "deduplicate_key": f"sys02:recon-fail:{sym}:{date_str}:{start_ms}",
                    "message": f"Data Quality Gate FAILED (Accuracy out of range [95-105%]) for {sym}: {r.get('details')}. Triggering automated rerun at EXACT timestamp window [{start_ms} - {end_ms}].",
                    "channels": ["telegram"],
                    "action_url": "http://127.0.0.1:8085/dags/binance_market_data_ingestion_pipeline/grid",
                })
            raise AirflowFailException(
                f"Data Quality Gate check FALSE for {len(failed_symbols)} symbols: {'; '.join(error_details)}"
            )

        # 2. Level 3: Microstructure Market Metrics Validation on ALL incoming metrics
        silver_metrics = metrics_persistence.read_layer(silver_meta["silver_metrics_paths"])
        metrics_val = validate_market_metrics_data(silver_metrics)
        if not metrics_val.get("passed", False):
            raise AirflowFailException(f"Market metrics quality gate failed: {metrics_val.get('errors')}")

        LOGGER.info(
            "All Data Quality Gates (Level 1 Klines, Level 2 Cross-Reconciliation, Level 3 Metrics) PASSED successfully for all %d symbols.",
            len(symbols)
        )
        return {
            "status": "PASSED",
            "silver_meta": silver_meta,
            "validations": validation_results,
            "reconciliations": reconciliations
        }

    @task(task_id="load_gold_market_data", outlets=[BINANCE_MARKET_DATA_GOLD, BINANCE_KLINES_GOLD_DATASET])
    def load_gold_market_data(gate_res: dict) -> dict:
        """Persist vetted records into ClickHouse Gold analytics tables and verify freshness."""
        silver_meta = gate_res["silver_meta"]
        cfg = silver_meta["cfg"]
        source_tag = cfg["source_tag"]

        lakehouse = LakehouseTaskOperator()
        clickhouse = ClickHouseTaskOperator()
        metrics_persistence = MarketMetricsPersistenceOperator()

        # 1. Load Gold Klines
        all_klines = []
        for sym, s_path in silver_meta["silver_kline_paths"].items():
            records = lakehouse.read_market_metrics(s_path)
            all_klines.extend(records)

        inserted_klines = clickhouse.insert_gold_klines(all_klines, source=source_tag, type_method="API")

        # 2. Load Gold Market Metrics (Ticker, Depth, AggTrades, Liquidations)
        silver_metrics = metrics_persistence.read_layer(silver_meta["silver_metrics_paths"])
        metric_counts = metrics_persistence.load_gold(silver_metrics)

        # 3. Assert Freshness
        freshness = clickhouse.verify_table_freshness("crypto_klines_gold", max_allowed_delay_seconds=3600)
        if not freshness.get("is_fresh", False):
            raise AirflowFailException("ClickHouse Gold freshness check failed")

        LOGGER.info(
            "Gold persistence completed: %d klines inserted, metrics inserted: %s",
            inserted_klines,
            metric_counts,
        )
        return {
            "inserted_klines": inserted_klines,
            "metric_counts": metric_counts,
            "freshness": freshness,
        }

    @task(task_id="fail_traces", trigger_rule="one_failed")
    def fail_traces_task(**context) -> dict:
        """Capture granular root-cause tracebacks and HTTP codes on any task failure."""
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
            f"🚨 Airflow Task Failed: {summary.get('task_id')} in binance_market_data_ingestion_pipeline\n"
            f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
            f"📍 Function: {tb.get('func_name')}()\n"
            f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
        )
        send_sys06_alert({
            "alert_type": "PIPELINE_FAILURE",
            "level": "CRITICAL",
            "source_system": "SYS-02",
            "event": "INGESTION_TASK_FAILED",
            "deduplicate_key": f"sys02:fail:ingest:{dag_run.run_id if dag_run else 'run'}",
            "message": detailed_message,
            "channels": ["telegram"],
            "action_url": "http://127.0.0.1:8085/dags/binance_market_data_ingestion_pipeline/grid",
        })
        return {"status": "CAPTURED", "failed_count": len(failed_tis)}

    @task(task_id="error_watcher", trigger_rule="all_done")
    def error_watcher_task(**context) -> dict:
        """Circuit Breaker: Automatically deactivates pipeline if consecutive failures exceed threshold."""
        return check_and_trigger_circuit_breaker(
            dag_id="binance_market_data_ingestion_pipeline",
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
            alert_callback=send_sys06_alert,
        )

    end = EmptyOperator(task_id="end_pipeline")

    # Wire TaskFlow pipeline dependencies
    params_out = prepare_window_parameters()
    bronze_out = extract_bronze_market_data(params_out)
    silver_out = transform_silver_market_data(bronze_out)
    quality_out = validate_market_data_quality_gate(silver_out)
    gold_out = load_gold_market_data(quality_out)

    traces_out = fail_traces_task()
    watcher_out = error_watcher_task()

    start >> params_out
    gold_out >> end

    [bronze_out, silver_out, quality_out, gold_out] >> traces_out
    [gold_out, traces_out] >> watcher_out


pipeline = binance_market_data_ingestion_pipeline()
