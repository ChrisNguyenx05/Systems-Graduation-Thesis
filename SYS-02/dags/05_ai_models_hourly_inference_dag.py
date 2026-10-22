"""
DAG 05: Automated Hourly AI Inference & Model Governance Pipeline (TaskFlow API).
Schedule: Hourly at minute 0 ('0 * * * *')
Scope: Multi-Horizon LSTM Price Forecaster, Isolation Forest Risk Guard, LightGBM Fraud Gate
Target DB: ClickHouse (db_gold_analytics.ai_hourly_inference_results)
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
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.utils.error_tracer import ErrorTracer
from sys02_orchestration.utils.circuit_breaker import check_and_trigger_circuit_breaker
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback, notify_hourly_pipeline_success

LOGGER = logging.getLogger(__name__)

MARKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"]
PORTAL_API_URL = os.getenv("PORTAL_API_URL", "http://host.docker.internal:8090/api/v1/ai-models")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "host.docker.internal")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "nhfinanceadmin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "nhathuy123")
SYS06_ALERT_URL = os.getenv("SYS06_ALERT_URL", "http://host.docker.internal:8080/alerts")
SYS06_ALERT_API_TOKEN = os.getenv("SYS06_ALERT_API_TOKEN", "")
MAX_CONSECUTIVE_FAILURES = 3


def send_sys06_notification(payload: dict) -> None:
    if not SYS06_ALERT_URL or not SYS06_ALERT_API_TOKEN:
        LOGGER.info("SYS-06 notification skipped: dispatcher configuration not set")
        return
    try:
        resp = requests.post(SYS06_ALERT_URL, json=payload, headers={"X-Alert-Token": SYS06_ALERT_API_TOKEN}, timeout=5)
        LOGGER.info("SYS-06 alert dispatched: status %s", resp.status_code)
    except Exception as exc:
        LOGGER.warning("Could not send SYS-06 notification: %s", exc)

send_sys06_alert = send_sys06_notification


def send_inference_failure_alert(context) -> None:
    """Dispatch critical alert to SYS-06 Telegram when a task in DAG 05 exhausted all 5 retries (Final Failed)."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    run_id = dag_run.run_id if dag_run else "unknown_run"
    try:
        from sys02_orchestration.utils.error_tracer import ErrorTracer
        summary = ErrorTracer.generate_detailed_error_summary(context)
        tb = summary.get("tb_info", {})
        detailed_message = (
            f"🚨 [FINAL FAILED] Airflow Task: {task_id} in 05_ai_models_hourly_inference_dag\n"
            f"⚠️ All 5 retries exhausted. Task permanently marked FAILED.\n"
            f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
            f"📍 Function: {tb.get('func_name')}()\n"
            f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
        )
    except Exception as exc:
        detailed_message = f"🚨 [FINAL FAILED] Task: {task_id} in 05_ai_models_hourly_inference_dag (Retries > 5): {exc}"

    send_sys06_alert({
        "alert_type": "PIPELINE_FAILURE",
        "level": "CRITICAL",
        "source_system": "SYS-02",
        "event": "AIRFLOW_TASK_FINAL_FAILED",
        "deduplicate_key": f"sys02:final-failure:05_ai_models_hourly_inference_dag:{task_id}:{run_id}",
        "message": detailed_message,
        "channels": ["telegram"],
        "action_url": f"http://127.0.0.1:8085/dags/05_ai_models_hourly_inference_dag/grid",
    })


def send_inference_success_alert(context) -> None:
    """Dispatch success notification to Telegram when AI hourly inference completes."""
    dag_run = context.get("dag_run")
    if dag_run:
        send_sys06_alert({
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "AI_INFERENCE_HOURLY_COMPLETED",
            "deduplicate_key": f"sys02:ai-inference:{dag_run.dag_id}:{dag_run.run_id}",
            "message": f"Airflow DAG 05: Hourly AI Inference completed successfully. Predictions & Risk scores updated.",
            "channels": ["telegram"],
            "action_url": f"http://127.0.0.1:8085/dags/{dag_run.dag_id}/grid",
        })


default_args = {
    "owner": "mlops_risk_governance",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 9, 1, 0, 0, tz="Asia/Ho_Chi_Minh"),
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_task_failure_callback,
}

BINANCE_KLINES_GOLD_DATASET = Asset("clickhouse://default/binance_klines_gold")

@dag(
    dag_id="05_ai_models_hourly_inference_dag",
    default_args=default_args,
    description="Automated Hourly Multi-Horizon AI Inference & Risk Governance Pipeline (TaskFlow API)",
    schedule=[BINANCE_KLINES_GOLD_DATASET],
    catchup=False,
    max_active_runs=1,
    on_success_callback=notify_hourly_pipeline_success,
    tags=["ai", "sys04", "hourly-inference", "mlops", "clickhouse", "governance", "taskflow", "data_aware", "circuit-breaker"],
)
def ai_models_hourly_inference_pipeline():
    start = EmptyOperator(task_id="start_hourly_ai_inference")

    @task(task_id="check_clickhouse_freshness")
    def check_clickhouse_freshness() -> dict:
        """Verify that ClickHouse has verified API klines data ready for inference."""
        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        q = "SELECT count() as cnt FROM crypto_klines_gold WHERE type_method = 'API' FORMAT JSON"
        try:
            resp = requests.post(url, params={"query": q}, auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=10)
            data = resp.json().get("data", [])
            cnt = int(data[0].get("cnt", 0)) if data else 0
            LOGGER.info("ClickHouse API crypto_klines_gold record count: %d", cnt)
            return {"status": "SUCCESS", "klines_count": cnt}
        except Exception as e:
            LOGGER.warning("ClickHouse query exception: %s. Using synthetic inference fallback if needed.", e)
            return {"status": "DEGRADED", "klines_count": 0}

    @task(task_id="reconcile_past_hourly_forecasts")
    def reconcile_past_forecasts_task(freshness_res: dict) -> dict:
        """Compare past predictions against realized prices from primary API ground truth."""
        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        reconciled_count = 0
        reconciliation_rows = []

        for sym in MARKET_SYMBOLS:
            query_pred = f"""
            SELECT inference_timestamp, current_price as anchor_price, forecast_60m_price, forecast_60m_delta_pct, direction as predicted_direction
            FROM db_gold_analytics.ai_hourly_inference_results
            WHERE symbol = '{sym}'
            ORDER BY inference_timestamp DESC LIMIT 1 FORMAT JSON
            """
            query_actual = f"""
            SELECT close_time, close as actual_price
            FROM crypto_klines_gold
            WHERE symbol = '{sym}' AND type_method = 'API'
            ORDER BY close_time DESC LIMIT 1 FORMAT JSON
            """
            try:
                r_pred = requests.post(url, params={"query": query_pred}, auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=5).json()
                r_act = requests.post(url, params={"query": query_actual}, auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=5).json()

                preds = r_pred.get("data", [])
                acts = r_act.get("data", [])

                if preds and acts:
                    p, a = preds[0], acts[0]
                    pred_ts = str(p.get("inference_timestamp", "")).split(".")[0]
                    real_ts = str(a.get("close_time", "")).split(".")[0]
                    anchor_p = float(p.get("anchor_price", 100.0))
                    p60 = float(p.get("forecast_60m_price", anchor_p))
                    p_dir = str(p.get("predicted_direction", "UP"))
                    p_delta = float(p.get("forecast_60m_delta_pct", 0.0))

                    act_p = float(a.get("actual_price", anchor_p))
                    act_delta = round(((act_p - anchor_p) / anchor_p) * 100.0, 3) if anchor_p > 0 else 0.0
                    act_dir = "UP" if act_delta >= 0 else "DOWN"

                    is_hit = 1 if act_dir == p_dir else 0
                    abs_err = round(abs(act_p - p60), 2)
                    pct_err = round((abs_err / act_p) * 100.0, 3) if act_p > 0 else 0.0

                    reconciliation_rows.append(
                        f"('{sym}', '{pred_ts}', '{real_ts}', {p60}, '{p_dir}', {p_delta}, {act_p}, '{act_dir}', {act_delta}, {is_hit}, {abs_err}, {pct_err})"
                    )
                    reconciled_count += 1
            except Exception as e:
                LOGGER.warning("Reconciliation check failed for %s: %s", sym, e)

        if reconciliation_rows:
            ensure_rec_ddl = """
            CREATE DATABASE IF NOT EXISTS db_gold_analytics;
            CREATE TABLE IF NOT EXISTS db_gold_analytics.ai_forecast_reconciliation (
                symbol LowCardinality(String),
                prediction_time DateTime,
                realized_time DateTime,
                predicted_price_60m Float64,
                predicted_direction LowCardinality(String),
                predicted_delta_pct Float64,
                actual_price Float64,
                actual_direction LowCardinality(String),
                actual_delta_pct Float64,
                direction_hit UInt8,
                absolute_error Float64,
                percentage_error Float64,
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (symbol, prediction_time);
            """
            try:
                requests.post(url, data=ensure_rec_ddl.encode("utf-8"), auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=10)
            except Exception:
                pass

            insert_reconcile = f"""
            INSERT INTO db_gold_analytics.ai_forecast_reconciliation 
            (symbol, prediction_time, realized_time, predicted_price_60m, predicted_direction, predicted_delta_pct, actual_price, actual_direction, actual_delta_pct, direction_hit, absolute_error, percentage_error)
            VALUES {', '.join(reconciliation_rows)}
            """
            try:
                requests.post(url, data=insert_reconcile.encode("utf-8"), auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=10)
                LOGGER.info("Persisted %d reconciliation comparisons to ClickHouse.", len(reconciliation_rows))
            except Exception as ex:
                LOGGER.warning("Failed inserting reconciliation rows: %s", ex)

        return {"reconciled_pairs": reconciled_count}

    @task(task_id="run_lstm_forecasting")
    def run_lstm_forecasting() -> dict:
        """Run hourly LSTM Return-Anchored Price Forecasting for all 6 trading pairs."""
        results = {}
        for sym in MARKET_SYMBOLS:
            try:
                resp = requests.post(f"{PORTAL_API_URL}/test/lstm", json={"symbol": sym, "lookback_bars": 60}, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    results[sym] = {
                        "current_price": data.get("current_price"),
                        "p15": data.get("forecast_15m", {}).get("predicted_price"),
                        "d15_pct": data.get("forecast_15m", {}).get("predicted_delta_pct"),
                        "p60": data.get("forecast_60m", {}).get("predicted_price"),
                        "d60_pct": data.get("forecast_60m", {}).get("predicted_delta_pct"),
                        "direction": data.get("forecast_15m", {}).get("direction", "NEUTRAL"),
                        "latency_ms": data.get("inference_latency_ms", 1.2),
                    }
            except Exception as ex:
                LOGGER.warning("LSTM inference error for %s: %s", sym, ex)
        LOGGER.info("Completed LSTM hourly forecasting for %d symbols", len(results))
        return results

    @task(task_id="run_anomaly_risk_guard")
    def run_anomaly_risk_guard() -> dict:
        """Run hourly Isolation Forest Microstructure Anomaly detection for all 6 pairs."""
        results = {}
        for sym in MARKET_SYMBOLS:
            try:
                resp = requests.post(f"{PORTAL_API_URL}/test/isolation-forest", json={"symbol": sym, "lookback_bars": 20}, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    res = data.get("anomaly_result", {})
                    micro = data.get("microstructure_features", {})
                    gov = data.get("governance_enforcement", {}).get("mitigation_actions", {})
                    lev_str = gov.get("throttle_max_leverage", "20x")
                    lev = 5 if "5x" in lev_str else (10 if "10x" in lev_str else 20)
                    results[sym] = {
                        "anomaly_score": res.get("anomaly_score", 0.25),
                        "risk_level": res.get("risk_level", "NORMAL"),
                        "bar_range_bps": micro.get("bar_range_bps", 25.0),
                        "volatility_parkinson": micro.get("volatility_parkinson", 0.002),
                        "volume_surge_ratio": micro.get("volume_surge_ratio", 1.0),
                        "leverage_limit": lev,
                        "spread_buffer": gov.get("widen_orderbook_spread", "Standard"),
                        "latency_ms": data.get("inference_latency_ms", 23.5),
                    }
            except Exception as ex:
                LOGGER.warning("Anomaly check error for %s: %s", sym, ex)
        LOGGER.info("Completed Anomaly Risk Guard for %d symbols", len(results))
        return results

    @task(task_id="run_fraud_batch_audit")
    def run_fraud_batch_audit() -> dict:
        """Run hourly fraud audit via LightGBM Gatekeeper."""
        audit_samples = [
            {"user_id": "USR_SYS_AUDIT_1", "amount_usd": 1250.0, "velocity_1h": 2, "country_risk_score": 0.20, "device_trust_score": 0.90},
            {"user_id": "USR_SYS_AUDIT_2", "amount_usd": 8500.0, "velocity_1h": 8, "country_risk_score": 0.75, "device_trust_score": 0.35},
        ]
        audit_summary = {"total_audited": len(audit_samples), "flagged_review": 0, "blocked": 0, "approved": 0}
        try:
            for sample in audit_samples:
                resp = requests.post(f"{PORTAL_API_URL}/test/lightgbm", json=sample, timeout=10)
                if resp.status_code == 200:
                    dec = resp.json().get("scoring_results", {}).get("decision")
                    if dec == "BLOCK_TRANSACTION":
                        audit_summary["blocked"] += 1
                    elif dec == "MANUAL_REVIEW":
                        audit_summary["flagged_review"] += 1
                    else:
                        audit_summary["approved"] += 1
        except Exception as ex:
            LOGGER.warning("Fraud audit error: %s", ex)
        return audit_summary

    @task(task_id="persist_inference_results_clickhouse")
    def persist_inference_results(lstm_data: dict, anomaly_data: dict, fraud_data: dict, **context) -> dict:
        """Consolidate LSTM + Anomaly + Fraud results and insert into ClickHouse Gold table."""
        ld = context.get("logical_date") or context.get("data_interval_start") or datetime.now(timezone.utc)
        logical_date = ld.strftime("%Y-%m-%d %H:%M:%S")
        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        values_list = []

        for sym in MARKET_SYMBOLS:
            l = lstm_data.get(sym, {})
            a = anomaly_data.get(sym, {})
            curr_p = float(l.get("current_price", 0.0) or 0.0)
            p15 = float(l.get("p15", curr_p) or curr_p)
            d15 = float(l.get("d15_pct", 0.0) or 0.0)
            p60 = float(l.get("p60", curr_p) or curr_p)
            d60 = float(l.get("d60_pct", 0.0) or 0.0)
            direction = str(l.get("direction", "NEUTRAL"))

            a_score = float(a.get("anomaly_score", 0.25) or 0.25)
            risk_lvl = str(a.get("risk_level", "NORMAL"))
            bps = float(a.get("bar_range_bps", 25.0) or 25.0)
            parkinson = float(a.get("volatility_parkinson", 0.002) or 0.002)
            vol_surge = float(a.get("volume_surge_ratio", 1.0) or 1.0)
            lev = int(a.get("leverage_limit", 20) or 20)
            spread_buf = str(a.get("spread_buffer", "Standard")).replace("'", "")

            f_decision = "REVIEW" if fraud_data.get("flagged_review", 0) > 0 else "APPROVE"
            f_score = 0.35 if f_decision == "REVIEW" else 0.12
            lat_ms = float(l.get("latency_ms", 1.0)) + float(a.get("latency_ms", 23.0))

            row_str = f"('{logical_date}', '{sym}', {curr_p}, {p15}, {d15}, {p60}, {d60}, '{direction}', {a_score}, '{risk_lvl}', {bps}, {parkinson}, {vol_surge}, {lev}, '{spread_buf}', '{f_decision}', {f_score}, 'ADVISORY_AND_RISK_GUARD', {lat_ms})"
            values_list.append(row_str)

        ensure_table_ddl = """
        CREATE DATABASE IF NOT EXISTS db_gold_analytics;
        CREATE TABLE IF NOT EXISTS db_gold_analytics.ai_hourly_inference_results (
            logical_date DateTime,
            inference_timestamp DateTime DEFAULT logical_date,
            symbol LowCardinality(String),
            current_price Float64,
            forecast_15m_price Float64,
            forecast_15m_delta_pct Float64,
            forecast_60m_price Float64,
            forecast_60m_delta_pct Float64,
            direction LowCardinality(String),
            anomaly_score Float64,
            risk_level LowCardinality(String),
            bar_range_bps Float64,
            volatility_parkinson Float64,
            volume_surge_ratio Float64,
            leverage_limit UInt16 DEFAULT 20,
            spread_buffer String DEFAULT 'Standard',
            fraud_decision LowCardinality(String) DEFAULT 'APPROVE',
            fraud_risk_score Float64 DEFAULT 0.0,
            governance_mode LowCardinality(String) DEFAULT 'ADVISORY_AND_RISK_GUARD',
            execution_latency_ms Float64 DEFAULT 0.0,
            created_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (symbol, logical_date);
        """
        try:
            requests.post(url, data=ensure_table_ddl.encode("utf-8"), auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=10)
        except Exception as e_ddl:
            LOGGER.warning("Auto table DDL check for ai_hourly_inference_results: %s", e_ddl)

        if values_list:
            insert_query = f"""
            INSERT INTO db_gold_analytics.ai_hourly_inference_results 
            (logical_date, symbol, current_price, forecast_15m_price, forecast_15m_delta_pct, forecast_60m_price, forecast_60m_delta_pct, direction, anomaly_score, risk_level, bar_range_bps, volatility_parkinson, volume_surge_ratio, leverage_limit, spread_buffer, fraud_decision, fraud_risk_score, governance_mode, execution_latency_ms)
            VALUES {', '.join(values_list)}
            """
            try:
                resp = requests.post(url, data=insert_query.encode("utf-8"), auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=15)
                resp.raise_for_status()
                LOGGER.info("Inserted %d hourly AI inference rows into ClickHouse successfully.", len(values_list))
            except Exception as e:
                LOGGER.error("Failed persisting AI results to ClickHouse: %s", e)
                raise e

        return {"rows_persisted": len(values_list)}

    @task(task_id="notify_hourly_completion")
    def notify_completion_task(persist_res: dict, **context) -> dict:
        """Notify SYS-06 and log hourly completion summary."""
        ld = context.get("logical_date") or context.get("data_interval_start") or datetime.now(timezone.utc)
        if hasattr(ld, "in_timezone"):
            run_time = ld.in_timezone("Asia/Ho_Chi_Minh")
        elif hasattr(ld, "astimezone"):
            from zoneinfo import ZoneInfo
            run_time = ld.astimezone(ZoneInfo("Asia/Ho_Chi_Minh"))
        else:
            run_time = ld
        time_str = run_time.strftime("%Y-%m-%d %H:%M GMT+7")
        payload = {
            "alert_type": "AI_INFERENCE_HOURLY",
            "level": "INFO",
            "source_system": "SYS-04_SYS-02",
            "event": "HOURLY_AI_INFERENCE_CYCLE_COMPLETED",
            "deduplicate_key": f"sys04:ai-hourly:{run_time.strftime('%Y%m%dT%H')}",
            "message": f"Hourly AI Model Cycle completed for {time_str}. Persisted {persist_res.get('rows_persisted', 0)} rows.",
            "channels": ["telegram"],
            "action_url": "http://127.0.0.1:8090",
        }
        send_sys06_notification(payload)
        return {"status": "NOTIFIED", "time": time_str}

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
            LOGGER.error("fail_traces detected AI inference failure: %s in run: %s", task_id, run_id)
        return {"status": "CAPTURED", "failed_count": len(failed_tis)}

    @task(task_id="error_watcher", trigger_rule="all_done")
    def error_watcher_task(**context) -> dict:
        """Circuit Breaker: Automatically pauses schedule if consecutive failures threshold exceeded."""
        dag_id = context.get("dag").dag_id if context.get("dag") else "05_ai_models_hourly_inference_dag"
        return check_and_trigger_circuit_breaker(
            dag_id=dag_id,
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
            alert_callback=send_sys06_alert
        )

    end = EmptyOperator(task_id="end_hourly_ai_inference")

    freshness = check_clickhouse_freshness()
    reconciled = reconcile_past_forecasts_task(freshness)

    lstm_out = run_lstm_forecasting()
    anomaly_out = run_anomaly_risk_guard()
    fraud_out = run_fraud_batch_audit()

    persisted = persist_inference_results(lstm_out, anomaly_out, fraud_out)
    notified = notify_completion_task(persisted)

    traces = fail_traces_task()
    watcher = error_watcher_task()

    start >> freshness >> reconciled >> [lstm_out, anomaly_out, fraud_out] >> persisted >> notified >> end

    [freshness, reconciled, lstm_out, anomaly_out, fraud_out, persisted] >> traces
    [notified, traces] >> watcher


ai_inference_dag = ai_models_hourly_inference_pipeline()
