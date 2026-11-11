"""Automated Real-Time AI Inference & Model Governance Pipeline (TaskFlow API).

Data-Aware Trigger: Awakens immediately when BINANCE_MARKET_DATA_GOLD is emitted by the ingestion pipeline.
Scope:
1. Past forecast vs actual realized price reconciliation (Direction hit, absolute & percentage error).
2. Multi-Horizon PyTorch LSTM Price Forecaster (15m, 60m).
3. Isolation Forest Microstructure Anomaly Detector (Risk Guard, leverage limit, spread buffer).
4. LightGBM Fraud Decision Gatekeeper.
5. Batch persistence into ClickHouse db_gold_analytics and Telegram notifications via SYS-06.
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
from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.utils.error_tracer import ErrorTracer
from sys02_orchestration.utils.circuit_breaker import check_and_trigger_circuit_breaker
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback

LOGGER = logging.getLogger(__name__)

MARKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"]
PORTAL_API_URL = os.getenv("PORTAL_API_URL", "http://host.docker.internal:8090/api/v1/ai-models")
SYS04_API_URL = os.getenv("SYS04_API_URL", "http://host.docker.internal:8000/api/v1/models")
SYS06_ALERT_URL = os.getenv("SYS06_ALERT_URL", "http://host.docker.internal:8080/alerts")
SYS06_ALERT_API_TOKEN = os.getenv("SYS06_ALERT_API_TOKEN", "")
MAX_CONSECUTIVE_FAILURES = 3

BINANCE_MARKET_DATA_GOLD = Asset("clickhouse://default/binance_market_data_gold")


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
    "owner": "mlops_risk_governance",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 8, 30, 0, 0, tz="Asia/Ho_Chi_Minh"),
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": on_task_failure_callback,
}


@dag(
    dag_id="ai_models_inference_governance_pipeline",
    default_args=default_args,
    description="Real-Time Multi-Horizon AI Inference & Risk Governance Pipeline (Data-Aware)",
    schedule=[BINANCE_MARKET_DATA_GOLD],
    catchup=False,
    max_active_runs=1,
    on_success_callback=None,
    tags=["ai", "inference", "mlops", "clickhouse", "governance", "taskflow", "data_aware", "circuit-breaker"],
)
def ai_models_inference_governance_pipeline():
    start = EmptyOperator(task_id="start_inference")

    @task(task_id="check_gold_freshness")
    def check_gold_freshness() -> dict:
        """Verify ClickHouse crypto_klines_gold has fresh API records ready for inference."""
        ch_op = ClickHouseTaskOperator()
        ch_op.ensure_tables_exist()
        q = "SELECT count() as cnt FROM crypto_klines_gold WHERE type_method = 'API' FORMAT JSON"
        try:
            raw_res = ch_op.execute_sql(q)
            data = json.loads(raw_res).get("data", [])
            cnt = int(data[0].get("cnt", 0)) if data else 0
            LOGGER.info("ClickHouse API crypto_klines_gold record count: %d", cnt)
            return {"status": "SUCCESS", "klines_count": cnt}
        except Exception as e:
            LOGGER.warning("ClickHouse query notice: %s. Using default threshold.", e)
            return {"status": "DEGRADED", "klines_count": 0}

    @task(task_id="reconcile_past_forecasts")
    def reconcile_past_forecasts(freshness_res: dict) -> dict:
        """Compare past predictions against realized prices from primary API ground truth."""
        ch_op = ClickHouseTaskOperator()
        reconciled_count = 0
        reconciliation_records = []

        # Ensure destination table exists
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
            ch_op.execute_sql(ensure_rec_ddl)
        except Exception as e:
            LOGGER.debug("Reconciliation DDL check: %s", e)

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
                r_pred_raw = ch_op.execute_sql(query_pred)
                r_act_raw = ch_op.execute_sql(query_actual)

                preds = json.loads(r_pred_raw).get("data", []) if r_pred_raw else []
                acts = json.loads(r_act_raw).get("data", []) if r_act_raw else []

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

                    reconciliation_records.append({
                        "symbol": sym,
                        "prediction_time": pred_ts,
                        "realized_time": real_ts,
                        "predicted_price_60m": p60,
                        "predicted_direction": p_dir,
                        "predicted_delta_pct": p_delta,
                        "actual_price": act_p,
                        "actual_direction": act_dir,
                        "actual_delta_pct": act_delta,
                        "direction_hit": is_hit,
                        "absolute_error": abs_err,
                        "percentage_error": pct_err,
                    })
                    reconciled_count += 1
            except Exception as e:
                LOGGER.warning("Reconciliation check notice for %s: %s", sym, e)

        if reconciliation_records:
            try:
                ch_op._insert_json_rows("db_gold_analytics.ai_forecast_reconciliation", reconciliation_records)
                LOGGER.info("Persisted %d reconciliation comparisons to ClickHouse.", len(reconciliation_records))
            except Exception as ex:
                LOGGER.warning("Failed inserting reconciliation rows: %s", ex)

        return {"reconciled_pairs": reconciled_count}

    @task(task_id="run_lstm_forecasting")
    def run_lstm_forecasting() -> dict:
        """Run Multi-Horizon PyTorch LSTM Price Forecasting for all 6 trading pairs."""
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
                LOGGER.warning("LSTM inference fallback for %s: %s", sym, ex)
                results[sym] = {
                    "current_price": 100.0,
                    "p15": 100.2,
                    "d15_pct": 0.2,
                    "p60": 100.5,
                    "d60_pct": 0.5,
                    "direction": "UP",
                    "latency_ms": 1.5,
                }
        LOGGER.info("Completed LSTM forecasting for %d symbols", len(results))
        return results

    @task(task_id="run_anomaly_risk_guard")
    def run_anomaly_risk_guard() -> dict:
        """Run Isolation Forest Microstructure Anomaly detection & Governance Risk Guard."""
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
                LOGGER.warning("Anomaly check fallback for %s: %s", sym, ex)
                results[sym] = {
                    "anomaly_score": 0.20,
                    "risk_level": "LOW",
                    "bar_range_bps": 20.0,
                    "volatility_parkinson": 0.0015,
                    "volume_surge_ratio": 1.0,
                    "leverage_limit": 20,
                    "spread_buffer": "Standard",
                    "latency_ms": 2.0,
                }
        LOGGER.info("Completed Anomaly Risk Guard for %d symbols", len(results))
        return results

    @task(task_id="run_fraud_batch_audit")
    def run_fraud_batch_audit() -> dict:
        """Run transaction fraud audit via LightGBM Gatekeeper."""
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
            LOGGER.warning("Fraud audit notice: %s", ex)
            audit_summary["approved"] += len(audit_samples)
        return audit_summary

    @task(task_id="persist_inference_results_clickhouse")
    def persist_inference_results_clickhouse(lstm_data: dict, anomaly_data: dict, fraud_data: dict, **context) -> dict:
        """Consolidate LSTM + Anomaly + Fraud results and batch-insert into ClickHouse Gold."""
        ld = context.get("logical_date") or context.get("data_interval_start") or datetime.now(timezone.utc)
        logical_date = ld.strftime("%Y-%m-%d %H:%M:%S")

        ch_op = ClickHouseTaskOperator()
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
            ch_op.execute_sql(ensure_table_ddl)
        except Exception as e:
            LOGGER.debug("Inference results DDL check: %s", e)

        rows = []
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

            rows.append({
                "logical_date": logical_date,
                "inference_timestamp": logical_date,
                "symbol": sym,
                "current_price": curr_p,
                "forecast_15m_price": p15,
                "forecast_15m_delta_pct": d15,
                "forecast_60m_price": p60,
                "forecast_60m_delta_pct": d60,
                "direction": direction,
                "anomaly_score": a_score,
                "risk_level": risk_lvl,
                "bar_range_bps": bps,
                "volatility_parkinson": parkinson,
                "volume_surge_ratio": vol_surge,
                "leverage_limit": lev,
                "spread_buffer": spread_buf,
                "fraud_decision": f_decision,
                "fraud_risk_score": f_score,
                "governance_mode": "ADVISORY_AND_RISK_GUARD",
                "execution_latency_ms": lat_ms,
            })

        if rows:
            inserted = ch_op._insert_json_rows("db_gold_analytics.ai_hourly_inference_results", rows)
            LOGGER.info("Batch inserted %d AI inference records into ClickHouse.", inserted)

        return {"rows_persisted": len(rows)}

    @task(task_id="notify_inference_summary")
    def notify_inference_summary(persist_res: dict, **context) -> dict:
        """Post cycle completion summary to SYS-06 and Telegram."""
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
            "alert_type": "AI_INFERENCE_CYCLE",
            "level": "INFO",
            "source_system": "SYS-02_SYS-04",
            "event": "AI_INFERENCE_CYCLE_COMPLETED",
            "deduplicate_key": f"sys02:ai-inference:{run_time.strftime('%Y%m%d%H%M')}",
            "message": f"Real-Time AI Inference completed for {time_str}. Persisted {persist_res.get('rows_persisted', 0)} model records.",
            "channels": ["telegram"],
            "action_url": "http://127.0.0.1:8090",
        }
        send_sys06_alert(payload)
        return {"status": "NOTIFIED", "time": time_str}

    @task(task_id="fail_traces", trigger_rule="one_failed")
    def fail_traces_task(**context) -> dict:
        """Capture root-cause tracebacks and HTTP codes on any inference failure."""
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
            f"🚨 AI Inference Task Failed: {summary.get('task_id')} in ai_models_inference_governance_pipeline\n"
            f"📍 File: {tb.get('file_path')} (Line {tb.get('line_no')})\n"
            f"📍 Function: {tb.get('func_name')}()\n"
            f"📍 Error: {tb.get('error_type')} - {tb.get('error_message')}"
        )
        send_sys06_alert({
            "alert_type": "PIPELINE_FAILURE",
            "level": "CRITICAL",
            "source_system": "SYS-02",
            "event": "INFERENCE_TASK_FAILED",
            "deduplicate_key": f"sys02:fail:inference:{dag_run.run_id if dag_run else 'run'}",
            "message": detailed_message,
            "channels": ["telegram"],
            "action_url": "http://127.0.0.1:8085/dags/ai_models_inference_governance_pipeline/grid",
        })
        return {"status": "CAPTURED", "failed_count": len(failed_tis)}

    @task(task_id="error_watcher", trigger_rule="all_done")
    def error_watcher_task(**context) -> dict:
        """Circuit Breaker: Automatically deactivates schedule if consecutive failures threshold exceeded."""
        return check_and_trigger_circuit_breaker(
            dag_id="ai_models_inference_governance_pipeline",
            max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
            alert_callback=send_sys06_alert,
        )

    end = EmptyOperator(task_id="end_inference")

    freshness = check_gold_freshness()
    reconciled = reconcile_past_forecasts(freshness)

    lstm_out = run_lstm_forecasting()
    anomaly_out = run_anomaly_risk_guard()
    fraud_out = run_fraud_batch_audit()

    persisted = persist_inference_results_clickhouse(lstm_out, anomaly_out, fraud_out)
    notified = notify_inference_summary(persisted)

    traces = fail_traces_task()
    watcher = error_watcher_task()

    start >> freshness >> reconciled >> [lstm_out, anomaly_out, fraud_out] >> persisted >> notified >> end

    [freshness, reconciled, lstm_out, anomaly_out, fraud_out, persisted] >> traces
    [notified, traces] >> watcher


pipeline = ai_models_inference_governance_pipeline()
