"""
SLA Miss, Immediate Pipeline Failure, and Hourly Aggregated Notification Hooks.
"""

import os
import time
import logging
import requests
from typing import Dict, Any, Optional
import pendulum

from sys02_orchestration.config import config

logger = logging.getLogger(__name__)


def send_sys06_alert(payload: dict) -> bool:
    """Dispatches alert payload to SYS-06 Alert Dispatcher via Docker Network."""
    if not config.alert.sla_alert_enabled:
        return True
    url = os.getenv("SYS06_ALERT_URL", config.alert.sys06_alert_url).strip()
    token = os.getenv("SYS06_ALERT_API_TOKEN", config.alert.sys06_alert_token).strip()

    if url and token:
        try:
            resp = requests.post(url, json=payload, headers={"X-Alert-Token": token}, timeout=10)
            if resp.status_code in [200, 202]:
                return True
            logger.warning("SYS-06 returned status %s: %s", resp.status_code, resp.text)
        except Exception as exc:
            logger.warning("SYS-06 dispatcher unreachable (%s). Attempting direct Telegram fallback.", exc)

    # Direct fallback if SYS-06 is unavailable
    raw_message = payload.get("message", "")
    level = payload.get("level", "INFO")
    return send_telegram_alert(raw_message, level=level)


def send_telegram_alert(message: str, level: str = "CRITICAL") -> bool:
    """Sends notification directly to Telegram API if configured."""
    if not config.alert.sla_alert_enabled:
        return True
    token = config.alert.telegram_bot_token
    chat_id = config.alert.telegram_chat_id

    if not token or not chat_id or token.startswith("mock_"):
        logger.warning("Telegram Bot credentials not configured or mock mode. Alert [%s]: %s", level, message)
        return True

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }
    try:
        resp = requests.post(url, json=payload, timeout=8)
        return resp.status_code == 200
    except Exception as exc:
        logger.error("Failed to deliver direct Telegram notification: %s", exc)
        return False


def on_task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow callback triggered IMMEDIATELY when any task fails.
    Explicitly highlights which DAG and Task failed with granular code traces.
    """
    if not config.alert.sla_alert_enabled:
        return
    from sys02_orchestration.utils.error_tracer import ErrorTracer

    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    dag_id = getattr(ti, "dag_id", None) or getattr(dag, "dag_id", "unknown_dag")
    task_id = getattr(ti, "task_id", "unknown_task")
    run_id = getattr(dag_run, "run_id", "unknown_run")

    try:
        summary = ErrorTracer.generate_detailed_error_summary(context)
        tb = summary.get("tb_info", {})
        file_path = tb.get("file_path", "unknown")
        line_no = tb.get("line_no", "?")
        func_name = tb.get("func_name", "unknown")
        code_snippet = tb.get("code_snippet", "N/A")
        error_type = tb.get("error_type", "TaskFailure")
        error_msg = tb.get("error_message", "Task execution failed.")
    except Exception as exc:
        summary = {"dag_id": dag_id, "task_id": task_id, "run_id": run_id}
        file_path = "N/A"
        line_no = "?"
        func_name = "N/A"
        code_snippet = "N/A"
        error_type = type(exc).__name__
        error_msg = str(exc)

    detailed_message = (
        f"🚨 *[TASK FAILED] in DAG:* `{dag_id}`\n"
        f"----------------------------------------\n"
        f"📁 *DAG ID:* `{dag_id}`\n"
        f"⚙️ *Task ID:* `{task_id}`\n"
        f"🔁 *Run ID:* `{run_id}`\n"
        f"📍 *File:* `{file_path}` (Line `{line_no}`)\n"
        f"📍 *Function:* `{func_name}()`\n"
        f"📍 *Code:* `{code_snippet}`\n"
        f"❌ *Error:* `{error_type}: {error_msg}`\n"
        f"----------------------------------------\n"
        f"🔗 [Open Airflow Grid View](http://localhost:{config.airflow.webserver_port}/dags/{dag_id}/grid)"
    )

    logger.error("🚨 Airflow Task Failed in DAG: %s | Task: %s | Error: %s", dag_id, task_id, error_msg)

    send_sys06_alert(
        {
            "alert_type": "PIPELINE_FAILURE",
            "level": "CRITICAL",
            "source_system": "SYS-02",
            "event": "AIRFLOW_TASK_FAILED",
            "deduplicate_key": f"sys02:failure:{dag_id}:{task_id}:{run_id}:{int(time.time())}",
            "message": detailed_message,
            "channels": ["telegram"],
            "action_url": f"http://localhost:{config.airflow.webserver_port}/dags/{dag_id}/grid",
        }
    )

    try:
        ErrorTracer.persist_error_to_metadata(summary)
    except Exception as exc:
        logger.debug("Failed to persist error to metadata: %s", exc)


def notify_hourly_pipeline_success(context: Dict[str, Any]) -> None:
    """
    Unified Hourly Pipeline Success Notification.
    Dispatched once every hour when DAG 05 completes, bundling the status of:
      - DAG 01 (Binance Klines Hourly ETL)
      - DAG 04 (Binance Market Metrics 15m continuous stream)
      - DAG 05 (AI Models Hourly Multi-Horizon Inference & Risk Governance)
    """
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    now_vn = pendulum.now("Asia/Ho_Chi_Minh")
    time_str = now_vn.strftime("%Y-%m-%d %H:%M:%S ICT")

    message = (
        f"🟢 *[SYS-02 HOURLY PIPELINE COMPLETED - ALL TASKS OK]*\n"
        f"----------------------------------------\n"
        f"📂 *Type:* `SYSTEM_HEALTH`\n"
        f"⏰ *Cycle:* `Hourly Ingestion, Continuous Metrics & AI Models`\n"
        f"----------------------------------------\n"
        f"✅ *DAG 01 (Binance Klines ETL)*: Ingested 1m candles into Gold (API Ground Truth)\n"
        f"✅ *DAG 04 (Market Metrics 15m)*: Continuous sub-hourly stream active (Orderbook & Liquidations)\n"
        f"✅ *DAG 05 (AI Models Hourly)*: Multi-Horizon Forecasts & Risk Governance updated\n"
        f"----------------------------------------\n"
        f"⏱️ *Timestamp:* `{time_str}`\n"
        f"🔗 [Airflow Control Center](http://localhost:{config.airflow.webserver_port})\n"
        f"🖥️ *Platform:* Mini Company FinTech Engine (SYS-02 & SYS-06)"
    )

    send_sys06_alert(
        {
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "HOURLY_PIPELINE_CYCLE_COMPLETED",
            "deduplicate_key": f"sys02:hourly-cycle:{run_id}",
            "message": message,
            "channels": ["telegram"],
            "action_url": f"http://localhost:{config.airflow.webserver_port}",
        }
    )


def notify_daily_retrain_success(context: Dict[str, Any]) -> None:
    """Dispatched when DAG 02 daily AI model retrain completes."""
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    now_vn = pendulum.now("Asia/Ho_Chi_Minh")
    time_str = now_vn.strftime("%Y-%m-%d %H:%M:%S ICT")

    message = (
        f"🟢 *[SYS-02 DAILY MLOPS RETRAIN COMPLETED]*\n"
        f"----------------------------------------\n"
        f"📁 *DAG:* `02_ml_model_retrain_dag`\n"
        f"🤖 *Models:* LSTM Price Forecast, LightGBM Fraud, Isolation Forest\n"
        f"📦 *MLflow Registry:* Active Version Promoted to Production\n"
        f"⏱️ *Timestamp:* `{time_str}`\n"
        f"🔗 [Airflow Control Center](http://localhost:{config.airflow.webserver_port})"
    )

    send_sys06_alert(
        {
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "DAILY_RETRAIN_COMPLETED",
            "deduplicate_key": f"sys02:daily-retrain:{run_id}",
            "message": message,
            "channels": ["telegram"],
            "action_url": f"http://localhost:{config.airflow.webserver_port}/dags/02_ml_model_retrain_dag/grid",
        }
    )


def notify_weekly_maintenance_success(context: Dict[str, Any]) -> None:
    """Dispatched when DAG 03 weekly maintenance completes."""
    dag_run = context.get("dag_run")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    now_vn = pendulum.now("Asia/Ho_Chi_Minh")
    time_str = now_vn.strftime("%Y-%m-%d %H:%M:%S ICT")

    message = (
        f"🟢 *[SYS-02 WEEKLY LAKEHOUSE MAINTENANCE COMPLETED]*\n"
        f"----------------------------------------\n"
        f"📁 *DAG:* `03_lakehouse_maintenance_dag`\n"
        f"🧹 *Tasks:* MinIO 30-day retention purge, ClickHouse OPTIMIZE FINAL, Airflow DB vacuum\n"
        f"⏱️ *Timestamp:* `{time_str}`\n"
        f"🔗 [Airflow Control Center](http://localhost:{config.airflow.webserver_port})"
    )

    send_sys06_alert(
        {
            "alert_type": "SYSTEM_HEALTH",
            "level": "INFO",
            "source_system": "SYS-02",
            "event": "WEEKLY_MAINTENANCE_COMPLETED",
            "deduplicate_key": f"sys02:weekly-maintenance:{run_id}",
            "message": message,
            "channels": ["telegram"],
            "action_url": f"http://localhost:{config.airflow.webserver_port}/dags/03_lakehouse_maintenance_dag/grid",
        }
    )


def on_sla_miss_callback(dag: Any, task_list: str, blocking_task_list: str, slas: str, blocking_tis: str) -> None:
    """Airflow callback function triggered when an SLA threshold is breached."""
    dag_id = getattr(dag, "dag_id", "unknown_dag")
    msg = (
        f"⚠️ *[AIRFLOW SLA MISS WARNING] in DAG:* `{dag_id}`\n"
        f"----------------------------------------\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Tasks Impacted*: `{task_list}`\n"
        f"• *Blocking Task*: `{blocking_task_list}`\n"
        f"• *System SLA*: Exceeded target duration threshold.\n"
        f"----------------------------------------\n"
        f"🔗 [Airflow Grid](http://localhost:{config.airflow.webserver_port}/dags/{dag_id}/grid)"
    )
    logger.warning(msg)
    send_sys06_alert(
        {
            "alert_type": "SLA_BREACH",
            "level": "WARNING",
            "source_system": "SYS-02",
            "event": "AIRFLOW_SLA_MISSED",
            "deduplicate_key": f"sys02:sla-miss:{dag_id}:{int(time.time())}",
            "message": msg,
            "channels": ["telegram"],
            "action_url": f"http://localhost:{config.airflow.webserver_port}/dags/{dag_id}/grid",
        }
    )
