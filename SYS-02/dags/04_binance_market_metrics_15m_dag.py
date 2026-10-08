"""Collect workbook-defined Binance metrics every 15 minutes."""
from datetime import datetime, timedelta, timezone
import logging
import pendulum
import requests
import os, sys
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from sys02_orchestration.operators.market_metrics_operator import BinanceMarketMetricsOperator, MarketMetricsPersistenceOperator
MARKET_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT")

LOGGER = logging.getLogger(__name__)
NOTIFY_TIMEZONE = "Asia/Ho_Chi_Minh"

def send_sys06_alert(payload):
    url, token = os.getenv("SYS06_ALERT_URL", "").strip(), os.getenv("SYS06_ALERT_API_TOKEN", "").strip()
    if not url or not token:
        LOGGER.warning("SYS-06 notification skipped: dispatcher configuration is missing")
        return
    try:
        response = requests.post(url, json=payload, headers={"X-Alert-Token": token}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("SYS-06 notification could not be delivered: %s", exc)

def notify_hourly_collection_success(context):
    """Notify after the second 15-minute run, then once each following hour."""
    run_time = context["logical_date"].in_timezone(NOTIFY_TIMEZONE)
    if run_time.minute != 15:
        return
    dag_run = context["dag_run"]
    send_sys06_alert({
        "alert_type": "SYSTEM_HEALTH", "level": "INFO", "source_system": "SYS-02",
        "event": "MARKET_METRICS_HOURLY_COLLECTION_COMPLETED",
        "deduplicate_key": f"sys02:market-metrics-hourly:{run_time.strftime('%Y%m%dT%H%M')}",
        "message": f"Two or more 15-minute market-metrics collections completed; hourly status for {run_time.strftime('%Y-%m-%d %H:%M %Z')} is ready.",
        "channels": ["telegram"],
        "action_url": f"http://127.0.0.1:8085/dags/{dag_run.dag_id}/grid",
    })

def extract_market_metrics():
    now = int(datetime.now(timezone.utc).timestamp()*1000); c = BinanceMarketMetricsOperator(MARKET_SYMBOLS); active, unavailable = c.available_symbols()
    rows = c.collect_spot(active, now); rows["futures_liquidations"] = c.collect_liquidations(int(os.getenv("LIQUIDATION_COLLECTION_SECONDS", "15")), now)
    key = datetime.fromtimestamp(now/1000, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return {"run_key": key, "available_symbols": active, "unavailable_symbols": unavailable, "bronze_paths": MarketMetricsPersistenceOperator().write_layer("bronze", key, rows)}
def transform_to_silver(ti):
    extracted = ti.xcom_pull(task_ids="extract_market_metrics"); p = MarketMetricsPersistenceOperator()
    return {"silver_paths": p.write_layer("silver", extracted["run_key"], p.read_layer(extracted["bronze_paths"]))}
def load_market_metrics_gold(ti):
    p = MarketMetricsPersistenceOperator(); return p.load_gold(p.read_layer(ti.xcom_pull(task_ids="transform_market_metrics_silver")["silver_paths"]))
default_args = {"owner": "data_engineering", "retries": 2, "retry_delay": timedelta(minutes=3), "start_date": pendulum.datetime(2026, 8, 30, 22, 0, tz="Asia/Ho_Chi_Minh")}
with DAG("04_binance_market_metrics_15m_dag", default_args=default_args, schedule="*/15 * * * *", catchup=False, max_active_runs=1, on_success_callback=notify_hourly_collection_success, tags=["binance","15m","market-metrics","minio","clickhouse"]) as dag:
    start=EmptyOperator(task_id="start_pipeline")
    extract=PythonOperator(task_id="extract_market_metrics", python_callable=extract_market_metrics, execution_timeout=timedelta(minutes=15))
    transform=PythonOperator(task_id="transform_market_metrics_silver", python_callable=transform_to_silver)
    load=PythonOperator(task_id="load_market_metrics_gold", python_callable=load_market_metrics_gold)
    end=EmptyOperator(task_id="end_pipeline")
    start >> extract >> transform >> load >> end
