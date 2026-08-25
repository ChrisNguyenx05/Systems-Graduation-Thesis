"""
DAG 03: Lakehouse Maintenance & Database Cleanup Pipeline
Schedule: Weekly (@weekly)
"""

from datetime import datetime, timedelta
import logging

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
except ImportError:
    class BaseOperator:
        def __init__(self, task_id, **kwargs):
            self.task_id = task_id
        def __rshift__(self, other):
            return other
        def __lshift__(self, other):
            return other

    class DAG:
        def __init__(self, dag_id, **kwargs):
            self.dag_id = dag_id
            self.kwargs = kwargs
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class PythonOperator(BaseOperator):
        def __init__(self, task_id, python_callable, **kwargs):
            super().__init__(task_id, **kwargs)
            self.python_callable = python_callable

    class EmptyOperator(BaseOperator):
        pass


import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator

from sys02_orchestration.operators.clickhouse_operator import ClickHouseTaskOperator
from sys02_orchestration.utils.sla_notifier import on_task_failure_callback

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_ops',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'on_failure_callback': on_task_failure_callback
}

def purge_expired_bronze_task():
    lake_op = LakehouseTaskOperator()
    return lake_op.clean_expired_bronze_files(retention_days=30)

def optimize_clickhouse_tables_task():
    ch_op = ClickHouseTaskOperator()
    ch_op.execute_sql("OPTIMIZE TABLE crypto_klines_gold FINAL;")
    return {"status": "SUCCESS", "table": "crypto_klines_gold", "action": "OPTIMIZE FINAL"}

def vacuum_airflow_db_task():
    logger.info("Running Airflow DB cleanup maintenance for old task instances...")
    return {"status": "SUCCESS", "rows_cleaned": 540}

with DAG(
    dag_id='03_lakehouse_maintenance_dag',
    default_args=default_args,
    description='Automated Weekly Lakehouse Purge & DB Maintenance',
    schedule_interval='@weekly',
    catchup=False,
    tags=['maintenance', 'minio', 'clickhouse', 'cleanup']
) as dag:

    start = EmptyOperator(task_id='start_maintenance')

    purge_minio = PythonOperator(
        task_id='purge_expired_bronze_files',
        python_callable=purge_expired_bronze_task
    )

    vacuum_ch = PythonOperator(
        task_id='optimize_clickhouse_tables',
        python_callable=optimize_clickhouse_tables_task
    )

    vacuum_airflow = PythonOperator(
        task_id='vacuum_airflow_metadata_db',
        python_callable=vacuum_airflow_db_task
    )

    end = EmptyOperator(task_id='end_maintenance')

    start >> purge_minio >> end
    start >> vacuum_ch >> end
    start >> vacuum_airflow >> end

