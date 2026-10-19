FROM apache/airflow:3.3.0-python3.11

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-fab \
    clickhouse-driver>=0.2.6 \
    minio>=7.2.7 \
    requests>=2.31.0 \
    python-dotenv>=1.0.1 \
    websocket-client>=1.8.0

