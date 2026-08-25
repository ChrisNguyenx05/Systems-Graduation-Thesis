"""
Configuration Settings for SYS-02 Orchestration Engine
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class AirflowConfig:
    project_name: str = os.getenv("AIRFLOW_PROJECT_NAME", "sys02_orchestration")
    webserver_port: int = int(os.getenv("AIRFLOW_WEBSERVER_PORT", "8085"))
    postgres_host_port: int = int(os.getenv("POSTGRES_HOST_PORT", "5433"))

    load_examples: bool = os.getenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false").lower() == "true"

@dataclass
class LakehouseConfig:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket: str = "crypto-bronze"
    silver_bucket: str = "crypto-silver"
    gold_bucket: str = "crypto-gold"

@dataclass
class ClickHouseConfig:
    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    database: str = "default"

@dataclass
class AlertConfig:
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    sla_alert_enabled: bool = os.getenv("SLA_ALERT_ENABLED", "true").lower() == "true"

class ConfigManager:
    """Central configuration manager for SYS-02 orchestration tasks."""
    def __init__(self):
        self.airflow = AirflowConfig()
        self.lakehouse = LakehouseConfig()
        self.clickhouse = ClickHouseConfig()
        self.alert = AlertConfig()

config = ConfigManager()
