"""Real ClickHouse Gold-layer persistence operations."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from sys02_orchestration.config import config


class ClickHouseTaskOperator:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        self.host = host or config.clickhouse.host
        self.port = port or config.clickhouse.port
        self.user = config.clickhouse.user
        self.password = config.clickhouse.password
        self.url = f"http://{self.host}:{self.port}/"

    def execute_sql(self, query: str) -> str:
        try:
            response = requests.post(self.url, params={"query": query}, auth=(self.user, self.password), timeout=15)
            response.raise_for_status()
            return response.text.strip()
        except Exception as exc:
            raise RuntimeError(f"ClickHouse query failed at {self.host}:{self.port}: {exc}") from exc

    def ensure_tables_exist(self) -> None:
        self.execute_sql("""
            CREATE TABLE IF NOT EXISTS crypto_klines_gold (
                symbol LowCardinality(String), open_time DateTime64(3, 'UTC'),
                close_time DateTime64(3, 'UTC'), open Float64, high Float64,
                low Float64, close Float64, volume Float64,
                quote_asset_volume Float64, number_of_trades UInt32,
                sma_20 Float64, rsi_14 Float64,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (symbol, open_time)
        """)

    def insert_gold_klines(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            raise ValueError("Cannot insert an empty Gold batch")
        self.ensure_tables_exist()
        values = []
        for row in records:
            symbol = str(row["symbol"])
            if not symbol.isalnum():
                raise ValueError(f"Invalid symbol: {symbol}")
            open_time = datetime.fromtimestamp(int(row["timestamp"]), timezone.utc)
            close_time = open_time.replace(second=59, microsecond=999000)
            open_text = open_time.strftime("%Y-%m-%d %H:%M:%S.000")
            close_text = close_time.strftime("%Y-%m-%d %H:%M:%S.999")
            close = float(row["close"])
            volume = float(row["volume"])
            values.append(
                f"('{symbol}','{open_text}','{close_text}',{float(row['open'])},"
                f"{float(row['high'])},{float(row['low'])},{close},{volume},"
                f"{close * volume},0,0,0)"
            )
        self.execute_sql(
            "INSERT INTO crypto_klines_gold "
            "(symbol,open_time,close_time,open,high,low,close,volume,"
            "quote_asset_volume,number_of_trades,sma_20,rsi_14) VALUES "
            + ",".join(values)
        )
        return len(records)

    def verify_table_freshness(self, table_name: str, max_allowed_delay_seconds: int = 3600) -> Dict[str, Any]:
        row_count = int(self.execute_sql(f"SELECT count() FROM {table_name} WHERE updated_at >= now() - INTERVAL {max_allowed_delay_seconds} SECOND"))
        return {"table_name": table_name, "row_count": row_count, "is_fresh": row_count > 0, "max_delay_seconds": max_allowed_delay_seconds}
