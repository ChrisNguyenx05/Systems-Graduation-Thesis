"""Real MinIO-backed Bronze and Silver lakehouse operations."""

import io
import json
from typing import Any, Dict, List

from minio import Minio
from sys02_orchestration.config import config


class LakehouseTaskOperator:
    def __init__(self, endpoint: str = config.lakehouse.minio_endpoint):
        self.endpoint = endpoint
        self.access_key = config.lakehouse.minio_access_key
        self.secret_key = config.lakehouse.minio_secret_key
        self.bronze_bucket = config.lakehouse.bronze_bucket
        self.silver_bucket = config.lakehouse.silver_bucket

    def _client(self) -> Minio:
        # host.docker.internal is intentionally preserved: this code runs inside
        # an Airflow container and must reach the SYS-05 service on the host.
        return Minio(self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=False)

    def _write(self, bucket: str, object_name: str, payload: Dict[str, Any]) -> str:
        encoded = json.dumps(payload, default=str).encode("utf-8")
        try:
            client = self._client()
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            client.put_object(bucket, object_name, io.BytesIO(encoded), len(encoded), content_type="application/json")
        except Exception as exc:
            raise RuntimeError(f"MinIO write failed for s3://{bucket}/{object_name}: {exc}") from exc
        return f"s3://{bucket}/{object_name}"

    def write_bronze_klines(self, records: List[Dict[str, Any]], symbol: str, date_str: str) -> Dict[str, Any]:
        if not records:
            raise ValueError("Cannot write an empty Bronze batch")
        path = self._write(self.bronze_bucket, f"klines/symbol={symbol}/date={date_str}/bronze.json", {"symbol": symbol, "records": records})
        return {"status": "SUCCESS", "bronze_output_path": path, "records": records}

    def process_bronze_to_silver(self, records: List[Dict[str, Any]], symbol: str, date_str: str) -> Dict[str, Any]:
        if not records:
            raise ValueError("Cannot transform an empty Bronze batch")
        fields = ("symbol", "timestamp", "open", "high", "low", "close", "volume")
        cleaned = [{field: row[field] for field in fields} for row in records]
        path = self._write(self.silver_bucket, f"klines/symbol={symbol}/date={date_str}/silver.json", {"symbol": symbol, "records": cleaned})
        return {"status": "SUCCESS", "silver_output_path": path, "cleaned_ticks": len(cleaned), "records": cleaned}
