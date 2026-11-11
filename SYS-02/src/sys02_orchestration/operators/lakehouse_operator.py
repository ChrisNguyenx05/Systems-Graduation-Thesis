"""Real MinIO-backed Bronze and Silver lakehouse operations."""

import io
import json
from datetime import datetime, timedelta, timezone
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

    def write_bronze_klines(self, records: List[Dict[str, Any]], symbol: str, date_str: str, source: str = "v1.0.0_api_klines", stream_type: str = "api") -> Dict[str, Any]:
        if not records:
            raise ValueError("Cannot write an empty Bronze batch")
        path = self._write(
            self.bronze_bucket,
            f"{stream_type}/klines/symbol={symbol}/date={date_str}/bronze.json",
            {"symbol": symbol, "source": source, "stream_type": stream_type, "records": records}
        )
        return {"status": "SUCCESS", "bronze_output_path": path, "records": records, "source": source, "stream_type": stream_type}

    def process_bronze_to_silver(self, records: List[Dict[str, Any]], symbol: str, date_str: str, source: str = "v1.0.0_api_klines", stream_type: str = "api") -> Dict[str, Any]:
        if not records:
            raise ValueError("Cannot transform an empty Bronze batch")
        fields = ("symbol", "timestamp", "open", "high", "low", "close", "volume")
        cleaned = [{field: row[field] for field in fields if field in row} for row in records]
        for row in cleaned:
            row["stream_type"] = stream_type
            row["source"] = source
        path = self._write(
            self.silver_bucket,
            f"{stream_type}/klines/symbol={symbol}/date={date_str}/silver.json",
            {"symbol": symbol, "source": source, "stream_type": stream_type, "records": cleaned}
        )
        if stream_type == "api":
            self._write(self.silver_bucket, f"klines/symbol={symbol}/date={date_str}/silver.json", {"symbol": symbol, "source": source, "records": cleaned})
        return {"status": "SUCCESS", "silver_output_path": path, "cleaned_ticks": len(cleaned), "records": cleaned, "source": source, "stream_type": stream_type}

    def fetch_wsis_silver_records(self, symbol: str, date_str: str, start_time_ms: Any = None, end_time_ms: Any = None, interval: str = "1m") -> List[Dict[str, Any]]:
        """
        Extract WSIS WebSocket stream records from MinIO Bronze, transform closed candles to Silver format,
        and persist them to s3://crypto-silver/wsis/...
        """
        import gzip
        client = self._client()
        if not client.bucket_exists(self.bronze_bucket):
            return []

        if start_time_ms:
            start_dt = datetime.fromtimestamp(int(start_time_ms) / 1000 if int(start_time_ms) > 100000000000 else int(start_time_ms), timezone.utc)
            day_prefix = f"wsis/klines/year={start_dt.year}/month={start_dt.month:02d}/day={start_dt.day:02d}/"
            wsis_files = list(client.list_objects(self.bronze_bucket, prefix=day_prefix, recursive=True))
            if not wsis_files:
                wsis_files = list(client.list_objects(self.bronze_bucket, prefix="wsis/klines/", recursive=True))
        else:
            wsis_files = list(client.list_objects(self.bronze_bucket, prefix="wsis/klines/", recursive=True))

        if not wsis_files:
            return []

        sorted_files = sorted(wsis_files, key=lambda x: str(x.object_name), reverse=True)
        records = []
        for obj in sorted_files[:10]:
            try:
                resp = client.get_object(self.bronze_bucket, obj.object_name)
                try:
                    raw_bytes = resp.read()
                    if obj.object_name.endswith(".gz"):
                        raw_bytes = gzip.decompress(raw_bytes)
                    for line in raw_bytes.decode("utf-8").splitlines():
                        if not line.strip():
                            continue
                        row = json.loads(line)
                        if row.get("symbol", "").upper() != symbol.upper():
                            continue
                        if not row.get("is_closed", True):
                            continue
                        if row.get("interval") and row.get("interval") != interval:
                            continue
                        ts = int(row.get("open_time", 0))
                        if start_time_ms and ts < int(start_time_ms):
                            continue
                        if end_time_ms and ts > int(end_time_ms):
                            continue
                        records.append({
                            "symbol": symbol.upper(),
                            "timestamp": int(ts // 1000) if ts > 100000000000 else int(ts),
                            "open_time": ts,
                            "open": float(row.get("open", 0.0)),
                            "high": float(row.get("high", 0.0)),
                            "low": float(row.get("low", 0.0)),
                            "close": float(row.get("close", 0.0)),
                            "volume": float(row.get("volume", 0.0)),
                            "interval": row.get("interval", interval),
                            "is_closed": True,
                            "source": "wsis_stream",
                            "stream_type": "wsis"
                        })
                finally:
                    resp.close()
                    resp.release_conn()
            except Exception:
                continue

        seen = set()
        deduped = []
        for r in records:
            if r["timestamp"] not in seen:
                seen.add(r["timestamp"])
                deduped.append(r)

        # Race-condition safeguard: If no records found but requested end_time_ms is very recent,
        # wait 6 seconds once to let WSIS finish its 15-minute file upload to MinIO
        if not deduped and end_time_ms:
            now_ms = datetime.now(timezone.utc).timestamp() * 1000
            if abs(now_ms - int(end_time_ms)) < 180000:  # within 3 minutes of now
                import time
                time.sleep(6)
                # Re-check files
                wsis_files = list(client.list_objects(self.bronze_bucket, prefix=day_prefix if 'day_prefix' in locals() else "wsis/klines/", recursive=True))
                for obj in sorted(wsis_files, key=lambda x: str(x.object_name), reverse=True)[:5]:
                    try:
                        resp = client.get_object(self.bronze_bucket, obj.object_name)
                        raw_bytes = gzip.decompress(resp.read()) if obj.object_name.endswith(".gz") else resp.read()
                        for line in raw_bytes.decode("utf-8").splitlines():
                            if not line.strip(): continue
                            row = json.loads(line)
                            if row.get("symbol", "").upper() != symbol.upper() or not row.get("is_closed", True): continue
                            if row.get("interval") and row.get("interval") != interval: continue
                            ts = int(row.get("open_time", 0))
                            if start_time_ms and ts < int(start_time_ms): continue
                            if end_time_ms and ts > int(end_time_ms): continue
                            rec = {
                                "symbol": symbol.upper(),
                                "timestamp": int(ts // 1000) if ts > 100000000000 else int(ts),
                                "open_time": ts,
                                "open": float(row.get("open", 0.0)),
                                "high": float(row.get("high", 0.0)),
                                "low": float(row.get("low", 0.0)),
                                "close": float(row.get("close", 0.0)),
                                "volume": float(row.get("volume", 0.0)),
                                "interval": row.get("interval", interval),
                                "is_closed": True,
                                "source": "wsis_stream",
                                "stream_type": "wsis"
                            }
                            if rec["timestamp"] not in seen:
                                seen.add(rec["timestamp"])
                                deduped.append(rec)
                    except Exception:
                        pass

        if deduped:
            self._write(
                self.silver_bucket,
                f"wsis/klines/symbol={symbol}/date={date_str}/silver.json",
                {"symbol": symbol, "source": "wsis_stream", "stream_type": "wsis", "records": deduped}
            )
        return deduped

    def write_market_metrics(self, bucket, dataset, run_key, records):
        source_map = {
            "klines": "v1.0.0_api_klines",
            "agg_trades": "v1.0.0_api_aggtrade",
            "order_book_depth": "v1.0.0_api_depth",
            "futures_liquidations": "v1.0.0_api_liquidations"
        }
        source_tag = source_map.get(dataset, f"v1.0.0_api_{dataset}")
        return self._write(bucket, f"market_metrics/{dataset}/run={run_key}/data.json", {"dataset": dataset, "source": source_tag, "records": records})
    def read_market_metrics(self, path):
        if not path.startswith("s3://"): raise ValueError(f"Invalid MinIO path: {path}")
        bucket, obj = path[5:].split("/", 1); response = self._client().get_object(bucket, obj)
        try: return json.loads(response.read().decode("utf-8"))["records"]
        finally: response.close(); response.release_conn()

    def clean_expired_bronze_files(self, retention_days: int = 30) -> Dict[str, Any]:
        """Delete only Bronze objects older than the configured retention period."""
        if retention_days <= 0:
            raise ValueError("retention_days must be greater than zero")
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        files_purged = reclaimed_bytes = 0
        try:
            client = self._client()
            if not client.bucket_exists(self.bronze_bucket):
                return {"bucket": self.bronze_bucket, "retention_days": retention_days, "files_purged": 0, "reclaimed_bytes": 0, "status": "SUCCESS"}
            for obj in client.list_objects(self.bronze_bucket, recursive=True):
                modified = obj.last_modified
                if modified.tzinfo is None:
                    modified = modified.replace(tzinfo=timezone.utc)
                if modified >= cutoff:
                    continue
                client.remove_object(self.bronze_bucket, obj.object_name)
                files_purged += 1
                reclaimed_bytes += obj.size or 0
        except Exception as exc:
            raise RuntimeError(f"MinIO Bronze cleanup failed: {exc}") from exc
        return {"bucket": self.bronze_bucket, "retention_days": retention_days, "files_purged": files_purged, "reclaimed_bytes": reclaimed_bytes, "status": "SUCCESS"}
