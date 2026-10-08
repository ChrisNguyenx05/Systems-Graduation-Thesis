"""MinIO Bronze S3 Sink for WSIS Batches."""
import gzip
import json
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from config.settings import settings
from src.utils.logger import logger

class MinIOBronzeSink:
    def __init__(self):
        self.endpoint = settings.MINIO_ENDPOINT
        self.bucket = settings.MINIO_BRONZE_BUCKET
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            use_ssl=settings.MINIO_SECURE,
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except Exception:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket)
                logger.info(f"Created MinIO Bronze bucket: {self.bucket}")
            except Exception as e:
                logger.warning(f"Head/Create bucket check notice: {e}")

    def write_window_dataset(self, dataset_name: str, run_key: str, records: list) -> str:
        """
        Compresses records into JSONL.GZ and uploads to MinIO S3 Bronze layer.
        Object Path: wsis/{dataset}/year=YYYY/month=MM/day=DD/{run_key}.jsonl.gz
        """
        if not records:
            return ""

        # Parse date from run_key (e.g. 20260918T160000Z)
        try:
            dt = datetime.strptime(run_key, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = datetime.now(timezone.utc)

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        object_key = f"wsis/{dataset_name}/year={year}/month={month}/day={day}/{run_key}.jsonl.gz"

        # Serialize to JSON Lines compressed with GZIP
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            for item in records:
                line = json.dumps(item, separators=(",", ":")) + "\n"
                gz.write(line.encode("utf-8"))

        compressed_data = buffer.getvalue()
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=compressed_data,
            ContentType="application/gzip"
        )
        logger.info(f"Uploaded {len(records)} {dataset_name} records to s3://{self.bucket}/{object_key} ({len(compressed_data)} bytes)")
        return object_key
