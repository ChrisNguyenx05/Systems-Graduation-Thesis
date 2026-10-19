"""
Data Quality Validation Gates for ETL Tasks
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

REQUIRED_KLINE_FIELDS = ("open", "high", "low", "close", "volume")
TIME_FIELDS = ("open_time", "timestamp")


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_timestamp(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
    return None


def _record_time(record: Dict[str, Any]) -> Optional[float]:
    for field in TIME_FIELDS:
        ts = _as_timestamp(record.get(field))
        if ts is not None:
            return ts
    return None


def validate_klines_data(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Validates Binance Kline records before they can move into Silver/Gold."""
    if not records:
        return {
            "passed": False,
            "total_records": 0,
            "failed_records": 0,
            "errors": ["Record batch is empty"],
            "status": "FAILED"
        }

    missing_required_count = 0
    invalid_numeric_count = 0
    null_close_count = 0
    negative_volume_count = 0
    invalid_ohlc_count = 0
    missing_timestamp_count = 0
    duplicate_key_count = 0
    interval_gap_count = 0
    errors = []
    seen_keys = set()
    timestamps = []

    for record in records:
        missing_fields = [field for field in REQUIRED_KLINE_FIELDS if field not in record]
        if missing_fields:
            missing_required_count += 1
            errors.append(f"Missing required fields: {','.join(missing_fields)}")

        open_price = _as_float(record.get("open"))
        high = _as_float(record.get("high"))
        low = _as_float(record.get("low"))
        close = _as_float(record.get("close"))
        volume = _as_float(record.get("volume"))

        numeric_values = [open_price, high, low, close, volume]
        if any(value is None for value in numeric_values):
            invalid_numeric_count += 1

        if close is None:
            null_close_count += 1

        if volume is not None and volume < 0:
            negative_volume_count += 1

        if all(value is not None for value in [open_price, high, low, close]):
            if high < low or high < open_price or high < close or low > open_price or low > close:
                invalid_ohlc_count += 1

        timestamp = _record_time(record)
        if timestamp is None:
            missing_timestamp_count += 1
        else:
            timestamps.append(timestamp)

        symbol = record.get("symbol", "")
        key = (symbol, timestamp)
        if timestamp is not None:
            if key in seen_keys:
                duplicate_key_count += 1
            seen_keys.add(key)

    sorted_timestamps = sorted(set(timestamps))
    if len(sorted_timestamps) >= 2:
        deltas = [
            sorted_timestamps[idx] - sorted_timestamps[idx - 1]
            for idx in range(1, len(sorted_timestamps))
        ]
        positive_deltas = [delta for delta in deltas if delta > 0]
        if positive_deltas:
            expected_interval = min(positive_deltas)
            interval_gap_count = sum(1 for delta in positive_deltas if delta > expected_interval * 1.5)

    failed_records = sum([
        missing_required_count,
        invalid_numeric_count,
        null_close_count,
        negative_volume_count,
        invalid_ohlc_count,
        missing_timestamp_count,
        duplicate_key_count,
        interval_gap_count,
    ])

    passed = failed_records == 0

    result = {
        "passed": passed,
        "total_records": len(records),
        "failed_records": failed_records,
        "missing_required_count": missing_required_count,
        "invalid_numeric_count": invalid_numeric_count,
        "null_close_count": null_close_count,
        "negative_volume_count": negative_volume_count,
        "invalid_ohlc_count": invalid_ohlc_count,
        "missing_timestamp_count": missing_timestamp_count,
        "duplicate_key_count": duplicate_key_count,
        "interval_gap_count": interval_gap_count,
        "errors": sorted(set(errors)),
        "status": "PASSED" if passed else "FAILED"
    }
    logger.info(f"Data Quality Gate Check Result: {result}")
    return result


def reconcile_api_with_wsis_bronze(
    api_records: List[Dict[str, Any]],
    symbol: str,
    tolerance_pct: float = 0.5
) -> Dict[str, Any]:
    """
    Cross-reconciliation between Primary API records and Secondary WSIS Bronze stream.
    WSIS functions strictly as a Quality Control (QC) and Auditing gate.
    If WSIS data is unavailable or lagging, this gate records a notice and allows
    API ingestion to proceed because API is the primary ground truth.
    """
    if not api_records:
        return {
            "reconciliation_attempted": False,
            "wsis_available": False,
            "matched": True,
            "max_price_delta_pct": 0.0,
            "status": "NO_API_RECORDS",
            "details": "No API records provided for reconciliation."
        }

    try:
        import gzip
        import json
        from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
        lakehouse = LakehouseTaskOperator()
        client = lakehouse._client()

        prefix = "wsis/klines/"
        if not client.bucket_exists(lakehouse.bronze_bucket):
            return {
                "reconciliation_attempted": True,
                "wsis_available": False,
                "matched": True,
                "max_price_delta_pct": 0.0,
                "status": "WSIS_OFFLINE_API_AUTHORIZED",
                "details": f"Bronze bucket {lakehouse.bronze_bucket} not ready. Primary API authorized."
            }

        wsis_files = list(client.list_objects(lakehouse.bronze_bucket, prefix=prefix, recursive=True))
        if not wsis_files:
            logger.info("QC Notice: No WSIS klines stream files found in MinIO Bronze. Primary API authorized.")
            return {
                "reconciliation_attempted": True,
                "wsis_available": False,
                "matched": True,
                "max_price_delta_pct": 0.0,
                "status": "WSIS_OFFLINE_API_AUTHORIZED",
                "details": "WSIS stream data not present for current window. Primary API authorized."
            }

        # Read latest WSIS stream file
        sorted_files = sorted(wsis_files, key=lambda x: str(x.object_name), reverse=True)
        latest_obj = sorted_files[0]
        resp = client.get_object(lakehouse.bronze_bucket, latest_obj.object_name)
        try:
            raw_bytes = resp.read()
            if latest_obj.object_name.endswith(".gz"):
                raw_bytes = gzip.decompress(raw_bytes)
            wsis_rows = [json.loads(line) for line in raw_bytes.decode("utf-8").splitlines() if line.strip()]
        finally:
            resp.close()
            resp.release_conn()

        # Find WSIS record for matching symbol
        wsis_match = next((r for r in reversed(wsis_rows) if r.get("symbol", "").upper() == symbol.upper()), None)
        if not wsis_match:
            return {
                "reconciliation_attempted": True,
                "wsis_available": False,
                "matched": True,
                "max_price_delta_pct": 0.0,
                "status": "WSIS_SYMBOL_NOT_FOUND",
                "details": f"WSIS file found but symbol {symbol} not present. Primary API authorized."
            }

        latest_api_record = api_records[-1]
        api_close = float(latest_api_record.get("close", 0.0))
        wsis_close = float(wsis_match.get("close", 0.0))

        if api_close > 0 and wsis_close > 0:
            delta = abs(api_close - wsis_close)
            delta_pct = (delta / api_close) * 100.0
            matched = delta_pct <= tolerance_pct
            status = "GOLD_VERIFIED" if matched else "DESYNC_WARNING"
            details = (
                f"API Close={api_close}, WSIS Close={wsis_close}, "
                f"Delta={delta:.4f} ({delta_pct:.3f}%). Tolerance={tolerance_pct}%"
            )
            logger.info("WSIS QC Cross-Reconciliation Result: %s | %s", status, details)
            return {
                "reconciliation_attempted": True,
                "wsis_available": True,
                "matched": matched,
                "max_price_delta_pct": round(delta_pct, 4),
                "api_close": api_close,
                "wsis_close": wsis_close,
                "status": status,
                "details": details
            }
        return {
            "reconciliation_attempted": True,
            "wsis_available": True,
            "matched": True,
            "max_price_delta_pct": 0.0,
            "status": "GOLD_VERIFIED",
            "details": "Zero price comparison encountered; passed."
        }
    except Exception as exc:
        logger.warning("QC Reconciliation notice (WSIS audit check non-blocking): %s", exc)
        return {
            "reconciliation_attempted": True,
            "wsis_available": False,
            "matched": True,
            "max_price_delta_pct": 0.0,
            "status": "WSIS_ERROR_API_AUTHORIZED",
            "details": f"WSIS check encountered error: {exc}. Primary API authorized."
        }


def validate_market_metrics_data(data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Validates API-sourced market metrics (agg_trades, order_book_depth, klines, futures_liquidations)
    prior to persisting into ClickHouse Gold layer.
    """
    if not isinstance(data, dict):
        return {"passed": False, "status": "FAILED", "errors": ["Input data must be a dictionary of datasets"]}

    errors = []
    trades = data.get("agg_trades", [])
    depth = data.get("order_book_depth", [])
    klines = data.get("klines", [])

    # 1. Validate agg_trades if present
    for t in trades[:100]:  # check sample
        price = _as_float(t.get("price"))
        qty = _as_float(t.get("quantity"))
        if price is None or price <= 0:
            errors.append(f"Invalid trade price: {t.get('price')}")
            break
        if qty is None or qty < 0:
            errors.append(f"Invalid trade quantity: {t.get('quantity')}")
            break

    # 2. Validate order_book_depth if present
    for d in depth:
        imbalance = _as_float(d.get("order_book_imbalance"))
        if imbalance is not None and (imbalance < -1.0 or imbalance > 1.0):
            errors.append(f"Order book imbalance out of range [-1, 1]: {imbalance}")
            break

    # 3. Validate klines if present
    for k in klines:
        c = _as_float(k.get("close"))
        if c is None or c <= 0:
            errors.append(f"Invalid kline close: {k.get('close')}")
            break

    passed = len(errors) == 0
    result = {
        "passed": passed,
        "status": "PASSED" if passed else "FAILED",
        "datasets_checked": list(data.keys()),
        "agg_trades_count": len(trades),
        "order_book_depth_count": len(depth),
        "klines_count": len(klines),
        "errors": errors
    }
    logger.info("Market Metrics Quality Gate Result: %s", result)
    return result

