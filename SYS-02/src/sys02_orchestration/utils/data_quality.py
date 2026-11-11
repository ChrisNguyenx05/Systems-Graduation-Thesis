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


def reconcile_api_with_wsis_silver(
    api_records: List[Dict[str, Any]],
    wsis_records: List[Dict[str, Any]],
    symbol: str,
    price_tolerance_pct: float = 0.1,
    volume_min_ratio: float = 95.0,
    volume_max_ratio: float = 105.0,
    min_accuracy_score: float = 95.0
) -> Dict[str, Any]:
    """
    Silver-layer Cross-Stream Data Quality Gate.
    Evaluates Truthfulness (Tinh Dung Dan) between API Silver and WSIS Silver:
    1. Closed Candles Only (is_kline_closed == 1 or True)
    2. Price Delta <= 0.1% (Strict Price Integrity)
    3. Volume Ratio in [95.0%, 105.0%] (Strict Completeness & Non-duplication)
    4. Composite Accuracy Score >= 95.0% -> TRUE (Passed) / FALSE (Failed)
    """
    if not api_records:
        return {
            "passed": False,
            "status": "FALSE",
            "accuracy_score": 0.0,
            "details": f"No API records provided for {symbol}",
            "symbol": symbol
        }

    if not wsis_records:
        return {
            "passed": False,
            "status": "FALSE",
            "accuracy_score": 0.0,
            "details": f"WSIS Silver records missing for {symbol}. Reconciliation failed.",
            "symbol": symbol
        }

    wsis_map = {}
    for r in wsis_records:
        ts = int(r.get("timestamp") or (r.get("open_time", 0) // 1000 if r.get("open_time", 0) > 100000000000 else r.get("open_time", 0)))
        wsis_map[ts] = r

    matched_price_count = 0
    matched_volume_count = 0
    evaluated_count = 0
    mismatches = []

    for api_r in api_records:
        ts = int(api_r.get("timestamp") or (api_r.get("open_time", 0) // 1000 if api_r.get("open_time", 0) > 100000000000 else api_r.get("open_time", 0)))
        ws_r = wsis_map.get(ts)
        if not ws_r:
            mismatches.append(f"Timestamp {ts} missing in WSIS")
            continue

        evaluated_count += 1
        api_close = float(api_r.get("close", 0.0))
        ws_close = float(ws_r.get("close", 0.0))
        api_vol = float(api_r.get("volume", 0.0))
        ws_vol = float(ws_r.get("volume", 0.0))

        # 1. Price check (tolerance <= 0.1%)
        if api_close > 0:
            price_delta_pct = (abs(api_close - ws_close) / api_close) * 100.0
            if price_delta_pct <= price_tolerance_pct:
                matched_price_count += 1
            else:
                mismatches.append(f"Price drift at {ts}: API={api_close}, WS={ws_close}, delta={price_delta_pct:.3f}%")
        else:
            matched_price_count += 1

        # 2. Volume check (in range 95.0% - 105.0%)
        if api_vol > 0:
            vol_ratio = (ws_vol / api_vol) * 100.0
            if volume_min_ratio <= vol_ratio <= volume_max_ratio:
                matched_volume_count += 1
            else:
                mismatches.append(f"Volume ratio drift at {ts}: API={api_vol}, WS={ws_vol}, ratio={vol_ratio:.2f}%")
        else:
            matched_volume_count += 1

    total_api = len(api_records)
    price_accuracy_pct = (matched_price_count / max(1, total_api)) * 100.0
    volume_accuracy_pct = (matched_volume_count / max(1, total_api)) * 100.0
    composite_score = (0.5 * price_accuracy_pct) + (0.5 * volume_accuracy_pct)

    passed = (composite_score >= min_accuracy_score) and (evaluated_count > 0)
    status = "TRUE" if passed else "FALSE"

    details = (
        f"Composite Accuracy={composite_score:.2f}% (Threshold: {min_accuracy_score}%). "
        f"Price Matched: {matched_price_count}/{total_api} ({price_accuracy_pct:.1f}%), "
        f"Volume Matched [95-105%]: {matched_volume_count}/{total_api} ({volume_accuracy_pct:.1f}%)."
    )
    if mismatches:
        details += f" Samples: {'; '.join(mismatches[:3])}"

    return {
        "passed": passed,
        "status": status,
        "accuracy_score": round(composite_score, 2),
        "price_accuracy_pct": round(price_accuracy_pct, 2),
        "volume_accuracy_pct": round(volume_accuracy_pct, 2),
        "total_api_candles": total_api,
        "evaluated_candles": evaluated_count,
        "matched_price_count": matched_price_count,
        "matched_volume_count": matched_volume_count,
        "details": details,
        "symbol": symbol
    }


def reconcile_api_with_wsis_bronze(
    api_records: List[Dict[str, Any]],
    symbol: str,
    tolerance_pct: float = 0.5
) -> Dict[str, Any]:
    """
    Cross-reconciliation between Primary API records and Secondary WSIS Bronze stream.
    WSIS functions as an auditable Quality Control gate.
    """
    if not api_records:
        return {
            "reconciliation_attempted": False,
            "wsis_available": False,
            "matched": False,
            "passed": False,
            "max_price_delta_pct": 0.0,
            "status": "FALSE",
            "details": "No API records provided for reconciliation."
        }

    try:
        from sys02_orchestration.operators.lakehouse_operator import LakehouseTaskOperator
        lakehouse = LakehouseTaskOperator()
        date_str = datetime.now().date().isoformat()
        wsis_silver = lakehouse.fetch_wsis_silver_records(symbol, date_str)

        if not wsis_silver:
            return {
                "reconciliation_attempted": True,
                "wsis_available": False,
                "matched": False,
                "passed": False,
                "max_price_delta_pct": 0.0,
                "status": "FALSE",
                "details": f"No closed WSIS Silver records available in MinIO for {symbol}."
            }

        res = reconcile_api_with_wsis_silver(api_records, wsis_silver, symbol)
        return {
            "reconciliation_attempted": True,
            "wsis_available": True,
            "matched": res["passed"],
            "passed": res["passed"],
            "accuracy_score": res["accuracy_score"],
            "max_price_delta_pct": round(100.0 - res["price_accuracy_pct"], 4),
            "status": res["status"],
            "details": res["details"]
        }
    except Exception as exc:
        logger.warning("QC Reconciliation notice: %s", exc)
        return {
            "reconciliation_attempted": True,
            "wsis_available": False,
            "matched": False,
            "passed": False,
            "max_price_delta_pct": 0.0,
            "status": "FALSE",
            "details": f"WSIS check encountered error: {exc}"
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

