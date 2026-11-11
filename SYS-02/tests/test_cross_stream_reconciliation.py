import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from sys02_orchestration.utils.data_quality import reconcile_api_with_wsis_silver


def test_reconcile_api_with_wsis_silver_pass():
    """Verify that when volume is within 95-105% and price delta <= 0.1%, gate returns TRUE."""
    api_records = [
        {"symbol": "BTCUSDT", "timestamp": 1700000000, "close": 50000.0, "volume": 100.0, "is_closed": True},
        {"symbol": "BTCUSDT", "timestamp": 1700000060, "close": 50100.0, "volume": 150.0, "is_closed": True},
    ]
    wsis_records = [
        {"symbol": "BTCUSDT", "timestamp": 1700000000, "close": 50005.0, "volume": 97.0, "is_closed": True},  # +0.01% price, 97% vol
        {"symbol": "BTCUSDT", "timestamp": 1700000060, "close": 50095.0, "volume": 152.0, "is_closed": True}, # -0.01% price, 101.3% vol
    ]
    result = reconcile_api_with_wsis_silver(api_records, wsis_records, "BTCUSDT")
    assert result["passed"] is True
    assert result["status"] == "TRUE"
    assert result["accuracy_score"] == 100.0


def test_reconcile_api_with_wsis_silver_fail_volume_dropped():
    """Verify that packet drop (< 95% volume) fails the Quality Gate."""
    api_records = [
        {"symbol": "BTCUSDT", "timestamp": 1700000000, "close": 50000.0, "volume": 100.0, "is_closed": True},
    ]
    wsis_records = [
        {"symbol": "BTCUSDT", "timestamp": 1700000000, "close": 50000.0, "volume": 85.0, "is_closed": True},  # 85% vol -> FAIL
    ]
    result = reconcile_api_with_wsis_silver(api_records, wsis_records, "BTCUSDT")
    assert result["passed"] is False
    assert result["status"] == "FALSE"
    assert result["volume_accuracy_pct"] == 0.0


def test_reconcile_api_with_wsis_silver_fail_price_drift():
    """Verify that price discrepancy > 0.1% fails the Quality Gate."""
    api_records = [
        {"symbol": "BTCUSDT", "timestamp": 1700000000, "close": 50000.0, "volume": 100.0, "is_closed": True},
    ]
    wsis_records = [
        {"symbol": "BTCUSDT", "timestamp": 1700000000, "close": 50200.0, "volume": 100.0, "is_closed": True},  # +0.4% price -> FAIL
    ]
    result = reconcile_api_with_wsis_silver(api_records, wsis_records, "BTCUSDT")
    assert result["passed"] is False
    assert result["status"] == "FALSE"
    assert result["price_accuracy_pct"] == 0.0
