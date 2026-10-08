"""Unit Tests for WSIS 15-Minute Windowing and Airflow Synchronization."""
import unittest
import time
from datetime import datetime, timezone

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import settings, SYMBOL_TO_PAIR_ID
from src.windowing.time_window_manager import TimeWindowManager
from src.buffer.batch_aggregator import TimeWindowBatchAggregator

class TestWSISWindowing(unittest.TestCase):
    def setUp(self):
        self.wm = TimeWindowManager(window_duration_ms=900000, grace_period_seconds=10)

    def test_window_start_alignment(self):
        """Verify that any timestamp within a 15m block rounds down to the exact boundary."""
        # 2026-09-18 16:07:30 UTC = 1789747650000 ms
        # 16:07:30 is in the [16:00:00, 16:15:00] window
        # 16:00:00 UTC = 1789747200000 ms
        ts = 1789747650000
        expected_window_start = 1789747200000
        calculated = self.wm.get_window_start(ts, 900000)
        self.assertEqual(calculated, expected_window_start)
        self.assertEqual(calculated % 900000, 0)

    def test_run_key_matches_airflow_convention(self):
        """Verify that run_key format strictly matches Airflow YYYYMMDDTHHMMSSZ."""
        window_start = 1789747200000
        run_key = self.wm.get_window_run_key(window_start)
        dt = datetime.fromtimestamp(window_start / 1000, timezone.utc)
        expected_key = dt.strftime("%Y%m%dT%H%M%SZ")
        self.assertEqual(run_key, expected_key)
        self.assertTrue(run_key.endswith("Z"))
        self.assertIn("T", run_key)

    def test_pair_id_mapping(self):
        """Verify that all 6 target symbols have correct pair_id from 1 to 6."""
        self.assertEqual(SYMBOL_TO_PAIR_ID.get("BTCUSDT"), 1)
        self.assertEqual(SYMBOL_TO_PAIR_ID.get("ETHUSDT"), 2)
        self.assertEqual(SYMBOL_TO_PAIR_ID.get("BNBUSDT"), 3)
        self.assertEqual(SYMBOL_TO_PAIR_ID.get("AAPLBUSDT"), 4)
        self.assertEqual(SYMBOL_TO_PAIR_ID.get("AMZNBUSDT"), 5)
        self.assertEqual(SYMBOL_TO_PAIR_ID.get("DELLBUSDT"), 6)

class MockSink:
    def __init__(self):
        self.flushed_batches = []

    def write_window_dataset(self, dataset_name, run_key, records):
        self.flushed_batches.append((dataset_name, run_key, len(records)))
        return f"mock/{dataset_name}/{run_key}"

class TestBatchAggregator(unittest.IsolatedAsyncioTestCase):
    async def test_batch_aggregation_and_flush(self):
        sink = MockSink()
        wm = TimeWindowManager(window_duration_ms=900000, grace_period_seconds=10)
        aggregator = TimeWindowBatchAggregator(sink, wm)

        base_ts = 1789747200000  # 16:00:00 UTC
        # Add 3 records in this 15m window
        await aggregator.add_record("klines", base_ts + 5000, {"symbol": "BTCUSDT", "close": 60000.0})
        await aggregator.add_record("agg_trades", base_ts + 10000, {"symbol": "BTCUSDT", "price": 60001.0})
        await aggregator.add_record("futures_liquidations", base_ts + 15000, {"symbol": "BTCUSDT", "qty": 1.5})
        await aggregator.update_depth_snapshot("BTCUSDT", {"symbol": "BTCUSDT", "bid_vol": 100})

        # Flush window
        await aggregator.flush_window(base_ts)

        # Verify sink received all datasets
        datasets_flushed = [item[0] for item in sink.flushed_batches]
        self.assertIn("klines", datasets_flushed)
        self.assertIn("agg_trades", datasets_flushed)
        self.assertIn("futures_liquidations", datasets_flushed)
        self.assertIn("order_book_depth", datasets_flushed)

if __name__ == "__main__":
    unittest.main()
