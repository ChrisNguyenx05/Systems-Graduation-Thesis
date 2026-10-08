import unittest

from src.reconciliation.klines import compare_closed_klines


def kline(**overrides):
    record = {
        "symbol": "BTCUSDT",
        "open_time": 1_000,
        "close_time": 1_899,
        "open": "100.00",
        "high": "110.00",
        "low": "99.00",
        "close": "105.00",
        "volume": "12.5",
        "quote_asset_volume": "1312.5",
        "number_of_trades": 42,
        "taker_buy_base_asset_volume": "7.1",
        "taker_buy_quote_asset_volume": "745.5",
        "collected_at": 2_000,
        "source": "test",
    }
    record.update(overrides)
    return record


class KlineReconciliationTests(unittest.TestCase):
    def test_decimal_equivalent_records_match(self):
        api = kline(open=100, close=105.0, collected_at=2_100, source="binance_rest_klines")
        result = compare_closed_klines(kline(), api)
        self.assertTrue(result["matched"])
        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["mismatched_fields"], [])

    def test_difference_is_reported_by_field(self):
        result = compare_closed_klines(kline(), kline(close="105.01"))
        self.assertFalse(result["matched"])
        self.assertEqual(result["status"], "mismatch")
        self.assertEqual(result["mismatched_fields"], ["close"])

    def test_missing_api_is_not_silently_accepted(self):
        result = compare_closed_klines(kline(), None)
        self.assertFalse(result["matched"])
        self.assertEqual(result["status"], "api_missing")


if __name__ == "__main__":
    unittest.main()
