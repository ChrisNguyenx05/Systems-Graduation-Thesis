"""
Unit Tests for SYS-04 MLOps & AI Serving Center
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import server


class TestSys04Server(unittest.TestCase):
    def test_app_metadata(self):
        self.assertIn("SYS-04", server.app.title)
        self.assertIsNotNone(server.app.version)

    def test_supported_symbols(self):
        expected = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"]
        for s in expected:
            self.assertIn(s, server.MARKET_SYMBOLS)

    def test_health_check_logic(self):
        result = server.health_check()
        self.assertIn("status", result)
        self.assertEqual(result["subsystem"], "SYS-04")
        self.assertIn("models_loaded", result)

    def test_baseline_candles_generation(self):
        candles = server.generate_baseline_candles("BTCUSDT", count=10)
        self.assertEqual(len(candles), 10)
        self.assertTrue(candles[0]["is_closed"])
        self.assertIn("close", candles[0])


if __name__ == "__main__":
    unittest.main()
