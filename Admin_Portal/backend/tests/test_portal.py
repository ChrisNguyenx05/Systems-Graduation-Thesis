"""
Unit Tests for NHFinance Admin Portal Backend Gateway
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.main import app, healthz


class TestAdminPortal(unittest.TestCase):
    def test_portal_metadata(self):
        self.assertIn("Admin Portal", app.title)
        self.assertIsNotNone(app.version)

    def test_healthz_function(self):
        res = healthz()
        self.assertEqual(res, {"status": "ok", "app": "admin-portal"})

    def test_registered_routes(self):
        routes = [r.path for r in app.routes if hasattr(r, "path") and r.path]
        self.assertIn("/healthz", routes)
        self.assertIn("/readyz", routes)
        self.assertIn("/api/v1/system/status", routes)
        openapi_paths = list(app.openapi()["paths"].keys())
        self.assertIn("/api/v1/schedules/pipeline-traces", openapi_paths)

    def test_duration_formatting(self):
        from app.routers.schedule_routes import format_duration
        self.assertEqual(format_duration(38.4), "38 s")
        self.assertEqual(format_duration(135.0), "2 m 15 s")
        self.assertEqual(format_duration(6840.0), "1 h 54 min")
        self.assertEqual(format_duration(None), "--")

    def test_pipeline_traces_structure(self):
        from app.routers.schedule_routes import list_pipeline_traces
        res = list_pipeline_traces(limit=2)
        self.assertIn("runs", res)
        self.assertIn("total", res)
        if len(res["runs"]) > 0:
            first = res["runs"][0]
            self.assertIn("stage_segments", first)
            self.assertIn("stage_label", first)
            self.assertIn("duration", first)
            self.assertIn("start_time", first)
            self.assertIn("instance", first)
            self.assertEqual(len(first["stage_segments"]), 3)

    def test_wsis_routes_registered(self):
        openapi_paths = list(app.openapi()["paths"].keys())
        self.assertIn("/api/v1/wsis/status", openapi_paths)
        self.assertIn("/api/v1/wsis/history", openapi_paths)
        self.assertIn("/api/v1/wsis/refresh", openapi_paths)

    def test_wsis_status_tolerance_and_history(self):
        from app.connectors.wsis import wsis_connector
        res = wsis_connector.get_status(force=True)
        self.assertIn("system_overall_match_rate", res)
        self.assertIn("pair_reconciliation_matrix", res)
        self.assertIn("historical_match_rates", res)
        self.assertGreaterEqual(len(res["historical_match_rates"]), 1)
        
        # Verify BNBUSDT matched under tolerance
        bnb = [p for p in res["pair_reconciliation_matrix"] if p["symbol"] == "BNBUSDT"]
        self.assertTrue(len(bnb) > 0)
        self.assertEqual(bnb[0]["status"], "MATCHED")
        self.assertGreater(bnb[0]["klines_rate"], 99.0)


if __name__ == "__main__":
    unittest.main()

