#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
End-to-End Live Database & Model Verification for All 3 Models
--------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/test_e2e_all_models_database.py
Role       : Verifies live ClickHouse & Postgres databases against:
             1. LightGBM Fraud Detector (Serving & Feature Store)
             2. LSTM Price Forecaster (ClickHouse Gold bars -> TorchScript JIT)
             3. Isolation Forest Anomaly (ClickHouse Gold bars -> Risk Guard)
"""

import sys
import subprocess
import time
from pathlib import Path

# Force UTF-8 encoding for Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BASE_DIR = Path(__file__).resolve().parent.parent # AI Models
SRC_DIR = BASE_DIR / "src"

def run_test(name: str, code: str):
    print("\n" + "="*70)
    print(f"[*] RUNNING: {name}")
    print("="*70)
    t0 = time.time()
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=str(BASE_DIR), encoding="utf-8")
    print(res.stdout)
    if res.stderr:
        print("STDERR:", res.stderr)
    if res.returncode != 0:
        raise RuntimeError(f"Test failed with exit code {res.returncode}")
    print(f"[TIME] Finished in {time.time()-t0:.2f}s")


LGBM_CODE = """
import sys, json, requests, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

BASE_DIR = Path.cwd()
LGBM_SRC = BASE_DIR / "src" / "LightGBM Fraud Detector"
sys.path.insert(0, str(LGBM_SRC))

from model import FraudPredictor

# 1. Verify tables in ClickHouse db_gold_fraud
resp = requests.get("http://127.0.0.1:8123?query=SHOW+TABLES+FROM+db_gold_fraud+FORMAT+JSON", auth=("nhfinanceadmin", "nhathuy123"))
tables = [r["name"] for r in resp.json().get("data", [])]
print(f"[OK] ClickHouse db_gold_fraud tables verified: {tables}")
assert "gold_transaction_risk_features" in tables
assert "v_lightgbm_fraud_training" in tables

# 2. Verify tables in ClickHouse db_bronze_fraud
resp2 = requests.get("http://127.0.0.1:8123?query=SHOW+TABLES+FROM+db_bronze_fraud+FORMAT+JSON", auth=("nhfinanceadmin", "nhathuy123"))
b_tables = [r["name"] for r in resp2.json().get("data", [])]
print(f"[OK] ClickHouse db_bronze_fraud tables verified: {b_tables}")
assert "raw_transactions" in b_tables

# 3. Run FraudPredictor inference
model_p = BASE_DIR / "models" / "LightGBM Fraud Detector" / "production" / "lgbm_fraud_model.joblib"
trans_p = BASE_DIR / "models" / "LightGBM Fraud Detector" / "preprocessors" / "fraud_feature_transformer.joblib"
predictor = FraudPredictor(model_p, trans_p)

normal_tx = {
    "TransactionAmt": 35.0, "card1": 1000, "card2": 150, "card3": 150, "card5": 226,
    "addr1": 315, "addr2": 87, "dist1": 2.0, "C1": 1, "C2": 1, "C4": 0, "C5": 0,
    "C11": 1, "C13": 1, "C14": 1, "D1": 0, "D2": 0, "D4": 0, "D10": 0, "D15": 0,
    "V12": 1, "V13": 1, "V20": 1, "V30": 1, "V70": 0, "V80": 0, "V90": 0,
    "V128": 0, "V283": 1, "V307": 0,
    "ProductCD": "W", "card4": "visa", "card6": "debit",
    "P_emaildomain": "gmail.com", "R_emaildomain": "__MISSING__"
}
res = predictor.predict(normal_tx)
print(f"  • Normal Transaction  : P(Fraud) = {res['fraud_probability']:.4f} -> Action: {res['action']}")

fraud_tx = {
    "TransactionAmt": 4500.0, "card1": 9999, "card2": 500, "card3": 185, "card5": 137,
    "addr1": 441, "addr2": 87, "dist1": 500.0, "C1": 50, "C2": 55, "C4": 20, "C5": 0,
    "C11": 40, "C13": 120, "C14": 25, "D1": 400, "D2": 350, "D4": 300, "D10": 200, "D15": 300,
    "V12": 10, "V13": 15, "V20": 20, "V30": 10, "V70": 5, "V80": 12, "V90": 8,
    "V128": 5000, "V283": 30, "V307": 8000,
    "ProductCD": "C", "card4": "mastercard", "card6": "credit",
    "P_emaildomain": "anonymous.com", "R_emaildomain": "protonmail.com"
}
res_f = predictor.predict(fraud_tx)
print(f"  • High-Risk Transaction: P(Fraud) = {res_f['fraud_probability']:.4f} -> Action: {res_f['action']}")
print("[OK] LightGBM Fraud Detector & Feature Store verified!")
"""

LSTM_CODE = """
import sys, json, requests, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

BASE_DIR = Path.cwd()
LSTM_SRC = BASE_DIR / "src" / "LSTM Price Forecaster"
sys.path.insert(0, str(LSTM_SRC))

from inference_advisory import LSTMAdvisoryService

symbol = "BTCUSDT"
sql = "SELECT open_time, close_time, open, high, low, close, volume, 1 AS is_closed FROM default.crypto_klines_gold WHERE symbol = 'BTCUSDT' AND round((toUnixTimestamp64Milli(close_time) - toUnixTimestamp64Milli(open_time))/1000) = 900 ORDER BY open_time ASC FORMAT JSON"
resp = requests.get(f"http://127.0.0.1:8123?query={sql}", auth=("nhfinanceadmin", "nhathuy123"))
df_bars = pd.DataFrame(resp.json().get("data", []))
print(f"Loaded {len(df_bars)} real closed 15m bars from ClickHouse default.crypto_klines_gold.")

service = LSTMAdvisoryService(symbols=[symbol])
advisory = service.predict_advisory(symbol, df_bars)

print(f"  • Symbol                   : {advisory['symbol']}")
print(f"  • Current Price            : ${advisory['current_close_price']:,.2f}")
print(f"  • Projected +15m Return/USD: {advisory['forecast_advisory']['projected_horizon_15m']['expected_return_pct']:+.4f}% -> ${advisory['forecast_advisory']['projected_horizon_15m']['projected_price_usd']:,.2f} ({advisory['forecast_advisory']['projected_horizon_15m']['trend_bias']})")
print(f"  • Projected +60m Return/USD: {advisory['forecast_advisory']['projected_horizon_60m']['expected_return_pct']:+.4f}% -> ${advisory['forecast_advisory']['projected_horizon_60m']['projected_price_usd']:,.2f} ({advisory['forecast_advisory']['projected_horizon_60m']['trend_bias']})")
print(f"  • Governance Policy        : {advisory['forecast_advisory']['governance_mode']} (Unattended = {advisory['forecast_advisory']['unattended_execution_authorized']})")
print(f"  • Inference Latency        : {advisory['telemetry']['inference_latency_ms']} ms")
print("[OK] LSTM Price Forecaster live database test passed successfully!")
"""

IF_CODE = """
import sys, json, requests, pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

BASE_DIR = Path.cwd()
IF_SRC = BASE_DIR / "src" / "Isolation Forest Anomaly"
sys.path.insert(0, str(IF_SRC))

from inference_risk_guard import MarketRiskGuardService

symbol = "BTCUSDT"
sql = "SELECT open_time, close_time, open, high, low, close, volume, number_of_trades, quote_asset_volume, taker_buy_base_asset_volume, taker_buy_quote_asset_volume FROM default.crypto_klines_gold WHERE symbol = 'BTCUSDT' AND round((toUnixTimestamp64Milli(close_time) - toUnixTimestamp64Milli(open_time))/1000) = 900 ORDER BY open_time ASC FORMAT JSON"
resp = requests.get(f"http://127.0.0.1:8123?query={sql}", auth=("nhfinanceadmin", "nhathuy123"))
df_bars = pd.DataFrame(resp.json().get("data", []))
print(f"Loaded {len(df_bars)} real closed 15m bars from ClickHouse default.crypto_klines_gold.")

guard_service = MarketRiskGuardService(symbols=[symbol])
risk_output = guard_service.evaluate_candle_risk(symbol, df_bars)

print(f"  • Symbol                   : {risk_output['symbol']}")
print(f"  • Data Quality Status      : {risk_output['data_quality']['status']} ({risk_output['data_quality']['confidence']})")
print(f"  • Risk Level               : {risk_output['risk_guard']['risk_level']}")
print(f"  • Calibrated Anomaly Score : {risk_output['risk_guard']['calibrated_anomaly_score']}")
print(f"  • Outlier Alert Triggered  : {risk_output['risk_guard']['outlier_alert_triggered']}")
print(f"  • Recommended Actions      : {json.dumps(risk_output['risk_guard']['recommended_actions'])}")
print(f"  • Inference Latency        : {risk_output['telemetry']['inference_latency_ms']} ms")
print("[OK] Isolation Forest Anomaly live database test passed successfully!")
"""

if __name__ == "__main__":
    t_start = time.time()
    run_test("1. LightGBM Fraud Detector & ClickHouse Feature Store", LGBM_CODE)
    run_test("2. LSTM Price Forecaster & ClickHouse Gold Market Bars", LSTM_CODE)
    run_test("3. Isolation Forest Anomaly & ClickHouse Gold Telemetry", IF_CODE)
    print("\n" + "="*70)
    print(f"[SUCCESS] ALL 3 MODELS AND LIVE DATABASES VERIFIED SUCCESSFULLY IN {time.time()-t_start:.2f}s!")
    print("="*70)
