#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production Serving Engine for Isolation Forest Market Anomaly Detector (Risk Guard Mode)
----------------------------------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/inference_risk_guard.py
Governance : ASYNC_RISK_GUARD_FILTER (Early warning market risk filter; disallow auto-banning)
"""

import os
import time
from typing import List, Dict, Any, Union
import numpy as np
import pandas as pd
import joblib

from model import MarketAnomalyDetector

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODELS_DIR = os.path.join(BASE_DIR, "models", "Isolation Forest Anomaly")
PRODUCTION_DIR = os.path.join(MODELS_DIR, "production")


class MarketRiskGuardService:
    """
    Production serving service that wraps pre-trained Isolation Forest models,
    extracts microstructure features from raw database candles, monitors data quality,
    and returns actionable risk mitigations for the Order Engine and Risk Center.
    """

    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"]
        self.detectors: Dict[str, MarketAnomalyDetector] = {}
        self._load_detectors()

    def _load_detectors(self):
        """Pre-load serialized model artifacts for fast online evaluation."""
        for sym in self.symbols:
            model_path = os.path.join(PRODUCTION_DIR, f"isolation_forest_{sym}.joblib")
            if os.path.exists(model_path):
                self.detectors[sym] = MarketAnomalyDetector.load(model_path)
            else:
                raise FileNotFoundError(f"Missing anomaly detector artifact for {sym} at {model_path}")

    def evaluate_candle_risk(
        self,
        symbol: str,
        raw_candles: Union[pd.DataFrame, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        Evaluate the latest closed candle against historical baseline.
        Expects >= 20 recent candles to compute rolling SMA and volatility.
        """
        t_start = time.perf_counter()
        
        if symbol not in self.detectors:
            raise ValueError(f"Symbol '{symbol}' not supported. Registered symbols: {list(self.detectors.keys())}")

        if isinstance(raw_candles, list):
            df = pd.DataFrame(raw_candles)
        else:
            df = raw_candles.copy()

        detector = self.detectors[symbol]
        raw_result = detector.predict_from_raw_candles(df)
        
        latency_ms = round((time.perf_counter() - t_start) * 1000.0, 3)
        score = raw_result["anomaly_score"]
        is_anomaly = raw_result["is_anomaly"]
        quality = raw_result["data_quality"]

        # Do not let imputed telemetry trigger an operational action.
        if quality.get("status") == "DEGRADED":
            return {
                "symbol": symbol,
                "timestamp_utc": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                "close_price": raw_result["close_price"],
                "risk_guard": {
                    "governance_mode": "ASYNC_RISK_GUARD_FILTER",
                    "outlier_alert_triggered": False,
                    "calibrated_anomaly_score": None,
                    "risk_level": "SIGNAL_WITHHELD",
                    "recommended_actions": {"widen_orderbook_spread": False, "throttle_max_leverage": False, "send_admin_alert": True, "auto_ban_user_account": False},
                    "risk_summary": "DATA_QUALITY_BLOCKED: required Gold telemetry is incomplete; no model action was emitted.",
                    "governance_disclaimer": "A degraded microstructure vector is not valid for operational risk action."
                },
                "data_quality": quality,
                "microstructure_features": raw_result["microstructure_features"],
                "telemetry": {"device": "cpu", "inference_latency_ms": latency_ms, "execution_profile": "ASYNC_BAR_CLOSE"}
            }

        # Risk Actions Engine Mapping
        # Rule 1: High Anomaly (Score >= 0.70 or scikit-learn is_anomaly == True)
        if is_anomaly or score >= 0.70:
            recommended_actions = {
                "widen_orderbook_spread": True,
                "throttle_max_leverage": True,
                "send_admin_alert": True,
                "auto_ban_user_account": False  # Strictly forbidden for unsupervised models
            }
            risk_summary = "CRITICAL_MARKET_OUTLIER: Extreme volatility/volume spike detected on closed bar."
        elif score >= 0.50:
            recommended_actions = {
                "widen_orderbook_spread": False,
                "throttle_max_leverage": True,
                "send_admin_alert": False,
                "auto_ban_user_account": False
            }
            risk_summary = "ELEVATED_VOLATILITY: Moderate deviation from historical microstructure baseline."
        else:
            recommended_actions = {
                "widen_orderbook_spread": False,
                "throttle_max_leverage": False,
                "send_admin_alert": False,
                "auto_ban_user_account": False
            }
            risk_summary = "NORMAL_MARKET_CONDITION: Microstructure features align with typical distribution."

        return {
            "symbol": symbol,
            "timestamp_utc": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "close_price": raw_result["close_price"],
            "risk_guard": {
                "governance_mode": "ASYNC_RISK_GUARD_FILTER",
                "outlier_alert_triggered": is_anomaly,
                "calibrated_anomaly_score": score,
                "risk_level": raw_result["risk_level"],
                "recommended_actions": recommended_actions,
                "risk_summary": risk_summary,
                "governance_disclaimer": (
                    "This model detects statistical candle anomalies without ground-truth fraud labels. "
                    "It is authorized for Liquidity Provider spread protection and leverage throttling only. "
                    "Automated account bans or punitive user actions are disallowed under SYS-04 policy."
                )
            },
            "data_quality": quality,
            "microstructure_features": raw_result["microstructure_features"],
            "telemetry": {
                "device": "cpu",
                "inference_latency_ms": latency_ms,
                "execution_profile": "ASYNC_BAR_CLOSE"
            }
        }


if __name__ == "__main__":
    print("Testing MarketRiskGuardService...")
    service = MarketRiskGuardService(symbols=["BTCUSDT"])
    
    # 25 dummy candles
    base_p = 76000.0
    synth_candles = []
    for i in range(25):
        p = base_p + np.random.randn() * 50
        synth_candles.append({
            "close_time": 1727712000000 + i * 900000,
            "open": p - 5,
            "high": p + 15,
            "low": p - 20,
            "close": p,
            "volume": 100.0,
            "number_of_trades": 500,
            "quote_asset_volume": p * 100.0,
            "taker_buy_base_asset_volume": 50.0,
            "taker_buy_quote_asset_volume": p * 50.0
        })
        
    res = service.evaluate_candle_risk("BTCUSDT", synth_candles)
    import json
    print(json.dumps(res, indent=2))
