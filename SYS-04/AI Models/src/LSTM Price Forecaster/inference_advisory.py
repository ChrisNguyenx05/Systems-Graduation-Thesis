#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production Serving Engine for LSTM Price Forecaster (Advisory Mode)
--------------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/LSTM Price Forecaster/inference_advisory.py
Governance : ADVISORY_MODE_ONLY (Unattended automated execution disallowed)
"""

import os
import time
from typing import List, Dict, Any, Union
import numpy as np
import pandas as pd
import torch
import joblib

from dataset import compute_stationary_features, STATIONARY_FEATURES
from gold_adapter import prepare_closed_15m_bars

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODELS_DIR = os.path.join(BASE_DIR, "models", "LSTM Price Forecaster")
PREPROCESSORS_DIR = os.path.join(MODELS_DIR, "preprocessors")
PRODUCTION_DIR = os.path.join(MODELS_DIR, "production")


class LSTMAdvisoryService:
    """
    Production serving class that loads optimized TorchScript JIT models
    and RobustScalers, transforms raw database candle series into stationary
    features, and produces advisory price forecasts with governance metadata.
    """

    def __init__(self, symbols: List[str] = None):
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"]
        self.device = torch.device("cpu")
        self.models: Dict[str, torch.jit.ScriptModule] = {}
        self.scalers: Dict[str, Any] = {}
        self._load_artifacts()

    def _load_artifacts(self):
        """Pre-load TorchScript models and scalers into memory for sub-3ms latency."""
        for sym in self.symbols:
            model_path = os.path.join(PRODUCTION_DIR, f"lstm_price_model_{sym}.torchscript")
            scaler_path = os.path.join(PREPROCESSORS_DIR, f"price_scaler_{sym}.joblib")
            
            if os.path.exists(model_path) and os.path.exists(scaler_path):
                self.models[sym] = torch.jit.load(model_path, map_location=self.device)
                self.models[sym].eval()
                self.scalers[sym] = joblib.load(scaler_path)
            else:
                raise FileNotFoundError(f"Missing artifacts for symbol {sym} at {model_path} or {scaler_path}")

    def predict_advisory(self, symbol: str, raw_candles: Union[pd.DataFrame, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Accepts raw closed candles from database (minimum 60 candles with OHLCV),
        computes stationary features, runs inference, and returns advisory envelope.
        """
        t_start = time.perf_counter()
        
        if symbol not in self.models:
            raise ValueError(f"Symbol '{symbol}' not supported. Registered symbols: {list(self.models.keys())}")

        if isinstance(raw_candles, list):
            df = pd.DataFrame(raw_candles)
        else:
            df = raw_candles.copy()

        df = prepare_closed_15m_bars(df, minimum_bars=60)

        # 1. Feature Engineering (7 Stationary Features)
        features_df = compute_stationary_features(df)
        last_60_features = features_df.tail(60).values.astype(np.float32)
        
        # 2. Scale using fitted RobustScaler
        scaler = self.scalers[symbol]
        scaled_features = scaler.transform(last_60_features) # Shape (60, 7)
        
        # 3. Create input tensor
        input_tensor = torch.tensor(scaled_features, dtype=torch.float32, device=self.device).unsqueeze(0) # Shape (1, 60, 7)
        
        # 4. TorchScript JIT Inference
        with torch.no_grad():
            preds = self.models[symbol](input_tensor).cpu().numpy()[0] # [r_15m, r_60m]

        r_15m = float(preds[0])
        r_60m = float(preds[1])
        current_close = float(df["close"].iloc[-1])
        
        # 5. Anchor returns to current price
        predicted_15m_usd = current_close * (1.0 + r_15m)
        predicted_60m_usd = current_close * (1.0 + r_60m)

        # 6. Trend Bias Classification (deadzone = 0.05%)
        trend_15m = "BULLISH" if r_15m > 0.0005 else ("BEARISH" if r_15m < -0.0005 else "NEUTRAL")
        trend_60m = "BULLISH" if r_60m > 0.0005 else ("BEARISH" if r_60m < -0.0005 else "NEUTRAL")
        
        latency_ms = round((time.perf_counter() - t_start) * 1000.0, 3)

        # 7. Production Advisory Envelope with Governance Rules
        return {
            "symbol": symbol,
            "timestamp_utc": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "current_close_price": round(current_close, 4),
            "forecast_advisory": {
                "governance_mode": "ADVISORY_ANALYTICS_ONLY",
                "unattended_execution_authorized": False,
                "projected_horizon_15m": {
                    "expected_return_pct": round(r_15m * 100.0, 4),
                    "projected_price_usd": round(predicted_15m_usd, 4),
                    "trend_bias": trend_15m
                },
                "projected_horizon_60m": {
                    "expected_return_pct": round(r_60m * 100.0, 4),
                    "projected_price_usd": round(predicted_60m_usd, 4),
                    "trend_bias": trend_60m
                },
                "confidence_level": "MEDIUM",
                "disclaimer": (
                    "This forecast is restricted to Advisory Decision Support for UI charts and portfolio weighting. "
                    "Automatic unattended trade execution is strictly prohibited under SYS-04 Model Governance policy."
                )
            },
            "telemetry": {
                "device": "cpu",
                "inference_latency_ms": latency_ms,
                "input_bars_evaluated": len(df)
            }
        }


if __name__ == "__main__":
    # Self-test on synthetic candles
    print("Testing LSTMAdvisoryService...")
    service = LSTMAdvisoryService(symbols=["BTCUSDT"])
    
    # Generate 65 dummy candles
    base_p = 76000.0
    synth_candles = []
    for i in range(65):
        p = base_p + np.random.randn() * 100
        synth_candles.append({
            "open": p - 10,
            "high": p + 25,
            "low": p - 30,
            "close": p,
            "volume": 120.0 + np.random.rand() * 50
        })
        
    result = service.predict_advisory("BTCUSDT", synth_candles)
    import json
    print(json.dumps(result, indent=2))
