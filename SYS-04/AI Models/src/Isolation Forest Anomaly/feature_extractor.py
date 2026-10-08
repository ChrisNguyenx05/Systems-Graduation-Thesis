#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Microstructure Feature Extractor for Anomaly Detection
------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/feature_extractor.py
Role       : Transforms raw OHLCV and trade bars (from Database or API)
             into the standardized 8-dimensional microstructure feature vector.
             Explicitly tracks and flags Data Quality to prevent silent imputation.
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any, Union, Optional, Tuple

FEATURE_COLUMNS = [
    "price_velocity",
    "price_velocity_1h",
    "volume_surge_ratio",
    "trades_surge_ratio",
    "volatility_parkinson",
    "bar_range_bps",
    "taker_buy_ratio",
    "quote_net_flow_ratio"
]


class MarketFeatureExtractor:
    """
    Online & Batch Feature Extractor for Market Microstructure Anomaly Detection.
    Converts raw database rows (ClickHouse / PostgreSQL / Binance) into model-ready features.
    Provides explicit telemetry on missing fields and data degradation.
    """

    def __init__(self, rolling_window: int = 20):
        self.rolling_window = rolling_window
        self.feature_columns = FEATURE_COLUMNS
        self.latest_quality_report: Dict[str, Any] = {}

    def extract_features(
        self,
        df_raw: pd.DataFrame,
        return_quality: bool = False
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Extract 8 microstructure features from a DataFrame of raw candles.
        
        Expected raw columns:
        - Required: open, high, low, close, volume
        - Optional: number_of_trades, quote_asset_volume,
                    taker_buy_base_asset_volume, taker_buy_quote_asset_volume
        """
        if df_raw.empty:
            empty_df = pd.DataFrame(columns=self.feature_columns)
            report = {"status": "EMPTY_INPUT", "confidence": "NONE", "missing_fields": []}
            return (empty_df, report) if return_quality else empty_df

        df = df_raw.copy()
        missing_fields = []
        imputed_features = []

        # Standardize column names to lowercase
        df.columns = [str(c).lower().strip() for c in df.columns]

        # 1. Verify Essential OHLCV Columns
        for col in ["open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                raise ValueError(f"Missing required raw price column: '{col}'")
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        # 2. Price Velocities (% return)
        shift_1 = df["close"].shift(1)
        df["price_velocity"] = ((df["close"] - shift_1) / (shift_1 + 1e-9) * 100.0).fillna(0.0)

        shift_4 = df["close"].shift(4)
        df["price_velocity_1h"] = ((df["close"] - shift_4) / (shift_4 + 1e-9) * 100.0).fillna(0.0)

        # 3. Volume Surge Ratio vs Rolling SMA20
        vol_sma = df["volume"].rolling(window=self.rolling_window, min_periods=1).mean()
        df["volume_surge_ratio"] = (df["volume"] / (vol_sma + 1e-9)).fillna(1.0)

        # 4. Trades Surge Ratio vs Rolling SMA20
        trades_col = None
        for candidate in ["number_of_trades", "trades", "trades_count", "trade_count"]:
            if candidate in df.columns:
                trades_col = candidate
                break

        if trades_col is not None:
            trades_series = pd.to_numeric(df[trades_col], errors="coerce").fillna(0.0)
            trades_sma = trades_series.rolling(window=self.rolling_window, min_periods=1).mean()
            df["trades_surge_ratio"] = (trades_series / (trades_sma + 1e-9)).fillna(1.0)
        else:
            missing_fields.append("number_of_trades")
            imputed_features.append("trades_surge_ratio")
            df["trades_surge_ratio"] = 1.0

        # 5. Parkinson Extreme Volatility Estimator
        hl_ratio = np.maximum(df["high"] / np.maximum(df["low"], 1e-9), 1.0)
        df["volatility_parkinson"] = np.sqrt((np.log(hl_ratio) ** 2) / (4.0 * np.log(2.0))).fillna(0.0)

        # 6. Bar High-Low Range in Basis Points (bps)
        df["bar_range_bps"] = (((df["high"] - df["low"]) / (df["close"] + 1e-9)) * 10000.0).fillna(0.0)

        # 7. Taker Buy Pressure Ratio
        taker_base_col = None
        for candidate in ["taker_buy_base_asset_volume", "taker_buy_base_volume", "taker_buy_volume"]:
            if candidate in df.columns:
                taker_base_col = candidate
                break

        if taker_base_col is not None:
            taker_base = pd.to_numeric(df[taker_base_col], errors="coerce").fillna(0.0)
            df["taker_buy_ratio"] = (taker_base / (df["volume"] + 1e-9)).clip(0.0, 1.0).fillna(0.5)
        else:
            missing_fields.append("taker_buy_base_asset_volume")
            imputed_features.append("taker_buy_ratio")
            df["taker_buy_ratio"] = 0.5

        # 8. Quote Net Flow Ratio
        quote_vol_col = None
        for candidate in ["quote_asset_volume", "quote_volume"]:
            if candidate in df.columns:
                quote_vol_col = candidate
                break

        taker_quote_col = None
        for candidate in ["taker_buy_quote_asset_volume", "taker_buy_quote_volume"]:
            if candidate in df.columns:
                taker_quote_col = candidate
                break

        if quote_vol_col is not None and taker_quote_col is not None:
            quote_vol = pd.to_numeric(df[quote_vol_col], errors="coerce").fillna(0.0)
            taker_quote = pd.to_numeric(df[taker_quote_col], errors="coerce").fillna(0.0)
            df["quote_net_flow_ratio"] = ((2.0 * taker_quote - quote_vol) / (quote_vol + 1e-9)).clip(-1.0, 1.0).fillna(0.0)
        else:
            if quote_vol_col is None:
                missing_fields.append("quote_asset_volume")
            if taker_quote_col is None:
                missing_fields.append("taker_buy_quote_asset_volume")
            imputed_features.append("quote_net_flow_ratio")
            df["quote_net_flow_ratio"] = (2.0 * df["taker_buy_ratio"] - 1.0).clip(-1.0, 1.0).fillna(0.0)

        # Enforce float32 and replace infinities
        for col in self.feature_columns:
            df[col] = df[col].replace([np.inf, -np.inf], 0.0).fillna(0.0).astype("float32")

        # Compile Quality Report
        is_degraded = len(missing_fields) > 0 or len(df_raw) < self.rolling_window
        quality_status = "DEGRADED" if is_degraded else "COMPLETE"
        confidence = "HIGH_CONFIDENCE" if not is_degraded else ("MEDIUM_CONFIDENCE" if len(missing_fields) <= 1 else "LOW_CONFIDENCE")

        quality_report = {
            "status": quality_status,
            "confidence": confidence,
            "total_bars_provided": len(df_raw),
            "rolling_window_required": self.rolling_window,
            "missing_fields": list(set(missing_fields)),
            "imputed_features": list(set(imputed_features)),
            "action": "ACCEPT_FULL_TELEMETRY" if not is_degraded else "FLAG_DEGRADED_TELEMETRY"
        }
        self.latest_quality_report = quality_report

        feat_df = df[self.feature_columns]
        return (feat_df, quality_report) if return_quality else feat_df
