#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature-Parity Automated Verification Test
------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/test_feature_parity.py
Role       : Validates that the online/production MarketFeatureExtractor reproduces
             identical feature values (parity) to historical training datasets,
             preventing train/serving feature drift.
"""

import os
import sys
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Feature_Parity_Test")

from feature_extractor import MarketFeatureExtractor, FEATURE_COLUMNS

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
AI_MODELS_DIR = os.path.dirname(os.path.dirname(SRC_DIR))
DATA_DIR = os.path.join(AI_MODELS_DIR, "Dataset", "Isolation Forest Anomaly", "data")

ALL_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "AAPLBUSDT", "AMZNBUSDT", "DELLBUSDT"]


def run_feature_parity_test_for_symbol(symbol: str, window_size: int = 25) -> dict:
    """
    Test feature parity on the last N bars of a symbol:
    1. Reads historical parquet with precomputed features.
    2. Takes the raw OHLCV & volume columns of the last N bars.
    3. Runs MarketFeatureExtractor to recompute features from scratch.
    4. Compares the newly computed latest row against the stored row in the dataset.
    """
    parquet_path = os.path.join(DATA_DIR, f"features_microstructure_{symbol}_15m_20260801_20260930.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Dataset not found: {parquet_path}")

    df_hist = pd.read_parquet(parquet_path).sort_values(by="open_time").reset_index(drop=True)
    if len(df_hist) < window_size:
        raise ValueError(f"Insufficient history in dataset ({len(df_hist)} bars, need {window_size})")

    # Select the last window_size bars
    df_window = df_hist.iloc[-window_size:].copy().reset_index(drop=True)

    # Raw input columns available from database (matching ClickHouse crypto_klines_gold)
    raw_cols = ["open", "high", "low", "close", "volume", "quote_asset_volume", "number_of_trades"]
    
    # If the stored historical dataset only kept the ratios, supply the underlying raw base/quote taker volumes
    if "taker_buy_base_asset_volume" not in df_window.columns and "taker_buy_ratio" in df_window.columns:
        df_window["taker_buy_base_asset_volume"] = df_window["taker_buy_ratio"] * df_window["volume"]
    if "taker_buy_quote_asset_volume" not in df_window.columns and "quote_net_flow_ratio" in df_window.columns:
        df_window["taker_buy_quote_asset_volume"] = df_window["quote_asset_volume"] * (df_window["quote_net_flow_ratio"] + 1.0) / 2.0

    for opt in ["taker_buy_base_volume", "taker_buy_base_asset_volume"]:
        if opt in df_window.columns and opt not in raw_cols:
            raw_cols.append(opt)
    for opt in ["taker_buy_quote_volume", "taker_buy_quote_asset_volume"]:
        if opt in df_window.columns and opt not in raw_cols:
            raw_cols.append(opt)

    df_raw_slice = df_window[raw_cols].copy()

    # Run production online feature extractor
    extractor = MarketFeatureExtractor(rolling_window=20)
    feat_computed, quality_report = extractor.extract_features(df_raw_slice, return_quality=True)

    # Compare the latest bar
    row_computed = feat_computed.iloc[-1]
    row_historical = df_window.iloc[-1]

    parity_results = {}
    all_passed = True
    max_diff = 0.0

    for feat in FEATURE_COLUMNS:
        val_computed = float(row_computed[feat])
        val_historical = float(row_historical[feat])
        diff = abs(val_computed - val_historical)
        max_diff = max(max_diff, diff)
        passed = diff <= 1e-4

        parity_results[feat] = {
            "computed": round(val_computed, 5),
            "historical": round(val_historical, 5),
            "abs_difference": round(diff, 6),
            "passed": passed
        }
        if not passed:
            all_passed = False

    return {
        "symbol": symbol,
        "test_timestamp_utc": str(row_historical.get("close_time_utc", "")),
        "all_passed": all_passed,
        "max_difference": round(max_diff, 6),
        "data_quality_report": quality_report,
        "features": parity_results
    }


def run_all_feature_parity_tests() -> dict:
    logger.info("🧪 RUNNING SYSTEM-WIDE FEATURE-PARITY TESTS ACROSS ALL 6 SYMBOLS...")
    overall_passed = True
    summary = {}

    for sym in ALL_SYMBOLS:
        res = run_feature_parity_test_for_symbol(sym)
        summary[sym] = res
        status = "PASSED ✅" if res["all_passed"] else "FAILED ❌"
        logger.info(f"  • {sym:<10} : {status} (Max Diff: {res['max_difference']:.6f})")
        if not res["all_passed"]:
            overall_passed = False

    logger.info(f"Overall Feature Parity Status: {'ALL PASSED ✅' if overall_passed else 'SOME FAILED ❌'}")
    return {
        "overall_passed": overall_passed,
        "symbols_tested": len(ALL_SYMBOLS),
        "details": summary
    }


if __name__ == "__main__":
    results = run_all_feature_parity_tests()
    if not results["overall_passed"]:
        sys.exit(1)
