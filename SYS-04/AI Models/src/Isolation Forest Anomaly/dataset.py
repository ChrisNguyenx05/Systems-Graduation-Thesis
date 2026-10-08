#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dataset Loader and Preprocessing for Isolation Forest Anomaly Detection
-----------------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/dataset.py
Role       : Loads parquet datasets, executes chronological train/test splitting,
             and fits RobustScaler strictly on train data to prevent leakage.
"""

import os
import logging
import joblib
import numpy as np
import pandas as pd
from typing import Tuple, Dict, Any, Optional
from sklearn.preprocessing import RobustScaler

from feature_extractor import FEATURE_COLUMNS

logger = logging.getLogger("Anomaly_Dataset")


def load_and_prepare_anomaly_data(
    parquet_path: str,
    symbol: str,
    train_ratio: float = 0.70,
    scaler_save_path: Optional[str] = None
) -> Tuple[np.ndarray, np.ndarray, pd.DataFrame, pd.DataFrame, RobustScaler, Dict[str, Any]]:
    """
    Load microstructure dataset, perform chronological split, and scale features.

    Returns:
        X_train_raw: Unscaled train features (N_train, 8)
        X_test_raw : Unscaled test features (N_test, 8)
        df_train   : Train DataFrame with all metadata
        df_test    : Test DataFrame with all metadata
        scaler     : Fitted RobustScaler
        metadata   : Summary dictionary with split timestamps and counts
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Anomaly dataset not found: {parquet_path}")

    logger.info(f"Loading anomaly dataset: {parquet_path}")
    df = pd.read_parquet(parquet_path)

    # Sort chronologically
    df.sort_values(by="open_time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Verify features exist
    missing_cols = [c for c in FEATURE_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Parquet missing required feature columns: {missing_cols}")

    total_rows = len(df)
    train_size = int(total_rows * train_ratio)

    df_train = df.iloc[:train_size].copy().reset_index(drop=True)
    df_test = df.iloc[train_size:].copy().reset_index(drop=True)

    X_train_raw = df_train[FEATURE_COLUMNS].values.astype(np.float32)
    X_test_raw = df_test[FEATURE_COLUMNS].values.astype(np.float32)

    # Fit RobustScaler STRICTLY on the train slice
    scaler = RobustScaler()
    scaler.fit(X_train_raw)

    if scaler_save_path:
        os.makedirs(os.path.dirname(scaler_save_path), exist_ok=True)
        joblib.dump(scaler, scaler_save_path)
        logger.info(f"RobustScaler saved to: {scaler_save_path}")

    metadata = {
        "symbol": symbol,
        "total_candles": total_rows,
        "train_candles": len(df_train),
        "test_candles": len(df_test),
        "train_start_utc": str(df_train["open_time_utc"].iloc[0]),
        "train_end_utc": str(df_train["close_time_utc"].iloc[-1]),
        "test_start_utc": str(df_test["open_time_utc"].iloc[0]),
        "test_end_utc": str(df_test["close_time_utc"].iloc[-1]),
        "feature_columns": FEATURE_COLUMNS
    }

    logger.info(f"Splits summary for {symbol}:")
    logger.info(f"  • Total : {total_rows} candles")
    logger.info(f"  • Train : {len(df_train)} candles ({metadata['train_start_utc']} -> {metadata['train_end_utc']})")
    logger.info(f"  • Test  : {len(df_test)} candles ({metadata['test_start_utc']} -> {metadata['test_end_utc']})")

    return X_train_raw, X_test_raw, df_train, df_test, scaler, metadata
