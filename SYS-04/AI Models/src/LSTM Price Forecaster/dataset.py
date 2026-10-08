#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stationary Feature Pipeline & Return-Anchored Dataset for LSTM Forecaster
------------------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/LSTM Price Forecaster/dataset.py
Role       : Transforms raw OHLCV into stationary I(0) features and relative
             percentage return targets [r_15m, r_60m] anchored to P_current.
"""

import os
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler
import torch
from torch.utils.data import TensorDataset, DataLoader

STATIONARY_FEATURES = [
    "norm_open",             # (Open - Close) / Close
    "norm_high",             # (High - Close) / Close
    "norm_low",              # (Low - Close) / Close
    "log_return",            # ln(Close_t / Close_{t-1})
    "volume_surge",          # Volume / SMA20(Volume)
    "rsi_norm",              # (RSI_14 - 50) / 50 in [-1, 1]
    "volatility_parkinson"   # Parkinson extreme volatility estimator
]


def compute_stationary_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Compute scale-invariant, stationary I(0) features from raw klines.
    Eliminates distribution drift between different price regimes (e.g. $60k vs $85k BTC).
    """
    df = df_raw.copy()
    
    # 1. Price offsets normalized by close
    close = df["close"] + 1e-9
    df["norm_open"] = (df["open"] - df["close"]) / close
    df["norm_high"] = (df["high"] - df["close"]) / close
    df["norm_low"] = (df["low"] - df["close"]) / close

    # 2. Log return
    df["log_return"] = np.log(df["close"] / (df["close"].shift(1) + 1e-9)).fillna(0.0)

    # 3. Volume surge vs rolling SMA20
    vol_sma = df["volume"].rolling(window=20, min_periods=1).mean()
    df["volume_surge"] = (df["volume"] / (vol_sma + 1e-9)).fillna(1.0)

    # 4. Normalized RSI (14 periods) -> centered in [-1.0, 1.0]
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0.0)).rolling(window=14, min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=14, min_periods=1).mean()
    rs = gain / (loss + 1e-9)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    df["rsi_norm"] = ((rsi - 50.0) / 50.0).fillna(0.0)

    # 5. Parkinson extreme volatility
    hl_ratio = np.maximum(df["high"] / np.maximum(df["low"], 1e-9), 1.0)
    df["volatility_parkinson"] = np.sqrt((np.log(hl_ratio) ** 2) / (4.0 * np.log(2.0))).fillna(0.0)

    # Clean infinities and force float32
    for col in STATIONARY_FEATURES:
        df[col] = df[col].replace([np.inf, -np.inf], 0.0).fillna(0.0).astype(np.float32)

    return df[STATIONARY_FEATURES]


def create_return_anchored_windows(
    scaled_features: np.ndarray,
    prices: np.ndarray,
    seq_len: int = 60,
    horizon_1: int = 1,  # 15m ahead
    horizon_2: int = 4   # 60m ahead
) -> tuple:
    """
    Generate sliding windows predicting relative returns:
      r_15m = (P_{t+1} - P_t) / P_t
      r_60m = (P_{t+4} - P_t) / P_t
    And store P_current and actual USD prices for exact evaluation.
    """
    X_list, y_return_list, p_curr_list, y_usd_list = [], [], [], []
    max_h = max(horizon_1, horizon_2)
    n = len(scaled_features)

    for i in range(n - seq_len - max_h + 1):
        window = scaled_features[i : i + seq_len]
        curr_price = prices[i + seq_len - 1]
        
        future_p1 = prices[i + seq_len - 1 + horizon_1]
        future_p4 = prices[i + seq_len - 1 + horizon_2]
        
        # Target returns relative to P_current
        r1 = (future_p1 - curr_price) / (curr_price + 1e-9)
        r4 = (future_p4 - curr_price) / (curr_price + 1e-9)
        
        X_list.append(window)
        y_return_list.append([r1, r4])
        p_curr_list.append(curr_price)
        y_usd_list.append([future_p1, future_p4])

    X = torch.tensor(np.array(X_list), dtype=torch.float32)
    y_return = torch.tensor(np.array(y_return_list), dtype=torch.float32)
    p_curr = np.array(p_curr_list, dtype=np.float32)
    y_usd = np.array(y_usd_list, dtype=np.float32)

    return X, y_return, p_curr, y_usd


def load_and_prepare_data(
    parquet_path: str,
    symbol: str = None,
    scaler_save_path: str = None,
    seq_len: int = 60,
    batch_size: int = 64,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15
) -> dict:
    """
    End-to-End data loader: computes stationary features, splits chronologically,
    fits RobustScaler on train, and generates return-anchored sliding window loaders.
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet dataset not found at: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    if "symbol" in df.columns and symbol:
        df = df[df["symbol"] == symbol].copy()
        if df.empty:
            raise ValueError(f"Strict filtering error: Symbol '{symbol}' not found in {parquet_path}")

    time_column = "open_time_utc" if "open_time_utc" in df.columns else "open_time"
    if time_column not in df.columns:
        raise ValueError("Dataset requires open_time_utc or Gold-compatible open_time.")
    df.sort_values(by=time_column, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 1. Feature Engineering
    features_df = compute_stationary_features(df)
    features_raw = features_df.values.astype(np.float32)
    prices_raw = df["close"].values.astype(np.float32)
    total_rows = len(df)

    # 2. Chronological Splitting
    train_end = int(total_rows * train_ratio)
    val_end = int(total_rows * (train_ratio + val_ratio))

    train_feats = features_raw[:train_end]
    train_prices = prices_raw[:train_end]

    val_feats = features_raw[train_end:val_end]
    val_prices = prices_raw[train_end:val_end]

    test_feats = features_raw[val_end:]
    test_prices = prices_raw[val_end:]

    # 3. Fit RobustScaler strictly on Train features
    scaler = RobustScaler()
    scaler.fit(train_feats)

    if scaler_save_path:
        os.makedirs(os.path.dirname(scaler_save_path), exist_ok=True)
        joblib.dump(scaler, scaler_save_path)

    train_scaled = scaler.transform(train_feats)
    val_scaled = scaler.transform(val_feats)
    test_scaled = scaler.transform(test_feats)

    # 4. Generate Sliding Windows
    X_train, y_train_ret, p_train_curr, y_train_usd = create_return_anchored_windows(train_scaled, train_prices, seq_len)
    X_val, y_val_ret, p_val_curr, y_val_usd = create_return_anchored_windows(val_scaled, val_prices, seq_len)
    X_test, y_test_ret, p_test_curr, y_test_usd = create_return_anchored_windows(test_scaled, test_prices, seq_len)

    # 5. DataLoaders
    train_ds = TensorDataset(X_train, y_train_ret)
    val_ds = TensorDataset(X_val, y_val_ret)
    test_ds = TensorDataset(X_test, y_test_ret)

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)
    test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False)

    return {
        "train_loader": train_loader,
        "val_loader": val_loader,
        "test_loader": test_loader,
        "X_test": X_test,
        "p_test_curr": p_test_curr,
        "y_test_usd": y_test_usd,
        "scaler": scaler,
        "feature_names": STATIONARY_FEATURES,
        "num_features": len(STATIONARY_FEATURES),
        "splits_info": {
            "total_candles": total_rows,
            "train_candles": len(train_feats),
            "val_candles": len(val_feats),
            "test_candles": len(test_feats),
            "train_windows": len(X_train),
            "val_windows": len(X_val),
            "test_windows": len(X_test),
            "start_time": str(df[time_column].iloc[0]),
            "end_time": str(df[time_column].iloc[-1]),
            "val_start_time": str(df[time_column].iloc[train_end]),
            "test_start_time": str(df[time_column].iloc[val_end])
        }
    }
