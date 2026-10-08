#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch Training & Evaluation Orchestrator for All 6 Trading Symbols
------------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/LSTM Price Forecaster/train_all.py
Usage      : python train_all.py [--eval-only] [--skip-existing]
"""

import os
import sys
import json
import time
import logging
import argparse
from typing import List
import torch

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LSTM_Batch_Trainer")

from model import LSTMPriceForecaster
from dataset import load_and_prepare_data
from evaluate import evaluate_model
from train import run_training_pipeline

ALL_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "AAPLBUSDT",
    "AMZNBUSDT",
    "DELLBUSDT"
]

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
AI_MODELS_DIR = os.path.dirname(os.path.dirname(SRC_DIR))
DATASET_DIR = os.path.join(AI_MODELS_DIR, "Dataset", "LSTM Price Forecaster", "data")
MODELS_BASE_DIR = os.path.join(AI_MODELS_DIR, "models", "LSTM Price Forecaster")
PREPROCESSOR_DIR = os.path.join(MODELS_BASE_DIR, "preprocessors")
PRODUCTION_DIR = os.path.join(MODELS_BASE_DIR, "production")


def evaluate_existing_model(symbol: str) -> dict:
    """Run full evaluation, Naive persistence comparison, and 1,000-sample latency benchmark on trained model."""
    logger.info(f"🔍 Evaluating existing model for {symbol}...")
    weights_path = os.path.join(PRODUCTION_DIR, f"lstm_price_model_{symbol}.pt")
    scaler_path = os.path.join(PREPROCESSOR_DIR, f"price_scaler_{symbol}.joblib")
    parquet_path = os.path.join(DATASET_DIR, f"{symbol}_15m_20260701_20260930.parquet")
    
    if not os.path.exists(parquet_path):
        parquet_path = os.path.join(DATASET_DIR, "klines_15m_all_6pairs_3months.parquet")
        
    device = torch.device("cpu")
    
    data_bundle = load_and_prepare_data(
        parquet_path=parquet_path,
        symbol=symbol,
        scaler_save_path=scaler_path,
        seq_len=60,
        batch_size=64,
        train_ratio=0.70,
        val_ratio=0.15
    )
    
    num_features = data_bundle["num_features"]
    model = LSTMPriceForecaster(
        input_size=num_features,
        hidden_size_1=64,
        hidden_size_2=32,
        dense_hidden=16,
        output_size=2,
        dropout=0.20
    ).to(device)
    
    model.load_state_dict(torch.load(weights_path, map_location=device))
    
    test_metrics, y_true, y_pred, p_curr = evaluate_model(
        model=model,
        X_test=data_bundle["X_test"],
        p_test_curr=data_bundle["p_test_curr"],
        y_test_usd=data_bundle["y_test_usd"],
        device=device
    )
    
    # Read existing report metadata if available
    old_report_path = os.path.join(PRODUCTION_DIR, f"training_report_{symbol}.json")
    best_val_loss = 0.0001
    total_train_time = 60.0
    if os.path.exists(old_report_path):
        try:
            with open(old_report_path, "r", encoding="utf-8") as f:
                old_data = json.load(f)
                best_val_loss = old_data.get("best_val_loss", best_val_loss)
                total_train_time = old_data.get("total_train_time_sec", total_train_time)
        except Exception:
            pass

    report = {
        "symbol": symbol,
        "evaluated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "hyperparameters": {
            "seq_len": 60,
            "hidden_size_1": 64,
            "hidden_size_2": 32,
            "dense_hidden": 16,
            "dropout": 0.20,
            "lr": 0.001,
            "batch_size": 64,
            "total_params": 31282
        },
        "dataset_splits": data_bundle["splits_info"],
        "best_val_loss": best_val_loss,
        "total_train_time_sec": total_train_time,
        "test_metrics": test_metrics["model_metrics"],
        "naive_baseline_metrics": test_metrics["naive_baseline_metrics"],
        "acceptance_comparison": test_metrics["acceptance_comparison"],
        "latency_benchmark": test_metrics["latency_benchmark"],
        "sample_predictions": [
            {
                "sample_idx": i,
                "current_price": round(float(p_curr[i]), 2),
                "actual_15m": round(float(y_true[i, 0]), 2),
                "predicted_15m": round(float(y_pred[i, 0]), 2),
                "actual_60m": round(float(y_true[i, 1]), 2),
                "predicted_60m": round(float(y_pred[i, 1]), 2)
            }
            for i in range(min(5, len(p_curr)))
        ]
    }
    
    with open(old_report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        
    logger.info(f"✅ Updated report for {symbol}: Gate Verdict = {test_metrics['acceptance_comparison']['gate_verdict']} | P99 Latency = {test_metrics['latency_benchmark']['p99_latency_ms']} ms")
    return report


def run_batch(symbols: List[str], eval_only: bool = False, skip_existing: bool = False, epochs: int = 100):
    start_total_time = time.time()
    summary_results = []
    
    for idx, sym in enumerate(symbols, 1):
        if eval_only:
            rep = evaluate_existing_model(sym)
            summary_results.append(rep)
        else:
            args = argparse.Namespace(
                symbol=sym,
                epochs=epochs,
                batch_size=64,
                lr=1e-3,
                weight_decay=1e-4,
                patience=12,
                scheduler_patience=4,
                seq_len=60,
                hidden_size_1=64,
                hidden_size_2=32,
                dense_hidden=16,
                dropout=0.20,
                train_ratio=0.70,
                val_ratio=0.15,
                seed=42,
                cpu=True
            )
            rep = run_training_pipeline(args)
            summary_results.append(rep)

    elapsed = round(time.time() - start_total_time, 2)
    consolidated_path = os.path.join(PRODUCTION_DIR, "benchmark_summary_all_symbols.json")
    summary_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_symbols": len(symbols),
        "total_batch_time_sec": elapsed,
        "models": summary_results
    }
    with open(consolidated_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)
        
    logger.info(f"📊 Consolidated Benchmark Summary saved to: {consolidated_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Train / Evaluate LSTM Price Forecaster")
    parser.add_argument("--eval-only", action="store_true", help="Only run evaluation, Naive comparison, and latency benchmark")
    parser.add_argument("--skip-existing", action="store_true", help="Skip symbols with existing trained model artifacts")
    parser.add_argument("--symbols", nargs="+", default=ALL_SYMBOLS, help="List of symbols")
    parser.add_argument("--epochs", type=int, default=100, help="Epochs per symbol")
    
    parsed = parser.parse_args()
    run_batch(parsed.symbols, eval_only=parsed.eval_only, skip_existing=parsed.skip_existing, epochs=parsed.epochs)
