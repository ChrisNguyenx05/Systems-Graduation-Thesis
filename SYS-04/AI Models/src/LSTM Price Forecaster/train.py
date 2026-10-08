#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Training Pipeline for PyTorch LSTM Price Forecaster
--------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/LSTM Price Forecaster/train.py
Usage      : python train.py --symbol BTCUSDT --epochs 100 --batch-size 64
"""

import os
import sys
import json
import time
import random
import argparse
import logging
import numpy as np
import torch
import torch.nn as nn

# Local module imports
from model import LSTMPriceForecaster
from dataset import load_and_prepare_data
from evaluate import evaluate_model

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LSTM_Trainer")

# Root Directories
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
AI_MODELS_DIR = os.path.dirname(os.path.dirname(SRC_DIR))
DATASET_DIR = os.path.join(AI_MODELS_DIR, "Dataset", "LSTM Price Forecaster", "data")
MODELS_BASE_DIR = os.path.join(AI_MODELS_DIR, "models", "LSTM Price Forecaster")
PREPROCESSOR_DIR = os.path.join(MODELS_BASE_DIR, "preprocessors")
PRODUCTION_DIR = os.path.join(MODELS_BASE_DIR, "production")

os.makedirs(PREPROCESSOR_DIR, exist_ok=True)
os.makedirs(PRODUCTION_DIR, exist_ok=True)


def set_seed(seed: int = 42):
    """Ensure reproducibility across runs."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def train_single_epoch(
    model: nn.Module,
    loader: torch.utils.data.DataLoader,
    criterion: nn.Module,
    optimizer: torch.optim.Optimizer,
    device: torch.device
) -> float:
    """Train model for one epoch and return average batch loss."""
    model.train()
    total_loss = 0.0
    
    for X_batch, y_batch in loader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)
        optimizer.zero_grad()
        preds = model(X_batch)
        loss = criterion(preds, y_batch)
        loss.backward()
        
        # Gradient clipping to prevent exploding gradients in recurrent networks
        nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        
        optimizer.step()
        total_loss += loss.item() * len(X_batch)
        
    return total_loss / len(loader.dataset)


def validate_single_epoch(
    model: nn.Module,
    loader: torch.utils.data.DataLoader,
    criterion: nn.Module,
    device: torch.device
) -> float:
    """Evaluate model on validation loader and return average loss."""
    model.eval()
    total_loss = 0.0
    
    with torch.no_grad():
        for X_batch, y_batch in loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            preds = model(X_batch)
            loss = criterion(preds, y_batch)
            total_loss += loss.item() * len(X_batch)
            
    return total_loss / len(loader.dataset)


def run_training_pipeline(args):
    set_seed(args.seed)
    device = torch.device("cuda" if torch.cuda.is_available() and not args.cpu else "cpu")
    logger.info(f"Using device: {device} | Target Symbol: {args.symbol} | Max Epochs: {args.epochs}")

    # 1. Dataset Loading & Preprocessing
    parquet_filename = f"{args.symbol}_15m_20260701_20260930.parquet"
    parquet_path = os.path.join(DATASET_DIR, parquet_filename)
    
    if not os.path.exists(parquet_path):
        # Fallback to combined parquet if single pair parquet not found
        combined_path = os.path.join(DATASET_DIR, "klines_15m_all_6pairs_3months.parquet")
        if os.path.exists(combined_path):
            parquet_path = combined_path
            logger.info(f"Using combined dataset: {parquet_path}")
        else:
            raise FileNotFoundError(f"Cannot find dataset for {args.symbol} at {parquet_path}")
    else:
        logger.info(f"Loading pair dataset: {parquet_path}")

    scaler_path = os.path.join(PREPROCESSOR_DIR, f"price_scaler_{args.symbol}.joblib")
    data_bundle = load_and_prepare_data(
        parquet_path=parquet_path,
        symbol=args.symbol,
        scaler_save_path=scaler_path,
        seq_len=args.seq_len,
        batch_size=args.batch_size,
        train_ratio=args.train_ratio,
        val_ratio=args.val_ratio
    )
    
    train_loader = data_bundle["train_loader"]
    val_loader = data_bundle["val_loader"]
    test_loader = data_bundle["test_loader"]
    X_test = data_bundle["X_test"]
    p_test_curr = data_bundle["p_test_curr"]
    y_test_usd = data_bundle["y_test_usd"]
    num_features = data_bundle["num_features"]
    splits = data_bundle["splits_info"]

    logger.info("Dataset Splitting Summary:")
    logger.info(f"  • Total candles   : {splits['total_candles']:,}")
    logger.info(f"  • Train set       : {splits['train_candles']:,} candles -> {splits['train_windows']:,} windows")
    logger.info(f"  • Validation set  : {splits['val_candles']:,} candles -> {splits['val_windows']:,} windows")
    logger.info(f"  • Test set (OOT)  : {splits['test_candles']:,} candles -> {splits['test_windows']:,} windows")
    logger.info(f"  • Scaler saved to : {scaler_path}")

    # 2. Model, Loss, Optimizer, Scheduler Setup
    model = LSTMPriceForecaster(
        input_size=num_features,
        hidden_size_1=args.hidden_size_1,
        hidden_size_2=args.hidden_size_2,
        dense_hidden=args.dense_hidden,
        output_size=2,
        dropout=args.dropout
    ).to(device)
    
    total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info(f"Model instantiated with {total_params:,} trainable parameters.")

    criterion = nn.HuberLoss(delta=0.005)
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=args.weight_decay)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=args.scheduler_patience, min_lr=1e-6
    )

    # 3. Training Loop with Early Stopping
    best_val_loss = float("inf")
    best_weights_path = os.path.join(PRODUCTION_DIR, f"lstm_price_model_{args.symbol}.pt")
    early_stop_counter = 0
    history = {"train_loss": [], "val_loss": [], "lr": []}
    
    logger.info(f"=== Starting Training for {args.epochs} Epochs ===")
    t_start = time.time()

    for epoch in range(1, args.epochs + 1):
        t_epoch_start = time.time()
        train_loss = train_single_epoch(model, train_loader, criterion, optimizer, device)
        val_loss = validate_single_epoch(model, val_loader, criterion, device)
        current_lr = optimizer.param_groups[0]["lr"]
        
        history["train_loss"].append(train_loss)
        history["val_loss"].append(val_loss)
        history["lr"].append(current_lr)
        
        scheduler.step(val_loss)
        epoch_sec = time.time() - t_epoch_start

        is_best = val_loss < best_val_loss
        if is_best:
            best_val_loss = val_loss
            early_stop_counter = 0
            torch.save(model.state_dict(), best_weights_path)
            flag = "⭐ [BEST MODEL SAVED]"
        else:
            early_stop_counter += 1
            flag = ""

        if epoch % 5 == 0 or is_best or epoch == 1:
            logger.info(
                f"Epoch [{epoch:03d}/{args.epochs:03d}] "
                f"Train Loss: {train_loss:.6f} | "
                f"Val Loss: {val_loss:.6f} | "
                f"LR: {current_lr:.1e} | "
                f"Time: {epoch_sec:.2f}s {flag}"
            )

        if early_stop_counter >= args.patience:
            logger.info(f"⏹️ Early stopping triggered at epoch {epoch} (no validation improvement for {args.patience} epochs).")
            break

    total_train_time = time.time() - t_start
    logger.info(f"Training completed in {total_train_time:.1f}s. Best Val Loss: {best_val_loss:.6f}")

    # 4. Independent Out-of-Time Test Evaluation
    logger.info("=== Loading Best Weights for Out-of-Time Test Set Evaluation ===")
    model.load_state_dict(torch.load(best_weights_path))
    
    test_metrics, y_true, y_pred, p_curr = evaluate_model(
        model=model,
        X_test=X_test,
        p_test_curr=p_test_curr,
        y_test_usd=y_test_usd,
        device=device
    )

    m_metrics = test_metrics["model_metrics"]
    n_metrics = test_metrics["naive_baseline_metrics"]
    acc_cmp = test_metrics["acceptance_comparison"]
    lat_bench = test_metrics["latency_benchmark"]

    logger.info("📊 TEST SET BENCHMARK RESULTS:")
    logger.info(f"  • LSTM RMSE (t+15m / t+60m)       : ${m_metrics['rmse_15m_usd']:,} / ${m_metrics['rmse_60m_usd']:,} USD")
    logger.info(f"  • Naïve RMSE (t+15m / t+60m)      : ${n_metrics['rmse_15m_usd']:,} / ${n_metrics['rmse_60m_usd']:,} USD")
    logger.info(f"  • LSTM MAE  (t+15m / t+60m)       : ${m_metrics['mae_15m_usd']:,} / ${m_metrics['mae_60m_usd']:,} USD")
    logger.info(f"  • Naïve MAE  (t+15m / t+60m)      : ${n_metrics['mae_15m_usd']:,} / ${n_metrics['mae_60m_usd']:,} USD")
    logger.info(f"  • LSTM MAPE (t+15m / t+60m)       : {m_metrics['mape_15m_pct']}% / {m_metrics['mape_60m_pct']}%")
    logger.info(f"  • Naïve MAPE (t+15m / t+60m)      : {n_metrics['mape_15m_pct']}% / {n_metrics['mape_60m_pct']}%")
    logger.info(f"  • Directional Accuracy (15m / 60m): {m_metrics['directional_accuracy_15m_pct']}% / {m_metrics['directional_accuracy_60m_pct']}% (vs 50% coin-toss, Naïve=0%)")
    logger.info(f"  • MAE Improvement vs Naïve (60m)  : {acc_cmp['mae_60m_improvement_pct']:+}% (Passed: {acc_cmp['passed_directional_gate']})")
    logger.info("⚡ EMPIRICAL INFERENCE LATENCY BENCHMARK (1,000 runs, 100 warmups):")
    logger.info(f"  • Mean: {lat_bench['mean_latency_ms']} ms | P50: {lat_bench['p50_latency_ms']} ms | P95: {lat_bench['p95_latency_ms']} ms | P99: {lat_bench['p99_latency_ms']} ms | Max: {lat_bench['max_latency_ms']} ms")
    logger.info(f"  • Throughput: {lat_bench['throughput_fps']} inferences/sec on {lat_bench['device']}")

    # 5. Export TorchScript JIT Model (for sub-35ms Serving)
    torchscript_path = os.path.join(PRODUCTION_DIR, f"lstm_price_model_{args.symbol}.torchscript")
    model.eval()
    dummy_input = torch.randn(1, args.seq_len, num_features, device=device)
    traced_model = torch.jit.trace(model, dummy_input)
    traced_model.save(torchscript_path)
    logger.info(f"🚀 TorchScript JIT artifact exported: {torchscript_path} ({os.path.getsize(torchscript_path)/1024:.1f} KB)")

    # 6. Save Full Execution Report
    report = {
        "symbol": args.symbol,
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "hyperparameters": {
            "seq_len": args.seq_len,
            "hidden_size_1": args.hidden_size_1,
            "hidden_size_2": args.hidden_size_2,
            "dense_hidden": args.dense_hidden,
            "dropout": args.dropout,
            "lr": args.lr,
            "weight_decay": args.weight_decay,
            "batch_size": args.batch_size,
            "max_epochs": args.epochs,
            "total_params": total_params
        },
        "dataset_splits": splits,
        "best_val_loss": round(best_val_loss, 6),
        "total_train_time_sec": round(total_train_time, 2),
        "test_metrics": test_metrics,
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
    
    report_path = os.path.join(PRODUCTION_DIR, f"training_report_{args.symbol}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info(f"📄 Full training report saved: {report_path}")
    logger.info("🎉 LSTM Price Forecaster pipeline completed successfully!")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train LSTM Price Forecaster")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair symbol (default: BTCUSDT)")
    parser.add_argument("--epochs", type=int, default=100, help="Maximum training epochs (default: 100)")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size (default: 64)")
    parser.add_argument("--lr", type=float, default=1e-3, help="Initial learning rate (default: 0.001)")
    parser.add_argument("--weight-decay", type=float, default=1e-4, help="Weight decay for AdamW (default: 0.0001)")
    parser.add_argument("--patience", type=int, default=12, help="Early stopping patience (default: 12)")
    parser.add_argument("--scheduler-patience", type=int, default=4, help="Scheduler patience (default: 4)")
    parser.add_argument("--seq-len", type=int, default=60, help="Sequence length in bars (default: 60)")
    parser.add_argument("--hidden-size-1", type=int, default=64, help="First LSTM hidden size (default: 64)")
    parser.add_argument("--hidden-size-2", type=int, default=32, help="Second LSTM hidden size (default: 32)")
    parser.add_argument("--dense-hidden", type=int, default=16, help="Dense layer hidden size (default: 16)")
    parser.add_argument("--dropout", type=float, default=0.20, help="Dropout probability (default: 0.20)")
    parser.add_argument("--train-ratio", type=float, default=0.70, help="Train ratio (default: 0.70)")
    parser.add_argument("--val-ratio", type=float, default=0.15, help="Val ratio (default: 0.15)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--cpu", action="store_true", help="Force CPU training")
    
    args = parser.parse_args()
    run_training_pipeline(args)
