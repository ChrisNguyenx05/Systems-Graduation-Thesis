#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Training Pipeline for Isolation Forest Anomaly Detector
------------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/train.py
Usage      : python train.py --symbol BTCUSDT --contamination 0.02 --n-estimators 200
"""

import os
import sys
import json
import time
import random
import argparse
import logging
import numpy as np
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Anomaly_Trainer")

# Local imports
from model import MarketAnomalyDetector
from dataset import load_and_prepare_anomaly_data
from evaluate import evaluate_anomaly_model
from test_feature_parity import run_feature_parity_test_for_symbol

# Directories
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
AI_MODELS_DIR = os.path.dirname(os.path.dirname(SRC_DIR))
DATA_DIR = os.path.join(AI_MODELS_DIR, "Dataset", "Isolation Forest Anomaly", "data")
MODELS_DIR = os.path.join(AI_MODELS_DIR, "models", "Isolation Forest Anomaly")
PREPROCESSOR_DIR = os.path.join(MODELS_DIR, "preprocessors")
PRODUCTION_DIR = os.path.join(MODELS_DIR, "production")

os.makedirs(PREPROCESSOR_DIR, exist_ok=True)
os.makedirs(PRODUCTION_DIR, exist_ok=True)


def set_seed(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)


def benchmark_anomaly_latency(detector: MarketAnomalyDetector, warmup_iters: int = 100, benchmark_iters: int = 1000) -> dict:
    """Empirical latency benchmark on single-candle inference over 1,000 runs."""
    dummy_features = np.random.randn(1, 8).astype(np.float32)
    # Warmup
    for _ in range(warmup_iters):
        _ = detector.predict_features(dummy_features)

    # Benchmark
    latencies = []
    for _ in range(benchmark_iters):
        t0 = time.perf_counter()
        _ = detector.predict_features(dummy_features)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)

    lat_arr = np.array(latencies)
    return {
        "warmup_iterations": warmup_iters,
        "benchmark_iterations": benchmark_iters,
        "mean_latency_ms": round(float(np.mean(lat_arr)), 3),
        "p50_latency_ms": round(float(np.percentile(lat_arr, 50)), 3),
        "p90_latency_ms": round(float(np.percentile(lat_arr, 90)), 3),
        "p95_latency_ms": round(float(np.percentile(lat_arr, 95)), 3),
        "p99_latency_ms": round(float(np.percentile(lat_arr, 99)), 3),
        "max_latency_ms": round(float(np.max(lat_arr)), 3),
        "throughput_fps": round(1000.0 / float(np.mean(lat_arr)), 1)
    }


def run_training_pipeline(args):
    start_time = time.time()
    set_seed(args.seed)
    logger.info(f"Target Symbol: {args.symbol} | Contamination: {args.contamination} | Estimators: {args.n_estimators}")

    # 1. Dataset Path
    parquet_path = os.path.join(DATA_DIR, f"features_microstructure_{args.symbol}_15m_20260801_20260930.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Missing dataset for symbol {args.symbol} at: {parquet_path}")

    scaler_path = os.path.join(PREPROCESSOR_DIR, f"scaler_{args.symbol}.joblib")

    # 2. Load & Split Data
    X_train_raw, X_test_raw, df_train, df_test, scaler, split_meta = load_and_prepare_anomaly_data(
        parquet_path=parquet_path,
        symbol=args.symbol,
        train_ratio=args.train_ratio,
        scaler_save_path=scaler_path
    )

    # 3. Instantiate & Train Isolation Forest
    logger.info("Instantiating and fitting MarketAnomalyDetector...")
    detector = MarketAnomalyDetector(
        contamination=args.contamination,
        n_estimators=args.n_estimators,
        max_samples=args.max_samples,
        random_state=args.seed,
        symbol=args.symbol
    )
    detector.fit(X_train_raw, scaler=scaler)

    train_time = round(time.time() - start_time, 2)
    logger.info(f"Model fitted in {train_time}s. Decision threshold offset: {detector.threshold_offset_:.6f}")

    # 4. Out-of-Time Test Evaluation
    logger.info("Evaluating detector on Out-of-Time test split...")
    eval_results = evaluate_anomaly_model(detector, X_test_raw, df_test)

    # 5. Automated Feature-Parity Verification Test
    logger.info("Executing automated Feature-Parity verification test...")
    parity_report = run_feature_parity_test_for_symbol(args.symbol, window_size=25)
    logger.info(f"Feature-Parity Passed: {parity_report['all_passed']} (Max Diff: {parity_report['max_difference']:.8f})")

    # 6. Empirical Latency Benchmarking (1,000 iterations, 100 warmups)
    logger.info("Executing empirical 1,000-run inference latency benchmark...")
    latency_report = benchmark_anomaly_latency(detector, warmup_iters=100, benchmark_iters=1000)
    logger.info(f"⚡ Latency: Mean={latency_report['mean_latency_ms']} ms | P95={latency_report['p95_latency_ms']} ms | P99={latency_report['p99_latency_ms']} ms")

    # 7. Verify End-to-End Raw Database Candle Inference Pipeline
    logger.info("Verifying raw DB candles simulation inference...")
    sample_raw_db_candles = df_test.tail(25).copy()
    db_inference_sample = detector.predict_from_raw_candles(sample_raw_db_candles)
    logger.info(f"✅ DB Pipeline Test: Result={db_inference_sample['risk_level']} | Score={db_inference_sample['anomaly_score']} | DataQuality={db_inference_sample['data_quality']['status']}")

    # 8. Save Artifacts
    model_save_path = os.path.join(PRODUCTION_DIR, f"isolation_forest_{args.symbol}.joblib")
    detector.save(model_save_path)
    logger.info(f"💾 Production model artifact saved: {model_save_path}")

    # 9. Generate Full Report
    report = {
        "symbol": args.symbol,
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "hyperparameters": {
            "contamination": args.contamination,
            "n_estimators": args.n_estimators,
            "max_samples": args.max_samples,
            "random_state": args.seed,
            "train_ratio": args.train_ratio
        },
        "dataset_splits": split_meta,
        "calibration": {
            "threshold_offset": round(detector.threshold_offset_, 6),
            "score_min": round(detector.score_min_, 6),
            "score_max": round(detector.score_max_, 6)
        },
        "evaluation": eval_results,
        "feature_parity_verification": {
            "passed": parity_report["all_passed"],
            "max_difference": parity_report["max_difference"],
            "test_timestamp_utc": parity_report["test_timestamp_utc"]
        },
        "empirical_latency_benchmark": latency_report,
        "raw_db_candle_inference_test": db_inference_sample,
        "train_time_sec": train_time
    }

    report_path = os.path.join(PRODUCTION_DIR, f"anomaly_report_{args.symbol}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info(f"📄 Full Anomaly Report saved: {report_path}")

    # Summary Log
    logger.info("\n" + "="*60)
    logger.info(f"ANOMALY BENCHMARK SUMMARY - {args.symbol}")
    logger.info(f"  • Test Samples     : {eval_results['total_test_samples']}")
    logger.info(f"  • Outliers Flagged : {eval_results['outliers_flagged']} (Alert Rate: {eval_results['alert_rate_pct']}%)")
    logger.info(f"  • Mean Score       : {eval_results['score_distribution']['mean_score']}")
    logger.info(f"  • Risk Distribution: {eval_results['risk_level_counts']}")
    logger.info(f"  • Feature Parity   : {'PASSED ✅' if parity_report['all_passed'] else 'FAILED ❌'}")
    logger.info(f"  • P99 Latency      : {latency_report['p99_latency_ms']} ms")
    logger.info("="*60)

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Market Anomaly Detector (Isolation Forest)")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair symbol (default: BTCUSDT)")
    parser.add_argument("--contamination", type=float, default=0.02, help="Expected contamination rate (default: 0.02)")
    parser.add_argument("--n-estimators", type=int, default=200, help="Number of iTrees in forest (default: 200)")
    parser.add_argument("--max-samples", type=int, default=256, help="Subsample size per tree (default: 256)")
    parser.add_argument("--train-ratio", type=float, default=0.70, help="Chronological train ratio (default: 0.70)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")

    args = parser.parse_args()
    run_training_pipeline(args)
