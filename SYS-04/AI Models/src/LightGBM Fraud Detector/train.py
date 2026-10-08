"""Train and promote an offline LightGBM fraud detector from historical labels."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import joblib
import lightgbm as lgb
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

from feature_contract import FEATURE_NAMES, FEATURE_SCHEMA_VERSION, TARGET_COLUMN, TIME_COLUMN, FraudFeatureTransformer


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA = ROOT / "Dataset" / "LightGBM Fraud Detector" / "processed" / "train_full.parquet"
MODEL_DIR = ROOT / "models" / "LightGBM Fraud Detector"
PRODUCTION_DIR = MODEL_DIR / "production"
PREPROCESSOR_DIR = MODEL_DIR / "preprocessors"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--validation-fraction", type=float, default=0.20)
    parser.add_argument("--max-rows", type=int, default=None, help="Optional deterministic sample for a quick experiment.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    columns = [TIME_COLUMN, TARGET_COLUMN, *FEATURE_NAMES]
    frame = pd.read_parquet(args.data, columns=columns)
    if args.max_rows:
        frame = frame.sample(n=min(args.max_rows, len(frame)), random_state=42)

    frame = frame.sort_values(TIME_COLUMN, kind="stable").reset_index(drop=True)
    split_at = int(len(frame) * (1 - args.validation_fraction))
    train_frame, validation_frame = frame.iloc[:split_at], frame.iloc[split_at:]
    y_train, y_validation = train_frame[TARGET_COLUMN], validation_frame[TARGET_COLUMN]
    transformer = FraudFeatureTransformer().fit(train_frame)
    x_train = transformer.transform(train_frame)
    x_validation = transformer.transform(validation_frame)

    positives = int(y_train.sum())
    negatives = len(y_train) - positives
    classifier = lgb.LGBMClassifier(
        objective="binary",
        learning_rate=0.05,
        n_estimators=1000,
        num_leaves=127,
        max_depth=-1,
        colsample_bytree=0.8,
        subsample=0.8,
        subsample_freq=1,
        min_child_samples=50,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=4,
    )
    classifier.fit(
        x_train,
        y_train,
        eval_set=[(x_validation, y_validation)],
        eval_metric="auc",
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )
    probabilities = classifier.predict_proba(x_validation)[:, 1]

    # Empirical 1,000-run latency benchmark on single-transaction serving payload
    sample_row = x_validation.iloc[[0]]
    for _ in range(100): # Warmup
        _ = classifier.predict_proba(sample_row)
    
    latencies = []
    for _ in range(1000):
        t0 = time.perf_counter()
        _ = classifier.predict_proba(sample_row)
        latencies.append((time.perf_counter() - t0) * 1000.0)
    
    import numpy as np
    lat_arr = np.array(latencies)
    latency_report = {
        "warmup_iterations": 100,
        "benchmark_iterations": 1000,
        "device": "cpu",
        "mean_latency_ms": round(float(np.mean(lat_arr)), 3),
        "p50_latency_ms": round(float(np.percentile(lat_arr, 50)), 3),
        "p95_latency_ms": round(float(np.percentile(lat_arr, 95)), 3),
        "p99_latency_ms": round(float(np.percentile(lat_arr, 99)), 3),
        "max_latency_ms": round(float(np.max(lat_arr)), 3),
        "throughput_fps": round(1000.0 / float(np.mean(lat_arr)), 1)
    }

    report = {
        "model": "LGBM-FRAUD-02",
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_count": len(FEATURE_NAMES),
        "training_rows": int(len(train_frame)),
        "validation_rows": int(len(validation_frame)),
        "training_fraud_rate": float(y_train.mean()),
        "validation_fraud_rate": float(y_validation.mean()),
        "best_iteration": int(classifier.best_iteration_ or classifier.n_estimators),
        "roc_auc": round(float(roc_auc_score(y_validation, probabilities)), 4),
        "pr_auc": round(float(average_precision_score(y_validation, probabilities)), 4),
        "latency_benchmark": latency_report,
        "training_seconds": round(time.perf_counter() - started, 3),
        "split": "chronological TransactionDT 80/20 holdout",
        "promotion": "candidate_only_until_real_transaction_labels_are_available",
    }

    PRODUCTION_DIR.mkdir(parents=True, exist_ok=True)
    PREPROCESSOR_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(classifier, PRODUCTION_DIR / "lgbm_fraud_model.joblib")
    joblib.dump(transformer, PREPROCESSOR_DIR / "fraud_feature_transformer.joblib")
    (PRODUCTION_DIR / "fraud_training_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (PREPROCESSOR_DIR / "fraud_feature_names.json").write_text(json.dumps(FEATURE_NAMES, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
