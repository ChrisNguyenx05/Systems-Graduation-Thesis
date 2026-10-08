#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evaluation and Profiling for Market Anomaly Detection
----------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/evaluate.py
Role       : Evaluates score distributions, contrastive feature profiles
             (anomalous vs normal bars), and extracts extreme market anomalies.
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple
from model import MarketAnomalyDetector
from feature_extractor import FEATURE_COLUMNS


def evaluate_anomaly_model(
    detector: MarketAnomalyDetector,
    X_test_raw: np.ndarray,
    df_test: pd.DataFrame
) -> Dict[str, Any]:
    """
    Perform deep statistical profiling of the model on the Out-of-Time test set.
    """
    is_anomaly, scores, risk_levels = detector.predict_features(X_test_raw)

    total_test_samples = len(df_test)
    anomaly_count = int(np.sum(is_anomaly))
    anomaly_rate_pct = round((anomaly_count / max(1, total_test_samples)) * 100.0, 3)

    # 1. Score Distribution
    score_stats = {
        "mean_score": round(float(np.mean(scores)), 4),
        "std_score": round(float(np.std(scores)), 4),
        "median_score": round(float(np.median(scores)), 4),
        "p75_score": round(float(np.percentile(scores, 75)), 4),
        "p95_score": round(float(np.percentile(scores, 95)), 4),
        "p99_score": round(float(np.percentile(scores, 99)), 4),
        "max_score": round(float(np.max(scores)), 4),
    }

    # 2. Risk Level Counts
    unique_levels, counts = np.unique(risk_levels, return_counts=True)
    risk_level_distribution = {level: int(c) for level, c in zip(unique_levels, counts)}

    # 3. Contrastive Feature Profiling (Normal vs Anomaly)
    df_eval = df_test.copy()
    df_eval["is_anomaly"] = is_anomaly
    df_eval["anomaly_score"] = scores
    df_eval["risk_level"] = risk_levels

    normal_mask = ~is_anomaly
    anomaly_mask = is_anomaly

    contrastive_profile = {}
    for feat in FEATURE_COLUMNS:
        norm_mean = float(df_eval.loc[normal_mask, feat].mean()) if np.any(normal_mask) else 0.0
        anom_mean = float(df_eval.loc[anomaly_mask, feat].mean()) if np.any(anomaly_mask) else 0.0
        ratio = round(anom_mean / (norm_mean + 1e-9), 2)
        contrastive_profile[feat] = {
            "normal_mean": round(norm_mean, 4),
            "anomaly_mean": round(anom_mean, 4),
            "surge_multiplier": ratio
        }

    # 4. Top 5 Most Anomalous Events in Test Set
    df_sorted = df_eval.sort_values(by="anomaly_score", ascending=False)
    top_anomalies = []
    for idx, row in df_sorted.head(5).iterrows():
        top_anomalies.append({
            "timestamp_utc": str(row.get("close_time_utc", "")),
            "close_price": round(float(row.get("close", 0.0)), 2),
            "anomaly_score": round(float(row.get("anomaly_score", 0.0)), 4),
            "risk_level": str(row.get("risk_level", "")),
            "bar_range_bps": round(float(row.get("bar_range_bps", 0.0)), 2),
            "volume_surge_ratio": round(float(row.get("volume_surge_ratio", 0.0)), 2),
            "trades_surge_ratio": round(float(row.get("trades_surge_ratio", 0.0)), 2),
            "volatility_parkinson": round(float(row.get("volatility_parkinson", 0.0)), 5)
        })

    return {
        "total_test_samples": total_test_samples,
        "outliers_flagged": anomaly_count,
        "alert_rate_pct": anomaly_rate_pct,
        "target_contamination_pct": round(detector.contamination * 100.0, 2),
        "methodology_note": "Alert Rate is governed by the unsupervised contamination prior (nu=0.02) establishing a calibrated threshold for outlier flagging, NOT a supervised detection rate of verified fraudulent transactions.",
        "score_distribution": score_stats,
        "risk_level_counts": risk_level_distribution,
        "contrastive_feature_profile": contrastive_profile,
        "top_detected_anomalies": top_anomalies,
        # Backwards compatibility aliases
        "anomalies_detected": anomaly_count,
        "anomaly_rate_pct": anomaly_rate_pct
    }
