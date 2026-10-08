#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Market Anomaly Detector using Isolation Forest
----------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/model.py
Role       : Implements the Isolation Forest model wrapper with score calibration,
             severity categorizations, and end-to-end inference from raw candles.
"""

import os
import joblib
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple, Union
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler

from feature_extractor import MarketFeatureExtractor, FEATURE_COLUMNS


class MarketAnomalyDetector:
    """
    Production-grade Wrapper around Scikit-Learn IsolationForest.
    Provides calibrated risk scoring [0.0, 1.0] and raw-candle inference.
    """

    def __init__(
        self,
        contamination: float = 0.02,
        n_estimators: int = 200,
        max_samples: int = 256,
        random_state: int = 42,
        symbol: str = "GENERIC"
    ):
        self.symbol = symbol
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.random_state = random_state

        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            max_samples=max_samples,
            random_state=random_state,
            n_jobs=-1
        )
        self.scaler: Optional[RobustScaler] = None
        self.feature_extractor = MarketFeatureExtractor(rolling_window=20)
        
        # Score calibration parameters (fitted during training)
        self.score_min_: float = -0.5
        self.score_max_: float = 0.5
        self.threshold_offset_: float = 0.0  # decision_function offset from sklearn

    def fit(self, X: np.ndarray, scaler: Optional[RobustScaler] = None):
        """Fit Isolation Forest on feature matrix X (already scaled or unscaled)."""
        if scaler is not None:
            self.scaler = scaler
            X_scaled = self.scaler.transform(X)
        else:
            X_scaled = X

        self.model.fit(X_scaled)
        
        # Scikit-learn offset (threshold for contamination)
        self.threshold_offset_ = float(self.model.offset_)
        
        # Compute min and max raw decision scores for calibration
        raw_scores = self.model.decision_function(X_scaled)
        self.score_min_ = float(np.percentile(raw_scores, 0.5))  # robust min
        self.score_max_ = float(np.percentile(raw_scores, 99.5)) # robust max
        return self

    def raw_decision_function(self, X_scaled: np.ndarray) -> np.ndarray:
        """Raw decision score from Isolation Forest (negative = anomaly, positive = inlier)."""
        return self.model.decision_function(X_scaled)

    def calculate_calibrated_anomaly_score(self, raw_scores: np.ndarray) -> np.ndarray:
        """
        Calibrate scikit-learn decision_function score (where 0.0 is the exact anomaly boundary):
        - When raw_score == score_max (most normal point) -> score = 0.05
        - When raw_score == 0.0 (exact contamination decision boundary) -> score = 0.65
        - When raw_score <= score_min (extreme anomaly) -> score = 1.0
        """
        calibrated = np.zeros_like(raw_scores, dtype=np.float32)
        
        # Points safer than threshold (inliers: raw_scores >= 0.0, score in [0.05, 0.65])
        inlier_mask = raw_scores >= 0.0
        if np.any(inlier_mask):
            denom = max(1e-6, self.score_max_)
            norm = 1.0 - (raw_scores[inlier_mask] / denom)
            calibrated[inlier_mask] = 0.05 + 0.60 * np.clip(norm, 0.0, 1.0)

        # Points worse than threshold (anomalies: raw_scores < 0.0, score in [0.65, 1.0])
        outlier_mask = ~inlier_mask
        if np.any(outlier_mask):
            denom = max(1e-6, abs(self.score_min_))
            norm = np.abs(raw_scores[outlier_mask]) / denom
            calibrated[outlier_mask] = 0.65 + 0.35 * np.clip(norm, 0.0, 1.0)

        return np.clip(calibrated, 0.0, 1.0)

    @staticmethod
    def get_risk_level(score: float) -> str:
        """Categorize calibrated score into business risk levels."""
        if score < 0.45:
            return "NORMAL"
        elif score < 0.65:
            return "ELEVATED"
        elif score < 0.85:
            return "ANOMALY_WARNING"
        else:
            return "CRITICAL_ANOMALY"

    def predict_features(self, X_raw_features: np.ndarray) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """
        Predict from 8-dimensional feature matrix.
        Returns:
            is_anomaly: boolean array (True if anomaly, False if normal)
            scores: float array of calibrated scores [0.0, 1.0]
            risk_levels: list of risk level strings
        """
        if self.scaler is not None:
            X_scaled = self.scaler.transform(X_raw_features)
        else:
            X_scaled = X_raw_features

        raw_scores = self.model.decision_function(X_scaled)
        # scikit-learn: -1 for outlier, 1 for inlier
        preds = self.model.predict(X_scaled)
        is_anomaly = preds == -1

        scores = self.calculate_calibrated_anomaly_score(raw_scores)
        risk_levels = [self.get_risk_level(float(s)) for s in scores]

        return is_anomaly, scores, risk_levels

    def predict_from_raw_candles(self, df_recent_candles: pd.DataFrame) -> Dict[str, Any]:
        """
        Complete Production Pipeline from raw database candles to real-time risk signal.
        Expects at least 20 raw candles from ClickHouse/PostgreSQL for accurate rolling features.
        Evaluates the latest candle.
        """
        if len(df_recent_candles) < 5:
            raise ValueError(f"Need at least 5 candles (ideally 20+) to extract rolling features, got {len(df_recent_candles)}")

        # Step 1: Feature Extraction with Quality Telemetry
        feat_df, quality_report = self.feature_extractor.extract_features(df_recent_candles, return_quality=True)
        latest_features = feat_df.iloc[[-1]].values

        # Step 2: Prediction & Calibration
        is_anomaly, scores, risk_levels = self.predict_features(latest_features)

        latest_candle = df_recent_candles.iloc[-1]
        
        # In production, if data is severely degraded, downgrade signal confidence
        assigned_risk = risk_levels[0]
        if quality_report["status"] == "DEGRADED":
            # If critical fields like trades or taker volume are missing, do not emit deceptive NORMAL blindly
            assigned_risk = f"{assigned_risk} (DATA_DEGRADED)"
        
        raw_close_time = latest_candle.get("close_time", 0)
        try:
            timestamp_val = int(pd.to_datetime(raw_close_time).timestamp() * 1000)
        except Exception:
            try:
                timestamp_val = int(raw_close_time)
            except Exception:
                timestamp_val = 0

        return {
            "symbol": self.symbol,
            "timestamp": timestamp_val,
            "close_price": float(latest_candle.get("close", 0.0)),
            "is_anomaly": bool(is_anomaly[0]),
            "anomaly_score": round(float(scores[0]), 4),
            "risk_level": assigned_risk,
            "data_quality": quality_report,
            "microstructure_features": {
                col: round(float(feat_df.iloc[-1][col]), 4)
                for col in FEATURE_COLUMNS
            }
        }

    def save(self, model_path: str):
        """Save detector state with metadata."""
        artifact = {
            "symbol": self.symbol,
            "contamination": self.contamination,
            "n_estimators": self.n_estimators,
            "max_samples": self.max_samples,
            "random_state": self.random_state,
            "score_min_": self.score_min_,
            "score_max_": self.score_max_,
            "threshold_offset_": self.threshold_offset_,
            "model": self.model,
            "scaler": self.scaler,
            "feature_columns": FEATURE_COLUMNS
        }
        joblib.dump(artifact, model_path)

    @classmethod
    def load(cls, model_path: str) -> "MarketAnomalyDetector":
        """Load detector from saved artifact."""
        artifact = joblib.load(model_path)
        detector = cls(
            contamination=artifact["contamination"],
            n_estimators=artifact["n_estimators"],
            max_samples=artifact["max_samples"],
            random_state=artifact["random_state"],
            symbol=artifact["symbol"]
        )
        detector.model = artifact["model"]
        detector.scaler = artifact.get("scaler")
        detector.score_min_ = artifact.get("score_min_", -0.5)
        detector.score_max_ = artifact.get("score_max_", 0.5)
        detector.threshold_offset_ = artifact.get("threshold_offset_", 0.0)
        return detector
