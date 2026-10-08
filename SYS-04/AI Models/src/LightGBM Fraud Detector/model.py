"""Prediction and action policy for the trained LightGBM fraud detector."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib

from feature_contract import FraudFeatureTransformer, validate_serving_payload


def action_for_probability(probability: float) -> str:
    if probability >= 0.75:
        return "BLOCK_TRANSACTION"
    if probability >= 0.30:
        return "REVIEW"
    return "APPROVE"


class FraudPredictor:
    def __init__(self, model_path: Path, transformer_path: Path) -> None:
        self.model = joblib.load(model_path)
        self.transformer: FraudFeatureTransformer = joblib.load(transformer_path)

    def predict(self, payload: dict[str, Any]) -> dict[str, Any]:
        frame = validate_serving_payload(payload)
        probability = float(self.model.predict_proba(self.transformer.transform(frame))[0, 1])
        return {
            "fraud_probability": probability,
            "action": action_for_probability(probability),
            "feature_schema_version": "fraud-ieee-v1",
        }
