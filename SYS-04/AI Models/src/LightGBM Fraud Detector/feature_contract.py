"""Stable feature contract shared by LightGBM training and online inference."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


FEATURE_SCHEMA_VERSION = "fraud-ieee-v1"
TARGET_COLUMN = "isFraud"
TIME_COLUMN = "TransactionDT"

NUMERIC_FEATURES = [
    "TransactionAmt", "card1", "card2", "card3", "card5", "addr1", "addr2", "dist1",
    "C1", "C2", "C4", "C5", "C11", "C13", "C14", "D1", "D2", "D4", "D10", "D15",
    "V12", "V13", "V20", "V30", "V70", "V80", "V90", "V128", "V283", "V307",
]
CATEGORICAL_FEATURES = ["ProductCD", "card4", "card6", "P_emaildomain", "R_emaildomain"]
FEATURE_NAMES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


@dataclass
class FraudFeatureTransformer:
    """Median-impute numeric values and map categories without training/serving skew."""

    numeric_medians: dict[str, float] = field(default_factory=dict)
    category_maps: dict[str, dict[str, int]] = field(default_factory=dict)

    def fit(self, frame: pd.DataFrame) -> "FraudFeatureTransformer":
        _require_columns(frame, FEATURE_NAMES)
        self.numeric_medians = {
            name: float(pd.to_numeric(frame[name], errors="coerce").median())
            for name in NUMERIC_FEATURES
        }
        self.category_maps = {}
        for name in CATEGORICAL_FEATURES:
            values = frame[name].fillna("__MISSING__").astype(str)
            self.category_maps[name] = {value: index for index, value in enumerate(sorted(values.unique()))}
        return self

    def transform(self, frame: pd.DataFrame) -> pd.DataFrame:
        _require_columns(frame, FEATURE_NAMES)
        if not self.numeric_medians or not self.category_maps:
            raise RuntimeError("Feature transformer has not been fitted.")
        output = pd.DataFrame(index=frame.index)
        for name in NUMERIC_FEATURES:
            values = pd.to_numeric(frame[name], errors="coerce")
            output[name] = values.fillna(self.numeric_medians[name]).astype(np.float32)
        for name in CATEGORICAL_FEATURES:
            values = frame[name].fillna("__MISSING__").astype(str)
            output[name] = values.map(self.category_maps[name]).fillna(-1).astype(np.int32)
        return output[FEATURE_NAMES]


def _require_columns(frame: pd.DataFrame, expected: list[str]) -> None:
    missing = [name for name in expected if name not in frame.columns]
    if missing:
        raise ValueError(f"Feature contract violation; missing columns: {', '.join(missing)}")


def validate_serving_payload(payload: dict[str, Any]) -> pd.DataFrame:
    """Reject partial requests instead of silently replacing absent risk fields."""
    missing = [name for name in FEATURE_NAMES if name not in payload]
    if missing:
        raise ValueError(f"Serving payload is missing required fields: {', '.join(missing)}")
    return pd.DataFrame([{name: payload[name] for name in FEATURE_NAMES}])
