"""Convert Gold market rows into the closed 15-minute LSTM serving contract."""
from __future__ import annotations

import pandas as pd


REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


def prepare_closed_15m_bars(rows: pd.DataFrame, minimum_bars: int = 60) -> pd.DataFrame:
    """Accept 1m or 15m Gold rows; reject gaps, open candles, and short history."""
    frame = rows.copy()
    missing = [name for name in ["open_time", *REQUIRED_COLUMNS] if name not in frame.columns]
    if missing:
        raise ValueError(f"Gold contract missing columns: {', '.join(missing)}")
    if "is_closed" in frame.columns:
        frame = frame[frame["is_closed"].astype(bool)]
    if frame.empty:
        raise ValueError("Gold query returned no closed bars.")

    frame["open_time"] = pd.to_datetime(frame["open_time"], utc=True)
    frame = frame.sort_values("open_time").drop_duplicates("open_time", keep="last")
    for name in REQUIRED_COLUMNS:
        frame[name] = pd.to_numeric(frame[name], errors="raise")

    observed_minutes = frame["open_time"].diff().dropna().dt.total_seconds().div(60).min()
    if observed_minutes is not None and observed_minutes < 15:
        frame = (
            frame.set_index("open_time")
            .resample("15min", label="left", closed="left")
            .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
            .dropna()
            .reset_index()
        )
    if len(frame) < minimum_bars:
        raise ValueError(f"LSTM requires {minimum_bars} closed 15-minute bars; received {len(frame)}.")
    return frame.tail(minimum_bars).reset_index(drop=True)
