#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch Training Orchestrator for Market Anomaly Detectors
-------------------------------------------------------
Sub-system : SYS-04 MLOps & Model Governance Center
Location   : AI Models/src/Isolation Forest Anomaly/train_all.py
Usage      : python train_all.py [--contamination 0.02] [--n-estimators 200]
"""

import os
import sys
import json
import time
import logging
import argparse
from typing import List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Anomaly_Batch_Trainer")

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
MODELS_BASE_DIR = os.path.join(AI_MODELS_DIR, "models", "Isolation Forest Anomaly")
PRODUCTION_DIR = os.path.join(MODELS_BASE_DIR, "production")


def train_all_symbols(symbols: List[str], contamination: float = 0.02, n_estimators: int = 200):
    start_total_time = time.time()
    logger.info(f"🚀 Starting Anomaly Batch Training for {len(symbols)} symbols: {symbols}")

    summary_results = []

    for idx, sym in enumerate(symbols, 1):
        logger.info(f"\n{'='*70}\n[{idx}/{len(symbols)}] INITIATING ANOMALY TRAINING FOR: {sym}\n{'='*70}")
        args = argparse.Namespace(
            symbol=sym,
            contamination=contamination,
            n_estimators=n_estimators,
            max_samples=256,
            train_ratio=0.70,
            seed=42
        )
        try:
            report = run_training_pipeline(args)
            summary_results.append(report)
        except Exception as e:
            logger.error(f"❌ Failed anomaly training for {sym}: {e}", exc_info=True)

    elapsed = round(time.time() - start_total_time, 2)
    logger.info(f"\n{'='*70}\nBATCH ANOMALY TRAINING COMPLETED IN {elapsed}s\n{'='*70}")

    consolidated_path = os.path.join(PRODUCTION_DIR, "benchmark_summary_all_symbols.json")
    summary_data = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_symbols": len(symbols),
        "total_batch_time_sec": elapsed,
        "models": summary_results
    }
    with open(consolidated_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)

    logger.info(f"📊 Consolidated Anomaly Benchmark Summary saved to: {consolidated_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Train Isolation Forest for all symbols")
    parser.add_argument("--symbols", nargs="+", default=ALL_SYMBOLS, help="List of symbols to train")
    parser.add_argument("--contamination", type=float, default=0.02, help="Contamination rate (default: 0.02)")
    parser.add_argument("--n-estimators", type=int, default=200, help="Number of trees per forest (default: 200)")

    parsed = parser.parse_args()
    train_all_symbols(parsed.symbols, contamination=parsed.contamination, n_estimators=parsed.n_estimators)
