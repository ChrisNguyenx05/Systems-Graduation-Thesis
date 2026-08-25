"""Main entry point for SYS-06 Operational Alerting & Notification Service."""

import asyncio
import io
import json
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Ensure UTF-8 output on Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("SYS-06.Main")

from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload


async def run_sample_alerts():
    """Execute sample alert demonstrations across all 5 source systems."""
    logger.info("Starting SYS-06 Alert Dispatcher Engine Demo...")
    dispatcher = AlertDispatcherEngine()

    examples_dir = Path(__file__).parent.parent.parent / "phase-01-alert-contracts" / "examples"
    
    if not examples_dir.exists():
        logger.error(f"Examples directory not found at {examples_dir}")
        return

    example_files = list(examples_dir.glob("*.json"))
    logger.info(f"Found {len(example_files)} sample alert payloads in {examples_dir.name}")

    for file_path in example_files:
        logger.info(f"--- Loading sample: {file_path.name} ---")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            alert = AlertPayload(**data)
            result = await dispatcher.dispatch(alert)
            logger.info(f"Result for {alert.event}: Success={result.success}, Suppressed={result.suppressed}, RateLimited={result.rate_limited}")
        except Exception as ex:
            logger.error(f"Error processing {file_path.name}: {ex}", exc_info=True)


def main():
    """CLI execution wrapper."""
    try:
        asyncio.run(run_sample_alerts())
    except KeyboardInterrupt:
        logger.info("Service stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
