#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Container Entrypoint & Model Persistence Guard for SYS-04 AI Service
-------------------------------------------------------------------
Guarantees that AI models are NEVER lost when quitting or restarting Docker:
1. If the mounted volume /app/models is empty or missing artifacts,
   it automatically restores from the baked-in image backup (/app/models_backup).
2. Sets up proper directories and starts the production Uvicorn server.
"""

import os
import shutil
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("model-persistence-guard")

BACKUP_DIR = os.getenv("BACKUP_DIR", "/app/models_backup")
MODELS_DIR = os.getenv("MODELS_DIR", "/app/models")


def ensure_models_persisted():
    logger.info("Initializing Model Persistence Guard...")
    logger.info("Target Models Directory: %s", MODELS_DIR)
    logger.info("Baked Backup Directory : %s", BACKUP_DIR)

    os.makedirs(MODELS_DIR, exist_ok=True)

    if not os.path.exists(BACKUP_DIR):
        logger.warning("Backup directory %s not found. Proceeding with existing models.", BACKUP_DIR)
        return

    restored_count = 0
    for root, dirs, files in os.walk(BACKUP_DIR):
        rel_path = os.path.relpath(root, BACKUP_DIR)
        target_dir = os.path.join(MODELS_DIR, rel_path) if rel_path != "." else MODELS_DIR
        os.makedirs(target_dir, exist_ok=True)

        for file in files:
            target_file = os.path.join(target_dir, file)
            backup_file = os.path.join(root, file)

            if not os.path.exists(target_file) or os.path.getsize(target_file) == 0:
                logger.info("Restoring missing model artifact: %s", os.path.relpath(target_file, MODELS_DIR))
                shutil.copy2(backup_file, target_file)
                restored_count += 1

    if restored_count > 0:
        logger.info("Successfully restored %d missing model artifact(s) from baked backup.", restored_count)
    else:
        logger.info("All model artifacts are verified present and intact in persistent storage.")


if __name__ == "__main__":
    ensure_models_persisted()

    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    workers = int(os.getenv("WORKERS", "1"))

    logger.info("Starting SYS-04 FastAPI Model Serving Microservice on %s:%d...", host, port)
    sys.stdout.flush()

    cmd = [
        "uvicorn",
        "server:app",
        "--host", host,
        "--port", str(port),
        "--workers", str(workers),
        "--access-log"
    ]
    sys.exit(subprocess.call(cmd))
