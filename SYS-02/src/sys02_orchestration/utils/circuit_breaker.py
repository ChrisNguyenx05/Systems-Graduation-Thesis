"""
Circuit Breaker & Schedule Watcher Utility for Airflow Orchestration (Airflow 3.x Compatible).
Queries Airflow REST API to track consecutive run failures without direct SQLAlchemy ORM access.
"""

import os
import logging
from typing import Dict, Any, Optional, Callable
import requests
from requests.auth import HTTPBasicAuth

LOGGER = logging.getLogger(__name__)

def _get_auth_headers(base_url: str, username: str, password: str) -> Dict[str, str]:
    try:
        resp = requests.post(
            f"{base_url}/auth/token",
            json={"username": username, "password": password},
            timeout=5
        )
        if resp.status_code in [200, 201]:
            token = resp.json().get("access_token")
            if token:
                return {"Authorization": f"Bearer {token}"}
    except Exception as exc:
        LOGGER.warning("Failed to obtain JWT auth token: %s", exc)
    return {}

def check_and_trigger_circuit_breaker(
    dag_id: str,
    max_consecutive_failures: int = 3,
    alert_callback: Optional[Callable[[Dict[str, Any]], None]] = None
) -> Dict[str, Any]:
    """
    Checks recent DAG run outcomes via Airflow 3 REST API v2.
    If consecutive failure count reaches threshold, automatically pauses the DAG schedule.
    """
    server_url = os.getenv("AIRFLOW_SERVER_URL", "http://airflow_webserver:8080")
    username = os.getenv("AIRFLOW_ADMIN_USERNAME", "nhfinanceadmin")
    password = os.getenv("AIRFLOW_ADMIN_PASSWORD", "nhathuy123")
    headers = _get_auth_headers(server_url, username, password)

    consecutive_failures = 0
    circuit_breaker_triggered = False

    try:
        url = f"{server_url}/api/v2/dags/{dag_id}/dagRuns"
        resp = requests.get(
            url,
            params={"order_by": "-start_date", "limit": max_consecutive_failures + 1},
            headers=headers,
            timeout=5
        )
        if resp.status_code == 200:
            runs = resp.json().get("dag_runs", [])
            for r in runs:
                state = r.get("state")
                if state == "failed":
                    consecutive_failures += 1
                elif state == "success":
                    break
        else:
            resp_fallback = requests.get(url, params={"limit": max_consecutive_failures + 1}, headers=headers, timeout=5)
            if resp_fallback.status_code == 200:
                runs = resp_fallback.json().get("dag_runs", [])
                for r in reversed(runs):
                    state = r.get("state")
                    if state == "failed":
                        consecutive_failures += 1
                    elif state == "success":
                        break
    except Exception as exc:
        LOGGER.warning("Circuit breaker could not query Airflow REST API: %s", exc)

    if consecutive_failures >= max_consecutive_failures:
        LOGGER.warning(
            "Circuit breaker threshold exceeded for DAG %s (%d consecutive failures). Pausing schedule.",
            dag_id,
            consecutive_failures
        )
        try:
            patch_url = f"{server_url}/api/v2/dags/{dag_id}"
            patch_resp = requests.patch(patch_url, json={"is_paused": True}, headers=headers, timeout=5)
            if patch_resp.status_code in [200, 204]:
                circuit_breaker_triggered = True
                LOGGER.warning("DAG %s successfully paused by Circuit Breaker.", dag_id)
                if alert_callback:
                    alert_callback({
                        "alert_type": "CIRCUIT_BREAKER_TRIPPED",
                        "level": "CRITICAL",
                        "source_system": "SYS-02",
                        "event": "CIRCUIT_BREAKER_ACTIVE",
                        "message": f"Circuit breaker tripped: DAG {dag_id} paused after {consecutive_failures} consecutive failures.",
                        "channels": ["telegram"]
                    })
        except Exception as exc:
            LOGGER.warning("Circuit breaker failed to pause DAG: %s", exc)

    return {
        "consecutive_failures": consecutive_failures,
        "threshold": max_consecutive_failures,
        "circuit_breaker_triggered": circuit_breaker_triggered
    }
