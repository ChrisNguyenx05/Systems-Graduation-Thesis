"""Reusable Python Alert Client for SYS-01 through SYS-05 to interact with SYS-06."""

import asyncio
import logging
from typing import Any, Dict, List, Optional
import httpx
from pydantic import BaseModel

logger = logging.getLogger("SYS06Client")


class Sys06AlertClient:
    """
    Lightweight client helper to construct and dispatch standardized alerts into SYS-06.
    Can operate via HTTP endpoint or direct local engine embedding.
    """

    def __init__(
        self,
        source_system: str,
        dispatcher_url: Optional[str] = "http://localhost:8000/api/v1/alerts",
        timeout_seconds: float = 3.0,
    ):
        self.source_system = source_system
        self.dispatcher_url = dispatcher_url
        self.timeout_seconds = timeout_seconds

    async def emit_alert(
        self,
        alert_type: str,
        level: str,
        event: str,
        deduplicate_key: str,
        message: str,
        receiver_roles: Optional[List[str]] = None,
        channels: Optional[List[str]] = None,
        action_url: Optional[str] = None,
        suggested_action: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Send an alert payload to SYS-06."""
        payload = {
            "alert_type": alert_type,
            "level": level,
            "source_system": self.source_system,
            "event": event,
            "deduplicate_key": deduplicate_key,
            "message": message,
            "receiver_roles": receiver_roles or ["devops_sre"],
            "channels": channels or ["telegram"],
            "action_url": action_url,
            "suggested_action": suggested_action,
            "metadata": metadata or {},
        }

        logger.info(f"[{self.source_system}] Emitting alert '{event}' (Level: {level})")

        # In local/dry-run mode without active HTTP server, we can fallback to direct dispatcher invocation
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                resp = await client.post(self.dispatcher_url, json=payload)
                if resp.status_code in (200, 202):
                    return resp.json()
                else:
                    return {"success": False, "status_code": resp.status_code, "error": resp.text}
        except Exception as ex:
            logger.warning(f"HTTP dispatch to {self.dispatcher_url} failed ({ex}).")
            return {"success": False, "error": str(ex), "payload": payload}

    def emit_sync(self, **kwargs) -> Dict[str, Any]:
        """Synchronous wrapper for Airflow tasks or synchronous scripts."""
        return asyncio.run(self.emit_alert(**kwargs))
