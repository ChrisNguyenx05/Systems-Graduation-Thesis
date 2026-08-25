"""Dry-run / Console Notification Channel Adapter."""

import logging
import sys
from typing import Tuple
from src.alert_dispatcher.channels.base import BaseNotificationChannel
from src.alert_dispatcher.formatter import format_telegram_markdown
from src.alert_dispatcher.models import AlertPayload

logger = logging.getLogger("alert_dispatcher.dry_run")


class DryRunChannel(BaseNotificationChannel):
    """Logs the formatted alert without making any live network calls."""

    @property
    def channel_name(self) -> str:
        return "console"

    def is_configured(self) -> bool:
        return True

    async def send_alert(self, alert: AlertPayload) -> Tuple[bool, str]:
        formatted = format_telegram_markdown(alert)
        log_output = (
            f"\n==================== [DRY RUN ALERT DISPATCH] ====================\n"
            f"Source System : {alert.source_system}\n"
            f"Event         : {alert.event}\n"
            f"Severity      : {alert.level}\n"
            f"Dedup Key     : {alert.deduplicate_key}\n"
            f"Target Roles  : {', '.join([str(r) for r in alert.receiver_roles])}\n"
            f"------------------------------------------------------------------\n"
            f"{formatted}\n"
            f"==================================================================\n"
        )
        try:
            # Safely write to stdout using UTF-8 or buffer write
            if hasattr(sys.stdout, "buffer"):
                sys.stdout.buffer.write(log_output.encode("utf-8", errors="replace"))
                sys.stdout.buffer.write(b"\n")
                sys.stdout.buffer.flush()
            else:
                print(log_output.encode("utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8", errors="replace"))
        except Exception:
            # Fallback ascii log
            logger.info(f"Dry-run alert dispatch for {alert.event} ({alert.deduplicate_key})")

        logger.info(f"Dry-run alert processed: {alert.event} ({alert.deduplicate_key})")
        return True, "Dry-run simulated delivery successful"
