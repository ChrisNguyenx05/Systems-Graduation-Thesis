"""Message formatting logic for Telegram Markdown and Slack payloads."""

from typing import Any, Dict
from src.alert_dispatcher.models import AlertPayload, SeverityLevel


def get_severity_emoji(level: SeverityLevel) -> str:
    """Return corresponding emoji icon for a severity level."""
    mapping = {
        SeverityLevel.CRITICAL: "🔴",
        SeverityLevel.WARNING: "⚠️",
        SeverityLevel.INFO: "🟢",
    }
    return mapping.get(level, "ℹ️")


def format_telegram_markdown(alert: AlertPayload) -> str:
    """
    Format the alert into rich Telegram-compatible Markdown text.
    Ensures clean emoji indicators, metadata rendering, and remediation links.
    """
    emoji = get_severity_emoji(alert.level)
    level_str = str(alert.level).upper()
    source = str(alert.source_system)
    alert_type = str(alert.alert_type)
    
    lines = [
        f"{emoji} *[{level_str} - {source}]* - *{alert.event}*",
        "----------------------------------------",
        f"📂 *Type:* `{alert_type}`",
        f"📝 *Message:* {alert.message}",
    ]

    if alert.suggested_action:
        lines.append(f"💡 *Suggested Action:* {alert.suggested_action}")

    if alert.metadata:
        lines.append("📊 *Metadata:*")
        for k, v in alert.metadata.items():
            lines.append(f"  • `{k}`: {v}")

    lines.append("----------------------------------------")
    lines.append(f"⏰ *Time:* {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    if alert.action_url:
        lines.append(f"🔗 [Open Dashboard/Portal]({alert.action_url})")

    lines.append("🖥️ *Platform:* Mini Company FinTech Engine (SYS-06)")
    
    return "\n".join(lines)


def format_slack_payload(alert: AlertPayload) -> Dict[str, Any]:
    """
    Format the alert into a structured Slack Webhook JSON payload with Block Kit styling.
    """
    emoji = get_severity_emoji(alert.level)
    level_str = str(alert.level).upper()
    source = str(alert.source_system)
    
    header_text = f"{emoji} [{level_str} ALERT] {alert.event} ({source})"
    
    metadata_fields = []
    metadata_fields.append({"type": "mrkdwn", "text": f"*Source:* {source}"})
    metadata_fields.append({"type": "mrkdwn", "text": f"*Type:* {alert.alert_type}"})
    metadata_fields.append({"type": "mrkdwn", "text": f"*Time:* {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"})
    
    if alert.metadata:
        for k, v in list(alert.metadata.items())[:6]:  # Limit to 6 fields in Slack grid
            metadata_fields.append({"type": "mrkdwn", "text": f"*{k}:* `{v}`"})

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": header_text[:150],
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Details:* {alert.message}"
            }
        },
        {
            "type": "section",
            "fields": metadata_fields
        }
    ]

    if alert.suggested_action:
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"💡 *Suggested Action:* {alert.suggested_action}"
                }
            ]
        })

    if alert.action_url:
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "View Portal / Dashboard"
                    },
                    "url": alert.action_url,
                    "style": "danger" if alert.level == SeverityLevel.CRITICAL else "primary"
                }
            ]
        })

    return {
        "text": f"{header_text}: {alert.message}",
        "blocks": blocks
    }
