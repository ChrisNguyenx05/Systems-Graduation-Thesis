"""Unit tests for Telegram Markdown and Slack formatters."""

from src.alert_dispatcher.formatter import (
    format_slack_payload,
    format_telegram_markdown,
    get_severity_emoji,
)
from src.alert_dispatcher.models import AlertPayload, SeverityLevel


def test_severity_emoji_mapping():
    assert get_severity_emoji(SeverityLevel.CRITICAL) == "🔴"
    assert get_severity_emoji(SeverityLevel.WARNING) == "⚠️"
    assert get_severity_emoji(SeverityLevel.INFO) == "🟢"


def test_format_telegram_markdown():
    alert = AlertPayload(
        alert_type="INFRASTRUCTURE_FAILURE",
        level="CRITICAL",
        source_system="SYS-03",
        event="DOCKER_CONTAINER_DOWN",
        deduplicate_key="infra:kafka:down",
        message="Kafka container stopped unexpectedly.",
        suggested_action="Restart kafka via docker-compose.",
        action_url="http://localhost:3000",
        metadata={"container": "kafka-1", "exit_code": 137},
    )

    markdown = format_telegram_markdown(alert)
    assert "🔴 *[CRITICAL - SYS-03]* - *DOCKER_CONTAINER_DOWN*" in markdown
    assert "Kafka container stopped unexpectedly." in markdown
    assert "💡 *Suggested Action:* Restart kafka via docker-compose." in markdown
    assert "• `container`: kafka-1" in markdown
    assert "[Open Dashboard/Portal](http://localhost:3000)" in markdown


def test_format_slack_payload():
    alert = AlertPayload(
        alert_type="FRAUD_TRANSACTION",
        level="WARNING",
        source_system="SYS-04",
        event="HIGH_FRAUD_TRANSACTION_FLAGGED",
        deduplicate_key="fraud:tx_123",
        message="Suspicious transaction held for review.",
        metadata={"tx_id": "TX_123", "score": 0.89},
    )

    payload = format_slack_payload(alert)
    assert "text" in payload
    assert "blocks" in payload
    assert len(payload["blocks"]) >= 3
    assert payload["blocks"][0]["type"] == "header"
