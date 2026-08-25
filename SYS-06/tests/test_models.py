"""Unit tests for Alert Payload data models and schema validation."""

import pytest
from pydantic import ValidationError
from src.alert_dispatcher.models import (
    AlertPayload,
    AlertType,
    NotificationChannel,
    ReceiverRole,
    SeverityLevel,
    SourceSystem,
)


def test_valid_alert_payload():
    payload_data = {
        "alert_type": "MARKET_ANOMALY",
        "level": "CRITICAL",
        "source_system": "SYS-01",
        "event": "PUMP_AND_DUMP_DETECTED",
        "deduplicate_key": "pump_btc_001",
        "message": "Unusual trading volume spike detected.",
        "metadata": {"symbol": "BTCUSDT", "volume_spike": 320.0},
    }
    alert = AlertPayload(**payload_data)
    assert alert.alert_type == AlertType.MARKET_ANOMALY
    assert alert.level == SeverityLevel.CRITICAL
    assert alert.source_system == SourceSystem.SYS_01
    assert alert.event == "PUMP_AND_DUMP_DETECTED"
    assert alert.metadata["symbol"] == "BTCUSDT"
    # Test defaults
    assert alert.receiver_roles == [ReceiverRole.DEVOPS_SRE]
    assert alert.channels == [NotificationChannel.TELEGRAM]


def test_missing_required_fields():
    with pytest.raises(ValidationError):
        AlertPayload(
            alert_type="MARKET_ANOMALY",
            # Missing level, source_system, event, deduplicate_key, message
        )


def test_invalid_severity_level():
    with pytest.raises(ValidationError):
        AlertPayload(
            alert_type="MARKET_ANOMALY",
            level="SUPER_CRITICAL",  # Invalid enum value
            source_system="SYS-01",
            event="PUMP_AND_DUMP_DETECTED",
            deduplicate_key="test_key",
            message="Test message",
        )
