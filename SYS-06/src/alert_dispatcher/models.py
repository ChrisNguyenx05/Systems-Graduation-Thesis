"""Data models and enums for SYS-06 Operational Alerting & Notification System."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class SeverityLevel(str, Enum):
    """Operational severity level."""
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


class AlertType(str, Enum):
    """Category of operational alert."""
    MARKET_ANOMALY = "MARKET_ANOMALY"
    FRAUD_TRANSACTION = "FRAUD_TRANSACTION"
    INFRASTRUCTURE_FAILURE = "INFRASTRUCTURE_FAILURE"
    PIPELINE_FAILURE = "PIPELINE_FAILURE"
    DATA_QUALITY = "DATA_QUALITY"
    STORAGE_FAILURE = "STORAGE_FAILURE"
    MODEL_GOVERNANCE = "MODEL_GOVERNANCE"
    BUSINESS_RISK = "BUSINESS_RISK"
    SYSTEM_HEALTH = "SYSTEM_HEALTH"


class SourceSystem(str, Enum):
    """Originating system identifier in the 6-system architecture."""
    SYS_01 = "SYS-01"
    SYS_02 = "SYS-02"
    SYS_03 = "SYS-03"
    SYS_04 = "SYS-04"
    SYS_05 = "SYS-05"
    SYS_06 = "SYS-06"


class ReceiverRole(str, Enum):
    """Target user/role receiving the alert."""
    DEVOPS_SRE = "devops_sre"
    DATA_ENGINEER = "data_engineer"
    ML_ENGINEER = "ml_engineer"
    RISK_MANAGER = "risk_manager"
    TRADER = "trader"
    COMPLIANCE = "compliance"
    ALL = "all"


class NotificationChannel(str, Enum):
    """Target notification channel."""
    TELEGRAM = "telegram"
    SLACK = "slack"
    CONSOLE = "console"
    EMAIL = "email"
    SMS = "sms"


class AlertPayload(BaseModel):
    """Standardized Alert Event JSON Payload."""
    model_config = ConfigDict(use_enum_values=True)

    alert_type: AlertType = Field(..., description="High-level category of alert.")
    level: SeverityLevel = Field(..., description="Severity level: CRITICAL, WARNING, INFO.")
    source_system: SourceSystem = Field(..., description="Originating system ID (e.g. SYS-01).")
    event: str = Field(..., min_length=3, max_length=100, description="Unique event code in UPPER_SNAKE_CASE.")
    deduplicate_key: str = Field(..., min_length=3, max_length=255, description="Unique key for 300s window deduplication.")
    message: str = Field(..., min_length=1, description="Human-readable explanation of the alert.")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="ISO 8601 UTC timestamp.")
    receiver_roles: List[ReceiverRole] = Field(default_factory=lambda: [ReceiverRole.DEVOPS_SRE], description="Audience roles.")
    channels: List[NotificationChannel] = Field(default_factory=lambda: [NotificationChannel.TELEGRAM], description="Requested channels.")
    action_url: Optional[str] = Field(None, description="Optional remediation link.")
    suggested_action: Optional[str] = Field(None, description="Optional quick action recommendation.")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Custom contextual metrics.")


class DispatchResult(BaseModel):
    """Outcome report for an alert dispatch operation."""
    model_config = ConfigDict(use_enum_values=True)

    success: bool
    suppressed: bool = False
    rate_limited: bool = False
    channel_results: Dict[str, bool] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)
    formatted_message: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
