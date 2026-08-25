"""Example: SYS-04 (MLOps & Fraud Center) emitting high fraud risk transaction alert."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload


def trigger_fraud_alert():
    print("[SYS-04 MLOps] Triggering High Fraud Risk Alert...")
    payload = AlertPayload(
        alert_type="FRAUD_TRANSACTION",
        level="CRITICAL",
        source_system="SYS-04",
        event="HIGH_FRAUD_TRANSACTION_FLAGGED",
        deduplicate_key="fraud:tx_998124:suspicious",
        message="Mô hình LightGBM phát hiện giao dịch rút tiền 50,000 USDT bất thường từ địa chỉ IP lạ.",
        receiver_roles=["risk_manager", "compliance"],
        channels=["telegram", "slack"],
        action_url="http://localhost:5000/fraud/tx_998124",
        suggested_action="Tạm giữ lệnh rút tiền và yêu cầu xác thực OTP 2FA lần 2.",
        metadata={
            "transaction_id": "tx_998124",
            "amount_usdt": 50000.0,
            "fraud_score": 0.93,
            "model_version": "lightgbm_fraud_v3.1",
            "rule_violation": "GEO_IP_JUMP_1000KM"
        }
    )

    import asyncio
    engine = AlertDispatcherEngine()
    result = asyncio.run(engine.dispatch(payload))
    print(f"Fraud Alert Result: {result.model_dump()}")


if __name__ == "__main__":
    trigger_fraud_alert()
