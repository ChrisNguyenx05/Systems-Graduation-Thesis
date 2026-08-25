"""Example: SYS-01 (Dashboard) emitting a market anomaly alert."""

import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

from src.alert_dispatcher.dispatcher import AlertDispatcherEngine
from src.alert_dispatcher.models import AlertPayload


def trigger_pump_dump_alert():
    print("[SYS-01] Triggering Market Anomaly Detection Alert...")
    payload = AlertPayload(
        alert_type="MARKET_ANOMALY",
        level="CRITICAL",
        source_system="SYS-01",
        event="PUMP_AND_DUMP_DETECTED",
        deduplicate_key="market_anomaly:ETHUSDT:pump_dump",
        message="Phát hiện dòng tiền bất thường đẩy giá ETH/USDT tăng 15% kèm volume gấp 4 lần trung bình 24h.",
        receiver_roles=["trader", "risk_manager"],
        channels=["telegram", "slack"],
        action_url="http://localhost:8501/market-overview?symbol=ETHUSDT",
        suggested_action="Kiểm tra depth chart và cảnh báo cho các trading bot tự động.",
        metadata={
            "symbol": "ETHUSDT",
            "price_current": 3450.2,
            "price_change_5m_pct": 15.2,
            "volume_spike_ratio": 4.1,
            "algorithm": "IsolationForest_v2"
        }
    )

    import asyncio
    engine = AlertDispatcherEngine()
    result = asyncio.run(engine.dispatch(payload))
    print(f"Alert Dispatch Result: {result.model_dump()}")


if __name__ == "__main__":
    trigger_pump_dump_alert()
