"""Compare finalized kline records from REST and WebSocket sources."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


KLINE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "close_time",
)


def _equal(left: Any, right: Any) -> bool:
    """Compare numbers exactly at the exchange-provided decimal precision."""
    if left is None or right is None:
        return left is right
    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, ValueError):
        return left == right


def compare_closed_klines(websocket: dict[str, Any], api: dict[str, Any] | None) -> dict[str, Any]:
    """Return an auditable comparison; never treat an unavailable API row as equal."""
    base = {
        "event_type": "kline_reconciliation",
        "symbol": websocket["symbol"],
        "interval": "15m",
        "open_time": websocket["open_time"],
        "websocket_collected_at": websocket["collected_at"],
        "source": "wsis_api_websocket_reconciliation",
    }
    if api is None:
        return {**base, "status": "api_missing", "matched": False, "mismatched_fields": list(KLINE_FIELDS)}

    mismatched = [field for field in KLINE_FIELDS if not _equal(websocket.get(field), api.get(field))]
    return {
        **base,
        "api_collected_at": api["collected_at"],
        "api_source": api["source"],
        "status": "matched" if not mismatched else "mismatch",
        "matched": not mismatched,
        "mismatched_fields": mismatched,
    }

