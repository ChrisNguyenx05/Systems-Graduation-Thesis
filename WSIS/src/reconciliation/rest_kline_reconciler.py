"""Fetch the REST version of each finalized WebSocket candle and reconcile it."""
from __future__ import annotations

import time
from typing import Any

import aiohttp

from src.reconciliation.klines import compare_closed_klines
from src.utils.logger import logger


class RestKlineReconciler:
    """REST is a reconciliation source, not a silent replacement for WebSocket data."""

    KLINES_URL = "https://api.binance.com/api/v3/klines"

    def __init__(self, aggregator: Any, interval: str = "15m") -> None:
        self.aggregator = aggregator
        self.interval = interval
        self._session: aiohttp.ClientSession | None = None

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def reconcile_closed_kline(self, websocket_kline: dict[str, Any]) -> None:
        if not websocket_kline.get("is_closed", False):
            return

        api_kline = await self._fetch_closed_kline(websocket_kline)
        comparison = compare_closed_klines(websocket_kline, api_kline)
        event_time = int(websocket_kline["close_time"])

        if api_kline is not None:
            await self.aggregator.add_record("api_klines", event_time, api_kline)
        await self.aggregator.add_record("reconciliation", event_time, comparison)

        if comparison["matched"]:
            logger.info("REST/WebSocket kline match: %s %s", comparison["symbol"], comparison["open_time"])
        else:
            logger.warning("REST/WebSocket kline %s: %s %s", comparison["status"], comparison["symbol"], comparison["open_time"])

    async def _fetch_closed_kline(self, websocket_kline: dict[str, Any]) -> dict[str, Any] | None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

        params = {
            "symbol": websocket_kline["symbol"],
            "interval": self.interval,
            "startTime": websocket_kline["open_time"],
            "endTime": websocket_kline["close_time"],
            "limit": 1,
        }
        try:
            async with self._session.get(self.KLINES_URL, params=params) as response:
                response.raise_for_status()
                rows = await response.json()
        except (aiohttp.ClientError, TimeoutError) as exc:
            logger.warning("REST reconciliation request failed for %s: %s", websocket_kline["symbol"], exc)
            return None

        if not rows:
            return None
        row = rows[0]
        if int(row[0]) != int(websocket_kline["open_time"]):
            logger.warning("REST returned an unexpected candle for %s", websocket_kline["symbol"])
            return None

        return {
            "pair_id": websocket_kline["pair_id"],
            "symbol": websocket_kline["symbol"],
            "open_time": int(row[0]),
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "close_time": int(row[6]),
            "quote_asset_volume": row[7],
            "number_of_trades": int(row[8]),
            "taker_buy_base_asset_volume": row[9],
            "taker_buy_quote_asset_volume": row[10],
            "is_closed": True,
            "collected_at": int(time.time() * 1000),
            "source": "binance_rest_klines",
        }

