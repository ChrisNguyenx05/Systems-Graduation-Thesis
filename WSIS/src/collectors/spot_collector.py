"""Binance Spot Combined Streams Collector (klines_15m & aggTrade)."""
import json
import time
from config.settings import settings, SYMBOL_TO_PAIR_ID
from src.collectors.base_collector import BaseWebSocketCollector
from src.buffer.batch_aggregator import TimeWindowBatchAggregator
from src.utils.logger import logger

class SpotCombinedCollector(BaseWebSocketCollector):
    def __init__(self, aggregator: TimeWindowBatchAggregator, reconciler=None):
        self.aggregator = aggregator
        self.reconciler = reconciler
        # Construct stream names: <symbol>@kline_15m and <symbol>@aggTrade
        stream_list = []
        for sym in settings.symbols:
            s_lower = sym.lower()
            stream_list.append(f"{s_lower}@kline_15m")
            stream_list.append(f"{s_lower}@aggTrade")

        url = f"{settings.BINANCE_SPOT_WS_BASE}/stream?streams=" + "/".join(stream_list)
        super().__init__(name="SpotCollector", url=url)

    async def handle_message(self, raw_message: str):
        payload = json.loads(raw_message)
        data = payload.get("data", {})
        event_type = data.get("e")
        now_ms = int(time.time() * 1000)

        # 1. Handle 15m Kline
        if event_type == "kline":
            k = data.get("k", {})
            sym = data.get("s", "").upper()
            pid = SYMBOL_TO_PAIR_ID.get(sym, 0)
            open_time = int(k.get("t", 0))

            kline_record = {
                "pair_id": pid,
                "symbol": sym,
                "open_time": open_time,
                "open": float(k.get("o", 0.0)),
                "high": float(k.get("h", 0.0)),
                "low": float(k.get("l", 0.0)),
                "close": float(k.get("c", 0.0)),
                "volume": float(k.get("v", 0.0)),
                "close_time": int(k.get("T", 0)),
                "quote_asset_volume": float(k.get("q", 0.0)),
                "number_of_trades": int(k.get("n", 0)),
                "taker_buy_base_asset_volume": float(k.get("V", 0.0)),
                "taker_buy_quote_asset_volume": float(k.get("Q", 0.0)),
                "is_closed": bool(k.get("x", False)),
                "collected_at": now_ms,
                "source": "v1.0.0_wsis_klines"
            }
            # Add to 15m window based on kline open_time
            await self.aggregator.add_record("klines", open_time, kline_record)
            if kline_record["is_closed"] and self.reconciler is not None:
                await self.reconciler.reconcile_closed_kline(kline_record)

        # 2. Handle aggTrade
        elif event_type == "aggTrade":
            sym = data.get("s", "").upper()
            pid = SYMBOL_TO_PAIR_ID.get(sym, 0)
            trade_time = int(data.get("T", now_ms))

            trade_record = {
                "pair_id": pid,
                "event_type": "aggTrade",
                "event_time": int(data.get("E", now_ms)),
                "symbol": sym,
                "aggregate_trade_id": int(data.get("a", 0)),
                "price": float(data.get("p", 0.0)),
                "quantity": float(data.get("q", 0.0)),
                "first_trade_id": int(data.get("f", 0)),
                "last_trade_id": int(data.get("l", 0)),
                "trade_time": trade_time,
                "buyer_is_market_maker": bool(data.get("m", False)),
                "collected_at": now_ms,
                "source": "v1.0.0_wsis_aggtrade"
            }
            # Add to 15m window based on trade_time
            await self.aggregator.add_record("agg_trades", trade_time, trade_record)
