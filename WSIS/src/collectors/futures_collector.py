"""Binance Futures Streams Collector (depth20@100ms & !forceOrder@arr)."""
import json
import time
from config.settings import settings, SYMBOL_TO_PAIR_ID
from src.collectors.base_collector import BaseWebSocketCollector
from src.buffer.batch_aggregator import TimeWindowBatchAggregator
from src.utils.logger import logger

class FuturesCombinedCollector(BaseWebSocketCollector):
    def __init__(self, aggregator: TimeWindowBatchAggregator):
        self.aggregator = aggregator
        # Listen to !forceOrder@arr and depth for the top crypto pairs
        crypto_symbols = [s.lower() for s in settings.symbols if not s.startswith("AAPL") and not s.startswith("AMZN") and not s.startswith("DELL")]
        stream_list = ["!forceOrder@arr"]
        for sym in crypto_symbols:
            stream_list.append(f"{sym}@depth20@100ms")

        url = f"{settings.BINANCE_FUTURES_WS_BASE}/stream?streams=" + "/".join(stream_list)
        super().__init__(name="FuturesCollector", url=url)

    async def handle_message(self, raw_message: str):
        payload = json.loads(raw_message)
        data = payload.get("data", {})
        event_type = data.get("e")
        now_ms = int(time.time() * 1000)

        # 1. Handle Liquidation Event (forceOrder)
        if event_type == "forceOrder":
            order = data.get("o", {})
            sym = order.get("s", "").upper()
            pid = SYMBOL_TO_PAIR_ID.get(sym, 0)
            trade_time = int(order.get("T", now_ms))

            liq_record = {
                "pair_id": pid,
                "event_type": "forceOrder",
                "event_time": int(data.get("E", now_ms)),
                "symbol": sym,
                "side": order.get("S", ""),
                "price": float(order.get("p", 0.0)),
                "quantity": float(order.get("q", 0.0)),
                "order_id": int(order.get("i", 0)),
                "order_type": order.get("o", ""),
                "time_in_force": order.get("f", ""),
                "order_status": order.get("X", ""),
                "average_price": float(order.get("ap", 0.0)),
                "filled_quantity": float(order.get("z", 0.0)),
                "order_time": trade_time,
                "collected_at": now_ms,
                "source": "v1.0.0_wsis_liquidations"
            }
            await self.aggregator.add_record("futures_liquidations", trade_time, liq_record)

        # 2. Handle Depth20 Orderbook Snapshot
        elif "bids" in data and "asks" in data:
            stream_name = payload.get("stream", "")
            sym = stream_name.split("@")[0].upper()
            pid = SYMBOL_TO_PAIR_ID.get(sym, 0)
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            bv = sum(float(x[1]) for x in bids)
            av = sum(float(x[1]) for x in asks)
            imbalance = (bv - av) / (bv + av) if (bv + av) > 0 else 0.0

            depth_record = {
                "pair_id": pid,
                "symbol": sym,
                "last_update_id": int(data.get("E", now_ms)),
                "bids": bids,
                "asks": asks,
                "bid_volume": bv,
                "ask_volume": av,
                "order_book_imbalance": imbalance,
                "observed_at": now_ms,
                "source": "v1.0.0_wsis_depth"
            }
            await self.aggregator.update_depth_snapshot(sym, depth_record)
