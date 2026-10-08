"""15-Minute Window Batch Aggregator for WSIS."""
import asyncio
import time
from typing import Dict, List, Any
from src.windowing.time_window_manager import TimeWindowManager
from src.storage.minio_sink import MinIOBronzeSink
from src.utils.logger import logger

class TimeWindowBatchAggregator:
    DATASETS = ("klines", "api_klines", "reconciliation", "agg_trades", "order_book_depth", "futures_liquidations")

    def __init__(self, sink: MinIOBronzeSink, window_mgr: TimeWindowManager):
        self.sink = sink
        self.window_mgr = window_mgr
        self.lock = asyncio.Lock()

        # In-memory windows: { window_start_ms: { dataset: list_of_records } }
        self.windows: Dict[int, Dict[str, List[Any]]] = {}
        self.latest_depth_snapshots: Dict[str, Dict[str, Any]] = {}

    def _get_or_create_window(self, window_start_ms: int) -> Dict[str, List[Any]]:
        if window_start_ms not in self.windows:
            self.windows[window_start_ms] = {d: [] for d in self.DATASETS}
        return self.windows[window_start_ms]

    async def add_record(self, dataset: str, event_time_ms: int, record: Dict[str, Any]):
        """Directs the record into the proper 15-minute tumbling window bucket."""
        window_start = self.window_mgr.get_window_start(event_time_ms, self.window_mgr.window_duration_ms)
        async with self.lock:
            bucket = self._get_or_create_window(window_start)
            bucket[dataset].append(record)

    async def update_depth_snapshot(self, symbol: str, snapshot: Dict[str, Any]):
        """Caches the latest depth snapshot for the symbol to capture at window end."""
        async with self.lock:
            self.latest_depth_snapshots[symbol] = snapshot

    async def flush_window(self, window_start_ms: int):
        """Seals and flushes a finished 15-minute window to MinIO S3."""
        run_key = self.window_mgr.get_window_run_key(window_start_ms)
        records_to_flush: Dict[str, List[Any]] = {}

        async with self.lock:
            if window_start_ms in self.windows:
                records_to_flush = self.windows.pop(window_start_ms)
            else:
                records_to_flush = {d: [] for d in self.DATASETS}

            # Attach latest depth snapshot of each available symbol into order_book_depth
            for sym, snap in self.latest_depth_snapshots.items():
                snap_copy = dict(snap)
                snap_copy["window_key"] = run_key
                records_to_flush["order_book_depth"].append(snap_copy)

        logger.info(f"Sealing 15m Window [{run_key}] -> Flushing to MinIO Bronze...")
        for dataset in self.DATASETS:
            items = records_to_flush.get(dataset, [])
            if items:
                try:
                    self.sink.write_window_dataset(dataset, run_key, items)
                except Exception as e:
                    logger.error(f"Failed to write {dataset} for window {run_key}: {e}")

    async def window_rotation_loop(self):
        """Background coroutine that aligns with clock boundaries :00, :15, :30, :45."""
        logger.info("Started 15-Minute Window Rotation Loop (Clock Synchronized).")
        while True:
            # 1. Determine current window start
            now_ms = int(time.time() * 1000)
            current_window_start = self.window_mgr.get_window_start(now_ms, self.window_mgr.window_duration_ms)

            # 2. Wait until window boundary + grace period
            sleep_sec = self.window_mgr.seconds_until_next_flush()
            logger.info(f"Active 15m window: {self.window_mgr.get_window_run_key(current_window_start)} (next flush in {sleep_sec:.1f}s)")
            await asyncio.sleep(sleep_sec)

            # 3. Flush the completed window
            await self.flush_window(current_window_start)
