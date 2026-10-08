"""Entrypoint for WebSocket Ingestion Service (WSIS)."""
import asyncio
import signal
import sys
import time
from aiohttp import web
from config.settings import settings
from src.storage.minio_sink import MinIOBronzeSink
from src.windowing.time_window_manager import TimeWindowManager
from src.buffer.batch_aggregator import TimeWindowBatchAggregator
from src.collectors.spot_collector import SpotCombinedCollector
from src.collectors.futures_collector import FuturesCombinedCollector
from src.reconciliation.rest_kline_reconciler import RestKlineReconciler
from src.utils.logger import logger

async def run_wsis():
    logger.info("=================================================================")
    logger.info("🚀 INITIALIZING WEBSOCKET INGESTION SERVICE (WSIS)")
    logger.info("=================================================================")
    logger.info(f"Window Duration: {settings.WINDOW_DURATION_MS // 1000}s (15 Minutes - Clock Aligned)")
    logger.info(f"Target Symbols: {', '.join(settings.symbols)}")
    logger.info(f"MinIO Bronze S3 Endpoint: {settings.MINIO_ENDPOINT}")
    logger.info(f"Bronze Bucket: {settings.MINIO_BRONZE_BUCKET}")
    logger.info("=================================================================")

    # 1. Initialize Subsystems
    sink = MinIOBronzeSink()
    window_mgr = TimeWindowManager(
        window_duration_ms=settings.WINDOW_DURATION_MS,
        grace_period_seconds=settings.GRACE_PERIOD_SECONDS
    )
    aggregator = TimeWindowBatchAggregator(sink, window_mgr)
    reconciler = RestKlineReconciler(aggregator)

    # 2. Initialize Collectors
    spot_collector = SpotCombinedCollector(aggregator, reconciler)
    futures_collector = FuturesCombinedCollector(aggregator)

    # 3. Initialize HTTP Health & Telemetry Server (Port 8005)
    http_start_time = time.time()

    async def health_handler(request):
        start_ms, end_ms, run_key = window_mgr.current_window_bounds()
        return web.json_response({
            "status": "UP",
            "service": "wsis_collector",
            "uptime_seconds": int(time.time() - http_start_time),
            "window_duration_seconds": settings.WINDOW_DURATION_MS // 1000,
            "target_symbols": settings.symbols,
            "current_window": run_key,
            "window_start_ms": start_ms,
            "window_end_ms": end_ms,
            "seconds_until_next_flush": round(window_mgr.seconds_until_next_flush(), 1),
            "minio_endpoint": settings.MINIO_ENDPOINT,
            "minio_bucket": settings.MINIO_BRONZE_BUCKET,
            "collectors": {
                "spot": getattr(spot_collector, "is_running", True),
                "futures": getattr(futures_collector, "is_running", True)
            }
        })

    http_app = web.Application()
    http_app.router.add_get("/health", health_handler)
    http_app.router.add_get("/api/v1/health", health_handler)
    http_app.router.add_get("/", health_handler)

    runner = web.AppRunner(http_app)
    await runner.setup()
    http_port = getattr(settings, "WSIS_HTTP_PORT", 8005)
    site = web.TCPSite(runner, "0.0.0.0", http_port)
    await site.start()
    logger.info(f"WSIS Health & Telemetry HTTP Server active on http://0.0.0.0:{http_port}/health")

    # 4. Setup Graceful Shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def request_stop():
        logger.info("Received termination signal. Stopping WSIS gracefully...")
        spot_collector.stop()
        futures_collector.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            pass  # Windows event loop limitation

    # 5. Launch Tasks
    tasks = [
        asyncio.create_task(spot_collector.start(), name="SpotCollector"),
        asyncio.create_task(futures_collector.start(), name="FuturesCollector"),
        asyncio.create_task(aggregator.window_rotation_loop(), name="WindowRotationLoop")
    ]

    logger.info("All WSIS tasks started successfully. Listening for live events...")

    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await reconciler.close()
        await runner.cleanup()
        logger.info("WSIS shutdown complete.")

def main():
    try:
        asyncio.run(run_wsis())
    except (KeyboardInterrupt, SystemExit):
        logger.info("WSIS exited.")

if __name__ == "__main__":
    main()
