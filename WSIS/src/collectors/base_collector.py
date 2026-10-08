"""Abstract Base WebSocket Collector with Auto-Reconnect."""
import asyncio
import json
import websockets
from abc import ABC, abstractmethod
from src.utils.logger import logger

class BaseWebSocketCollector(ABC):
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.running = False
        self.reconnect_delay = 1.0
        self.max_reconnect_delay = 60.0

    @abstractmethod
    async def handle_message(self, message: str):
        """Processes incoming message frame."""
        pass

    async def start(self):
        self.running = True
        logger.info(f"Starting {self.name} connected to {self.url}...")

        while self.running:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=10 * 1024 * 1024
                ) as ws:
                    logger.info(f"[{self.name}] Connection established successfully.")
                    self.reconnect_delay = 1.0  # Reset delay on success

                    async for msg in ws:
                        if not self.running:
                            break
                        try:
                            await self.handle_message(msg)
                        except Exception as parse_err:
                            logger.error(f"[{self.name}] Error handling message: {parse_err}")

            except (websockets.ConnectionClosed, OSError, Exception) as conn_err:
                if not self.running:
                    break
                logger.warning(f"[{self.name}] Connection lost ({conn_err}). Retrying in {self.reconnect_delay:.1f}s...")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)

        logger.info(f"[{self.name}] Collector stopped.")

    def stop(self):
        self.running = False
