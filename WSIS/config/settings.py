"""Configuration and Settings for WebSocket Ingestion Service (WSIS)."""
import os
from typing import Dict, List

try:
    from pydantic_settings import BaseSettings
    class Settings(BaseSettings):
        MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
        MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "admin123")
        MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "admin123")
        MINIO_BRONZE_BUCKET: str = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")
        MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
        WINDOW_DURATION_MS: int = int(os.getenv("WINDOW_DURATION_MS", "900000"))
        GRACE_PERIOD_SECONDS: int = int(os.getenv("GRACE_PERIOD_SECONDS", "10"))
        BINANCE_SPOT_WS_BASE: str = os.getenv("BINANCE_SPOT_WS_BASE", "wss://stream.binance.com:9443")
        BINANCE_FUTURES_WS_BASE: str = os.getenv("BINANCE_FUTURES_WS_BASE", "wss://fstream.binance.com")
        TARGET_SYMBOLS: str = "BTCUSDT,ETHUSDT,BNBUSDT,AAPLBUSDT,AMZNBUSDT,DELLBUSDT"
        MAX_BUFFER_RECORDS: int = 50000
        FLUSH_BATCH_SIZE: int = 1000
        LOG_LEVEL: str = "INFO"
        WSIS_HTTP_PORT: int = int(os.getenv("WSIS_HTTP_PORT", "8005"))
        class Config:
            env_file = ".env"
            extra = "ignore"

        @property
        def symbols(self) -> List[str]:
            if isinstance(self.TARGET_SYMBOLS, list):
                return [s.upper() for s in self.TARGET_SYMBOLS]
            return [s.strip().upper() for s in str(self.TARGET_SYMBOLS).split(",") if s.strip()]
except ImportError:
    # Native fallback without pydantic_settings
    class Settings:
        MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
        MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "admin123")
        MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "admin123")
        MINIO_BRONZE_BUCKET: str = os.getenv("MINIO_BRONZE_BUCKET", "crypto-bronze")
        MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
        WINDOW_DURATION_MS: int = int(os.getenv("WINDOW_DURATION_MS", "900000"))
        GRACE_PERIOD_SECONDS: int = int(os.getenv("GRACE_PERIOD_SECONDS", "10"))
        BINANCE_SPOT_WS_BASE: str = os.getenv("BINANCE_SPOT_WS_BASE", "wss://stream.binance.com:9443")
        BINANCE_FUTURES_WS_BASE: str = os.getenv("BINANCE_FUTURES_WS_BASE", "wss://fstream.binance.com")
        TARGET_SYMBOLS: str = os.getenv("TARGET_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,AAPLBUSDT,AMZNBUSDT,DELLBUSDT")
        MAX_BUFFER_RECORDS: int = int(os.getenv("MAX_BUFFER_RECORDS", "50000"))
        FLUSH_BATCH_SIZE: int = int(os.getenv("FLUSH_BATCH_SIZE", "1000"))
        LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
        WSIS_HTTP_PORT: int = int(os.getenv("WSIS_HTTP_PORT", "8005"))

        @property
        def symbols(self) -> List[str]:
            if isinstance(self.TARGET_SYMBOLS, list):
                return [s.upper() for s in self.TARGET_SYMBOLS]
            return [s.strip().upper() for s in str(self.TARGET_SYMBOLS).split(",") if s.strip()]

# Centralized Dimension Mapping (pair_id 1..6)
TRADING_PAIRS: Dict[int, Dict[str, str]] = {
    1: {"symbol": "BTCUSDT", "trading_pair": "BTC/USDT", "base_coin": "BTC", "quote_coin": "USDT", "market_type": "crypto"},
    2: {"symbol": "ETHUSDT", "trading_pair": "ETH/USDT", "base_coin": "ETH", "quote_coin": "USDT", "market_type": "crypto"},
    3: {"symbol": "BNBUSDT", "trading_pair": "BNB/USDT", "base_coin": "BNB", "quote_coin": "USDT", "market_type": "crypto"},
    4: {"symbol": "AAPLBUSDT", "trading_pair": "AAPLB/USDT", "base_coin": "AAPLB", "quote_coin": "USDT", "market_type": "tokenized_stock"},
    5: {"symbol": "AMZNBUSDT", "trading_pair": "AMZNB/USDT", "base_coin": "AMZNB", "quote_coin": "USDT", "market_type": "tokenized_stock"},
    6: {"symbol": "DELLBUSDT", "trading_pair": "DELLB/USDT", "base_coin": "DELLB", "quote_coin": "USDT", "market_type": "tokenized_stock"}
}

SYMBOL_TO_PAIR_ID: Dict[str, int] = {v["symbol"]: k for k, v in TRADING_PAIRS.items()}

settings = Settings()
