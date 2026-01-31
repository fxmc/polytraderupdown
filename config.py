"""
Configuration constants for the terminal dashboard.
"""

from __future__ import annotations


LEFT_W: int = 70

UI_HZ: float = 10.0
FAKE_HZ: float = 10.0

BINANCE_WS_BASE: str = "wss://stream.binance.com:9443/ws"

BINANCE_SYMBOL_DEFAULT: str = "BTCUSDT"
BINANCE_STREAM_SUFFIX: str = "@trade"

RAW_LOG_DIR: str = "logs"
RAW_LOG_MAX_QUEUE: int = 50_000
RAW_LOG_BATCH_SIZE: int = 200
RAW_LOG_FLUSH_EVERY_S: float = 0.25
