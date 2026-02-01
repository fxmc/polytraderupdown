"""
Configuration constants for the terminal dashboard.
"""

from __future__ import annotations


LEFT_W: int = 75

UI_HZ: float = 10.0
FAKE_HZ: float = 10.0

BINANCE_WS_BASE: str = "wss://stream.binance.com:9443/ws"

BINANCE_SYMBOL_DEFAULT: str = "BTCUSDT"
BINANCE_STREAM_SUFFIX: str = "@trade"

RAW_LOG_DIR: str = "logs"
RAW_LOG_MAX_QUEUE: int = 50_000
RAW_LOG_BATCH_SIZE: int = 200
RAW_LOG_FLUSH_EVERY_S: float = 0.25

MOM_Z_HORIZONS_S = [5, 10, 15, 30, 60]
MOM_Z_LOOKBACK = 30
MOM_Z_MIN_COUNT = 20

# --- Volatility / fair value knobs ---

# Cap 1-second log returns to reduce microstructure spikes blowing up sigma.
# Values are abs(log-return) per second; 0.005 ~ 0.5% per second.
VOL_R_CLIP_BY_SYMBOL = {
    "BTCUSDT": 0.005,
    "ETHUSDT": 0.006,
    "SOLUSDT": 0.010,
    "XRPUSDT": 0.012,
}

# Which variance estimate drives pricing:
# - "rv60": rolling 60s realized variance
# - "ewma_slow": EWMA variance (slow)
# - "blend": w*rv60 + (1-w)*ewma_slow
VOL_DRIVER: str = "blend"

# Blend weight for rv60 when VOL_DRIVER="blend"
VOL_BLEND_W: float = 0.70

# --- Drift (physical trend) knobs ---
FV_USE_DRIFT: bool = True

# Which drift estimate drives pricing:
# - "mu60": rolling 60s mean of 1s log returns
# - "ewma_slow": EWMA mean (slow)
# - "blend": w*mu60 + (1-w)*ewma_slow
DRIFT_DRIVER: str = "blend"
DRIFT_BLEND_W: float = 0.70

# --- Polymarket RTDS (resolver price feed) ---
PM_RTDS_URL: str = "wss://ws-live-data.polymarket.com"
PM_RTDS_TOPIC: str = "crypto_prices_chainlink"
PM_PING_EVERY_S: float = 5.0  # docs suggest ping about every 5s :contentReference[oaicite:3]{index=3}

# Map Binance symbols to Chainlink RTDS symbols (filters expect slash form) :contentReference[oaicite:4]{index=4}
PM_CHAINLINK_SYMBOL_BY_BINANCE = {
    "BTCUSDT": "btc/usd",
    "ETHUSDT": "eth/usd",
    "SOLUSDT": "sol/usd",
    "XRPUSDT": "xrp/usd",
}
