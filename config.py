"""
Configuration constants for the terminal dashboard.
"""

from __future__ import annotations


LEFT_W: int = 100

UI_HZ: float = 20.0

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
PM_PING_EVERY_S: float = 5.0  # docs suggest ping about every 5s

# Map Binance symbols to Chainlink RTDS symbols (filters expect slash form)
PM_CHAINLINK_SYMBOL_BY_BINANCE = {
    "BTCUSDT": "btc/usd",
    "ETHUSDT": "eth/usd",
    "SOLUSDT": "sol/usd",
    "XRPUSDT": "xrp/usd",
}


# --- Polymarket CLOB (orderbook) ---
# WS endpoint for market subscriptions (orderbook snapshots / trade prints)
PM_CLOB_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# The CLOB WS expects an application-level PING message.
PM_CLOB_PING_EVERY_S: float = 10.0


# --- Polymarket Gamma API (market resolution) ---
PM_GET_MARKET_FROM_SLUG_URL: str = "https://gamma-api.polymarket.com/markets/slug"

# Map Binance symbols to Polymarket coin strings used in slugs
PM_COIN_BY_BINANCE = {
    "BTCUSDT": "btc",
    "ETHUSDT": "eth",
    "SOLUSDT": "sol",
    "XRPUSDT": "xrp",
}


FV_QUIET_MS = 2500.0
FV_SIGMA_FLOOR_K = 0.60
FV_SIGMA_HOLD_MS = 15000.0

CLOB_HOST = "https://clob.polymarket.com"