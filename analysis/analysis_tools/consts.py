from zoneinfo import ZoneInfo

NYC = ZoneInfo('America/New_York')
UTC = ZoneInfo('UTC')

WS_SUBSCRIPTIONS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

BINANCE_SYMBOL_MAP = {
    "btcusdt": "BTC-USD",
    "ethusdt": "ETH-USD",
    "solusdt": "SOL-USD",
    "xrpusdt": "XRP-USD",
}
BINANCE_WS_URL = "wss://stream.binance.com:443/stream?streams=" + "@trade/".join(BINANCE_SYMBOL_MAP.keys()) + "@trade"

CANONICAL_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]
POLY_SYMBOL_MAP = {
    'btc': 'BTC-USD',
    'eth': 'ETH-USD',
    'sol': 'SOL-USD',
    'xrp': 'XRP-USD'
}

CLOB_HOST = "https://clob.polymarket.com"

POLYGON_ANKR_URL = "https://rpc.ankr.com/polygon/a8b3f873595d38bbfc1144f59c921aa2726425592feb9ad4b20bd83c03fd437e"

GET_MARKET_FROM_SLUG_URL = "https://gamma-api.polymarket.com/markets/slug"
PUBLIC_PROFILE = "https://gamma-api.polymarket.com/public-profile"
TRADES_URL = "https://data-api.polymarket.com/trades"
PUBLIC_SEARCH = "https://gamma-api.polymarket.com/public-search"