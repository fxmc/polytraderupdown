"""
Minimal candle aggregation + ATR(2) from last 3 closed candles.

All operations are O(1). No scans. No history beyond three closed candles.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional


TF_1M_MS: int = 60_000
TF_5M_MS: int = 300_000
TF_15M_MS: int = 900_000


@dataclass(slots=True)
class Candle:
    """A single OHLC candle with start/end in ms."""
    start_ms: int
    end_ms: int
    open: float
    high: float
    low: float
    close: float
    n_trades: int = 0


@dataclass(slots=True)
class CandleSeries:
    """Rolling candle series for one timeframe."""
    tf_ms: int
    current: Optional[Candle] = None
    closed: Deque[Candle] = field(default_factory=lambda: deque(maxlen=3))


def bucket_start_ms(ts_ms: float, tf_ms: int) -> int:
    """Return the candle start for a timestamp in ms."""
    return int(ts_ms) // tf_ms * tf_ms


def start_new_candle(start_ms: int, tf_ms: int, price: float) -> Candle:
    """Create a new candle with OHLC initialized to price."""
    end_ms = start_ms + tf_ms
    return Candle(start_ms=start_ms, end_ms=end_ms, open=price, high=price, low=price, close=price, n_trades=1)


def update_candle(c: Candle, price: float) -> None:
    """Update an existing candle with a trade price."""
    if price > c.high:
        c.high = price
    if price < c.low:
        c.low = price
    c.close = price
    c.n_trades += 1


def update_series_with_trade(series: CandleSeries, trade_ms: float, price: float) -> None:
    """
    Update a CandleSeries with one trade.

    - Rolls the candle when trade bucket changes.
    - Pushes closed candle into deque (maxlen=3).
    """
    start_ms = bucket_start_ms(trade_ms, series.tf_ms)

    if series.current is None:
        series.current = start_new_candle(start_ms, series.tf_ms, price)
        return

    if start_ms == series.current.start_ms:
        update_candle(series.current, price)
        return

    series.closed.append(series.current)
    series.current = start_new_candle(start_ms, series.tf_ms, price)


def true_range(c: Candle, prev_close: float) -> float:
    """Compute True Range for a candle given previous close."""
    hl = c.high - c.low
    hc = abs(c.high - prev_close)
    lc = abs(c.low - prev_close)
    return max(hl, hc, lc)


def atr2_from_last3(closed: Deque[Candle]) -> float:
    """
    Compute ATR(2) using the last 3 closed candles.

    Uses TRs from the three candles:
    - atr0 = mean(TR1, TR2)
    - atr2 = Wilder smoothing with n=2 over TR3: atr = (atr0*(n-1) + TR3)/n

    Returns 0.0 if insufficient candles.
    """
    if len(closed) < 3:
        return 0.0

    c1 = closed[-3]
    c2 = closed[-2]
    c3 = closed[-1]

    tr1 = true_range(c1, c1.open)
    tr2 = true_range(c2, c1.close)
    tr3 = true_range(c3, c2.close)

    atr0 = 0.5 * (tr1 + tr2)
    return 0.5 * (atr0 + tr3)
