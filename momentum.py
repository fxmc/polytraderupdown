"""
Online momentum from a 1-second sampled price buffer.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Tuple


@dataclass(slots=True)
class SecPriceBuffer:
    """Rolling 1-second sampled price buffer."""
    max_secs: int = 300
    last_sec: int = 0
    last_px: float = 0.0
    sec_px: Deque[Tuple[int, float]] = field(default_factory=lambda: deque(maxlen=301))


def update_sec_price(buf: SecPriceBuffer, trade_ms: float, price: float) -> bool:
    """Update buffer with the latest trade; append a sample when the second changes. Return True on new second"""
    sec = int(trade_ms // 1000.0)
    buf.last_px = price

    if buf.last_sec == 0:
        buf.last_sec = sec
        buf.sec_px.append((sec, price))
        return True

    if sec == buf.last_sec:
        return False

    buf.last_sec = sec
    buf.sec_px.append((sec, price))
    return True

def _price_n_seconds_ago(buf: SecPriceBuffer, n: int) -> float:
    """Return price n seconds ago from the buffer, or 0.0 if unavailable."""
    if len(buf.sec_px) <= n:
        return 0.0
    return float(buf.sec_px[-(n + 1)][1])


def mom_points(buf: SecPriceBuffer, horizon_s: int) -> float:
    """Momentum in absolute points over horizon_s seconds."""
    p0 = _price_n_seconds_ago(buf, horizon_s)
    if p0 <= 0.0:
        return 0.0
    return buf.last_px - p0


def mom_pct(buf: SecPriceBuffer, horizon_s: int) -> float:
    """Momentum in percent over horizon_s seconds."""
    p0 = _price_n_seconds_ago(buf, horizon_s)
    if p0 <= 0.0:
        return 0.0
    return 100.0 * (buf.last_px / p0 - 1.0)
