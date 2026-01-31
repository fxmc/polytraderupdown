"""
State models and small state helpers.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List


@dataclass(slots=True)
class OrderbookLevel:
    """One orderbook level: price and size."""
    px: float
    sz: float


@dataclass(slots=True)
class OrderbookState:
    """Orderbook for YES and NO, each with bids and asks (L1–L5)."""
    yes_bids: List[OrderbookLevel] = field(default_factory=list)
    yes_asks: List[OrderbookLevel] = field(default_factory=list)
    no_bids: List[OrderbookLevel] = field(default_factory=list)
    no_asks: List[OrderbookLevel] = field(default_factory=list)
    fv_yes: float = 0.0
    fv_no: float = 0.0
    updates: int = 0
    last_change_ms: float = 0.0
    pulse: str = "."


@dataclass(slots=True)
class BurstState:
    """Burst tape lines (fixed length, newest on top)."""
    lines: Deque[str] = field(default_factory=lambda: deque(maxlen=10))
    n_updates: int = 0
    lag_ms: float = 0.0


@dataclass(slots=True)
class DriverHeaderState:
    """Header fields for Binance driver pane."""
    symbol: str = "BTCUSDT"
    strike: float = 0.0
    last: float = 0.0
    d_last: float = 0.0
    lag_ms: float = 0.0
    vol_30s: float = 0.0
    vol_1m: float = 0.0
    vol_5m: float = 0.0
    mom_30s: float = 0.0
    mom_1m: float = 0.0
    mom_5m: float = 0.0
    fv_yes: float = 0.0
    fv_no: float = 0.0
    atr_1m: float = 0.0
    atr_5m: float = 0.0
    atr_15m: float = 0.0


@dataclass(slots=True)
class ResolverHeaderState:
    """Header fields for Polymarket resolver pane."""
    last: float = 0.0
    d_last: float = 0.0
    lag_ms: float = 0.0
    vol_30s: float = 0.0
    vol_1m: float = 0.0
    vol_5m: float = 0.0
    mom_30s: float = 0.0
    mom_1m: float = 0.0
    mom_5m: float = 0.0
    mid: float = 0.0
    last_trade: float = 0.0
    updates: int = 0


@dataclass(slots=True)
class AppState:
    """Top-level app state read by renderer and written by reducers/ingestion tasks."""
    debug_left: bool = False
    book: OrderbookState = field(default_factory=OrderbookState)
    driver: DriverHeaderState = field(default_factory=DriverHeaderState)
    resolver: ResolverHeaderState = field(default_factory=ResolverHeaderState)
    tape_driver: BurstState = field(default_factory=BurstState)
    tape_resolver: BurstState = field(default_factory=BurstState)


def now_ms() -> float:
    """Return current epoch time in milliseconds."""
    return time.time() * 1000.0


def push_burst_line(tape: BurstState, last: float, d_last: float, lag_ms: float) -> None:
    """Push one newest-first burst tape line into the given tape."""
    t = time.strftime("%H:%M:%S")
    tape.n_updates += 1
    tape.lag_ms = lag_ms
    line = f"{t}  {last:0.2f}  Δ {d_last:+0.2f}  n {tape.n_updates:4d}  lag {lag_ms:0.0f}ms"
    tape.lines.appendleft(line)
