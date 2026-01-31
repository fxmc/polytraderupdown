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
    lines: Deque[str]
    n_updates: int = 0
    lag_ms: float = 0.0

    last_px: float = 0.0
    last_trade_ms: float = 0.0

    run_count: int = 0
    run_qty: float = 0.0

    rate_ewma: float = 0.0
    burst_level: int = 0
    burst_until_ms: float = 0.0

    last_sec: int = 0
    sec_count: int = 0

    sec_open_px: float = 0.0
    sec_move_px: float = 0.0


def make_driver_tape() -> BurstState:
    """Create the Binance tape (20 lines)."""
    return BurstState(lines=deque(maxlen=20))


def make_resolver_tape() -> BurstState:
    """Create the Polymarket tape (5 lines)."""
    return BurstState(lines=deque(maxlen=5))



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
    """Top-level app state read by renderer and written by reducers."""
    debug_left: bool = False
    book: OrderbookState = field(default_factory=OrderbookState)
    driver: DriverHeaderState = field(default_factory=DriverHeaderState)
    resolver: ResolverHeaderState = field(default_factory=ResolverHeaderState)
    tape_driver: BurstState = field(default_factory=make_driver_tape)
    tape_resolver: BurstState = field(default_factory=make_resolver_tape)


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


BURST_ALPHA: float = 0.15

BURST_ON_RPS: float = 280.0
BURST_OFF_RPS: float = 200.0

BURST_LVL2_RPS: float = 360.0
BURST_LVL3_RPS: float = 600.0

BURST_HOLD_MS: float = 1200.0


def burst_badge(level: int) -> str:
    """Return a small badge indicating burst intensity."""
    if level <= 0:
        return "   "
    if level == 1:
        return " ! "
    if level == 2:
        return "!! "
    return "!!!"


def update_rate_ewma(rate_ewma: float, dt_ms: float) -> float:
    """Update an EWMA estimate of trades/sec using the latest inter-arrival time."""
    if dt_ms <= 0.0:
        return rate_ewma
    inst = 1000.0 / dt_ms
    if rate_ewma <= 0.0:
        return inst
    return (1.0 - BURST_ALPHA) * rate_ewma + BURST_ALPHA * inst


def update_burst_gate(level: int, rate_ewma: float, now_ms_v: float, burst_until_ms: float) -> tuple[int, float]:
    """Update burst level + hold timer based on EWMA trades/sec."""
    if rate_ewma >= BURST_ON_RPS:
        if rate_ewma >= BURST_LVL3_RPS:
            lvl = 3
        elif rate_ewma >= BURST_LVL2_RPS:
            lvl = 2
        else:
            lvl = 1
        return (lvl, now_ms_v + BURST_HOLD_MS)

    if now_ms_v <= burst_until_ms:
        return (level, burst_until_ms)

    if rate_ewma <= BURST_OFF_RPS:
        return (0, 0.0)

    return (level, burst_until_ms)


def update_rate_from_second_bucket(tape: "BurstState", trade_ms: float) -> None:
    """
    Update tape.sec_count (current-second trades) and tape.rate_ewma.

    trade_ms is Binance T in milliseconds since epoch. We bucket by int(trade_ms//1000).
    EWMA is updated every message using the *current second's running count* so the
    badge reacts immediately rather than one second late.
    """
    sec = int(trade_ms // 1000.0)

    if tape.last_sec == 0:
        tape.last_sec = sec
        tape.sec_count = 0
        tape.rate_ewma = 0.0

    if sec != tape.last_sec:
        tape.last_sec = sec
        tape.sec_count = 0

    tape.sec_count += 1

    inst = float(tape.sec_count)
    tape.rate_ewma = inst if tape.rate_ewma <= 0.0 else (1.0 - BURST_ALPHA) * tape.rate_ewma + BURST_ALPHA * inst

BIG_MOVE_PX: float = 10.0  # you can tune


def ansi_bg_green(s: str) -> str:
    """Wrap a string in ANSI green background."""
    return f"\x1b[42m{s}\x1b[0m"


def ansi_bg_red(s: str) -> str:
    """Wrap a string in ANSI red background."""
    return f"\x1b[41m{s}\x1b[0m"


def ansi_yellow_bg(s: str) -> str:
    """Wrap a string in ANSI yellow background."""
    return f"\x1b[43m{s}\x1b[0m"


def maybe_highlight_line(s: str, burst_level: int) -> str:
    """Highlight the entire line when burst level is maximal."""
    if burst_level >= 3:
        return ansi_yellow_bg(s)
    return s


def format_tape_line(
    price: float,
    d_last: float,
    lag_ms: float,
    run_count: int,
    run_qty: float,
    rate_ewma: float,
    burst_level: int,
    is_buyer_maker: bool,
) -> str:
    """Format one tape line with run counters and burst indicator."""
    t = time.strftime("%H:%M:%S")
    badge = burst_badge(burst_level)
    side = "M" if is_buyer_maker else "T"
    rps = rate_ewma

    s = (
        f"{t}  {price:0.2f}  Δ {d_last:+0.2f}  "
        f"x{run_count:3d}  v {run_qty:7.3f}  r {rps:4.0f}/s  {badge}  {side}  lag {lag_ms:0.0f}ms"
    )
    return maybe_highlight_line(s, burst_level)


def update_tape_on_trade(
    tape: "BurstState",
    trade_ms: float,
    price: float,
    d_last: float,
    lag_ms: float,
    qty: float,
    is_buyer_maker: bool,
) -> None:
    """
    Option 2 tape behavior:

    - Append a new line only when price changes.
    - If price is unchanged, update the top line in place with run counters + burst indicator.
    - Maintain EWMA trades/sec and a short burst hold timer for highlighting.
    """
    tape.n_updates += 1
    tape.lag_ms = lag_ms

    dt_ms = trade_ms - tape.last_trade_ms if tape.last_trade_ms > 0.0 else 0.0
    tape.last_trade_ms = trade_ms

    # tape.rate_ewma = update_rate_ewma(tape.rate_ewma, dt_ms)
    # tape.burst_level, tape.burst_until_ms = update_burst_gate(
    #     tape.burst_level,
    #     tape.rate_ewma,
    #     trade_ms,
    #     tape.burst_until_ms,
    # )

    update_rate_from_second_bucket(tape, trade_ms)

    tape.burst_level, tape.burst_until_ms = update_burst_gate(
        tape.burst_level,
        tape.rate_ewma,
        trade_ms,
        tape.burst_until_ms,
    )

    if tape.last_px <= 0.0:
        tape.last_px = price

    if price != tape.last_px or not tape.lines:
        tape.last_px = price
        tape.run_count = 1
        tape.run_qty = qty
        tape.lines.appendleft(
            format_tape_line(
                price,
                d_last,
                lag_ms,
                tape.run_count,
                tape.run_qty,
                tape.rate_ewma,
                tape.burst_level,
                is_buyer_maker,
            )
        )
        return

    tape.run_count += 1
    tape.run_qty += qty
    tape.lines[0] = format_tape_line(
        price,
        d_last,
        lag_ms,
        tape.run_count,
        tape.run_qty,
        tape.rate_ewma,
        tape.burst_level,
        is_buyer_maker,
    )
