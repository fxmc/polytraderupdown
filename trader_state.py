# trader_state.py
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Deque, Dict, Set, Optional
from collections import deque

from trader_events import OrderEvent, TradeEvent


@dataclass(slots=True)
class Quote:
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    mid: Optional[Decimal]
    ts: int


@dataclass(slots=True)
class Position:
    shares: Decimal = Decimal("0")
    cost: Decimal = Decimal("0")       # cost basis of remaining shares
    realized_pnl: Decimal = Decimal("0")

    @property
    def avg_price(self) -> Decimal:
        if self.shares == 0:
            return Decimal("0")
        return self.cost / self.shares


@dataclass(slots=True)
class OrderInfo:
    order_id: str
    outcome: str
    side: str
    price: Decimal
    original_size: Decimal
    size_matched: Decimal
    lifecycle: str
    timestamp: int


@dataclass(slots=True)
class TraderState:
    positions: Dict[str, Position] = field(default_factory=lambda: {"Up": Position(), "Down": Position()})
    cash: Decimal = Decimal("0")  # optional, but useful for total P&L sanity

    orders: Dict[str, OrderInfo] = field(default_factory=dict)
    recent_events: Deque[str] = field(default_factory=lambda: deque(maxlen=200))
    recent_trades: Deque[TradeEvent] = field(default_factory=lambda: deque(maxlen=200))

    # mark prices from market WS: outcome -> mid (or whatever you choose)
    marks: Dict[str, Decimal] = field(default_factory=dict)  # "Up"/"Down" -> mark

    # --- NEW ---
    quotes: Dict[str, Quote] = field(default_factory=dict)

    applied_trade_fills: Set[str] = field(default_factory=set)      # trade_id fills applied (MATCHED only)
    trade_status_by_id: Dict[str, str] = field(default_factory=dict)  # trade_id -> latest status
    trade_first_seen_ts: Dict[str, int] = field(default_factory=dict) # trade_id -> timestamp (epoch sec)

    def set_quote(
        self,
        *,
        outcome: str,
        bid: Optional[Decimal],
        ask: Optional[Decimal],
        mid: Optional[Decimal],
        ts: int,
    ) -> None:
        self.quotes[outcome] = Quote(bid=bid, ask=ask, mid=mid, ts=ts)


def dollar_exposure(st: TraderState) -> tuple[Decimal, Decimal]:
    """
    Returns ($exposure_up, $exposure_down) using current mid marks if present.
    Exposure = shares * mark.
    """
    up = st.positions.get("Up", Position())
    down = st.positions.get("Down", Position())

    mup = st.marks.get("Up")
    mdn = st.marks.get("Down")

    exp_up = (up.shares * mup) if mup is not None else Decimal("0")
    exp_dn = (down.shares * mdn) if mdn is not None else Decimal("0")
    return exp_up, exp_dn


def apply_order_event(st: TraderState, ev: OrderEvent) -> None:
    st.orders[ev.order_id] = OrderInfo(
        order_id=ev.order_id,
        outcome=ev.outcome,
        side=ev.side,
        price=ev.price,
        original_size=ev.original_size,
        size_matched=ev.size_matched,
        lifecycle=ev.lifecycle,
        timestamp=ev.timestamp,
    )

    trades = ""
    if ev.associate_trades:
        trades = f" trades={len(ev.associate_trades)}"

    st.recent_events.appendleft(
        f"ORDER {ev.lifecycle} {ev.outcome} {ev.side} px={ev.price} orig={ev.original_size} matched={ev.size_matched}{trades} id={ev.order_id[:10]}…"
    )


def apply_trade_event(st: TraderState, ev: TradeEvent) -> None:
    # Always update lifecycle tracking
    st.trade_status_by_id[ev.trade_id] = ev.status
    st.trade_first_seen_ts.setdefault(ev.trade_id, ev.timestamp)

    # Update UI feed
    st.recent_trades.appendleft(ev)
    st.recent_events.appendleft(
        f"TRADE {ev.status} {ev.outcome} {ev.side} px={ev.price} sz={ev.size} id={ev.trade_id[:8]}…"
    )

    # If FAILED arrives, barf loudly (and do not apply fill here)
    if ev.status == "FAILED":
        st.recent_events.appendleft(
            f"!!! BARF: trade FAILED after MATCHED? id={ev.trade_id[:8]}… outcome={ev.outcome} side={ev.side} px={ev.price} sz={ev.size}"
        )
        return

    # Apply fills only once, and only on MATCHED
    if ev.status != "MATCHED":
        return
    if ev.trade_id in st.applied_trade_fills:
        return

    st.applied_trade_fills.add(ev.trade_id)

    pos = st.positions.setdefault(ev.outcome, Position())
    px = ev.price
    sz = ev.size

    if ev.side == "BUY":
        pos.shares += sz
        pos.cost += px * sz
        st.cash -= px * sz
        return

    if ev.side == "SELL":
        sell_sz = sz
        if pos.shares > 0 and sell_sz > pos.shares:
            sell_sz = pos.shares

        if pos.shares > 0:
            avg = pos.avg_price
            pos.realized_pnl += (px - avg) * sell_sz
            pos.shares -= sell_sz
            pos.cost -= avg * sell_sz
            st.cash += px * sell_sz
        else:
            pos.shares -= sell_sz
            pos.cost -= px * sell_sz
            st.cash += px * sell_sz
        return


def scenario_pnl(st: TraderState) -> tuple[Decimal, Decimal]:
    """
    Returns (pnl_if_up, pnl_if_down) relative to current cost basis + realized.
    Settlement is 1.0 for winning outcome and 0.0 for losing.
    """
    up = st.positions.get("Up", Position())
    down = st.positions.get("Down", Position())

    val_if_up = up.shares * Decimal("1") + down.shares * Decimal("0")
    val_if_down = up.shares * Decimal("0") + down.shares * Decimal("1")

    cost_total = up.cost + down.cost
    realized = up.realized_pnl + down.realized_pnl

    return (val_if_up - cost_total + realized, val_if_down - cost_total + realized)


def mark_to_market_pnl(st: TraderState) -> Decimal:
    """
    Uses st.marks[outcome] (e.g., mid price) to compute unrealized + realized.
    """
    total = Decimal("0")
    for outcome, pos in st.positions.items():
        mark = st.marks.get(outcome)
        if mark is None:
            continue
        total += (mark - pos.avg_price) * pos.shares + pos.realized_pnl
    return total


def check_stuck_trades(st: TraderState, now_ts: int, *, timeout_s: int = 30) -> None:
    """
    Barf when a trade was MATCHED but doesn't progress to MINED/CONFIRMED soon.
    We don't remove/rollback the fill automatically here; we just alert loudly.
    """
    for tid in list(st.applied_trade_fills):
        first = st.trade_first_seen_ts.get(tid)
        if first is None:
            continue
        age = now_ts - first
        if age < timeout_s:
            continue

        status = st.trade_status_by_id.get(tid, "MATCHED")
        if status in ("CONFIRMED", "MINED", "FAILED"):
            continue

        # still stuck in MATCHED/RETRYING after timeout
        st.recent_events.appendleft(
            f"!!! BARF: trade stuck ({status}) for {age}s after MATCHED. id={tid[:8]}…"
        )
        # Optional: don't spam repeatedly
        # mark as 'already warned' by flipping first_seen forward
        st.trade_first_seen_ts[tid] = now_ts
