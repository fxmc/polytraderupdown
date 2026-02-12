# trader_events.py
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Literal

EventType = Literal["order", "trade"]
Side = Literal["BUY", "SELL"]
Outcome = Literal["Up", "Down"]

OrderLifecycle = Literal["PLACEMENT", "UPDATE", "CANCELLATION"]
TradeStatus = Literal["MATCHED", "MINED", "CONFIRMED", "RETRYING", "FAILED"]

def _d(x: Any) -> Decimal:
    # Polymarket sends numeric fields as strings
    return Decimal(str(x))

def _i(x: Any) -> int:
    # epoch seconds, often encoded as string
    return int(str(x))

@dataclass(frozen=True, slots=True)
class MakerOrderFill:
    asset_id: str
    order_id: str
    outcome: Outcome
    owner: str
    price: Decimal
    matched_amount: Decimal

@dataclass(frozen=True, slots=True)
class TradeEvent:
    event_type: Literal["trade"]
    trade_id: str
    asset_id: str
    market: str  # condition_id
    outcome: Outcome
    owner: str  # api key of event owner
    trade_owner: str  # api key of trade owner
    side: Side
    price: Decimal
    size: Decimal
    status: TradeStatus
    taker_order_id: str
    maker_orders: List[MakerOrderFill]
    matchtime: int
    timestamp: int
    last_update: int
    type: Literal["TRADE"]

@dataclass(frozen=True, slots=True)
class OrderEvent:
    event_type: Literal["order"]
    order_id: str
    asset_id: str
    market: str
    outcome: Outcome
    owner: str
    order_owner: str
    side: Side
    price: Decimal
    original_size: Decimal
    size_matched: Decimal
    associate_trades: Optional[List[str]]
    timestamp: int
    lifecycle: OrderLifecycle
    type: OrderLifecycle

def normalize_user_ws_message(msg: Dict[str, Any]) -> TradeEvent | OrderEvent:
    """
    Normalize Polymarket user-channel WS payloads into strongly typed events.
    Raises ValueError for unknown event shapes.
    """
    et = msg.get("event_type")

    if et == "trade":
        mos_raw = msg.get("maker_orders") or []
        maker_orders: List[MakerOrderFill] = []
        for mo in mos_raw:
            maker_orders.append(
                MakerOrderFill(
                    asset_id=str(mo["asset_id"]),
                    matched_amount=_d(mo["matched_amount"]),
                    order_id=str(mo["order_id"]),
                    outcome=str(mo["outcome"]),  # "Up"/"Down"
                    owner=str(mo["owner"]),
                    price=_d(mo["price"]),
                )
            )

        status = str(msg["status"])
        if status not in ("MATCHED", "MINED", "CONFIRMED", "RETRYING", "FAILED"):
            # allow forward compatibility without crashing the app
            status = "MATCHED"

        return TradeEvent(
            event_type="trade",
            trade_id=str(msg["id"]),
            asset_id=str(msg["asset_id"]),
            market=str(msg["market"]),
            matchtime=_i(msg["matchtime"]),
            outcome=str(msg["outcome"]),
            owner=str(msg["owner"]),
            price=_d(msg["price"]),
            side=str(msg["side"]),
            size=_d(msg["size"]),
            status=status,  # type: ignore
            taker_order_id=str(msg["taker_order_id"]),
            timestamp=_i(msg["timestamp"]),
            trade_owner=str(msg["trade_owner"]),
            last_update=_i(msg["last_update"]),
            type="TRADE",
            maker_orders=maker_orders,
        )

    if et == "order":
        assoc = msg.get("associate_trades")
        associate_trades = None
        if isinstance(assoc, list):
            associate_trades = [str(x) for x in assoc]

        lifecycle = str(msg["type"])
        if lifecycle not in ("PLACEMENT", "UPDATE", "CANCELLATION"):
            lifecycle = "UPDATE"

        return OrderEvent(
            event_type="order",
            order_id=str(msg["id"]),
            asset_id=str(msg["asset_id"]),
            market=str(msg["market"]),
            associate_trades=associate_trades,
            outcome=str(msg["outcome"]),
            owner=str(msg["owner"]),
            price=_d(msg["price"]),
            side=str(msg["side"]),
            original_size=_d(msg["original_size"]),
            size_matched=_d(msg["size_matched"]),
            timestamp=_i(msg["timestamp"]),
            lifecycle=lifecycle,  # type: ignore
            type=lifecycle,       # keep both; sometimes handy for raw display
            order_owner=str(msg["order_owner"]),
        )

    raise ValueError(f"Unknown user WS message event_type={et!r}")
