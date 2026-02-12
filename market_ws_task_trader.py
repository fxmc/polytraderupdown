# market_ws_task_trader.py
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

try:
    import websockets
    from websockets.client import WebSocketClientProtocol
except Exception:  # pragma: no cover
    websockets = None  # type: ignore
    WebSocketClientProtocol = object  # type: ignore

from trader_state import TraderState

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ConnStatus:
    text: str = "mkt-ws: INIT"
    connected: bool = False
    last_msg_ts: int = 0
    last_err: str = ""


def _now_ts() -> int:
    return int(time.time())


def _d(x: Any) -> Decimal:
    return Decimal(str(x))


def _safe_json_loads(s: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(s)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _best_bid_ask_from_book(msg: Dict[str, Any]) -> tuple[Optional[Decimal], Optional[Decimal]]:
    bids = msg.get("bids") or []
    asks = msg.get("asks") or []

    best_bid = None
    best_ask = None

    try:
        if isinstance(bids, list) and bids:
            best_bid = max(_d(b["price"]) for b in bids if isinstance(b, dict) and "price" in b)
        if isinstance(asks, list) and asks:
            best_ask = min(_d(a["price"]) for a in asks if isinstance(a, dict) and "price" in a)
    except Exception:
        return None, None

    return best_bid, best_ask


async def _ping_loop(ws: WebSocketClientProtocol, status: ConnStatus, *, interval_s: int = 10) -> None:
    while True:
        await asyncio.sleep(interval_s)
        try:
            await ws.send("PING")
        except Exception as e:
            status.last_err = f"ping failed: {e!r}"
            raise


async def market_ws_task_trader(
    *,
    state: TraderState,
    conn_status: ConnStatus,
    # asset_id -> outcome name ("Up"/"Down")
    asset_id_to_outcome: Dict[str, str],
    url_base: str = "wss://ws-subscriptions-clob.polymarket.com",
    stop_event: Optional[asyncio.Event] = None,
    ping_interval_s: int = 10,
) -> None:
    """
    Connects to market WS and maintains bid/ask/mid marks for Up/Down.

    Subscribes with:
      {"assets_ids": [...], "type": "market"}

    Updates:
      state.quotes[outcome] = Quote(bid, ask, mid, ts)
      state.marks[outcome]  = mid
    """
    if websockets is None:
        raise RuntimeError("Missing dependency 'websockets'. Install with: pip install websockets")

    stop_event = stop_event or asyncio.Event()
    ws_url = f"{url_base.rstrip('/')}/ws/market"

    assets_ids = list(asset_id_to_outcome.keys())

    backoff_s = 1.0
    backoff_max = 30.0

    while not stop_event.is_set():
        conn_status.text = f"mkt-ws: CONNECTING {ws_url}"
        conn_status.connected = False
        conn_status.last_err = ""

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=None,
                close_timeout=5,
                max_queue=256,
            ) as ws:
                sub = {"assets_ids": assets_ids, "type": "market"}
                await ws.send(json.dumps(sub))
                state.recent_events.appendleft(f"MKT-WS: connected + subscribed assets={len(assets_ids)}")

                conn_status.connected = True
                conn_status.text = f"mkt-ws: CONNECTED assets={len(assets_ids)}"
                conn_status.last_msg_ts = _now_ts()
                backoff_s = 1.0

                ping_task = asyncio.create_task(_ping_loop(ws, conn_status, interval_s=ping_interval_s))

                try:
                    async for raw in ws:
                        if stop_event.is_set():
                            break

                        conn_status.last_msg_ts = _now_ts()

                        if isinstance(raw, (bytes, bytearray)):
                            raw = raw.decode("utf-8", errors="replace")
                        if not isinstance(raw, str):
                            continue
                        if raw in ("PONG", "PING"):
                            continue

                        msg = _safe_json_loads(raw)
                        if msg is None:
                            continue

                        # We only care about book updates for marks
                        if msg.get("event_type") != "book":
                            continue

                        asset_id = msg.get("asset_id")
                        if not isinstance(asset_id, str):
                            continue

                        outcome = asset_id_to_outcome.get(asset_id)
                        if outcome is None:
                            continue

                        bid, ask = _best_bid_ask_from_book(msg)
                        if bid is None and ask is None:
                            continue

                        # Compute mid
                        if bid is not None and ask is not None:
                            mid = (bid + ask) / Decimal("2")
                        elif bid is not None:
                            mid = bid
                        else:
                            mid = ask  # type: ignore

                        # Timestamp from feed is ms string; convert to epoch seconds for storage
                        ts_raw = msg.get("timestamp")
                        if ts_raw is not None:
                            try:
                                ts = int(int(str(ts_raw)) // 1000)
                            except Exception:
                                ts = _now_ts()
                        else:
                            ts = _now_ts()

                        state.set_quote(outcome=outcome, bid=bid, ask=ask, mid=mid, ts=ts)
                        state.marks[outcome] = mid

                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)

        except asyncio.CancelledError:
            conn_status.text = "mkt-ws: CANCELLED"
            conn_status.connected = False
            raise
        except Exception as e:
            conn_status.connected = False
            conn_status.last_err = repr(e)
            conn_status.text = f"mkt-ws: DISCONNECTED err={e!r}"
            # keep chatter low; UI header will show disconnect status

            sleep_s = backoff_s * (0.7 + 0.6 * random.random())
            await asyncio.sleep(min(sleep_s, backoff_max))
            backoff_s = min(backoff_s * 2.0, backoff_max)

    conn_status.text = "mkt-ws: STOPPED"
    conn_status.connected = False
