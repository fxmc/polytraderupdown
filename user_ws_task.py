# user_ws_task.py
from __future__ import annotations

import contextlib
import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    import websockets
    from websockets.client import WebSocketClientProtocol
except Exception as e:  # pragma: no cover
    websockets = None  # type: ignore
    WebSocketClientProtocol = object  # type: ignore

from trader_events import normalize_user_ws_message
from trader_state import TraderState, apply_order_event, apply_trade_event


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ConnStatus:
    """
    Minimal mutable object for UI to display.
    The UI can render status.text directly.
    """
    text: str = "user-ws: INIT"
    connected: bool = False
    last_msg_ts: int = 0
    last_err: str = ""


def _now_ts() -> int:
    return int(time.time())


def _safe_json_loads(s: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(s)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


async def _ping_loop(ws: WebSocketClientProtocol, status: ConnStatus, *, interval_s: int = 10) -> None:
    """
    Polymarket examples show sending literal "PING" periodically.
    We'll do that. If it errors, the outer connection loop will reconnect.
    """
    while True:
        await asyncio.sleep(interval_s)
        try:
            await ws.send("PING")
        except Exception as e:
            status.last_err = f"ping failed: {e!r}"
            raise


def _build_subscribe_msg(*, markets: List[str], api_key: str, secret: str, passphrase: str) -> Dict[str, Any]:
    """
    Subscribe message per docs/quickstart:
      {"markets": [...], "type": "user", "auth": {"apiKey":..., "secret":..., "passphrase":...}}
    """
    return {
        "markets": markets,
        "type": "user",
        "auth": {"apiKey": api_key, "secret": secret, "passphrase": passphrase},
        # optional in docs: "custom_feature_enabled": False
    }


async def user_ws_task(
    *,
    state: TraderState,
    conn_status: ConnStatus,
    markets: List[str],
    api_key: str,
    secret: str,
    passphrase: str,
    url_base: str = "wss://ws-subscriptions-clob.polymarket.com",
    stop_event: Optional[asyncio.Event] = None,
    ping_interval_s: int = 10,
    warn_every_n_bad_msgs: int = 10,
) -> None:
    """
    Authenticated Polymarket user-channel websocket consumer.

    Responsibilities:
      - connect/reconnect
      - send subscribe message on open
      - parse + normalize messages
      - apply to TraderState via apply_order_event/apply_trade_event
      - update conn_status for UI

    Notes:
      - Trades can emit multiple statuses (MATCHED -> MINED -> CONFIRMED -> ...).
        Your apply_trade_event() should be idempotent and only apply fill once on MATCHED.
    """
    if websockets is None:
        raise RuntimeError(
            "Missing dependency 'websockets'. Install with: pip install websockets"
        )

    stop_event = stop_event or asyncio.Event()
    ws_url = f"{url_base.rstrip('/')}/ws/user"

    # reconnect backoff
    backoff_s = 1.0
    backoff_s_max = 30.0

    bad_msg_count = 0

    while not stop_event.is_set():
        conn_status.text = f"user-ws: CONNECTING {ws_url}"
        conn_status.connected = False
        conn_status.last_err = ""

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=None,  # we do our own PING loop
                close_timeout=5,
                max_queue=256,
            ) as ws:
                # subscribe immediately
                sub = _build_subscribe_msg(
                    markets=markets,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                )
                await ws.send(json.dumps(sub))
                state.recent_events.appendleft(f"USER-WS: connected + subscribed markets={len(markets)}")

                conn_status.connected = True
                conn_status.text = f"user-ws: CONNECTED markets={len(markets)}"
                conn_status.last_msg_ts = _now_ts()

                # reset backoff after successful connection
                backoff_s = 1.0

                ping_task = asyncio.create_task(_ping_loop(ws, conn_status, interval_s=ping_interval_s))

                try:
                    async for raw in ws:
                        if stop_event.is_set():
                            break

                        conn_status.last_msg_ts = _now_ts()

                        # Some servers might send "PONG" or other keepalive text.
                        if isinstance(raw, (bytes, bytearray)):
                            try:
                                raw = raw.decode("utf-8", errors="replace")
                            except Exception:
                                continue
                        if not isinstance(raw, str):
                            continue

                        if raw in ("PONG", "PING"):
                            continue

                        msg = _safe_json_loads(raw)
                        if msg is None:
                            bad_msg_count += 1
                            if bad_msg_count % warn_every_n_bad_msgs == 0:
                                state.recent_events.appendleft(f"USER-WS: bad json msg x{bad_msg_count}")
                            continue

                        # Normalize + apply
                        try:
                            ev = normalize_user_ws_message(msg)
                        except Exception as e:
                            bad_msg_count += 1
                            state.recent_events.appendleft(
                                f"USER-WS: normalize failed ({e!r}) keys={list(msg.keys())}"
                            )
                            continue

                        try:
                            if ev.event_type == "order":
                                apply_order_event(state, ev)
                            else:
                                apply_trade_event(state, ev)
                        except Exception as e:
                            # Never let a state bug kill the WS task; scream and keep going.
                            state.recent_events.appendleft(f"USER-WS: APPLY ERROR {e!r}")
                            LOGGER.exception("User WS apply error")

                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)

        except asyncio.CancelledError:
            conn_status.text = "user-ws: CANCELLED"
            conn_status.connected = False
            raise
        except Exception as e:
            conn_status.connected = False
            conn_status.last_err = repr(e)
            conn_status.text = f"user-ws: DISCONNECTED err={e!r}"
            code = getattr(e, "code", None)
            reason = getattr(e, "reason", None)
            state.recent_events.appendleft(f"USER-WS: disconnected err={e!r} code={code} reason={reason!r}")

            # reconnect with jittered exponential backoff
            sleep_s = backoff_s * (0.7 + 0.6 * random.random())
            await asyncio.sleep(min(sleep_s, backoff_s_max))
            backoff_s = min(backoff_s * 2.0, backoff_s_max)

    conn_status.text = "user-ws: STOPPED"
    conn_status.connected = False
