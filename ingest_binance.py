"""
Binance ingestion task.

Connects to the Binance WS trade stream and updates:
- state.driver (last, d_last, lag_ms, strike)
- state.tape_driver (burst tape newest-first)
- raw logger (each inbound payload)
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

import websockets

from config import BINANCE_STREAM_SUFFIX, BINANCE_WS_BASE
from raw_logger import AsyncJsonlLogger
from state import AppState, now_ms, push_burst_line
from state import AppState, now_ms, update_tape_on_trade


def _stream_name(symbol: str) -> str:
    """Return Binance stream name for a symbol (lowercase)."""
    return f"{symbol.lower()}{BINANCE_STREAM_SUFFIX}"


def _ws_url(symbol: str) -> str:
    """Return the Binance WS URL for a symbol trade stream."""
    return f"{BINANCE_WS_BASE}/{_stream_name(symbol)}"


def _parse_trade_msg(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a Binance trade message into a small dict; return None if not a trade."""
    if "p" not in msg:
        return None

    price = float(msg["p"])
    trade_ts_ms = float(msg.get("T", 0.0))
    event_ts_ms = float(msg.get("E", 0.0))

    return {
        "price": price,
        "trade_ts_ms": trade_ts_ms,
        "event_ts_ms": event_ts_ms,
        "trade_id": msg.get("t"),
        "qty": float(msg.get("q", 0.0)) if "q" in msg else 0.0,
        "is_buyer_maker": bool(msg.get("m", False)),
    }


async def binance_ws_task(state: AppState, logger: AsyncJsonlLogger, symbol: str) -> None:
    """Run Binance WS ingestion and update driver state + tape (state-only mutation)."""
    url = _ws_url(symbol)
    state.driver.symbol = symbol

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                async for raw in ws:
                    recv_ms = now_ms()
                    msg = json.loads(raw)

                    parsed = _parse_trade_msg(msg)
                    if parsed is None:
                        continue

                    price = float(parsed["price"])
                    trade_ts_ms = float(parsed["trade_ts_ms"])
                    lag_ms = max(0.0, recv_ms - trade_ts_ms) if trade_ts_ms > 0 else 0.0

                    prev = state.driver.last
                    state.driver.last = price
                    state.driver.d_last = price - prev if prev != 0.0 else 0.0
                    state.driver.lag_ms = lag_ms

                    if state.driver.strike == 0.0:
                        state.driver.strike = price

                    qty = float(parsed["qty"])
                    is_buyer_maker = bool(parsed["is_buyer_maker"])

                    trade_ts_ms = float(parsed["trade_ts_ms"])
                    trade_ms = trade_ts_ms if trade_ts_ms > 0.0 else now_ms()

                    update_tape_on_trade(
                        state.tape_driver,
                        trade_ms,
                        state.driver.last,
                        state.driver.d_last,
                        lag_ms,
                        qty,
                        is_buyer_maker,
                    )

                    logger.log(
                        {
                            "ts_local_ms": recv_ms,
                            "source": "binance",
                            "type": "trade",
                            "symbol": symbol,
                            "lag_ms": lag_ms,
                            "payload": msg,
                        }
                    )

        except Exception as e:
            try:
                logger.log(
                    {
                        "ts_local_ms": now_ms(),
                        "source": "binance",
                        "type": "error",
                        "symbol": symbol,
                        "err": repr(e),
                    }
                )
            except Exception:
                pass
            await _backoff_sleep()


async def _backoff_sleep() -> None:
    """Small reconnect backoff to avoid hot-looping on failures."""
    await asyncio_sleep_s(1.0)


async def asyncio_sleep_s(s: float) -> None:
    """Async sleep wrapper to avoid inline awaits in the reconnect path."""
    import asyncio

    await asyncio.sleep(s)
