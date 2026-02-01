from __future__ import annotations

import asyncio
import json
import time
import websockets

from raw_logger import AsyncJsonlLogger
from state import AppState, push_burst_line
from config import (
    PM_RTDS_URL,
    PM_PING_EVERY_S,
    PM_CHAINLINK_SYMBOL_BY_BINANCE,
)
from candles import TF_15M_MS, bucket_start_ms


def _now_ms() -> float:
    return time.time() * 1000.0


def build_subscribe_message() -> dict:
    # Known-working subscription format (matches your ref code)
    return {
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices_chainlink", "type": "update"},
        ],
    }


async def ping_loop(ws, every_sec: float) -> None:
    try:
        while True:
            await ws.send("PING")
            await asyncio.sleep(every_sec)
    except asyncio.CancelledError:
        return


def _coerce_epoch_ms(ts) -> float:
    if ts is None:
        return 0.0
    try:
        v = float(ts)  # handles int/float/numeric string
    except Exception:
        return 0.0
    # seconds -> ms heuristic
    if 1e9 <= v < 1e11:
        v *= 1000.0
    return v


async def polymarket_rtds_task(state: AppState, logger: AsyncJsonlLogger, *, binance_symbol: str) -> None:
    r = state.resolver

    want_symbol = PM_CHAINLINK_SYMBOL_BY_BINANCE.get(binance_symbol)
    if want_symbol is None:
        raise RuntimeError(f"No Chainlink symbol mapping for {binance_symbol}")

    backoff = 1.0
    last_roll_15m_start: int = 0

    while True:
        try:
            async with websockets.connect(
                PM_RTDS_URL,
                ping_interval=None,
                ping_timeout=None,
            ) as ws:
                await ws.send(json.dumps(build_subscribe_message()))

                logger.log(
                    {
                        "ts_local_ms": _now_ms(),
                        "source": "polymarket_rtds",
                        "type": "subscribe",
                        "want_symbol": want_symbol,
                    }
                )

                ping_task = asyncio.create_task(ping_loop(ws, PM_PING_EVERY_S))

                try:
                    async for raw in ws:
                        t0 = time.perf_counter()
                        recv_ms = _now_ms()

                        if raw == "PONG":
                            continue

                        msg = None
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue
                        if msg.get("type") != "update":
                            continue

                        payload = msg.get("payload")
                        if not isinstance(payload, dict):
                            continue

                        # Filter in-code (matches ref approach)
                        sym = payload.get("symbol")
                        if sym != want_symbol:
                            continue

                        px = payload.get("value")
                        if px is None:
                            continue

                        prev = r.last
                        r.last = float(px)
                        r.d_last = r.last - prev
                        r.updates += 1

                        ts_raw = payload.get("timestamp") or payload.get("time") or msg.get("timestamp")
                        event_ms = _coerce_epoch_ms(ts_raw) or float(recv_ms)

                        r.lag_raw_ms = max(0.0, recv_ms - event_ms)
                        r.lag_ms = max(0.0, r.lag_raw_ms - float(state.diag.clock_offset_ms))

                        roll_15m_start = bucket_start_ms(event_ms, TF_15M_MS)
                        if last_roll_15m_start == 0:
                            last_roll_15m_start = roll_15m_start

                        if r.strike == 0.0:
                            r.strike = r.last

                        # Roll resolver strike at the first update in a new 15m bucket
                        if roll_15m_start != last_roll_15m_start:
                            last_roll_15m_start = roll_15m_start
                            r.strike = r.last  # strike = first price print we see for that 15m window

                        push_burst_line(state.tape_resolver, r.last, r.d_last, r.lag_raw_ms, r.lag_ms)

                        logger.log(
                            {
                                "ts_local_ms": recv_ms,
                                "source": "polymarket_rtds",
                                "type": "price_update",
                                "symbol": sym,
                                "price": r.last,
                                "lag_raw_ms": r.lag_raw_ms,
                                "lag_ms": r.lag_ms,
                            }
                        )

                        state.diag.rtds_apply_ms = (time.perf_counter() - t0) * 1000.0

                finally:
                    ping_task.cancel()

            backoff = 1.0

        except Exception as e:
            logger.log(
                {
                    "ts_local_ms": _now_ms(),
                    "source": "polymarket_rtds",
                    "type": "error",
                    "error": repr(e),
                    "retry_in_s": backoff,
                }
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
