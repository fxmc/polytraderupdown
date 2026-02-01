"""
Binance ingestion task.

Connects to the Binance WS trade stream and updates:
- state.driver (last, d_last, lag_ms, strike)
- state.tape_driver (burst tape newest-first)
- raw logger (each inbound payload)
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import websockets

from candles import (
    CandleSeries,
    TF_1M_MS,
    TF_5M_MS,
    TF_15M_MS,
    atr2_from_last3,
    bucket_start_ms,
    update_series_with_trade,
)
from config import BINANCE_STREAM_SUFFIX, BINANCE_WS_BASE
from raw_logger import AsyncJsonlLogger
from state import AppState, now_ms, update_tape_on_trade
from momentum import SecPriceBuffer, mom_pct, mom_points, update_sec_price
from mom_zscore import MomentumZConfig, MomentumZTracker


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
    cs_1m = CandleSeries(tf_ms=TF_1M_MS)
    cs_5m = CandleSeries(tf_ms=TF_5M_MS)
    cs_15m = CandleSeries(tf_ms=TF_15M_MS)

    mom_buf = SecPriceBuffer(max_secs=300)
    mom_horizons = [5, 10, 15, 30, 60]

    mom_cfg_fast = MomentumZConfig(horizons_s=mom_horizons, lookback=30, min_count=20)
    mom_cfg_slow = MomentumZConfig(horizons_s=mom_horizons, lookback=120, min_count=60)
    mom_z_fast = MomentumZTracker(cfg=mom_cfg_fast)
    mom_z_slow = MomentumZTracker(cfg=mom_cfg_slow)

    last_roll_15m_start: int = 0

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
                    qty = float(parsed["qty"])
                    is_buyer_maker = bool(parsed["is_buyer_maker"])
                    trade_ts_ms = float(parsed["trade_ts_ms"])
                    trade_ms = trade_ts_ms if trade_ts_ms > 0.0 else now_ms()
                    lag_ms = max(0.0, recv_ms - trade_ts_ms) if trade_ts_ms > 0 else 0.0

                    prev = state.driver.last
                    state.driver.last = price
                    state.driver.d_last = price - prev if prev != 0.0 else 0.0
                    state.driver.lag_ms = lag_ms

                    if state.driver.strike == 0.0:
                        state.driver.strike = price

                    new_sec = update_sec_price(mom_buf, trade_ms, price)

                    state.driver.mom_5s = mom_points(mom_buf, 5)
                    state.driver.mom_10s = mom_points(mom_buf, 10)
                    state.driver.mom_15s = mom_points(mom_buf, 15)
                    state.driver.mom_30s = mom_points(mom_buf, 30)
                    state.driver.mom_1m = mom_points(mom_buf, 60)
                    state.driver.mom_5m = mom_points(mom_buf, 300)

                    state.driver.mom_5s_pct = mom_pct(mom_buf, 5)
                    state.driver.mom_10s_pct = mom_pct(mom_buf, 10)
                    state.driver.mom_15s_pct = mom_pct(mom_buf, 15)
                    state.driver.mom_30s_pct = mom_pct(mom_buf, 30)
                    state.driver.mom_1m_pct = mom_pct(mom_buf, 60)
                    state.driver.mom_5m_pct = mom_pct(mom_buf, 300)

                    if new_sec:
                        mom_by_h = {
                            5: state.driver.mom_5s,
                            10: state.driver.mom_10s,
                            15: state.driver.mom_15s,
                            30: state.driver.mom_30s,
                            60: state.driver.mom_1m,
                        }

                        z_fast = mom_z_fast.update(mom_by_h)
                        z_slow = mom_z_slow.update(mom_by_h)

                        state.driver.mom_z_5s = float(z_fast.get(5, 0.0))
                        state.driver.mom_z_10s = float(z_fast.get(10, 0.0))
                        state.driver.mom_z_15s = float(z_fast.get(15, 0.0))
                        state.driver.mom_z_30s = float(z_fast.get(30, 0.0))
                        state.driver.mom_z_1m = float(z_fast.get(60, 0.0))

                        state.driver.mom_z_combo_fast = mom_z_fast.combo(z_fast)
                        state.driver.mom_z_combo_slow = mom_z_slow.combo(z_slow)

                    update_series_with_trade(cs_1m, trade_ms, price)
                    update_series_with_trade(cs_5m, trade_ms, price)
                    update_series_with_trade(cs_15m, trade_ms, price)

                    roll_15m_start = bucket_start_ms(trade_ms, TF_15M_MS)
                    if last_roll_15m_start == 0:
                        last_roll_15m_start = roll_15m_start

                    if roll_15m_start != last_roll_15m_start:
                        last_roll_15m_start = roll_15m_start
                        state.driver.strike = price

                        a1 = atr2_from_last3(cs_1m.closed)
                        a5 = atr2_from_last3(cs_5m.closed)
                        a15 = atr2_from_last3(cs_15m.closed)

                        if a1 > 0.0:
                            state.driver.atr_1m = a1
                        if a5 > 0.0:
                            state.driver.atr_5m = a5
                        if a15 > 0.0:
                            state.driver.atr_15m = a15

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
