"""
Main entrypoint: wires UI + tasks.

Phase 3.2:
- Binance driver is real ingestion
- Book + resolver remain fake for now
"""

from __future__ import annotations

import asyncio
import argparse
import math
import os
import time

from prompt_toolkit.application import Application

from config import (
    FAKE_HZ,
    RAW_LOG_BATCH_SIZE,
    RAW_LOG_DIR,
    RAW_LOG_FLUSH_EVERY_S,
    RAW_LOG_MAX_QUEUE,
    UI_HZ,
    BINANCE_SYMBOL_DEFAULT,
)
from ingest_binance import binance_ws_task
from raw_logger import AsyncJsonlLogger
from state import AppState, OrderbookLevel, push_burst_line
from ui import build_keybindings, build_layout, ui_refresh_loop
from candles import TF_15M_MS, bucket_start_ms
from ingest_polymarket_rtds import polymarket_rtds_task


def init_state(state: AppState) -> None:
    """Initialize state with sane starting values."""
    state.book.yes_bids = [OrderbookLevel(px=0.5000 - 0.0100 * i, sz=1200.0 - 50.0 * i) for i in range(5)]
    state.book.yes_asks = [OrderbookLevel(px=0.5100 + 0.0100 * i, sz=1042.0 + 40.0 * i) for i in range(5)]

    state.book.no_bids = [OrderbookLevel(px=0.5000 - 0.0100 * i, sz=1200.0 - 50.0 * i) for i in range(5)]
    state.book.no_asks = [OrderbookLevel(px=0.5100 + 0.0100 * i, sz=1042.0 + 40.0 * i) for i in range(5)]

    state.book.fv_yes = 0.5055
    state.book.fv_no = 0.4955

    for _ in range(20):
        state.tape_driver.lines.append("")

    for _ in range(5):
        state.tape_resolver.lines.append("")


async def fake_book_loop(state: AppState, hz: float) -> None:
    """Fake book updates (left pane only), stable cadence."""
    period = 1.0 / hz
    t0 = time.monotonic()

    while True:
        t = time.monotonic() - t0
        _update_fake_orderbook(state, t)
        await asyncio.sleep(period)


async def fake_resolver_loop(state: AppState, hz: float) -> None:
    """Fake resolver updates (right-bottom only), stable cadence."""
    period = 1.0 / hz
    t0 = time.monotonic()

    while True:
        t = time.monotonic() - t0
        _update_fake_resolver(state, t)
        await asyncio.sleep(period)


def _update_fake_orderbook(state: AppState, t: float) -> None:
    """Fake book updates: wiggle best sizes, pulse, and FV."""
    state.book.updates += 1
    state.book.pulse = "*" if (state.book.updates % 2 == 0) else "."
    state.book.last_change_ms = 100.0

    wiggle = 30.0 * (1.0 + math.sin(t * 3.0))

    if state.book.yes_bids:
        state.book.yes_bids[0].sz = 1200.0 + wiggle
    if state.book.yes_asks:
        state.book.yes_asks[0].sz = 1042.0 + 0.8 * wiggle

    if state.book.no_bids:
        state.book.no_bids[0].sz = 1200.0 + 0.9 * wiggle
    if state.book.no_asks:
        state.book.no_asks[0].sz = 1042.0 + 0.7 * wiggle


def _update_fake_resolver(state: AppState, t: float) -> None:
    """Fake resolver lags driver and pushes resolver tape."""
    r = state.resolver
    d = state.driver

    prev_r = r.last

    if d.last > 0.0:
        r.last = d.last - 5.0 * math.sin(t * 1.1)
        r.lag_ms = d.lag_ms + 35.0
    else:
        r.last = r.last + 0.0
        r.lag_ms = 0.0

    r.d_last = r.last - prev_r
    r.mid = r.last - 0.5
    r.last_trade = r.last + 0.25 * math.sin(t * 2.0)
    r.updates += 1

    push_burst_line(state.tape_resolver, r.last, r.d_last, r.lag_ms)


def _raw_log_path() -> str:
    """Return the default raw log path for this run."""
    os.makedirs(RAW_LOG_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(RAW_LOG_DIR, f"raw_{ts}.jsonl")


async def run_app(*, symbol: str, strike: float | None, polym_strike: float | None) -> None:
    """Create and run the application."""
    state = AppState()
    init_state(state)

    # Optional strike override (useful if you start mid-market and already know the strike).
    # We also set the current 15m window start/expiry based on local time so TTE/FV can work immediately.
    if strike is not None:
        state.driver.strike = float(strike)
        now_ms = time.time() * 1000.0
        ws = bucket_start_ms(now_ms, TF_15M_MS)
        state.driver.win_start_ms = float(ws)
        state.driver.expiry_ms = float(ws + TF_15M_MS)

    if polym_strike is not None:
        state.resolver.strike = float(polym_strike)

    log_path = _raw_log_path()
    logger = AsyncJsonlLogger.create(
        path=log_path,
        max_queue=RAW_LOG_MAX_QUEUE,
        batch_size=RAW_LOG_BATCH_SIZE,
        flush_every_s=RAW_LOG_FLUSH_EVERY_S,
    )
    logger.start()
    logger.log({"ts_local_ms": time.time() * 1000.0, "source": "app", "type": "logger_start"})

    layout = build_layout(state)
    kb = build_keybindings(state)
    app = Application(layout=layout, key_bindings=kb, full_screen=True)

    asyncio.create_task(ui_refresh_loop(app, hz=UI_HZ))
    asyncio.create_task(fake_book_loop(state, hz=FAKE_HZ))
    asyncio.create_task(polymarket_rtds_task(state, logger, binance_symbol=symbol))

    asyncio.create_task(binance_ws_task(state, logger, symbol=symbol))

    try:
        await app.run_async()
    finally:
        await logger.stop()


def main() -> None:
    """Program entrypoint."""
    p = argparse.ArgumentParser(description="Polymarket terminal dashboard (Binance driver).")
    p.add_argument(
        "--symbol",
        default=BINANCE_SYMBOL_DEFAULT,
        help=f"Binance symbol to subscribe to (default: {BINANCE_SYMBOL_DEFAULT})",
    )
    p.add_argument(
        "--strike",
        type=float,
        default=None,
        help="Optional strike override (set if starting mid-market).",
    )
    p.add_argument(
        "--polym-strike",
        type=float,
        default=None,
        help="Optional Polymarket strike override (shown in resolver pane only).",
    )

    args = p.parse_args()

    asyncio.run(run_app(symbol=args.symbol, strike=args.strike, polym_strike=args.polym_strike))


if __name__ == "__main__":
    main()
