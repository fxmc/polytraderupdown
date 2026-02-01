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

from clock_sync import cloudflare_ntp_offset_task
from config import (
    RAW_LOG_BATCH_SIZE,
    RAW_LOG_DIR,
    RAW_LOG_FLUSH_EVERY_S,
    RAW_LOG_MAX_QUEUE,
    UI_HZ,
    BINANCE_SYMBOL_DEFAULT,
)
from ingest_binance import binance_ws_task
from maker_metrics import danger_score_bid, danger_score_ask
from raw_logger import AsyncJsonlLogger
from state import AppState, OrderbookLevel, push_burst_line
from ui import build_keybindings, build_layout, ui_refresh_loop
from candles import TF_15M_MS, bucket_start_ms
from ingest_polymarket_rtds import polymarket_rtds_task
from ingest_polymarket_clob import polymarket_clob_autoresolve_task


def init_state(state: AppState) -> None:
    """Initialize state with sane starting values."""
    # Start empty; live CLOB ingestion will populate these.
    state.book.yes_bids = []
    state.book.yes_asks = []
    state.book.no_bids = []
    state.book.no_asks = []

    state.book.fv_yes = 0.5000
    state.book.fv_no = 0.5000

    for _ in range(20):
        state.tape_driver.lines.append("")

    for _ in range(5):
        state.tape_resolver.lines.append("")


def _raw_log_path() -> str:
    """Return the default raw log path for this run."""
    os.makedirs(RAW_LOG_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    return os.path.join(RAW_LOG_DIR, f"raw_{ts}.jsonl")


async def loop_drift_task(state: AppState) -> None:
    """
    Measures event loop scheduling delay (authoritative).
    If this climbs to hundreds/thousands of ms, your process is the bottleneck.
    """
    import time

    period = 0.05  # 50ms
    next_t = time.perf_counter() + period

    # Track a rolling worst over a short window (we'll decay it)
    worst = 0.0
    decay = 0.95  # per tick

    while True:
        await asyncio.sleep(max(0.0, next_t - time.perf_counter()))
        now = time.perf_counter()
        drift = max(0.0, now - next_t) * 1000.0

        # exponential decay on worst so it doesn't grow forever
        worst = max(drift, worst * decay)

        state.diag.loop_drift_ms = drift
        state.diag.loop_drift_worst_ms = worst

        next_t += period


async def run_app(
    *,
    symbol: str,
    strike: float | None = None,
    polym_strike: float | None = None,
) -> None:
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

    asyncio.create_task(
        polymarket_clob_autoresolve_task(
            state,
            logger,
            binance_symbol=symbol,
            max_levels=5,
        )
    )

    asyncio.create_task(polymarket_rtds_task(state, logger, binance_symbol=symbol))
    asyncio.create_task(binance_ws_task(state, logger, symbol=symbol))
    asyncio.create_task(metrics_1hz_task(state, logger))
    asyncio.create_task(loop_drift_task(state))
    asyncio.create_task(cloudflare_ntp_offset_task(state, every_s=120.0))

    try:
        await app.run_async()
    finally:
        await logger.stop()


async def metrics_1hz_task(state: AppState, logger: AsyncJsonlLogger) -> None:
    """
    1Hz snapshot logger. Reads semantic state and logs lightweight metrics.
    No WS payloads, no heavy compute.
    """
    import time
    while True:
        # align roughly to wall-clock seconds
        now = time.time()
        await asyncio.sleep(max(0.0, 1.0 - (now - int(now))))

        ts = time.time() * 1000.0

        y = state.book.metrics.yes
        n = state.book.metrics.no

        # YES components
        y_db, y_db_c = danger_score_bid(
            dep_ema=y.bid_dep_ema,
            flicker_ema=y.bid_flicker_ema,
            spread=y.spread,
            micro_bias=y.micro_bias,
        )
        y_da, y_da_c = danger_score_ask(
            dep_ema=y.ask_dep_ema,
            flicker_ema=y.ask_flicker_ema,
            spread=y.spread,
            micro_bias=y.micro_bias,
        )

        # NO components
        n_db, n_db_c = danger_score_bid(
            dep_ema=n.bid_dep_ema,
            flicker_ema=n.bid_flicker_ema,
            spread=n.spread,
            micro_bias=n.micro_bias,
        )
        n_da, n_da_c = danger_score_ask(
            dep_ema=n.ask_dep_ema,
            flicker_ema=n.ask_flicker_ema,
            spread=n.spread,
            micro_bias=n.micro_bias,
        )


        logger.log(
            {
                "ts_local_ms": ts,
                "source": "metrics",
                "type": "book_1s",
                "clob_book_lag_raw_ms": state.book.lag_raw_ms,
                "clob_book_lag_ms": state.book.lag_ms,
                "clob_updates": state.book.updates,
                "clob_l1": {
                    "yes_bid": state.book.yes_bids[0].px if state.book.yes_bids else 0.0,
                    "yes_ask": state.book.yes_asks[0].px if state.book.yes_asks else 0.0,
                    "no_bid":  state.book.no_bids[0].px  if state.book.no_bids  else 0.0,
                    "no_ask":  state.book.no_asks[0].px  if state.book.no_asks  else 0.0,
                },
                # Optional: include slug/id if you added it to state.book
                "market_slug": getattr(state.book, "market_slug", ""),
                "market_id": getattr(state.book, "market_id", 0),

                "maker": {
                    "yes": {
                        "micro_bias": state.book.metrics.yes.micro_bias,
                        "spread": state.book.metrics.yes.spread,
                        "bid_dep_ema": state.book.metrics.yes.bid_dep_ema,
                        "ask_dep_ema": state.book.metrics.yes.ask_dep_ema,
                        "bid_flicker_ema": state.book.metrics.yes.bid_flicker_ema,
                        "ask_flicker_ema": state.book.metrics.yes.ask_flicker_ema,
                        "bid_age_ms": max(0.0,
                                          ts - state.book.metrics.yes.bid_last_change_ms) if state.book.metrics.yes.bid_last_change_ms > 0 else 0.0,
                        "ask_age_ms": max(0.0,
                                          ts - state.book.metrics.yes.ask_last_change_ms) if state.book.metrics.yes.ask_last_change_ms > 0 else 0.0,
                        "danger_bid": state.book.metrics.yes.danger_bid,
                        "danger_ask": state.book.metrics.yes.danger_ask,
                        "danger_bid_components": y_db_c,
                        "danger_ask_components": y_da_c,

                    },
                    "no": {
                        "micro_bias": state.book.metrics.no.micro_bias,
                        "spread": state.book.metrics.no.spread,
                        "bid_dep_ema": state.book.metrics.no.bid_dep_ema,
                        "ask_dep_ema": state.book.metrics.no.ask_dep_ema,
                        "bid_flicker_ema": state.book.metrics.no.bid_flicker_ema,
                        "ask_flicker_ema": state.book.metrics.no.ask_flicker_ema,
                        "bid_age_ms": max(0.0,
                                          ts - state.book.metrics.no.bid_last_change_ms) if state.book.metrics.no.bid_last_change_ms > 0 else 0.0,
                        "ask_age_ms": max(0.0,
                                          ts - state.book.metrics.no.ask_last_change_ms) if state.book.metrics.no.ask_last_change_ms > 0 else 0.0,
                        "danger_bid": state.book.metrics.no.danger_bid,
                        "danger_ask": state.book.metrics.no.danger_ask,
                        "danger_bid_components": n_db_c,
                        "danger_ask_components": n_da_c,
                    },
                },
                "canon": {
                    "mid": state.book.canon.mid,
                    "spread": state.book.canon.spread,
                    "mid_vel_ema": state.book.canon.mid_vel_ema,
                    "touch_cross_risk": state.book.canon.touch_cross_risk,
                },
                "align": {
                    "pending": state.align.pending,
                    "resp_last_ms": state.align.resp_last_ms,
                    "resp_ema_ms": state.align.resp_ema_ms,
                    "n_impulses": state.align.n_impulses,
                    "n_matched": state.align.n_matched,
                    "n_missed": state.align.n_missed,
                    "dir": state.align.dir,
                    "mid0": state.align.mid0,
                    "spread0": state.align.spread0,
                    "expires_ms": state.align.expires_ms,
                },
            }
        )


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

    asyncio.run(
        run_app(
            symbol=args.symbol,
            strike=args.strike,
            polym_strike=args.polym_strike,
        )
    )


if __name__ == "__main__":
    main()
