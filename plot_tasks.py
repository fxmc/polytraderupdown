# plot_tasks.py
from __future__ import annotations

import asyncio
import time

from state import AppState, reset_plot_buffers


def _mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    if bid <= 0.0 or ask >= 1.0:
        return None
    if ask <= bid:
        return None
    return 0.5 * (bid + ask)


async def plot_sampler_task(state: AppState) -> None:
    """
    Sample state.book L1s at a fixed cadence and append to ring buffers.
    IMPORTANT: do *not* run inside WS ingest tasks.
    """
    next_t = time.perf_counter()

    while True:
        # --- cadence (supports live changes to sample_hz) ---
        hz = max(0.1, float(state.plot_ctl.sample_hz))  # allow <1Hz if you ever want
        dt = 1.0 / hz

        # detect 15m roll using driver window
        ws = float(state.driver.win_start_ms)
        ex = float(state.driver.expiry_ms)

        ctl = state.plot_ctl
        if ws > 0.0 and ws != ctl.active_win_start_ms:
            ctl.active_win_start_ms = ws
            ctl.active_expiry_ms = ex
            reset_plot_buffers(state)

        # sleep to next tick (monotonic schedule, not drift-prone)
        next_t += dt
        await asyncio.sleep(max(0.0, next_t - time.perf_counter()))

        # L1 extraction
        yb = state.book.yes_bids[0].px if state.book.yes_bids else None
        ya = state.book.yes_asks[0].px if state.book.yes_asks else None
        nb = state.book.no_bids[0].px if state.book.no_bids else None
        na = state.book.no_asks[0].px if state.book.no_asks else None

        ym = _mid(yb, ya)
        nm = _mid(nb, na)

        # If missing, skip (prevents flatlines with junk values)
        if ym is None and nm is None:
            continue

        ts = time.time()

        ps = state.plot
        ps.ts_s.append(ts)
        ps.yes_bid.append(yb if yb is not None else float("nan"))
        ps.yes_ask.append(ya if ya is not None else float("nan"))
        ps.no_bid.append(nb if nb is not None else float("nan"))
        ps.no_ask.append(na if na is not None else float("nan"))

        ps.yes_mid.append(ym if ym is not None else float("nan"))
        ps.no_mid.append(nm if nm is not None else float("nan"))

        # optional overlays (model and/or book FV)
        ps.fv_yes.append(float(state.book.fv_yes_nd) if state.book.fv_yes_nd > 0 else float("nan"))
        ps.fv_no.append(float(state.book.fv_no_nd) if state.book.fv_no_nd > 0 else float("nan"))
        # ps.fv_yes_nd.append(float(state.book.fv_yes_nd) if state.book.fv_yes_nd > 0 else float("nan"))
        # ps.fv_no_nd.append(float(state.book.fv_no_nd) if state.book.fv_no_nd > 0 else float("nan"))

        ctl.n_samples += 1
        ctl.last_sample_ms = ts * 1000.0

        # OPTIONAL: if you add this field to PlotControlState:
        # ctl.sample_version += 1
