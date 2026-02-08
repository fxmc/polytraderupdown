# plot_tasks.py
from __future__ import annotations

import asyncio
import time
import math

from plot_ipc import PlotSnap, PlotCtl
from state import AppState, reset_plot_buffers


def _mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    if bid <= 0.0 or ask >= 1.0:
        return None
    if ask <= bid:
        return None
    return 0.5 * (bid + ask)


def _nan(x: float | None) -> float:
    return float("nan") if x is None else float(x)


def _safe_spread(b: float | None, a: float | None) -> float:
    if b is None or a is None:
        return float("nan")
    if a <= b:
        return float("nan")
    return float(a - b)


def _imb(levels_bid, levels_ask, n: int) -> float:
    # mirror-safe imbalance definition
    b = sum(float(x.sz) for x in levels_bid[:n]) if levels_bid else 0.0
    a = sum(float(x.sz) for x in levels_ask[:n]) if levels_ask else 0.0
    denom = b + a
    if denom <= 1e-12:
        return 0.0
    return (b - a) / denom


async def plot_sampler_task(state: AppState, plot_q) -> None:
    next_t = time.perf_counter()

    while True:
        hz = max(0.1, float(state.plot_ctl.sample_hz))
        dt = 1.0 / hz

        # --- clock gate: do not plot until offset is populated ---
        src = getattr(state.diag, "clock_offset_src", "")
        off = float(getattr(state.diag, "clock_offset_ms", 0.0) or 0.0)

        # "waiting" string comes from your clock task; treat that as not ready
        offset_ready = (abs(off) > 1.0) and (src and "waiting" not in src)

        if not state.plot_ctl.clock_ready:
            if not offset_ready:
                # don’t emit anything yet; avoid the 0ms -> 10s jump
                await asyncio.sleep(0.25)
                continue

            # first time we become ready: latch + hard reset plot buffers
            state.plot_ctl.clock_ready = True
            reset_plot_buffers(state)

            # also tell plot process to clear and anchor at current window start
            ws = float(state.plot_ctl.active_win_start_ms or 0.0)
            try:
                state.plot_ctl_q.put_nowait(
                    PlotCtl(
                        show=state.plot_ctl.show,
                        enabled=state.plot_ctl.enabled,
                        reset=True,
                        win_start_s=(ws / 1000.0) if ws > 0.0 else 0.0,
                    )
                )
            except Exception:
                pass

        # 15m roll reset
        ws = float(state.book.market_start_ms) if state.book.market_start_ms else 0.0
        ex = float(state.book.market_end_ms) if state.book.market_end_ms else 0.0
        ctl = state.plot_ctl

        if ws > 0.0 and ws != ctl.active_win_start_ms:
            ctl.active_win_start_ms = ws
            ctl.active_expiry_ms = ex
            reset_plot_buffers(state)
            try:
                state.plot_ctl_q.put_nowait(
                    PlotCtl(
                        show=ctl.show,
                        enabled=ctl.enabled,
                        reset=True,
                        win_start_s=ws / 1000.0
                    )
                )
            except Exception:
                pass

        # schedule tick (skip backlog if we fell behind)
        next_t += dt
        now = time.perf_counter()
        if next_t < now - 2.0 * dt:
            next_t = now
        await asyncio.sleep(max(0.0, next_t - time.perf_counter()))

        # Only send if plotting window is meant to be shown
        if not ctl.show:
            continue

        # --- Polymarket L1 ---
        yb = state.book.yes_bids[0].px if state.book.yes_bids else None
        ya = state.book.yes_asks[0].px if state.book.yes_asks else None
        nb = state.book.no_bids[0].px if state.book.no_bids else None
        na = state.book.no_asks[0].px if state.book.no_asks else None

        ym = _mid(yb, ya)
        nm = _mid(nb, na)


        # --- FV (NO DRIFT) ---
        fv_y = float(state.book.fv_yes_nd) if state.book.fv_yes_nd > 0.0 else float("nan")
        fv_n = float(state.book.fv_no_nd)  if state.book.fv_no_nd  > 0.0 else float("nan")

        # --- Binance ---
        px_bin = float(state.driver.last) if state.driver.last > 0.0 else float("nan")
        strike = float(state.driver.strike) if state.driver.strike > 0.0 else float("nan")

        # --- Spread or imbalance ---
        yes_sp = _safe_spread(yb, ya)
        no_sp  = _safe_spread(nb, na)
        imb_y1 = _imb(state.book.yes_bids, state.book.yes_asks, 1)
        imb_n1 = _imb(state.book.no_bids, state.book.no_asks, 1)

        # --- Row 4 diagnostics (YES space, mirror-safe) ---
        c = state.book.canon
        canon_mid = float(c.mid) if c.mid > 0.0 else float("nan")
        canon_micro = float(c.micro) if c.micro > 0.0 else float("nan")

        fv_gap_nd = (fv_y - canon_mid) if (not math.isnan(fv_y) and not math.isnan(canon_mid)) else float("nan")
        mid_micro_gap = (canon_mid - canon_micro) if (not math.isnan(canon_mid) and not math.isnan(canon_micro)) else float("nan")

        local_now_ms = int(time.time() * 1000)
        clock_off_ms = float(getattr(state.diag, "clock_offset_ms", 0.0) or 0.0)
        pm_now_ms = int(local_now_ms - clock_off_ms)
        ts_s = pm_now_ms / 1000.0

        snap = PlotSnap(
            ts_s=ts_s,
            win_start_ms=ws,   # window id on every snap
            yes_mid=_nan(ym),
            no_mid=_nan(nm),
            fv_yes_nd=fv_y,
            fv_no_nd=fv_n,
            px_binance=px_bin,
            strike=strike,
            yes_spread=yes_sp,
            no_spread=no_sp,
            imb_yes_l1=imb_y1,
            imb_no_l1=imb_n1,
            fv_gap_nd=fv_gap_nd,
            mid_micro_gap=mid_micro_gap,
        )

        # Non-blocking queue put: if full, drop oldest and keep newest
        try:
            plot_q.put_nowait(snap)
        except Exception:
            try:
                _ = plot_q.get_nowait()
            except Exception:
                pass
            try:
                plot_q.put_nowait(snap)
            except Exception:
                pass
