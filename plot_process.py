# plot_process.py
from __future__ import annotations

import json
import traceback
import math
import time
import matplotlib.pyplot as plt
import numpy as np

from collections import deque
from plot_ipc import PlotSnap, PlotCtl, PlotMarker
from pathlib import Path

def _nanfilter(xs, ys):
    outx, outy = [], []
    for x, y in zip(xs, ys, strict=False):
        if y is None:
            continue
        if isinstance(y, float) and (math.isnan(y) or math.isinf(y)):
            continue
        outx.append(x)
        outy.append(y)
    return outx, outy


def plot_process_main(q, ctl_q=None, *, maxlen=1800):
    """
    Runs in its own process. Owns matplotlib GUI.
    Receives PlotSnap messages and maintains deques.
    """
    # --- ring buffers (15m @ 2Hz) ---

    err_path = Path("plot_process.err.jsonl")

    def _log_err(where: str, exc: BaseException):
        rec = {
            "type": "plot_process_error",
            "ts_ms": int(time.time() * 1000),
            "where": where,
            "exc_type": type(exc).__name__,
            "exc": str(exc),
            "traceback": traceback.format_exc(),
        }
        try:
            with err_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception:
            # last resort: don't crash error handler
            pass

    try:
        ts = deque(maxlen=maxlen)

        yes_mid = deque(maxlen=maxlen)
        no_mid = deque(maxlen=maxlen)
        fv_yes_nd = deque(maxlen=maxlen)
        fv_no_nd = deque(maxlen=maxlen)

        px_bin = deque(maxlen=maxlen)
        strike = deque(maxlen=maxlen)

        yes_spread = deque(maxlen=maxlen)
        no_spread = deque(maxlen=maxlen)

        imb_y1 = deque(maxlen=maxlen)
        imb_n1 = deque(maxlen=maxlen)
        imb_y5 = deque(maxlen=maxlen)
        imb_n5 = deque(maxlen=maxlen)

        # Row 4 momentum / diagnostics
        pressure = deque(maxlen=maxlen)
        momz_fast = deque(maxlen=maxlen)
        momz_slow = deque(maxlen=maxlen)
        mu_over_sigma = deque(maxlen=maxlen)
        sigma_eff = deque(maxlen=maxlen)  # optional debug

        fv_gap_nd = deque(maxlen=maxlen)
        mid_micro_gap = deque(maxlen=maxlen)

        # --- markers (buy/sell) ---
        mx_buy: deque[float] = deque(maxlen=maxlen)
        my_buy: deque[float] = deque(maxlen=maxlen)
        mx_sell: deque[float] = deque(maxlen=maxlen)
        my_sell: deque[float] = deque(maxlen=maxlen)

        enabled = True
        show = True

        # --- figure with 4 rows ---
        plt.ion()
        fig, axs = plt.subplots(
            4, 1, sharex=True,
            gridspec_kw={"height_ratios": [3.0, 2.0, 1.0, 1.0]},
        )
        fig.subplots_adjust(right=0.80)
        ax1, ax2, ax3, ax4 = axs

        # Row 1: YES/NO mids + dotted FV(ND)
        (l_yes,) = ax1.plot([], [], lw=1, label="YES mid")
        (l_no,)  = ax1.plot([], [], lw=1, label="NO mid")

        (l_fy,)  = ax1.plot([], [], lw=1, linestyle=":", alpha=0.8, label="FV YES (ND)")
        (l_fn,)  = ax1.plot([], [], lw=1, linestyle=":", alpha=0.8, label="FV NO (ND)")

        # --- marker scatters (no explicit colors; let matplotlib cycle/default) ---
        scat_buy = ax1.scatter([], [], marker="^", s=25, label="Trader BUY")
        scat_sell = ax1.scatter([], [], marker="v", s=25, label="Trader SELL")

        ax1.set_ylim(-0.02, 1.02)
        ax1.legend(
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
        )
        ax1.set_ylabel("prob")

        # Row 2: Binance price + strike
        (l_px,) = ax2.plot([], [], lw=1, label="Binance px")
        (l_k,)  = ax2.plot([], [], lw=1, linestyle=":", alpha=0.8, label="Strike")
        ax2.legend(
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
        )
        ax2.set_ylabel("px")

        # Row 3: spread OR imbalance (we’ll plot spread by default)
        (l_sp_y,) = ax3.plot([], [], lw=1, label="YES spread")
        # (l_sp_n,) = ax3.plot([], [], lw=1, label="NO spread")
        # (l_mm,)  = ax3.plot([], [], lw=1, label="mid-micro (YES)")
        ax3.legend(
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
        )
        ax3.set_ylabel("spr")
        ax3.set_ylim(0.0, 0.2)

        ax3b = ax3.twinx()
        (l_ib_y1,) = ax3b.plot([], [], lw=1, linestyle=":", alpha=0.8, label="imb YES L1")
        # (l_ib_n1,) = ax3b.plot([], [], lw=1, linestyle=":", alpha=0.8, label="imb NO L1")
        (l_ib_y5,) = ax3b.plot([], [], lw=1, linestyle="--", alpha=0.8, label="imb YES L5")
        # (l_ib_n5,) = ax3b.plot([], [], lw=1, linestyle="--", alpha=0.8, label="imb NO L5")
        ax3b.set_ylabel("imb")
        ax3b.set_ylim(-1.05, 1.05)

        # Row 4: diagnostics
        # (l_gap,) = ax4.plot([], [], lw=1, label="FVgap ND (YES)")
        (l_p,)   = ax4.plot([], [], lw=1, label="pressure")
        (l_f,)   = ax4.plot([], [], lw=1, label="momz_fast")
        (l_s,)   = ax4.plot([], [], lw=1, label="momz_slow")
        # (l_se,)  = ax4.plot([], [], lw=1, linestyle=":", alpha=0.8, label="sigma_eff")  # optional

        ax4b = ax4.twinx()
        (l_mus,) = ax4b.plot([], [], lw=1, label="mu/sigma", color='red')
        ax4b.set_ylabel("mu/sigma")
        ax4b.set_ylim(-10, 10)

        ax4.legend(
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
        )
        ax4.set_ylabel("edge")
        ax4.set_xlabel("t (s)")

        fig.canvas.manager.set_window_title("Polymarket 15m monitor")

        base_ws_s = 0.0
        last_ws = 0.0     # ms

        def _clear_all():
            ts.clear()
            yes_mid.clear()
            no_mid.clear()
            fv_yes_nd.clear()
            fv_no_nd.clear()

            mx_buy.clear()
            my_buy.clear()

            mx_sell.clear()
            my_sell.clear()

            px_bin.clear()
            strike.clear()
            yes_spread.clear()
            no_spread.clear()

            imb_y1.clear()
            imb_n1.clear()
            imb_y5.clear()
            imb_n5.clear()

            pressure.clear()
            momz_fast.clear()
            momz_slow.clear()
            mu_over_sigma.clear()
            sigma_eff.clear()

            fv_gap_nd.clear()
            mid_micro_gap.clear()

        while True:
            # pump GUI
            plt.pause(0.001)

            if not plt.fignum_exists(fig.number):
                break

            # optional control channel
            if ctl_q is not None:
                try:
                    while True:
                        ctl = ctl_q.get_nowait()
                        if isinstance(ctl, PlotCtl):
                            show = ctl.show
                            enabled = ctl.enabled
                            if getattr(ctl, "reset", False):
                                _clear_all()
                                if getattr(ctl, "win_start_s", 0.0) > 0.0:
                                    base_ws_s = float(ctl.win_start_s)
                except Exception:
                    pass

            if not show:
                time.sleep(0.02)
                continue

            # Drain queue (keep only newest effectively)
            got = False
            got_marker = False

            while True:
                try:
                    snap = q.get_nowait()
                except Exception:
                    break

                # --- markers ---
                if isinstance(snap, PlotMarker):
                    got_marker = True

                    if snap.side.upper() == "BUY":
                        mx_buy.append(snap.ts_s)
                        my_buy.append(snap.y)
                    else:
                        mx_sell.append(snap.ts_s)
                        my_sell.append(snap.y)

                # --- snaps ---
                if not isinstance(snap, PlotSnap):
                    continue

                got = True

                # backup reset deterction via snap window id
                if snap.win_start_ms > 0.0 and snap.win_start_ms != last_ws:
                    last_ws = snap.win_start_ms
                    _clear_all()
                    base_ws_s = snap.win_start_ms / 1000.0

                ts.append(snap.ts_s)

                yes_mid.append(snap.yes_mid)
                no_mid.append(snap.no_mid)
                fv_yes_nd.append(snap.fv_yes_nd)
                fv_no_nd.append(snap.fv_no_nd)

                px_bin.append(snap.px_binance)
                strike.append(snap.strike)

                yes_spread.append(snap.yes_spread)
                no_spread.append(snap.no_spread)

                imb_y1.append(snap.imb_yes_l1)
                imb_n1.append(snap.imb_no_l1)
                imb_y5.append(getattr(snap, "imb_yes_l5", float("nan")))
                imb_n5.append(getattr(snap, "imb_no_l5", float("nan")))

                # momentum panel
                pressure.append(getattr(snap, "pressure", float("nan")))
                momz_fast.append(getattr(snap, "momz_fast", float("nan")))
                momz_slow.append(getattr(snap, "momz_slow", float("nan")))
                mu_over_sigma.append(getattr(snap, "muT_over_sigmaT", float("nan")))
                sigma_eff.append(getattr(snap, "sigma_eff", float("nan")))

                fv_gap_nd.append(snap.fv_gap_nd)
                mid_micro_gap.append(snap.mid_micro_gap)

            # Only redraw when new data arrived and enabled
            if not enabled or (not got and not got_marker) or len(ts) < 2:
                time.sleep(0.01)
                continue

            # If we still don't know windows start, fall abck to first sample time
            if base_ws_s <= 0.0:
                base_ws_s = ts[0]

            xs = [t - base_ws_s for t in ts]
            xmax = xs[-1]

            # Markers: convert absolute ts_s to relative x like everything else.
            mbx = [t - base_ws_s for t in mx_buy]
            msx = [t - base_ws_s for t in mx_sell]

            bx, by = _nanfilter(mbx, list(my_buy))
            sx, sy = _nanfilter(msx, list(my_sell))

            if scat_buy is not None:
                if bx:
                    scat_buy.set_offsets(np.column_stack([bx, by]))
                else:
                    scat_buy.set_offsets(np.empty((0, 2)))

            if scat_sell is not None:
                if sx:
                    scat_sell.set_offsets(np.column_stack([sx, sy]))
                else:
                    scat_sell.set_offsets(np.empty((0, 2)))

            # Row 1
            x, y = _nanfilter(xs, list(yes_mid)); l_yes.set_data(x, y)
            x, y = _nanfilter(xs, list(no_mid));  l_no.set_data(x, y)
            x, y = _nanfilter(xs, list(fv_yes_nd)); l_fy.set_data(x, y)
            x, y = _nanfilter(xs, list(fv_no_nd));  l_fn.set_data(x, y)

            # Row 2
            x, y = _nanfilter(xs, list(px_bin));  l_px.set_data(x, y)
            x, y = _nanfilter(xs, list(strike));  l_k.set_data(x, y)

            # Row 3 (spread default)
            x, y = _nanfilter(xs, list(imb_y1)); l_ib_y1.set_data(x, y)
            # x, y = _nanfilter(xs, list(imb_n1)); l_ib_n1.set_data(x, y)
            x, y = _nanfilter(xs, list(imb_y5)); l_ib_y5.set_data(x, y)
            # x, y = _nanfilter(xs, list(imb_n5)); l_ib_n5.set_data(x, y)

            x, y = _nanfilter(xs, list(yes_spread)); l_sp_y.set_data(x, y)
            # x, y = _nanfilter(xs, list(no_spread));  l_sp_n.set_data(x, y)

            # Row 4
            # x, y = _nanfilter(xs, list(fv_gap_nd));     l_gap.set_data(x, y)
            x, y = _nanfilter(xs, list(pressure));      l_p.set_data(x, y)
            x, y = _nanfilter(xs, list(momz_fast));     l_f.set_data(x, y)
            x, y = _nanfilter(xs, list(momz_slow));     l_s.set_data(x, y)
            x, y = _nanfilter(xs, list(mu_over_sigma)); l_mus.set_data(x, y)
            # x, y = _nanfilter(xs, list(sigma_eff));     l_se.set_data(x, y)
            # x, y = _nanfilter(xs, list(mid_micro_gap)); l_mm.set_data(x, y)

            # Full 15m horizon
            ax4.set_xlim(0.0, max(10.0, min(900.0, xmax)))

            # autoscale y for rows 2–4
            ax2.relim(); ax2.autoscale_view(scaley=True)
            ax3.relim(); ax3.autoscale_view(scaley=True)
            ax4.relim(); ax4.autoscale_view(scaley=True)

            fig.canvas.draw_idle()

        try:
            plt.close(fig)
        except Exception:
            pass

    except Exception as e:
        _log_err("plot_process_main", e)
        # also print once so you see it immediately in console
        try:
            print(json.dumps({"type": "plot_process_fatal", "err": str(e)}))
        except Exception:
            pass
        # hard-exit the process (so main can restart later if you want)
        return
