# plot_live.py
from __future__ import annotations

import asyncio
import math
import matplotlib.pyplot as plt

from state import AppState


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


def init_plot():
    plt.ion()
    fig, ax = plt.subplots()
    (l_yes,) = ax.plot([], [], lw=1, label="YES mid")
    (l_no,) = ax.plot([], [], lw=1, label="NO mid")

    (l_fv_yes,) = ax.plot(
        [], [],
        lw=0.9,
        linestyle=":",
        alpha=0.7,
        label="FV yes (ND)",
    )

    (l_fv_no,) = ax.plot(
        [], [],
        lw=0.9,
        linestyle=":",
        alpha=0.7,
        label="FV no (ND)",
    )

    ax.set_title("Polymarket YES/NO (L1 mids)")
    ax.set_xlabel("t (s, relative)")
    ax.set_ylabel("prob")
    ax.set_ylim(-0.02, 1.02)
    ax.legend(loc="upper right")
    return fig, ax, l_yes, l_no, l_fv_yes, l_fv_no


async def plotter_task(state: AppState, update_hz: float = 10.0) -> None:
    """
    Matplotlib plot loop. Uses plt.pause() to pump GUI events.
    Runs in same asyncio thread; cadence-limited.
    """
    fig, ax, l_yes, l_no, l_fv_yes, l_fv_no = init_plot()
    dt = 1.0 / max(1.0, float(update_hz))

    while True:
        # Always pump GUI events so the window stays responsive
        plt.pause(0.001)

        # If user closed the window manually
        if not plt.fignum_exists(fig.number):
            state.plot_ctl.show = False
            state.plot_ctl.enabled = False
            break

        # If window isn't supposed to be shown, keep it alive but don't draw
        if not state.plot_ctl.show:
            await asyncio.sleep(dt)
            continue

        if state.plot_ctl.enabled:
            ps = state.plot
            if len(ps.ts_s) >= 2:
                t0 = ps.ts_s[0]
                xs = [t - t0 for t in ps.ts_s]

                x_yes, y_yes = _nanfilter(xs, list(ps.yes_mid))
                x_no, y_no = _nanfilter(xs, list(ps.no_mid))

                x_fy, y_fy = _nanfilter(xs, list(ps.fv_yes))
                x_fn, y_fn = _nanfilter(xs, list(ps.fv_no))

                if x_yes:
                    l_yes.set_data(x_yes, y_yes)
                if x_no:
                    l_no.set_data(x_no, y_no)

                # overlays (optional)
                if x_fy:
                    l_fv_yes.set_data(x_fy, y_fy)
                if x_fn:
                    l_fv_no.set_data(x_fn, y_fn)

                # autoscale x
                xmax = xs[-1]
                ax.set_xlim(0.0, max(10.0, xmax))
                # ax.set_xlim(max(0.0, xmax - 120.0), max(10.0, xmax))  # last 2 minutes view
                fig.canvas.draw_idle()

        await asyncio.sleep(dt)

    try:
        plt.close(fig)
    except Exception:
        pass