# plot_process.py
from __future__ import annotations

import json
import traceback
import math
import time
import matplotlib.pyplot as plt
import numpy as np
import os

from collections import deque
from plot_ipc import PlotSnap, PlotCtl, PlotMarker
from pathlib import Path
from bisect import insort


def _nanfilter(xs, ys):
    outx, outy = [], []
    for x, y in zip(xs, ys, strict=False):
        if y is None:
            continue

        try:
            yf = float(y)
            if math.isnan(yf) or math.isinf(yf):
                continue
            y = yf

        except Exception:
            # non-numeric y -> keep as-is (or skip; but current code keeps)
            pass

        outx.append(x)
        outy.append(y)
    return outx, outy


def plot_process_main(q, ctl_q=None, *, maxlen=1800, run_dir: str | None = None, trader_wallet: str | None = None, trader_name: str | None = None):
    """
    Runs in its own process. Owns matplotlib GUI.
    Receives PlotSnap messages and maintains deques.
    """
    # --- ring buffers (15m @ 2Hz) ---

    # All artifacts must live under logs/<run_id>/...
    if run_dir:
        try:
            Path(run_dir).mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        err_path = Path(run_dir) / "plot_process.err.jsonl"
    else:
        err_path = Path("logs") / "plot_process.err.jsonl"  # last-resort fallback

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

        pos = {"YES": 0.0, "NO": 0.0}
        cash = {"YES": 0.0, "NO": 0.0}  # net $ outflow (BUY +, SELL -)
        buy_qty = {"YES": 0.0, "NO": 0.0}
        buy_notional = {"YES": 0.0, "NO": 0.0}
        ledger_events: list[tuple[float, str, str, float, float]] = []

        # --- markers (YES/NO × BUY/SELL × MAKER/TAKER) ---
        # Token-id -> {"YES","NO"} mapping (optional exact override via env vars)
        yes_tok_env = (os.getenv("PLOT_YES_TOKEN_ID", "") or "").strip()
        no_tok_env = (os.getenv("PLOT_NO_TOKEN_ID", "") or "").strip()
        tokid_to_token: dict[int, str] = {}
        try:
            if yes_tok_env:
                tokid_to_token[int(yes_tok_env)] = "YES"
        except Exception:
            pass
        try:
            if no_tok_env:
                tokid_to_token[int(no_tok_env)] = "NO"
        except Exception:
            pass

        TOKENS = ("YES", "NO")
        SIDES = ("BUY", "SELL")
        ROLES = ("MAKER", "TAKER")

        # Strict token resolver: NO guessing. We only trust PlotCtl/env mapping.
        def _resolve_token(tokid: int) -> str | None:
            return tokid_to_token.get(tokid)

        # Buffer markers until we know mapping (startup / right-after-roll).
        pending_markers: deque[PlotMarker] = deque(maxlen=2000)

        # Per-group deques
        mxy: dict[tuple[str, str, str], tuple[deque[float], deque[float]]] = {}
        for token in TOKENS:
            for side in SIDES:
                for role in ROLES:
                    mxy[(token, side, role)] = (deque(maxlen=maxlen), deque(maxlen=maxlen))

        # Color mapping (explicit as requested)
        color_by = {
            ("YES", "BUY"): "green",
            ("YES", "SELL"): "red",
            ("NO", "BUY"): "blue",
            ("NO", "SELL"): "orange",
        }

        # --- control messages ---
        def _apply_ctl(ctl: PlotCtl) -> None:
            nonlocal err_path
            # ... existing logic handled elsewhere in loop; we only care about run_dir here
            if getattr(ctl, "run_dir", None):
                try:
                    Path(str(ctl.run_dir)).mkdir(parents=True, exist_ok=True)
                    err_path = Path(str(ctl.run_dir)) / "plot_process.err.jsonl"
                except Exception:
                    pass

        enabled = True
        show = True
        current_question: str = ""

        # --- figure with 4 rows ---
        plt.ion()
        fig = plt.figure()
        gs = fig.add_gridspec(
            nrows=5,
            ncols=2,
            height_ratios=[3.0, 1.5, 2.0, 1.0, 1.0],
            hspace=0.05,
            wspace=0.15,
        )

        # Leave room on the right for the Row 1 legend, and on top for the header.
        fig.subplots_adjust(left=0.045, right=0.90, bottom=0.04, top=0.94, hspace=0.05, wspace=0.14)

        # Row 1 (spans both columns)
        ax1 = fig.add_subplot(gs[0, :])

        # Row 2 (new): inventory + $ exposure side-by-side
        ax2L = fig.add_subplot(gs[1, 0], sharex=ax1)
        ax2R = fig.add_subplot(gs[1, 1], sharex=ax1)

        # Row 3..5 (span both columns)
        ax3 = fig.add_subplot(gs[2, :], sharex=ax1)
        ax4 = fig.add_subplot(gs[3, :], sharex=ax1)
        ax5 = fig.add_subplot(gs[4, :], sharex=ax1)

        # Header (replaces ax1 title). Updated live each refresh.
        hdr = fig.text(0.01, 0.99, "", ha="left", va="top")

        # Row 1: YES/NO mids + dotted FV(ND)
        (l_yes,) = ax1.plot([], [], lw=1, label="YES mid")
        (l_no,)  = ax1.plot([], [], lw=1, label="NO mid")

        (l_fy,)  = ax1.plot([], [], lw=1, linestyle=":", alpha=0.8, label="FV YES (ND)")
        (l_fn,)  = ax1.plot([], [], lw=1, linestyle=":", alpha=0.8, label="FV NO (ND)")

        # --- marker scatters (8 groups; compact legend) ---
        # MAKER = hollow (facecolors none, colored edge), TAKER = filled
        scat: dict[tuple[str, str, str], any] = {}
        for token in TOKENS:
            for side in SIDES:
                for role in ROLES:
                    c = color_by[(token, side)]
                    mk = "^" if side == "BUY" else "v"

                    # Only 4 legend entries total: use the TAKER scatter for labels
                    lbl = f"{token} {side}" if role == "TAKER" else "_nolegend_"

                    if role == "MAKER":
                        scat[(token, side, role)] = ax1.scatter(
                            [], [],
                            marker=mk,
                            s=28,
                            facecolors="none",
                            edgecolors=c,
                            linewidths=1.5,
                            label=lbl,
                        )
                    else:
                        scat[(token, side, role)] = ax1.scatter(
                            [], [],
                            marker=mk,
                            s=25,
                            c=c,
                            edgecolors="none",
                            label=lbl,
                        )

        ax1.set_ylim(-0.02, 1.02)
        ax1.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False)
        ax1.set_ylabel("prob")

        # Row 2 (NEW): Trader inventory (shares) + $ exposure (net outflow) as STEP plots
        (l_inv_yes,) = ax2L.step([], [], where="post", lw=1, label="YES pos")
        (l_inv_no,) = ax2L.step([], [], where="post", lw=1, label="NO pos")
        ax2L.set_ylabel("shares")
        # ax2L.legend(loc="upper left", bbox_to_anchor=(1.01, 0.5), frameon=False)

        (l_exp_yes,) = ax2R.step([], [], where="post", lw=1, label="YES")
        (l_exp_no,) = ax2R.step([], [], where="post", lw=1, label="NO")
        ax2R.set_ylabel("USD Exposure")
        ax2R.legend(loc="upper left", bbox_to_anchor=(1.01, 0.5), frameon=False)

        # Row 2: Binance price + strike
        (l_px,) = ax3.plot([], [], lw=1, label="Binance px")
        (l_k,) = ax3.plot([], [], lw=1, linestyle=":", alpha=0.8, label="Strike")
        ax3.legend(
            loc="center left", bbox_to_anchor=(1.01, 0.5),
            frameon=False,
        )
        ax3.set_ylabel("px")

        # Row 3: spread OR imbalance (we’ll plot spread by default)
        (l_sp_y,) = ax4.plot([], [], lw=1, label="YES spread")
        ax4.legend(
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            frameon=False,
        )
        ax4.set_ylabel("spr")
        ax4.set_ylim(0.0, 0.2)

        ax3b = ax4.twinx()
        (l_ib_y1,) = ax3b.plot([], [], lw=1, linestyle=":", alpha=0.8, label="imb YES L1")
        (l_ib_y5,) = ax3b.plot([], [], lw=1, linestyle="--", alpha=0.8, label="imb YES L5")
        ax3b.set_ylabel("imb")
        ax3b.set_ylim(-1.05, 1.05)

        # Row 4: diagnostics
        (l_p,) = ax5.plot([], [], lw=1, label="pressure")
        (l_f,) = ax5.plot([], [], lw=1, label="momz_fast")
        (l_s,) = ax5.plot([], [], lw=1, label="momz_slow")

        ax4b = ax5.twinx()
        (l_mus,) = ax4b.plot([], [], lw=1, label="mu/sigma", color='red')
        ax4b.set_ylabel("mu/sigma")
        ax4b.set_ylim(-10, 10)

        ax5.legend(
            loc="center left", bbox_to_anchor=(1.01, 0.5),
            frameon=False,
        )
        ax5.set_ylabel("edge")
        ax5.set_xlabel("t (s)")

        for ax in (ax1, ax2L, ax2R, ax3, ax4):
            ax.tick_params(labelbottom=False)

        fig.canvas.manager.set_window_title("Polymarket 15m monitor")

        base_ws_s = 0.0
        last_ws = 0.0     # ms

        def _clear_all():
            pending_markers.clear()
            ledger_events.clear()

            ts.clear()
            yes_mid.clear()
            no_mid.clear()
            fv_yes_nd.clear()
            fv_no_nd.clear()

            for (mx, my) in mxy.values():
                mx.clear()
                my.clear()

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

            pos["YES"] = pos["NO"] = 0.0
            cash["YES"] = cash["NO"] = 0.0
            buy_qty["YES"] = buy_qty["NO"] = 0.0
            buy_notional["YES"] = buy_notional["NO"] = 0.0

            try:
                hdr.set_text("")
            except Exception:
                pass

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

                            mq = getattr(ctl, "market_question", None)
                            if isinstance(mq, str) and mq.strip():
                                current_question = mq.strip()

                            # NEW: update run_dir for plot-side artifacts (e.g. plot_process.err.jsonl)
                            rd = getattr(ctl, "run_dir", None)
                            if isinstance(rd, str) and rd.strip():
                                try:
                                    Path(rd).mkdir(parents=True, exist_ok=True)
                                    err_path = Path(rd) / "plot_process.err.jsonl"
                                except Exception:
                                    pass

                            # NEW: deterministic YES/NO token mapping (and clear old mapping on reset)
                            yid = getattr(ctl, "yes_token_id", None)
                            nid = getattr(ctl, "no_token_id", None)

                            if getattr(ctl, "reset", False):
                                _clear_all()

                                # Only reset mapping if we were actually given fresh ids.
                                if (yid is not None) or (nid is not None):
                                    tokid_to_token.clear()
                                    if yid is not None:
                                        tokid_to_token[int(yid)] = "YES"
                                    if nid is not None:
                                        tokid_to_token[int(nid)] = "NO"
                                if getattr(ctl, "win_start_s", 0.0) > 0.0:
                                    base_ws_s = float(ctl.win_start_s)
                            else:
                                # non-reset updates are allowed too (just refresh mapping)
                                if yid is not None:
                                    tokid_to_token[int(yid)] = "YES"
                                if nid is not None:
                                    tokid_to_token[int(nid)] = "NO"
                except Exception:
                    pass

            if not show:
                time.sleep(0.02)
                continue

            # Drain queue (keep only newest effectively)
            got = False
            got_marker = False

            # If mapping is now known, process buffered markers first (in-order).
            if pending_markers:
                have_yes = ("YES" in tokid_to_token.values())
                have_no  = ("NO"  in tokid_to_token.values())
                if have_yes and have_no:
                    buffered = list(pending_markers)
                    pending_markers.clear()
                    # Process them by pushing back into the local loop via a small list
                    # (no IPC writes).
                    for mk in buffered:
                        try:
                            # emulate as if they came from q
                            snap = mk
                            side = (getattr(snap, "side", "") or "").upper()
                            role = (getattr(snap, "role", "") or "").upper()
                            if side not in ("BUY", "SELL"):
                                continue
                            if role not in ("MAKER", "TAKER"):
                                continue
                            tokid = int(getattr(snap, "token_id", -1) or -1)
                            token = _resolve_token(tokid)

                            if token is None:
                                # should not happen if have_yes+have_no, but keep safe
                                pending_markers.append(snap)
                                continue

                            got_marker = True
                            mx, my = mxy[(token, side, role)]
                            mx.append(float(snap.ts_s))
                            my.append(float(snap.y))

                            px = float(snap.price) if getattr(snap, "price", None) is not None else float(snap.y)
                            sz = float(getattr(snap, "size", 0.0) or 0.0)
                            if sz > 0.0 and math.isfinite(px):
                                insort(ledger_events, (float(snap.ts_s), token, side, float(sz), float(px)))
                                if len(ledger_events) > maxlen:
                                    del ledger_events[:-maxlen]

                        except Exception:
                            continue

            while True:
                try:
                    snap = q.get_nowait()
                except Exception:
                    break

                # --- markers ---
                if isinstance(snap, PlotMarker):

                    side = (getattr(snap, "side", "") or "").upper()
                    role = (getattr(snap, "role", "") or "").upper()
                    if side not in ("BUY", "SELL"):
                        continue
                    if role not in ("MAKER", "TAKER"):
                        continue

                    # If mapping not known yet (startup / right after roll), buffer marker.
                    tokid = int(getattr(snap, "token_id", -1) or -1)
                    token = _resolve_token(tokid)

                    if token is None:
                        pending_markers.append(snap)
                        continue

                    got_marker = True

                    mx, my = mxy[(token, side, role)]
                    mx.append(float(snap.ts_s))
                    my.append(float(snap.y))

                    px = float(snap.price) if getattr(snap, "price", None) is not None else float(snap.y)
                    sz = float(getattr(snap, "size", 0.0) or 0.0)

                    if sz > 0.0 and math.isfinite(px):
                        insort(ledger_events, (float(snap.ts_s), token, side, float(sz), float(px)))
                        if len(ledger_events) > maxlen:
                            del ledger_events[:-maxlen]

                    # IMPORTANT: PlotMarker must not fall through into PlotSnap handling
                    continue

                # --- snaps ---
                if not isinstance(snap, PlotSnap):
                    continue

                got = True

                # backup reset deterction via snap window id
                if snap.win_start_ms > 0.0 and snap.win_start_ms != last_ws:
                    last_ws = snap.win_start_ms
                    _clear_all()

                    base_ws_s = snap.win_start_ms / 1000.0
                # Do NOT touch tokid_to_token here. Mapping is owned by PlotCtl reset
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
            for key, (mx, my) in mxy.items():
                # token, side, role = key
                relx = [t - base_ws_s for t in mx]
                x2, y2 = _nanfilter(relx, list(my))
                sc = scat.get(key)
                if sc is None:
                    continue
                if x2:
                    sc.set_offsets(np.column_stack([x2, y2]))
                else:
                    sc.set_offsets(np.empty((0, 2)))

            # Row 2 (NEW): inventory / exposure
            if ledger_events:
                p_yes = p_no = 0.0
                c_yes = c_no = 0.0
                bq_yes = bq_no = 0.0
                bn_yes = bn_no = 0.0

                xs_led = []
                yes_pos = []
                no_pos = []
                yes_out = []
                no_out = []

                for t, tok, sd, sz, px in ledger_events:
                    if sd == "BUY":
                        if tok == "YES":
                            p_yes += sz;
                            c_yes += sz * px;
                            bq_yes += sz;
                            bn_yes += sz * px
                        else:
                            p_no += sz;
                            c_no += sz * px;
                            bq_no += sz;
                            bn_no += sz * px
                    else:  # SELL
                        if tok == "YES":
                            p_yes -= sz;
                            c_yes -= sz * px
                        else:
                            p_no -= sz;
                            c_no -= sz * px

                    xs_led.append(t - base_ws_s)
                    yes_pos.append(p_yes)
                    no_pos.append(p_no)
                    yes_out.append(c_yes)
                    no_out.append(c_no)

                # extend last point to current time so step stays visible
                now_x = float(xs[-1])
                xs2 = xs_led + [now_x]

                yes_pos2 = yes_pos + [yes_pos[-1]]
                no_pos2 = no_pos + [no_pos[-1]]
                yes_out2 = yes_out + [yes_out[-1]]
                no_out2 = no_out + [no_out[-1]]

                x, y = _nanfilter(xs2, yes_pos2);
                l_inv_yes.set_data(x, y)
                x, y = _nanfilter(xs2, no_pos2);
                l_inv_no.set_data(x, y)
                x, y = _nanfilter(xs2, yes_out2);
                l_exp_yes.set_data(x, y)
                x, y = _nanfilter(xs2, no_out2);
                l_exp_no.set_data(x, y)

                # update summary stats sources for header
                pos["YES"], pos["NO"] = p_yes, p_no
                cash["YES"], cash["NO"] = c_yes, c_no
                buy_qty["YES"], buy_qty["NO"] = bq_yes, bq_no
                buy_notional["YES"], buy_notional["NO"] = bn_yes, bn_no

            else:
                l_inv_yes.set_data([], [])
                l_inv_no.set_data([], [])
                l_exp_yes.set_data([], [])
                l_exp_no.set_data([], [])

                pos["YES"] = pos["NO"] = 0.0
                cash["YES"] = cash["NO"] = 0.0
                buy_qty["YES"] = buy_qty["NO"] = 0.0
                buy_notional["YES"] = buy_notional["NO"] = 0.0

            # Header stats (PnL, avg buy, scenario PnL). Updated live.
            try:
                mid_y = float(yes_mid[-1]) if len(yes_mid) else float("nan")
                mid_n = float(no_mid[-1]) if len(no_mid) else float("nan")

                net_spent = float(cash["YES"] + cash["NO"])
                value = float(pos["YES"] * mid_y + pos["NO"] * mid_n)
                pnl_mtm = value - net_spent

                avg_yes = (buy_notional["YES"] / buy_qty["YES"]) if buy_qty["YES"] > 0 else float("nan")
                avg_no  = (buy_notional["NO"]  / buy_qty["NO"])  if buy_qty["NO"]  > 0 else float("nan")

                pnl_if_yes = (pos["YES"] * 1.0) - net_spent
                pnl_if_no  = (pos["NO"]  * 1.0) - net_spent

                stats_line = (
                    f"PnL(MTM): ${pnl_mtm:,.2f} | "
                    f"Pos YES/NO: {pos['YES']:.0f}/{pos['NO']:.0f} | "
                    f"$ YES/NO: {cash['YES']:,.0f}/{cash['NO']:,.0f} | "
                    f"Avg YES: {avg_yes:.4f}  Avg NO: {avg_no:.4f} | "
                    f"If YES: ${pnl_if_yes:,.2f}  If NO: ${pnl_if_no:,.2f}  | "
                    f"Markers: hollow=MAKER, filled=TAKER"
                )

                tracked_line = None
                if trader_wallet:
                    w = trader_wallet.strip()
                    w_short = f"{w[:6]}…{w[-4:]}" if len(w) > 14 else w
                    name = (trader_name or "Unknown").strip()
                    tracked_line = f"Tracked: {name} ({w_short})"

                # IMPORTANT: escape $ to avoid mathtext (which italicizes text)
                stats_line = stats_line.replace("$", r"\$")
                if tracked_line:
                    tracked_line = tracked_line.replace("$", r"\$")

                if current_question:
                    lines = [current_question, stats_line]
                    if tracked_line:
                        lines.insert(1, tracked_line)  # right after question/title
                    hdr.set_text("\n".join(lines))
                else:
                    lines = [stats_line]
                    if tracked_line:
                        lines.insert(0, tracked_line)  # if no question, put tracked first
                    hdr.set_text("\n".join(lines))

            except Exception:
                pass

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
            x, y = _nanfilter(xs, list(imb_y5)); l_ib_y5.set_data(x, y)
            x, y = _nanfilter(xs, list(yes_spread)); l_sp_y.set_data(x, y)

            # Row 4
            x, y = _nanfilter(xs, list(pressure));      l_p.set_data(x, y)
            x, y = _nanfilter(xs, list(momz_fast));     l_f.set_data(x, y)
            x, y = _nanfilter(xs, list(momz_slow));     l_s.set_data(x, y)
            x, y = _nanfilter(xs, list(mu_over_sigma)); l_mus.set_data(x, y)

            # Growing 15m horizon
            ax4.set_xlim(0.0, max(10.0, min(900.0, xmax)))

            for ax in (ax2L, ax2R, ax3, ax4, ax5):
                ax.relim()
                ax.autoscale_view(scalex=False, scaley=True)

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
