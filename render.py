"""
Rendering functions for left and right panes.
"""

from __future__ import annotations

from typing import List

from prompt_toolkit.formatted_text import ANSI

from state import AppState, OrderbookLevel, pressure_imbalance_notional_5s_live, now_ms

# --- ANSI color helpers ---
RESET = "\x1b[0m"
FG_GREEN = "\x1b[32m"
FG_RED = "\x1b[31m"
FG_DIM = "\x1b[2m"


def size_imbalance(levels_bid: List[OrderbookLevel], levels_ask: List[OrderbookLevel], n: int) -> float:
    """Return ((B-A)/(B+A)) using sizes over levels 1..n. Result in [-1,+1]."""
    b = sum(float(x.sz) for x in levels_bid[:n])
    a = sum(float(x.sz) for x in levels_ask[:n])
    denom = b + a
    if denom <= 1e-12:
        return 0.0
    return (b - a) / denom


def align_mode(ema_ms: float) -> str:
    if ema_ms < 250:
        return "NO_REACT"
    elif ema_ms < 500:
        return "SELECTIVE"
    elif ema_ms < 900:
        return "AGGRESSIVE"
    else:
        return "DANGER"


def color_signed(x: float, s: str) -> str:
    """Color a preformatted signed string based on x."""
    if x > 0:
        return f"{FG_GREEN}{s}{RESET}"
    if x < 0:
        return f"{FG_RED}{s}{RESET}"
    return s


def color_signed_fmt(x: float, fmt: str) -> str:
    """Format + color a number in one go."""
    return color_signed(x, format(x, fmt))


def fv_indicator_ansi(fv: float, mid: float) -> str:
    """Return a + or - indicator with colored background based on FV vs mid."""
    if fv > mid:
        return "\x1b[42m + \x1b[0m"
    if fv < mid:
        return "\x1b[41m - \x1b[0m"
    return "   "


def format_px_sz_cum_header(px_w: int, sz_w: int, cum_w: int, prefix: str) -> str:
    """Header aligned to px/sz/cum columns."""
    return f"{prefix}{'px':>{px_w}} {'sz':>{sz_w}} {'cum sz':>{cum_w}}"


def format_px_sz_cum_sep(px_w: int, sz_w: int, cum_w: int, prefix: str) -> str:
    """Separator aligned to px/sz/cum columns."""
    return f"{prefix}{'-'*px_w} {'-'*sz_w} {'-'*cum_w}"


def _fmt_age(now_ms: float, last_ms: float) -> float:
    if last_ms <= 0.0:
        return 0.0
    v = now_ms - last_ms
    return v if v > 0.0 else 0.0


def format_level_row(
    tag: str,
    level: int,
    px: float,
    sz: float,
    cum_sz: float,
    px_w: int,
    sz_w: int,
    cum_w: int,
    debug: bool,
    tag_w: int,
) -> str:
    """Format one ladder row with px/sz/cum and optional ASK/BID tag."""
    tag_field = f"{tag:<{tag_w}}"
    prefix = f"L{level}  "
    indent = " "

    if debug:
        body = f"{indent}{px:>{px_w}.4f} @ {sz:>{sz_w}.1f} {cum_sz:>{cum_w}.1f}"
    else:
        body = f"{indent}{px:>{px_w}.4f} {sz:>{sz_w}.1f} {cum_sz:>{cum_w}.1f}"

    return f"{tag_field}{prefix}{body}"


def compute_cum_sizes(levels: List[OrderbookLevel]) -> List[float]:
    """Compute cumulative sizes in L1->L5 order."""
    out: List[float] = []
    s = 0.0
    for lvl in levels[:5]:
        s += float(lvl.sz)
        out.append(s)
    while len(out) < 5:
        out.append(0.0)
    return out


def format_side(
    levels: List[OrderbookLevel],
    px_w: int,
    sz_w: int,
    cum_w: int,
    debug: bool,
    descending: bool,
    first_tag: str,
    tag_w: int,
) -> List[str]:
    """Format L1–L5 rows with cum sz; descending shows L5..L1 but cum is still from L1."""
    rows: List[str] = []
    lvls = levels[:5]
    cum = compute_cum_sizes(levels)

    if descending:
        display_lvls = list(reversed(lvls))
        display_levels = [5, 4, 3, 2, 1]
        display_cum = list(reversed(cum))
        tag_index = 4
    else:
        display_lvls = lvls
        display_levels = [1, 2, 3, 4, 5]
        display_cum = cum
        tag_index = 0

    for i, (n, lvl, c) in enumerate(zip(display_levels, display_lvls, display_cum, strict=False)):
        tag = first_tag if i == tag_index else ""
        rows.append(format_level_row(tag, n, lvl.px, lvl.sz, c, px_w, sz_w, cum_w, debug, tag_w))

    while len(rows) < 5:
        n = display_levels[len(rows)]
        tag = first_tag if len(rows) == tag_index else ""
        tag_field = f"{tag:<{tag_w}}"
        rows.append(f"{tag_field}L{n}    {'-':>{px_w}} {'-':>{sz_w}} {'-':>{cum_w}}")

    return rows


def format_split_line(left: str, right: str, left_width: int) -> str:
    """Format a line with a fixed left column width and a stable vertical separator."""
    return f"{left:<{left_width}}| {right}"


def fit_to_height(lines: List[str], height: int) -> ANSI:
    """Pad or truncate rendered text to exactly `height` rows."""
    if len(lines) < height:
        lines = lines + [""] * (height - len(lines))
    else:
        lines = lines[:height]
    return ANSI("\n".join(lines))


def render_left(state: AppState, height: int) -> ANSI:
    """Render LEFT pane: YES/NO, each with asks (top) and bids (bottom)."""
    left_width = 36
    tag_w = 4
    px_w = 6
    sz_w = 7
    cum_w = 7
    mode = "DEBUG" if state.debug_left else "COMPACT"

    header_prefix = " " * (tag_w + 4 + 1)

    lines: List[str] = []
    lines.append(f"PM CLOB (LEFT) | MODE: {mode}   (d toggle, q quit)")
    lines.append(f"skew     : {state.diag.clock_offset_ms:4.0f}ms  ({getattr(state.diag, 'clock_offset_src', '')})")
    # --- Binance -> CLOB response (canonical, mirror-safe) ---
    a = state.align
    mode = align_mode( a.resp_ema_ms)
    pend = "*" if a.pending else "."
    lines.append(
        f"B→CLOB resp: last {a.resp_last_ms:4.0f}ms  ema {a.resp_ema_ms:4.0f}ms  pend {pend}  n {a.n_impulses}/{a.n_matched} miss {a.n_missed}  Mode: {mode}"
    )
    lines.append(
        f"align dbg : canon_mid {state.book.canon.mid:0.4f}  sec_move {state.tape_driver.sec_move_px:+6.2f}  atr1m {state.driver.atr_1m:0.2f}"
    )

    lines.append("-" * 60)

    lines.append(format_split_line("YES (UP)", "NO (DOWN)", left_width))
    lines.append(
        format_split_line(
            format_px_sz_cum_header(px_w, sz_w, cum_w, header_prefix),
            format_px_sz_cum_header(px_w, sz_w, cum_w, header_prefix),
            left_width,
        )
    )
    lines.append(
        format_split_line(
            format_px_sz_cum_sep(px_w, sz_w, cum_w, header_prefix),
            format_px_sz_cum_sep(px_w, sz_w, cum_w, header_prefix),
            left_width,
        )
    )

    yes_asks = format_side(state.book.yes_asks, px_w, sz_w, cum_w, state.debug_left, True, "ASK", tag_w)
    no_asks = format_side(state.book.no_asks, px_w, sz_w, cum_w, state.debug_left, True, "ASK", tag_w)
    for i in range(5):
        lines.append(format_split_line(yes_asks[i], no_asks[i], left_width))

    lines.append(format_split_line("", "", left_width))

    # --- Fair values ---
    # Book FV (what CLOB currently believes / derived from mid or your book logic)
    # Model FV (your Binance-driven pricing model; drift-aware)
    model_fv_left = f"MODEL FV {state.driver.fv_yes:0.4f}"
    model_fv_right = f"{state.driver.fv_no:0.4f}"
    lines.append(format_split_line(model_fv_left, f"{'':9}{model_fv_right}", left_width))

    # Model FV (no-drift baseline)
    nd_fv_left = f"MODEL ND {state.driver.fv_yes_nd:0.4f}"
    nd_fv_right = f"{state.driver.fv_no_nd:0.4f}"
    lines.append(format_split_line(nd_fv_left, f"{'':9}{nd_fv_right}", left_width))

    lines.append(format_split_line("", "", left_width))

    yes_bids = format_side(state.book.yes_bids, px_w, sz_w, cum_w, state.debug_left, False, "BID", tag_w)
    no_bids = format_side(state.book.no_bids, px_w, sz_w, cum_w, state.debug_left, False, "BID", tag_w)
    for i in range(5):
        lines.append(format_split_line(yes_bids[i], no_bids[i], left_width))

    lines.append("-" * 60)

    if state.book.yes_bids and state.book.yes_asks:
        yes_spread = state.book.yes_asks[0].px - state.book.yes_bids[0].px
        yes_mid = 0.5 * (state.book.yes_asks[0].px + state.book.yes_bids[0].px)
    else:
        yes_spread = 0.0
        yes_mid = 0.0

    if state.book.no_bids and state.book.no_asks:
        no_spread = state.book.no_asks[0].px - state.book.no_bids[0].px
        no_mid = 0.5 * (state.book.no_asks[0].px + state.book.no_bids[0].px)
    else:
        no_spread = 0.0
        no_mid = 0.0

    lines.append(f"updates   : {state.book.updates}   lag : {state.book.lag_ms:0.0f}ms   raw lag: {state.book.lag_raw_ms:0.0f}ms")
    if state.book.market_slug and state.book.question:
        lines.append(f"market    : {state.book.market_slug}")
        lines.append(f"title     : {state.book.question}")

    # --- book size imbalance metrics (online, O(1) with small N) ---
    y_l1 = size_imbalance(state.book.yes_bids, state.book.yes_asks, 1)
    y_l5 = size_imbalance(state.book.yes_bids, state.book.yes_asks, 5)
    n_l1 = size_imbalance(state.book.no_bids, state.book.no_asks, 1)
    n_l5 = size_imbalance(state.book.no_bids, state.book.no_asks, 5)

    imbalance_yes = f"imbalance : UP L1 {y_l1:+0.2f}  L5 {y_l5:+0.2f}"
    imbalance_no =  f"imbalance : DOWN  L1 {n_l1:+0.2f}  L5 {n_l5:+0.2f}"
    # lines.append(f"imbalance : YES L1 {y_l1:+0.2f}  L5 {y_l5:+0.2f}   |   NO L1 {n_l1:+0.2f}  L5 {n_l5:+0.2f}")
    nowv = now_ms()

    ym = state.book.metrics.yes
    nm = state.book.metrics.no

    y_bid_age = _fmt_age(nowv, ym.bid_last_change_ms)
    y_ask_age = _fmt_age(nowv, ym.ask_last_change_ms)
    n_bid_age = _fmt_age(nowv, nm.bid_last_change_ms)
    n_ask_age = _fmt_age(nowv, nm.ask_last_change_ms)

    x = state.book.canon.touch_cross_risk

    # line 1: micro-bias + spread
    left1  = f"micro(bias) UP    {ym.micro_bias:+0.2f}  spr {ym.spread:0.4f}"
    right1 = f"micro(bias) DOWN     {nm.micro_bias:+0.2f}  spr {nm.spread:0.4f}"
    # lines.append(format_split_line(left1, right1, left_width))
    # line 2: depletion + age + flicker
    left2  = f"dep b/a {ym.bid_dep_ema:5.1f}/{ym.ask_dep_ema:5.1f}  age b/a {y_bid_age:4.0f}/{y_ask_age:4.0f}ms  flick {ym.bid_flicker_ema:4.1f}/{ym.ask_flicker_ema:4.1f}/s  D b/a {ym.danger_bid:0.2f}/{ym.danger_ask:0.2f}  X {x:0.2f}"
    right2 = f"dep b/a {nm.bid_dep_ema:5.1f}/{nm.ask_dep_ema:5.1f}  age b/a {n_bid_age:4.0f}/{n_ask_age:4.0f}ms  flick {nm.bid_flicker_ema:4.1f}/{nm.ask_flicker_ema:4.1f}/s  D b/a {nm.danger_bid:0.2f}/{nm.danger_ask:0.2f}  X {x:0.2f}"
    # lines.append(format_split_line(left2, right2, left_width))

    lines.append("")
    lines.append("UP")
    lines.append("=======")
    lines.append(imbalance_yes)
    lines.append(left1)
    lines.append(left2)

    lines.append("")
    lines.append("DOWN")
    lines.append("=======")
    lines.append(imbalance_no)
    lines.append(right1)
    lines.append(right2)

    yes_badge = fv_indicator_ansi(state.book.fv_yes, yes_mid)
    no_badge = fv_indicator_ansi(state.book.fv_no, no_mid)

    yes_badge_nd = fv_indicator_ansi(state.book.fv_yes_nd, yes_mid)
    no_badge_nd = fv_indicator_ansi(state.book.fv_no_nd, no_mid)

    lines.append("")
    lines.append("With Drift")
    lines.append("==========")
    lines.append(f"UP    mid: {yes_mid:0.4f}  FV: {state.book.fv_yes:0.4f}  {yes_badge}")
    lines.append(f"DOWN  mid: {no_mid:0.4f}  FV: {state.book.fv_no:0.4f}  {no_badge}")
    lines.append("")
    lines.append("Without Drift")
    lines.append("=============")
    lines.append(f"UP    mid: {yes_mid:0.4f}  FV: {state.book.fv_yes_nd:0.4f}  {yes_badge_nd}")
    lines.append(f"DOWN  mid: {no_mid:0.4f}  FV: {state.book.fv_no_nd:0.4f}  {no_badge_nd}")

    lines.append("")
    lines.append(f"pulse     : {state.book.pulse}")
    lines.append(f"loop drift: {state.diag.loop_drift_ms:5.1f}ms  worst {state.diag.loop_drift_worst_ms:5.1f}ms  ")
    lines.append(f"apply ms: B {state.diag.binance_apply_ms:4.1f}  CLOB {state.diag.clob_apply_ms:4.1f}  RTDS {state.diag.rtds_apply_ms:4.1f}")

    return fit_to_height(lines, height)


def render_right_top(state: AppState, height: int) -> ANSI:
    """Render RIGHT-TOP pane (Binance driver)."""
    d = state.driver
    dist = d.last - d.strike
    lines: List[str] = []

    lines.append(f"BINANCE {d.symbol} (DRIVER) -- tte_s: {d.tte_s:6.1f}  ")
    lines.append(f"lag_raw_ms              : {d.lag_raw_ms:0.0f}")
    lines.append(f"lag_ms                  : {d.lag_ms:0.0f}")
    lines.append(f"vol15m_sigma%(30/60/300): {d.vol_30s:6.2f}% / {d.vol_1m:6.2f}% / {d.vol_5m:6.2f}%")
    lines.append(f"sigma_rem%              : {d.sigma_rem_pct:6.2f}%")

    lines.append(f"P_yes                   : {d.prob_yes:0.4f}  (no-drift {d.p_yes_nd:0.4f})  Δ {d.prob_yes - d.p_yes_nd:+0.4f}")
    lines.append(f"drift mu_hat/s          : {d.mu_hat_per_s:+0.6e}   mu_T(rem) {d.mu_T:+0.6e}")
    sigma_T = d.sigma_rem_pct / 100.0
    ratio = (d.mu_T / sigma_T) if sigma_T > 1e-12 else 0.0
    lines.append(f"drift/sigma ratio       : {ratio:+0.2f}  (mu_T/sigma_T)")

    lines.append(f"mom pts(%)              : 5s {d.mom_5s:+7.2f}({d.mom_5s_pct:+0.3f}%)  10s {d.mom_10s:+7.2f}({d.mom_10s_pct:+0.3f}%)  15s {d.mom_15s:+7.2f}({d.mom_15s_pct:+0.3f}%)")
    lines.append(f"                         30s {d.mom_30s:+7.2f}({d.mom_30s_pct:+0.3f}%)   1m {d.mom_1m:+7.2f}({d.mom_1m_pct:+0.3f}%)   5m {d.mom_5m:+7.2f}({d.mom_5m_pct:+0.3f}%)")
    lines.append(f"z 5/10/15/30/1m         : {d.mom_z_5s:+0.2f} {d.mom_z_10s:+0.2f} {d.mom_z_15s:+0.2f} {d.mom_z_30s:+0.2f} {d.mom_z_1m:+0.2f}")

    p5 = pressure_imbalance_notional_5s_live(state.tape_driver)
    b = state.tape_driver.buy_notional_5s + state.tape_driver.sec_buy_notional
    s = state.tape_driver.sell_notional_5s + state.tape_driver.sec_sell_notional

    # --- colored values ---
    p5_col = color_signed(p5, f"{p5:+0.2f}")
    fast_col = color_signed(d.mom_z_combo_fast, f"{d.mom_z_combo_fast:+0.2f}")
    slow_col = color_signed(d.mom_z_combo_slow, f"{d.mom_z_combo_slow:+0.2f}")
    dist_col = color_signed(dist, f"{dist:+10.2f}")

    lines.append("-" * 72)
    lines.append("BURST TAPE (newest first)")

    for line in reversed(list(state.tape_driver.lines)):
        lines.append(line)

    lines.append("")
    lines.append(f"ATR2 bands              : 1m: ±{d.atr_1m:5.2f}, 5m: ±{d.atr_5m:5.2f}, 15m: ±{d.atr_15m:5.2f}")
    lines.append(f"strike                  : {d.strike:10.2f}")
    lines.append(f"last                    : {d.last:10.2f}")
    lines.append(f"dist                    : {dist_col}")
    lines.append("")
    lines.append(f"pressure(5s)            : {p5_col}                B {b:,.0f}  S {s:,.0f}")
    lines.append(f"mom z fast              : {fast_col}")
    lines.append(f"mom z slow              : {slow_col}")
    lines.append(f"FV yes/no               : {d.fv_yes:0.4f} / {d.fv_no:0.4f}")
    lines.append(f"FV no-drift yes/no      : {d.fv_yes_nd:0.4f} / {d.fv_no_nd:0.4f}")
    lines.append(f"FV Δ(yes)               : {d.fv_yes - d.fv_yes_nd:+0.4f}")
    # lines.append(f"d_last: {d.d_last:+10.2f}")

    return fit_to_height(lines, height)


def render_right_bottom(state: AppState, height: int) -> ANSI:
    """Render RIGHT-BOTTOM pane (Polymarket resolver)."""
    r = state.resolver
    strike = r.strike
    dist = r.last - strike

    lines: List[str] = []
    lines.append("POLYMARKET CRYPTO (RESOLVER / LAGGING)")
    lines.append(f"lag_raw_ms: {r.lag_raw_ms:0.0f}")
    lines.append(f"lag_ms: {r.lag_ms:0.0f}")
    lines.append("-" * 72)
    lines.append("BURST TAPE (newest first)")

    for line in reversed(list(state.tape_resolver.lines)):
        lines.append(line)

    lines.append("")
    lines.append(f"strike: {strike:10.2f}")
    lines.append(f"last  : {r.last:10.2f}")
    lines.append(f"dist  : {dist:+10.2f}")

    return fit_to_height(lines, height)
