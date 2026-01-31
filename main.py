"""
Phase 2: UI + real AppState + fake data pump.

- State updated by a fake loop (will later be WS reducers)
- UI refresh loop invalidates at fixed cadence (10 Hz)
- Render reads state (no random prints inside render)
- No inline/nested functions: all handlers are top-level functions
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass, field
from functools import partial
from typing import Deque, List

from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import ANSI
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout
from prompt_toolkit.layout.containers import Window
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.layout.controls import FormattedTextControl

LEFT_W = 70

# -----------------------------
# State models
# -----------------------------

@dataclass(slots=True)
class OrderbookLevel:
    """One orderbook level: price and size."""
    px: float
    sz: float


@dataclass(slots=True)
class OrderbookState:
    """Orderbook for YES and NO, each with bids and asks (L1–L5)."""
    yes_bids: List[OrderbookLevel] = field(default_factory=list)
    yes_asks: List[OrderbookLevel] = field(default_factory=list)
    no_bids: List[OrderbookLevel] = field(default_factory=list)
    no_asks: List[OrderbookLevel] = field(default_factory=list)
    fv_yes: float = 0.0
    fv_no: float = 0.0
    updates: int = 0
    last_change_ms: float = 0.0
    pulse: str = "."


@dataclass(slots=True)
class BurstState:
    """Burst tape lines (fixed length, newest on top)."""
    lines: Deque[str] = field(default_factory=lambda: deque(maxlen=10))
    n_updates: int = 0
    lag_ms: float = 0.0


@dataclass(slots=True)
class DriverHeaderState:
    """Header fields for Binance driver pane."""
    symbol: str = "BTCUSDT"
    strike: float = 0.0
    last: float = 0.0
    d_last: float = 0.0
    lag_ms: float = 0.0
    vol_30s: float = 0.0
    vol_1m: float = 0.0
    vol_5m: float = 0.0
    mom_30s: float = 0.0
    mom_1m: float = 0.0
    mom_5m: float = 0.0
    fv_yes: float = 0.0
    fv_no: float = 0.0
    atr_1m: float = 0.0
    atr_5m: float = 0.0
    atr_15m: float = 0.0


@dataclass(slots=True)
class ResolverHeaderState:
    """Header fields for Polymarket resolver pane."""
    last: float = 0.0
    d_last: float = 0.0
    lag_ms: float = 0.0
    vol_30s: float = 0.0
    vol_1m: float = 0.0
    vol_5m: float = 0.0
    mom_30s: float = 0.0
    mom_1m: float = 0.0
    mom_5m: float = 0.0
    mid: float = 0.0
    last_trade: float = 0.0
    updates: int = 0


@dataclass(slots=True)
class AppState:
    """Top-level app state read by renderer and written by reducers/fake loop."""
    debug_left: bool = False
    book: OrderbookState = field(default_factory=OrderbookState)
    driver: DriverHeaderState = field(default_factory=DriverHeaderState)
    resolver: ResolverHeaderState = field(default_factory=ResolverHeaderState)
    tape_driver: BurstState = field(default_factory=BurstState)
    tape_resolver: BurstState = field(default_factory=BurstState)


# -----------------------------
# Key handlers (no inline funcs)
# -----------------------------

def on_toggle_debug(event, state: AppState) -> None:
    """Toggle left-pane debug mode."""
    state.debug_left = not state.debug_left
    event.app.invalidate()


def on_quit(event) -> None:
    """Quit the application cleanly."""
    event.app.exit()


def build_keybindings(state: AppState) -> KeyBindings:
    """Create keybindings for the application."""
    kb = KeyBindings()
    kb.add("d")(partial(on_toggle_debug, state=state))
    kb.add("q")(on_quit)
    return kb


# -----------------------------
# Rendering helpers
# -----------------------------
def fv_indicator_ansi(fv: float, mid: float) -> str:
    """Return a + or - indicator with colored background based on FV vs mid."""
    if fv > mid:
        return "\x1b[42m + \x1b[0m"  # green background
    if fv < mid:
        return "\x1b[41m - \x1b[0m"  # red background
    return "   "  # equal: blank/no signal


def format_px_sz_cum_header(px_w: int, sz_w: int, cum_w: int, prefix: str) -> str:
    """Header aligned to px/sz/cum columns."""
    return f"{prefix}{'px':>{px_w}} {'sz':>{sz_w}} {'cum sz':>{cum_w}}"


def format_px_sz_cum_sep(px_w: int, sz_w: int, cum_w: int, prefix: str) -> str:
    """Separator aligned to px/sz/cum columns."""
    return f"{prefix}{'-'*px_w} {'-'*sz_w} {'-'*cum_w}"


def format_level_row(tag: str, level: int, px: float, sz: float, cum_sz: float, px_w: int, sz_w: int, cum_w: int, debug: bool, tag_w: int) -> str:
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


def format_side(levels: List[OrderbookLevel], px_w: int, sz_w: int, cum_w: int, debug: bool, descending: bool, first_tag: str, tag_w: int) -> List[str]:
    """Format L1–L5 rows with cum sz; descending shows L5.L1 but cum is still from L1."""
    rows: List[str] = []
    lvls = levels[:5]
    cum = compute_cum_sizes(levels)

    if descending:
        # display order L5.L1 but need corresponding cum for that level
        display_lvls = list(reversed(lvls))
        display_levels = [5, 4, 3, 2, 1]
        display_cum = list(reversed(cum))  # cum[4]..cum[0]
        tag_index = 4  # label on L1 row (last displayed)
    else:
        display_lvls = lvls
        display_levels = [1, 2, 3, 4, 5]
        display_cum = cum
        tag_index = 0  # label on L1 row (first displayed)

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
    px_w = 6  # <<< 0.5500 is 6 chars
    sz_w = 7
    cum_w = 7
    mode = "DEBUG" if state.debug_left else "COMPACT"

    # prefix length = tag column + "L1  " + 1 indent space
    header_prefix = " " * (tag_w + 4 + 1)  # tag + 'L1  ' + indent

    lines: List[str] = []
    lines.append(f"PM CLOB (LEFT) | MODE: {mode}   (d toggle, q quit)")
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

    # --- ASKS block (display L5.L1), tag first row as ASK ---
    yes_asks = format_side(state.book.yes_asks, px_w, sz_w, cum_w, state.debug_left, True, "ASK", tag_w)
    no_asks = format_side(state.book.no_asks, px_w, sz_w, cum_w, state.debug_left, True, "ASK", tag_w)
    for i in range(5):
        lines.append(format_split_line(yes_asks[i], no_asks[i], left_width))

    # Blank line (keeps breathing room)
    lines.append(format_split_line("", "", left_width))

    # FV line (aligned within each half)
    fv_left = f"FV       {state.book.fv_yes:0.4f}"
    fv_right = f"{state.book.fv_no:0.4f}"
    lines.append(format_split_line(fv_left, f"{'':9}{fv_right}", left_width))

    # Blank line (separates FV from bids)
    lines.append(format_split_line("", "", left_width))

    # --- BIDS block (display L1.L5), tag first row as BID ---
    yes_bids = format_side(state.book.yes_bids, px_w, sz_w, cum_w, state.debug_left, False, "BID", tag_w)
    no_bids = format_side(state.book.no_bids, px_w, sz_w, cum_w, state.debug_left, False, "BID", tag_w)
    for i in range(5):
        lines.append(format_split_line(yes_bids[i], no_bids[i], left_width))

    lines.append("-" * 60)

    # Minimal metrics: show top-of-book for YES and NO
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

    # Footer: stacked lines (your requested layout)
    lines.append(f"updates   : {state.book.updates}")
    yes_badge = fv_indicator_ansi(state.book.fv_yes, yes_mid)
    no_badge = fv_indicator_ansi(state.book.fv_no, no_mid)

    lines.append(
        f"YES spread: {yes_spread:0.4f} mid: {yes_mid:0.4f}  FV: {state.book.fv_yes:0.4f}  {yes_badge}"
    )
    lines.append(
        f"NO  spread: {no_spread:0.4f} mid: {no_mid:0.4f}  FV: {state.book.fv_no:0.4f}  {no_badge}"
    )

    lines.append(f"pulse     : {state.book.pulse}")
    lines.append(f"last_ Δ_ms: {state.book.last_change_ms:0.0f}")

    return fit_to_height(lines, height)


def render_right_top(state: AppState, height: int) -> ANSI:
    """Render RIGHT-TOP pane (Binance driver)."""
    d = state.driver
    dist = d.last - d.strike
    lines: List[str] = []

    lines.append(f"BINANCE {d.symbol} (DRIVER)")
    lines.append(f"lag_ms: {d.lag_ms:0.0f}")
    lines.append(
        f"vol(30s/1m/5m): {d.vol_30s:7.4f} / {d.vol_1m:7.4f} / {d.vol_5m:7.4f}"
    )
    lines.append(
        f"mom(30s/1m/5m): {d.mom_30s:+0.4f} / {d.mom_1m:+0.4f} / {d.mom_5m:+0.4f}"
    )
    lines.append(f"ATR2 bands  1m: ±{d.atr_1m:10.2f}")
    lines.append(f"ATR2 bands  5m: ±{d.atr_5m:10.2f}")
    lines.append(f"ATR2 bands 15m: ±{d.atr_15m:10.2f}")
    lines.append("-" * 72)
    lines.append("BURST TAPE (newest first)")

    # Tape: render oldest->the newest so the newest lands on the last visible line
    for line in reversed(list(state.tape_driver.lines)):
        lines.append(line)

    # Footer at bottom (2 lines)
    lines.append("")
    lines.append(f"strike: {d.strike:10.2f}")
    lines.append(f"last  : {d.last:10.2f}")
    lines.append(f"dist  : {dist:+10.2f}")
    lines.append(f"d_last: {d.d_last:+10.2f}")

    return fit_to_height(lines, height)


def render_right_bottom(state: AppState, height: int) -> ANSI:
    """Render RIGHT-BOTTOM pane (Polymarket resolver) in the same layout as Binance."""
    r = state.resolver
    strike = state.driver.strike  # shared strike for the 15m market
    dist = r.last - strike

    lines: List[str] = []
    lines.append("POLYMARKET CRYPTO (RESOLVER / LAGGING)")
    lines.append(f"lag_ms: {r.lag_ms:0.0f}")
    lines.append(
        f"vol(30s/1m/5m): {r.vol_30s:7.4f} / {r.vol_1m:7.4f} / {r.vol_5m:7.4f}"
    )
    lines.append(
        f"mom(30s/1m/5m): {r.mom_30s:+0.4f} / {r.mom_1m:+0.4f} / {r.mom_5m:+0.4f}"
    )

    lines.append("-" * 72)
    lines.append("BURST TAPE (newest first)")

    # Tape: render oldest->newest so newest lands on the last visible line
    for line in reversed(list(state.tape_resolver.lines)):
        lines.append(line)

    lines.append("")
    lines.append(f"strike: {strike:10.2f}")
    lines.append(f"last  : {r.last:10.2f}")
    lines.append(f"dist  : {dist:+10.2f}")
    lines.append(f"d_last: {r.d_last:+10.2f}")

    return fit_to_height(lines, height)


# -----------------------------
# Layout + loops
# -----------------------------
def build_layout(state: AppState) -> Layout:
    """Build the prompt_toolkit layout with 35/65 split and right top/bottom split."""
    left_control = FormattedTextControl(lambda: render_left(state, height=55))
    top_control = FormattedTextControl(lambda: render_right_top(state, height=27))
    bot_control = FormattedTextControl(lambda: render_right_bottom(state, height=28))

    left_window = Window(
        content=left_control,
        width=Dimension.exact(LEFT_W),
        dont_extend_width=True,
        wrap_lines=False,
    )
    top_window = Window(content=top_control, wrap_lines=False)
    bot_window = Window(content=bot_control, wrap_lines=False)
    divider_h = Window(height=Dimension.exact(1), char="=")
    right_split = HSplit([top_window, divider_h, bot_window], padding=0)
    divider = Window(width=1, char="│")
    root = VSplit([left_window, divider, right_split], padding=1)

    return Layout(root)


async def ui_refresh_loop(app: Application, hz: float) -> None:
    """Invalidate the UI on a fixed cadence (renders happen here, not in writers)."""
    period = 1.0 / hz
    while True:
        await asyncio.sleep(period)
        app.invalidate()


def init_state(state: AppState) -> None:
    """Initialize state with sane starting values."""
    # YES book around 0.50
    state.book.yes_bids = [OrderbookLevel(px=0.5000 - 0.0100 * i, sz=1200.0 - 50.0 * i) for i in range(5)]
    state.book.yes_asks = [OrderbookLevel(px=0.5100 + 0.0100 * i, sz=1042.0 + 40.0 * i) for i in range(5)]

    # NO book around 0.50 (illustrative; fake symmetry)
    state.book.no_bids = [OrderbookLevel(px=0.5000 - 0.0100 * i, sz=1200.0 - 50.0 * i) for i in range(5)]
    state.book.no_asks = [OrderbookLevel(px=0.5100 + 0.0100 * i, sz=1042.0 + 40.0 * i) for i in range(5)]

    # Driver starts near a made-up strike.
    state.driver.strike = 42000.0
    state.driver.last = 42000.0

    state.book.fv_yes = 0.5055
    state.book.fv_no = 0.4955

    # Seed tapes stable.
    for _ in range(10):
        state.tape_driver.lines.append("")
        state.tape_resolver.lines.append("")


def compute_fake_price(t: float, base: float) -> float:
    """Deterministic, smooth-ish fake price."""
    return base + 35.0 * math.sin(t * 0.8) + 12.0 * math.sin(t * 2.3)


def push_burst_line(tape: BurstState, last: float, d_last: float) -> None:
    """Push one newest-first burst tape line."""
    now = time.strftime("%H:%M:%S")
    tape.n_updates += 1
    tape.lag_ms = 40.0 + (tape.n_updates % 7) * 3.0  # fake jitter
    line = f"{now}  {last:0.2f}  Δ {d_last:+0.2f}  n {tape.n_updates:4d}  lag {tape.lag_ms:0.0f}ms"
    tape.lines.appendleft(line)


def update_fake_orderbook(state: AppState, t: float) -> None:
    """Fake book updates: keep prices stable-ish, wiggle best bid/ask sizes (pulse behavior)."""
    state.book.updates += 1
    state.book.pulse = "*" if (state.book.updates % 2 == 0) else "."
    state.book.last_change_ms = 100.0

    wiggle = 30.0 * (1.0 + math.sin(t * 3.0))

    # YES best bid/ask size wiggle
    if state.book.yes_bids:
        state.book.yes_bids[0].sz = 1200.0 + wiggle
    if state.book.yes_asks:
        state.book.yes_asks[0].sz = 1042.0 + 0.8 * wiggle

    # NO best bid/ask size wiggle
    if state.book.no_bids:
        state.book.no_bids[0].sz = 1200.0 + 0.9 * wiggle
    if state.book.no_asks:
        state.book.no_asks[0].sz = 1042.0 + 0.7 * wiggle

    state.book.fv_yes = 0.505 + 0.001 * math.sin(t * 0.7)
    state.book.fv_no = 1.0 - state.book.fv_yes


def update_fake_headers(state: AppState, t: float) -> None:
    """Fake header fields for driver/resolver (strike/ATR/vol/mom)."""
    d = state.driver
    prev = d.last
    d.last = compute_fake_price(t, d.strike)
    d.d_last = d.last - prev
    d.lag_ms = 20.0 + (int(t * 10) % 5) * 4.0

    # Fake online metrics (placeholders you’ll replace with real reducers)
    d.vol_30s = 0.012 + 0.004 * abs(math.sin(t * 0.7))
    d.vol_1m = 0.010 + 0.003 * abs(math.sin(t * 0.4))
    d.vol_5m = 0.008 + 0.002 * abs(math.sin(t * 0.2))
    d.mom_30s = d.d_last / 1000.0
    d.mom_1m = d.d_last / 1400.0
    d.mom_5m = d.d_last / 2000.0

    # Fake fair value YES/NO (just to populate slots)
    d.fv_yes = 0.48 + 0.02 * math.sin(t * 0.6)
    d.fv_no = 1.0 - d.fv_yes

    # Fake ATR(2) bands (placeholders)
    d.atr_1m = 18.0 + 6.0 * abs(math.sin(t * 0.8))
    d.atr_5m = 35.0 + 10.0 * abs(math.sin(t * 0.5))
    d.atr_15m = 60.0 + 15.0 * abs(math.sin(t * 0.3))

    # Resolver lags driver slightly
    r = state.resolver
    prev_r = r.last
    r.last = d.last - 5.0 * math.sin(t * 1.1)
    r.d_last = r.last - prev_r
    r.lag_ms = d.lag_ms + 35.0
    r.mid = r.last - 0.5
    r.last_trade = r.last + 0.25 * math.sin(t * 2.0)
    r.updates += 1

    r.vol_30s = d.vol_30s * 0.95
    r.vol_1m = d.vol_1m * 0.95
    r.vol_5m = d.vol_5m * 0.95
    r.mom_30s = d.mom_30s * 0.90
    r.mom_1m = d.mom_1m * 0.90
    r.mom_5m = d.mom_5m * 0.90


async def fake_data_loop(state: AppState, hz: float) -> None:
    """Update state on a fixed cadence (stand-in for WS reducers)."""
    period = 1.0 / hz
    t0 = time.monotonic()

    while True:
        t = time.monotonic() - t0

        update_fake_orderbook(state, t)
        update_fake_headers(state, t)
        push_burst_line(state.tape_driver, state.driver.last, state.driver.d_last)
        push_burst_line(state.tape_resolver, state.resolver.last, state.resolver.d_last)

        await asyncio.sleep(period)


async def run_app() -> None:
    """Create and run the application."""
    state = AppState()
    init_state(state)

    layout = build_layout(state)
    kb = build_keybindings(state)

    app = Application(layout=layout, key_bindings=kb, full_screen=True)

    asyncio.create_task(ui_refresh_loop(app, hz=10.0))
    asyncio.create_task(fake_data_loop(state, hz=10.0))

    await app.run_async()


def main() -> None:
    """Program entrypoint."""
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
