"""
Phase 1: Minimal prompt_toolkit UI skeleton.

- 3 panes: LEFT orderbook, RIGHT-TOP Binance, RIGHT-BOTTOM Polymarket resolver
- Fixed refresh cadence (10 Hz)
- Hotkeys: d (toggle debug), q (quit)
- No ingestion yet. Pure stub rendering.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from prompt_toolkit.application import Application
from prompt_toolkit.formatted_text import ANSI
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout
from prompt_toolkit.layout.containers import Window
from prompt_toolkit.layout.controls import FormattedTextControl


@dataclass(slots=True)
class AppState:
    """In-memory app state used by the renderer."""
    debug_left: bool = False


def build_keybindings(state: AppState) -> KeyBindings:
    """Create keybindings for the application."""
    kb = KeyBindings()

    @kb.add("d")
    def _toggle_debug(event) -> None:
        """Toggle left-pane debug mode."""
        state.debug_left = not state.debug_left
        event.app.invalidate()

    @kb.add("q")
    def _quit(event) -> None:
        """Quit the application cleanly."""
        event.app.exit()

    return kb


def render_left(state: AppState, height: int) -> ANSI:
    """Render LEFT pane content with a fixed row budget."""
    mode = "DEBUG" if state.debug_left else "COMPACT"
    lines: list[str] = []

    lines.append(f"PM CLOB (LEFT) | MODE: {mode}   (press d toggle, q quit)")
    lines.append("-" * 60)
    lines.append("YES (UP)                 | NO (DOWN)")
    lines.append("px      sz               | px      sz")
    lines.append("----    ----             | ----    ----")

    # L1-L5 stub rows (always present)
    for level in range(1, 6):
        if state.debug_left:
            left = f"{0.5000 + level*0.0010:0.4f} @ {1000+level:4d}"
            right = f"{0.5000 - level*0.0010:0.4f} @ {900+level:4d}"
        else:
            left = f"{0.5000 + level*0.0010:0.4f}  {1000+level:4d}"
            right = f"{0.5000 - level*0.0010:0.4f}  {900+level:4d}"
        lines.append(f"L{level}  {left:<20} | L{level}  {right:<20}")

    lines.append("-" * 60)
    lines.append("spread: 0.0020   mid: 0.5000   imb: 0.00   micro: 0.5000")
    lines.append("pulse: .   last_change_ms: ----   updates: ----")
    lines.append("")
    lines.append("Reserved rows (future metrics)")

    return fit_to_height(lines, height)


def render_right_top(height: int) -> ANSI:
    """Render RIGHT-TOP pane (Binance driver) stub."""
    lines: list[str] = []
    lines.append("BINANCE BTCUSDT (DRIVER)")
    lines.append("last: ----   lag_ms: ----   d_last: ----")
    lines.append("vol(30s/1m/5m): ---- / ---- / ----    mom(30s/1m/5m): ---- / ---- / ----")
    lines.append("FV(YES/NO): ---- / ----    strike: ----    dist: ----")
    lines.append("ATR2 bands: 1m ----  | 5m ---- | 15m ----")
    lines.append("-" * 72)
    lines.append("BURST TAPE (one line per refresh interval)")
    for i in range(1, 11):
        lines.append(f"{i:02d}  time ----  open->last ----  Δ ----  [hi/lo ----/----]  n ----  lag ----")

    return fit_to_height(lines, height)


def render_right_bottom(height: int) -> ANSI:
    """Render RIGHT-BOTTOM pane (Polymarket resolver) stub."""
    lines: list[str] = []
    lines.append("POLYMARKET CRYPTO (RESOLVER / LAGGING)")
    lines.append("last: ----   lag_ms: ----   d_last: ----")
    lines.append("vol(30s/1m/5m): ---- / ---- / ----    mom(30s/1m/5m): ---- / ---- / ----")
    lines.append("mid: ----   last_trade: ----   updates: ----   (expect lag on spikes)")
    lines.append("-" * 72)
    lines.append("BURST TAPE (quieter)")
    for i in range(1, 11):
        lines.append(f"{i:02d}  time ----  open->last ----  Δ ----  [hi/lo ----/----]  n ----  lag ----")

    return fit_to_height(lines, height)


def fit_to_height(lines: list[str], height: int) -> ANSI:
    """Pad or truncate rendered text to exactly `height` rows."""
    if len(lines) < height:
        lines = lines + [""] * (height - len(lines))
    else:
        lines = lines[:height]
    return ANSI("\n".join(lines))


def build_layout(state: AppState) -> Layout:
    """Build the prompt_toolkit layout with 35/65 split and right top/bottom split."""
    # We keep controls stable; content updates via callable text functions.
    left_control = FormattedTextControl(lambda: render_left(state, height=55))
    top_control = FormattedTextControl(lambda: render_right_top(height=27))
    bot_control = FormattedTextControl(lambda: render_right_bottom(height=28))

    left_window = Window(content=left_control, wrap_lines=False)
    top_window = Window(content=top_control, wrap_lines=False)
    bot_window = Window(content=bot_control, wrap_lines=False)

    root = VSplit(
        [
            left_window,
            HSplit([top_window, bot_window]),
        ],
        padding=1,
    )

    return Layout(root)


async def ui_refresh_loop(app: Application, hz: float) -> None:
    """Invalidate the UI on a fixed cadence."""
    period = 1.0 / hz
    while True:
        await asyncio.sleep(period)
        app.invalidate()


async def run_app() -> None:
    """Create and run the application."""
    state = AppState()
    layout = build_layout(state)
    kb = build_keybindings(state)

    app = Application(layout=layout, key_bindings=kb, full_screen=True)

    # Start refresh loop (10 Hz). WS tasks will be added later.
    asyncio.create_task(ui_refresh_loop(app, hz=10.0))

    await app.run_async()


def main() -> None:
    """Program entrypoint."""
    asyncio.run(run_app())


if __name__ == "__main__":
    main()
