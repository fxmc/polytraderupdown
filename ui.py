"""
UI construction and UI refresh loop.
"""

from __future__ import annotations

import asyncio
from functools import partial

from prompt_toolkit.application import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout
from prompt_toolkit.layout.containers import Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import Dimension

from config import LEFT_W
from render import render_left, render_right_bottom, render_right_top
from state import AppState


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


def build_layout(state: AppState) -> Layout:
    """Build the prompt_toolkit layout with fixed left width and right top/bottom split."""
    left_control = FormattedTextControl(lambda: render_left(state, height=65))
    top_control = FormattedTextControl(lambda: render_right_top(state, height=45))
    bot_control = FormattedTextControl(lambda: render_right_bottom(state, height=20))

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
    root = VSplit([left_window, divider, right_split], padding=0)

    return Layout(root)


async def ui_refresh_loop(app: Application, hz: float) -> None:
    """Invalidate the UI on a fixed cadence (renders happen here, not in writers)."""
    period = 1.0 / hz
    while True:
        await asyncio.sleep(period)
        app.invalidate()
