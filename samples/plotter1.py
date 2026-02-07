"""
Asyncio skeleton: prompt_toolkit + matplotlib live plot fed by async "websocket" messages.

Keys:
  p = start/resume plotting (opens matplotlib window)
  s = stop/pause plotting updates
  q = quit

Requires:
  pip install prompt_toolkit matplotlib
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from dataclasses import dataclass

import matplotlib.pyplot as plt

from prompt_toolkit.application import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Layout
from prompt_toolkit.layout.containers import HSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style


# -----------------------------
# Shared state
# -----------------------------

@dataclass
class SharedState:
    running: bool = True
    plotting_enabled: bool = False
    n_received: int = 0
    last_value: float = 0.0
    last_ts: float = 0.0


state = SharedState()

SERIES = deque(maxlen=600)   # last N points for plotting
# In real life: queue receives parsed WS messages (or raw, then parse in reducer)
queue: asyncio.Queue[tuple[float, float]] = asyncio.Queue(maxsize=10_000)

# prompt_toolkit app handle so other tasks can invalidate UI
pt_app: Application | None = None


# -----------------------------
# Simulated "websocket" producer
# -----------------------------

async def fake_ws_producer():
    """
    Simulate incoming WS messages: (timestamp, value) every second.
    Replace with your real websocket reader task:
        async for msg in ws: ... await queue.put(parsed)
    """
    while state.running:
        ts = time.time()
        v = random.random()
        try:
            queue.put_nowait((ts, v))
        except asyncio.QueueFull:
            # If you're falling behind, you either:
            # - drop old data, or
            # - backpressure upstream, or
            # - reduce work per message
            pass
        await asyncio.sleep(1.0)


# -----------------------------
# Reducer: consumes queue, updates buffer/state
# -----------------------------

async def reducer():
    """
    Consume incoming messages and update in-memory state.
    Keep it lightweight; do heavier calculations in separate tasks if needed.
    """
    while state.running:
        ts, v = await queue.get()
        SERIES.append(v)
        state.n_received += 1
        state.last_value = v
        state.last_ts = ts

        # Wake TUI to redraw occasionally.
        # (Don't spam invalidate on every message if you're at 7k/s.)
        if pt_app is not None and (state.n_received % 10 == 0):
            pt_app.invalidate()


# -----------------------------
# Matplotlib plotter (async)
# -----------------------------

def init_plot():
    """
    Create the figure/line. This runs on the same thread as the asyncio loop.
    NOTE: matplotlib is not truly asyncio-native; we use plt.pause() in the async loop.
    """
    plt.ion()
    fig, ax = plt.subplots()
    (line,) = ax.plot([], [], lw=1)
    ax.set_title("Live async stream")
    ax.set_xlabel("t")
    ax.set_ylabel("value")
    ax.set_ylim(0, 1)
    return fig, ax, line


async def plotter(update_hz: float = 10.0):
    fig, ax, line = init_plot()
    dt = 1.0 / update_hz

    while state.running:
        if not plt.fignum_exists(fig.number):
            break
        # ---- ALWAYS pump Tk events ----
        plt.pause(0.001)

        # Window manually closed?
        if not plt.fignum_exists(fig.number):
            state.plotting_enabled = False
            break

        if state.plotting_enabled:
            ys = list(SERIES)
            if ys:
                xs = list(range(len(ys)))
                line.set_data(xs, ys)
                ax.set_xlim(0, max(10, len(xs) - 1))
                fig.canvas.draw_idle()

        await asyncio.sleep(dt)

    try:
        plt.close(fig)
    except Exception:
        pass


# -----------------------------
# prompt_toolkit UI (async)
# -----------------------------

def make_ui_control():
    def get_text():
        n = state.n_received
        last = state.last_value
        ts = state.last_ts
        plot_on = state.plotting_enabled
        qsize = queue.qsize()
        buf = len(SERIES)
        status = "ON" if plot_on else "OFF"

        return [
            ("class:title", "Async TUI + Matplotlib skeleton\n"),
            ("", "\n"),
            ("class:label", "Keys: "),
            ("class:key", "p"),
            ("", " plot/resume  "),
            ("class:key", "s"),
            ("", " stop/pause  "),
            ("class:key", "q"),
            ("", " quit\n"),
            ("", "\n"),
            ("class:label", "Msgs: "),
            ("", f"{n}  "),
            ("class:label", "Queue: "),
            ("", f"{qsize}  "),
            ("class:label", "Buffer: "),
            ("", f"{buf}\n"),
            ("class:label", "Last: "),
            ("", f"{last:.6f} @ {ts:.3f}\n"),
            ("class:label", "Plotting: "),
            ("", f"{status}\n"),
        ]

    return FormattedTextControl(get_text)


def build_app() -> Application:
    kb = KeyBindings()

    @kb.add("q")
    def _(event):
        state.running = False
        event.app.exit()

    @kb.add("p")
    def _(event):
        state.plotting_enabled = True
        event.app.invalidate()

    @kb.add("s")
    def _(event):
        state.plotting_enabled = False
        event.app.invalidate()

    root = HSplit(
        [
            Window(content=make_ui_control(), always_hide_cursor=True),
            Window(height=1, char="-"),
            Window(
                content=FormattedTextControl(
                    lambda: [("class:hint", "Structure: WS -> queue -> reducer -> plotter (fixed cadence)\n")]
                ),
                height=1,
            ),
        ]
    )

    style = Style.from_dict(
        {
            "title": "bold",
            "label": "bold",
            "key": "reverse",
            "hint": "",
        }
    )

    return Application(layout=Layout(root), key_bindings=kb, style=style, full_screen=True)


# -----------------------------
# Main
# -----------------------------

async def main():
    global pt_app

    # Seed some data
    for _ in range(20):
        SERIES.append(random.random())
        state.n_received += 1
    state.last_value = SERIES[-1]
    state.last_ts = time.time()

    pt_app = build_app()

    # Launch tasks (replace fake_ws_producer with your real websocket consumer)
    tasks = [
        asyncio.create_task(fake_ws_producer(), name="ws-producer"),
        asyncio.create_task(reducer(), name="reducer"),
        asyncio.create_task(plotter(update_hz=10.0), name="plotter"),
    ]

    try:
        # Run TUI inside asyncio loop
        await pt_app.run_async()
    finally:
        state.running = False
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())