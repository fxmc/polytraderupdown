from __future__ import annotations

import asyncio
import re
import statistics
import subprocess
import time
import logging

from state import AppState

# IMPORTANT: never write to stdout/stderr from background tasks in a prompt_toolkit app.
# This logger is intentionally silenced unless the app config attaches handlers elsewhere.
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())
LOGGER.propagate = False

_OFFSET_RE = re.compile(r"([+-]\d+\.\d+)s")


def _run_w32tm_stripchart(host: str, samples: int) -> str:
    """
    Blocking call to w32tm. Returns stdout+stderr as text.
    """
    cmd = [
        "w32tm",
        "/stripchart",
        f"/computer:{host}",
        f"/samples:{samples}",
        "/dataonly",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    # w32tm sometimes writes useful info to stdout even on non-zero exit
    return (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")


def _parse_server_minus_local_seconds(output: str) -> list[float]:
    """
    Extract offsets from w32tm output lines, returned as seconds.

    w32tm lines look like:
      23:54:27, -00.9800138s

    That number is effectively: (server_time - local_time) in seconds.
    """
    vals: list[float] = []
    for line in output.splitlines():
        m = _OFFSET_RE.search(line)
        if not m:
            continue
        try:
            vals.append(float(m.group(1)))
        except Exception:
            continue
    return vals


def _ewma(prev: float, x: float, alpha: float) -> float:
    return alpha * x + (1.0 - alpha) * prev


def _append_clock_sync_err(state: AppState, msg: str) -> None:
    """Write a short error line into the current run dir (never stdout/stderr)."""
    try:
        p = state.run_ctx.run_path("clock_sync.err.log")
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with p.open("a", encoding="utf-8") as f:
            f.write(f"{ts} {msg}\n")
    except Exception:
        # last resort: never print to terminal
        return


async def cloudflare_ntp_offset_task(
    state,
    *,
    host: str = "time.cloudflare.com",  # time.cloudflare.com, time.windows.com
    samples: int = 10,
    every_s: float = 30.0,
    alpha: float = 0.15,
) -> None:
    """
    Periodically estimates local clock offset using Cloudflare NTP via w32tm.

    Updates:
      state.diag.clock_offset_ms  (positive => local clock ahead)
      state.diag.clock_offset_src ("w32tm(time.cloudflare.com)")

    Notes:
    - Uses asyncio.to_thread so we don't block the asyncio loop.
    - Uses median(samples) then EWMA smoothing.
    """
    prev = float(getattr(state.diag, "clock_offset_ms", 0.0) or 0.0)
    if prev == 0.0:
        state.diag.clock_offset_src = "...waiting"
    else:
        state.diag.clock_offset_src = f"w32tm({host})"

    while True:
        try:
            out = await asyncio.to_thread(_run_w32tm_stripchart, host, samples)
            offsets = _parse_server_minus_local_seconds(out)

            if offsets:
                # offsets are (server - local) in seconds.
                # We want (local - server) in ms, so negate and *1000.
                median_s = statistics.median(offsets)
                meas_ms = (-median_s) * 1000.0

                # guardrail against insane jumps (bad parse / transient)
                if abs(meas_ms - prev) <= 30_000.0:
                    new = meas_ms if prev == 0.0 else _ewma(prev, meas_ms, alpha)
                    state.diag.clock_offset_ms = new
                    state.diag.clock_offset_src = f"w32tm({host})"
                    prev = new

        except subprocess.TimeoutExpired:
            # keep last good value; do NOT emit traceback to terminal
            state.diag.clock_offset_src = f"w32tm({host}) TIMEOUT"
            _append_clock_sync_err(state, f"TIMEOUT host={host} samples={samples}")

        except Exception as e:
            # keep last good value; do NOT emit traceback to terminal
            state.diag.clock_offset_src = f"w32tm({host}) ERROR"
            _append_clock_sync_err(
                state,
                f"ERROR host={host} samples={samples} err={type(e).__name__}: {e}",
            )

        await asyncio.sleep(every_s)


if __name__ == "__main__":
    state = AppState()
    asyncio.run(cloudflare_ntp_offset_task(state, host="time.cloudflare.com"))
