from __future__ import annotations

import asyncio
import re
import statistics
import subprocess
from dataclasses import dataclass
from typing import Optional

from state import AppState

_OFFSET_RE = re.compile(r"([+-]\d+\.\d+)s")


def _run_w32tm_stripchart(host: str, samples: int) -> str:
    """
    Blocking call to w32tm. Returns stdout as text.
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
    # warm start from whatever is currently in state
    prev = float(getattr(state.diag, "clock_offset_ms", 0.0))
    state.diag.clock_offset_ms = 0.0
    state.diag.clock_offset_src = "...waiting"
    while True:
        try:
            out = await asyncio.to_thread(_run_w32tm_stripchart, host, samples)
            offsets = _parse_server_minus_local_seconds(out)
            # print('offsets:', offsets)

            if offsets:
                # offsets are (server - local) in seconds.
                # We want (local - server) in ms, so negate and *1000.
                median_s = statistics.median(offsets)
                meas_ms = (-median_s) * 1000.0

                # guardrail against insane jumps (bad parse / transient)
                if abs(meas_ms - prev) <= 15_000.0:
                    if prev == 0.0:
                        new = meas_ms  # snap first estimate
                    else:
                        new = _ewma(prev, meas_ms, alpha)

                    state.diag.clock_offset_ms = new
                    state.diag.clock_offset_src = f"w32tm({host})"
                    prev = new

        except Exception as e:
            # keep last good value; no spam logging here
            print('Error: ', str(e))
            pass

        await asyncio.sleep(every_s)


if __name__ == "__main__":
    state = AppState()
    asyncio.run(cloudflare_ntp_offset_task(state, host="time.cloudflare.com"))
