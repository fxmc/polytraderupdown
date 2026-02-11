"""run_context.py

Single source of truth for *all* on-disk outputs.

Key idea:
  - Every process writes under:  logs/<run_id>/...
  - run_id is derived from the *current BTC 15m market window start* (UTC).
  - When the market rolls (every 15m), run_id (and therefore run_dir) rolls too.

This module is intentionally small and dependency-light so every module can use it.
"""

from __future__ import annotations

import datetime as dt
import os
import time
from dataclasses import dataclass
from pathlib import Path

from candles import TF_15M_MS


def _utc_run_id_from_start_ms(start_ms: int) -> str:
    """Format: run_YYYYMMDDTHHMMSSZ (UTC, no colons)."""
    if start_ms <= 0:
        start_ms = int(time.time() * 1000)
    t = dt.datetime.fromtimestamp(start_ms / 1000.0, tz=dt.timezone.utc)
    return "run_" + t.strftime("%Y%m%dT%H%M%SZ")


def bucket_start_ms_from_serverish_clock(
    *,
    last_clob_event_ms: int | float | None,
    clock_offset_ms: float | None,
    now_local_ms: int | None = None,
) -> int:
    """Best-effort "serverish" clock consistent with polymarket_clob_autoresolve_task.

    Priority:
      1) last_clob_event_ms (Polymarket WS timestamp)
      2) local wall clock corrected by clock_offset_ms (where offset = local - server)
      3) local wall clock
    """
    book_event_ms = int(last_clob_event_ms or 0)
    if book_event_ms > 0:
        now_ms = book_event_ms
    else:
        local_now_ms = int(now_local_ms if now_local_ms is not None else time.time() * 1000)
        off = float(clock_offset_ms or 0.0)
        now_ms = int(local_now_ms - off)
    return int((now_ms // TF_15M_MS) * TF_15M_MS)


@dataclass(slots=True)
class RunContext:
    """Holds the current run directory semantics."""

    base_dir: str = "logs"
    market_start_ms: int = 0

    def ensure(self) -> None:
        os.makedirs(self.run_dir, exist_ok=True)

    @property
    def run_id(self) -> str:
        return _utc_run_id_from_start_ms(int(self.market_start_ms))

    @property
    def run_dir(self) -> str:
        return str(Path(self.base_dir) / self.run_id)

    def set_market_start_ms(self, start_ms: int) -> bool:
        """Update run context. Returns True if this is a *new* run."""
        start_ms = int(start_ms)
        if start_ms <= 0:
            return False
        if int(self.market_start_ms) == start_ms:
            return False
        self.market_start_ms = start_ms
        self.ensure()
        return True

    def run_path(self, rel: str) -> Path:
        """Resolve a relative path under the current run directory."""
        p = Path(self.run_dir) / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
