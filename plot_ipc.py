# plot_ipc.py
from __future__ import annotations
from dataclasses import dataclass


@dataclass(slots=True)
class PlotSnap:
    ts_s: float
    win_start_ms: float  # NEW: used to detect 15m roll in the plot process
    # Row 1: Polymarket (prob space)
    yes_mid: float
    no_mid: float
    fv_yes_nd: float
    fv_no_nd: float

    # Row 2: Binance
    px_binance: float
    strike: float

    # Row 3: microstructure diagnostic
    yes_spread: float
    no_spread: float
    # optional alternative: imbalance (if you prefer)
    imb_yes_l1: float
    imb_no_l1: float

    # Row 4: edge diagnostics (optional but recommended)
    fv_gap_nd: float          # fv_yes_nd - canon.mid (YES-space)
    mid_micro_gap: float      # canon.mid - canon.micro (YES-space)


@dataclass(slots=True)
class PlotCtl:
    show: bool = True
    enabled: bool = True
    reset: bool = False
    win_start_s: float = 0.0   # <-- must be win_start_s (lowercase s)