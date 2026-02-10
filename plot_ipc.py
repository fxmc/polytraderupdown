# plot_ipc.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

_NAN = float("nan")


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

    # Imbalance (optional horizons)
    imb_yes_l1: float = _NAN
    imb_no_l1: float = _NAN
    imb_yes_l5: float = _NAN
    imb_no_l5: float = _NAN

    # Row 4: momentum / edge diagnostics (all optional; default NaN)
    pressure: float = _NAN
    momz_fast: float = _NAN
    momz_slow: float = _NAN
    mu_over_sigma: float = _NAN  # "drift normalized by vol" (unitless z-ish)
    sigma_eff: float = _NAN      # useful when debugging FV quiet/hold behavior

    muT_over_sigmaT: float = _NAN  # consistent with renderer (mu_T / sigma_T)

    # Row 4: edge diagnostics (optional but recommended)
    fv_gap_nd: float = _NAN          # fv_yes_nd - canon.mid (YES-space)
    mid_micro_gap: float = _NAN      # canon.mid - canon.micro (YES-space)


@dataclass(slots=True)
class PlotCtl:
    show: bool = True
    enabled: bool = True
    reset: bool = False
    win_start_s: float = 0.0   # <-- must be win_start_s (lowercase s)

    # NEW: market title/question for header (plot process has no access to AppState)
    market_question: Optional[str] = None

    # NEW: deterministic YES/NO token mapping for markers
    yes_token_id: Optional[int] = None
    no_token_id: Optional[int] = None


@dataclass(slots=True)
class PlotMarker:
    """
    Discrete trader fill marker to overlay on Row 1 chart (prob space).
    """
    ts_s: float
    win_start_ms: float

    y: float              # prob price in [0,1]
    token_id: int

    role: str             # "MAKER" | "TAKER"
    side: str             # "BUY" | "SELL"
    size: float           # outcome tokens (scaled)
    price: Optional[float]  # same as y usually, but keep explicit
    tx_hash: str
