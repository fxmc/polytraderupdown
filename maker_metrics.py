# maker_metrics.py
from __future__ import annotations
import math

# --- Defaults (tune later) ---
# dep_ema is "size per second" (shares/s). These are soft saturation points.
DEP0: float = 600.0     # at ~600 shares/s depletion -> dep term ~0.5
FLICK0: float = 2.5     # at ~2.5 changes/s -> flick term ~0.5

# spread is in price units (0..1). Polymarket ticks are often 0.001.
# We treat <= ~0.002 as "tight".
S0: float = 0.002

# Weights (sum ~ 1)
WD: float = 0.45  # depletion dominates
WF: float = 0.30  # discovery regime
WS: float = 0.15  # tight spread -> danger
WP: float = 0.10  # micro pressure against you

EPS: float = 1e-9


def clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else x)


def sat(x: float, x0: float) -> float:
    """
    Saturating map 0..∞ -> 0..1:
      x/(x+x0)
    """
    x = float(x)
    x0 = float(x0)
    if x <= 0.0:
        return 0.0
    return x / (x + max(EPS, x0))


def spread_term(spread: float) -> float:
    """
    Danger increases as spread gets tighter.
    Use sat(S0/spread, 1) so:
      spread=S0 -> 0.5
      spread<<S0 -> ~1
      spread>>S0 -> ~0
    """
    sp = float(spread)
    if sp <= EPS:
        return 1.0
    return sat(S0 / sp, 1.0)


def danger_score_bid(*, dep_ema: float, flicker_ema: float, spread: float, micro_bias: float) -> tuple[float, dict[str, float]]:
    """
    Maker danger if you are quoting a BID at touch.
    Pressure against a bid is micro_bias < 0.
    """
    dep = sat(dep_ema, DEP0)
    flick = sat(flicker_ema, FLICK0)
    spr = spread_term(spread)
    press = clamp01(-micro_bias)  # only penalize if against bid

    d = WD * dep + WF * flick + WS * spr + WP * press
    return clamp01(d), {"dep": dep, "flick": flick, "spr": spr, "press": press}


def danger_score_ask(*, dep_ema: float, flicker_ema: float, spread: float, micro_bias: float) -> tuple[float, dict[str, float]]:
    """
    Maker danger if you are quoting an ASK at touch.
    Pressure against an ask is micro_bias > 0 (upward pressure).
    """
    dep = sat(dep_ema, DEP0)
    flick = sat(flicker_ema, FLICK0)
    spr = spread_term(spread)
    press = clamp01(+micro_bias)  # only penalize if against ask

    d = WD * dep + WF * flick + WS * spr + WP * press
    return clamp01(d), {"dep": dep, "flick": flick, "spr": spr, "press": press}
