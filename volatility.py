from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


@dataclass(slots=True)
class RollingRV:
    """Rolling realized variance from 1s log-returns: maintains sum(r^2)."""
    n: int
    r2: Deque[float] = field(init=False)
    sum_r2: float = 0.0

    def __post_init__(self) -> None:
        self.r2 = deque(maxlen=self.n)

    def update(self, r: float) -> None:
        x = float(r) * float(r)
        if len(self.r2) == self.n:
            self.sum_r2 -= float(self.r2[0])
        self.r2.append(x)
        self.sum_r2 += x

    def var_hat(self) -> float:
        n = len(self.r2)
        return (self.sum_r2 / float(n)) if n > 0 else 0.0


@dataclass(slots=True)
class EwmaVar:
    """EWMA variance of 1s log-returns."""
    alpha: float
    v: float = 0.0
    initialized: bool = False

    def update(self, r: float) -> float:
        x = float(r) * float(r)
        if not self.initialized:
            self.v = x
            self.initialized = True
            return self.v
        self.v = (1.0 - self.alpha) * self.v + self.alpha * x
        return self.v


@dataclass(slots=True)
class VolStack:
    """
    Online volatility stack from 1s sampled prices.

    Outputs variance estimates in units of:
      var_hat = E[r^2], where r is 1-second log-return.
    """
    r_clip: float
    last_px: float = 0.0

    rv_30: RollingRV = field(default_factory=lambda: RollingRV(30))
    rv_60: RollingRV = field(default_factory=lambda: RollingRV(60))
    rv_300: RollingRV = field(default_factory=lambda: RollingRV(300))

    ewma_fast: EwmaVar = field(default_factory=lambda: EwmaVar(alpha=0.12))
    ewma_slow: EwmaVar = field(default_factory=lambda: EwmaVar(alpha=0.02))

    def update_price_1s(self, px: float) -> Dict[str, float]:
        px = float(px)
        if self.last_px <= 0.0 or px <= 0.0:
            self.last_px = px
            return {"v30": 0.0, "v60": 0.0, "v300": 0.0, "v_fast": 0.0, "v_slow": 0.0}

        r = math.log(px / self.last_px)
        self.last_px = px

        r = clamp(r, -self.r_clip, self.r_clip)

        self.rv_30.update(r)
        self.rv_60.update(r)
        self.rv_300.update(r)

        v_fast = self.ewma_fast.update(r)
        v_slow = self.ewma_slow.update(r)

        return {
            "v30": self.rv_30.var_hat(),
            "v60": self.rv_60.var_hat(),
            "v300": self.rv_300.var_hat(),
            "v_fast": v_fast,
            "v_slow": v_slow,
        }


def sigma_over_seconds(var_hat: float, t_s: float) -> float:
    """Convert per-step variance estimate (E[r^2]) to sigma over t_s seconds."""
    if var_hat <= 0.0 or t_s <= 0.0:
        return 0.0
    return math.sqrt(var_hat * float(t_s))
