"""
Rolling z-scores for momentum horizons and a simple composite score.

All updates are O(1), memory is O(lookback) per horizon.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List


@dataclass(slots=True)
class RollingZ:
    """Rolling z-score using a fixed-length window with sum/sumsq."""
    lookback: int
    min_count: int
    window: Deque[float] = field(init=False)
    sum_x: float = 0.0
    sum_x2: float = 0.0

    def __post_init__(self) -> None:
        """Initialize window."""
        self.window = deque(maxlen=self.lookback)

    def update(self, x: float) -> float:
        """Insert x and return current z-score; returns 0.0 until min_count and nonzero variance."""
        if len(self.window) == self.lookback:
            old = float(self.window[0])
            self.sum_x -= old
            self.sum_x2 -= old * old

        self.window.append(float(x))
        self.sum_x += float(x)
        self.sum_x2 += float(x) * float(x)

        n = len(self.window)
        if n < self.min_count:
            return 0.0

        mean = self.sum_x / float(n)
        var = self.sum_x2 / float(n) - mean * mean
        if var <= 1e-12:
            return 0.0

        return (float(x) - mean) / math.sqrt(var)


@dataclass(slots=True)
class MomentumZConfig:
    """Configuration for momentum z-score composite."""
    horizons_s: List[int]
    lookback: int = 30
    min_count: int = 20


@dataclass(slots=True)
class MomentumZTracker:
    """Tracks rolling z-scores per horizon and computes composite average."""
    cfg: MomentumZConfig
    trackers: Dict[int, RollingZ] = field(default_factory=dict)

    def ensure(self) -> None:
        """Ensure trackers exist for all horizons."""
        for h in self.cfg.horizons_s:
            if h not in self.trackers:
                self.trackers[h] = RollingZ(self.cfg.lookback, self.cfg.min_count)

    def update(self, mom_by_h: Dict[int, float]) -> Dict[int, float]:
        """Update all configured horizons and return z-scores by horizon."""
        self.ensure()
        out: Dict[int, float] = {}
        for h in self.cfg.horizons_s:
            out[h] = self.trackers[h].update(float(mom_by_h.get(h, 0.0)))
        return out

    def combo(self, z_by_h: Dict[int, float]) -> float:
        """Return average z-score across horizons that have enough samples."""
        if not self.cfg.horizons_s:
            return 0.0
        s = 0.0
        k = 0
        for h in self.cfg.horizons_s:
            rz = self.trackers.get(h)
            if rz is None:
                continue
            if len(rz.window) < rz.min_count:
                continue
            s += float(z_by_h.get(h, 0.0))
            k += 1
        if k == 0:
            return 0.0
        return s / float(k)

