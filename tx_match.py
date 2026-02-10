"""tx_match.py

Tx-hash join + adaptive lag estimator for aligning Polygon-chain fills to
Polymarket CLOB `last_trade_price` prints.

Design goals:
  - O(1) put/get
  - bounded memory (TTL + max_items)
  - robust lag estimate (EWMA + clipping + dispersion)
  - no dependencies on the rest of the app (import-safe)
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional


def _now_ms() -> float:
    return time.time() * 1000.0


def norm_tx_hash(tx_hash: str) -> str:
    """Normalize tx hash for dict keys."""
    return (tx_hash or "").strip().lower()


@dataclass(slots=True)
class ClobTradePrint:
    tx_hash: str
    ts_clob_ms: int
    ts_local_ms: int
    market: str
    asset_id: str
    side: str
    size: str
    price: str
    fee_rate_bps: str
    slug: str = ""


class TxMatchStore:
    """Bounded tx_hash -> ClobTradePrint mapping with TTL + LRU eviction."""

    def __init__(self, *, ttl_ms: int = 30 * 60_000, max_items: int = 20_000):
        self.ttl_ms = int(ttl_ms)
        self.max_items = int(max_items)
        self._d: "OrderedDict[str, ClobTradePrint]" = OrderedDict()

    def __len__(self) -> int:
        return len(self._d)

    def put(self, p: ClobTradePrint) -> None:
        txh = norm_tx_hash(p.tx_hash)
        if not txh:
            return

        # Keep the *latest* print if duplicates appear.
        prev = self._d.get(txh)
        if prev is not None and int(prev.ts_clob_ms) > int(p.ts_clob_ms):
            # Older print arrived late; keep the newer one.
            self._d.move_to_end(txh)
        else:
            self._d[txh] = p
            self._d.move_to_end(txh)

        self._evict()

    def get(self, tx_hash: str) -> Optional[ClobTradePrint]:
        txh = norm_tx_hash(tx_hash)
        if not txh:
            return None
        p = self._d.get(txh)
        if p is None:
            return None
        # Treat gets as LRU touches.
        self._d.move_to_end(txh)
        return p

    def _evict(self) -> None:
        now = int(_now_ms())

        # TTL eviction by *local arrival* time (robust for prints).
        if self.ttl_ms > 0:
            cutoff = now - self.ttl_ms
            while self._d:
                k0, v0 = next(iter(self._d.items()))
                if int(v0.ts_local_ms) >= cutoff:
                    break
                self._d.popitem(last=False)

        # Size cap eviction.
        while self.max_items > 0 and len(self._d) > self.max_items:
            self._d.popitem(last=False)


class LagEstimator:
    """Robust online estimator of (ts_clob_ms - ts_chain_ms)."""

    def __init__(
        self,
        *,
        alpha: float = 0.02,
        clip_ms: int = 30_000,
        min_samples: int = 10,
    ):
        self.alpha = float(alpha)
        self.clip_ms = int(clip_ms)
        self.min_samples = int(min_samples)

        self.n: int = 0
        self.ema: float = 0.0
        self.mad_ema: float = 0.0  # EWMA of absolute deviation

    def update(self, lag_ms: float) -> None:
        """Update with one lag sample in milliseconds."""
        if lag_ms != lag_ms:  # NaN
            return
        x = float(lag_ms)

        # clip against extreme jumps (bad block timestamp, parse error, etc.)
        if self.clip_ms > 0:
            if x > self.clip_ms:
                x = float(self.clip_ms)
            elif x < -self.clip_ms:
                x = float(-self.clip_ms)

        if self.n <= 0:
            self.ema = x
            self.mad_ema = 0.0
            self.n = 1
            return

        a = self.alpha
        self.ema = (1.0 - a) * self.ema + a * x
        self.mad_ema = (1.0 - a) * self.mad_ema + a * abs(x - self.ema)
        self.n += 1

    def estimate(self) -> float:
        return float(self.ema)

    def dispersion(self) -> float:
        """Scale proxy (EWMA absolute deviation), in ms."""
        return float(self.mad_ema)

    def confident(self) -> bool:
        return self.n >= self.min_samples

