"""
Polymarket CLOB (orderbook) ingestion task.

Phase 1 goal: pure ingestion + in-memory top-of-book state.

Design constraints enforced:
    - Single asyncio loop (this is an async task).
    - WS callbacks do *state mutations only*.
    - Bounded memory: we keep only top-N levels per side.
    - No expensive work in callbacks: parsing + slicing/reversing small lists only.
    - Raw logging at ingestion time (JSONL).

What this module does:
    - Connects to the Polymarket CLOB market WS.
    - Subscribes to orderbook events for two CLOB asset ids (YES / NO).
    - Applies incoming *book snapshot* messages to `state.book`.

Important notes:
    - The CLOB WS sends *snapshots* keyed by `asset_id` (one per outcome token).
    - The reference parser you provided assumes best bid/ask are at the end
      of the bids/asks arrays. We follow that convention and build L1..LN
      as: take last N and reverse => best-first.
"""

from __future__ import annotations

import aiohttp
import asyncio
import json
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import websockets

from config import PM_CLOB_PING_EVERY_S, PM_CLOB_WS_URL
from raw_logger import AsyncJsonlLogger
from state import AppState, OrderbookLevel, AlignState

from candles import TF_15M_MS, bucket_start_ms
from config import PM_COIN_BY_BINANCE
from resolve_polymarket_market import build_15m_slug, resolve_market_by_slug
from maker_metrics import danger_score_bid, danger_score_ask

# --- add to ingest_polymarket_clob.py ---

import math
from state import L1SideMetrics  # after you add it in state.py


def _ewma_dt(prev: float, x: float, dt_s: float, tau_s: float) -> float:
    """
    dt-aware EWMA using time-constant tau_s:
      alpha = 1 - exp(-dt/tau)
    """
    if dt_s <= 0.0:
        return prev
    if tau_s <= 1e-9:
        return x
    a = 1.0 - math.exp(-dt_s / tau_s)
    return (1.0 - a) * prev + a * x


def _update_l1_metrics(m: L1SideMetrics, bids, asks, recv_ms: float) -> None:
    """
    Update depletion/refill, microprice bias, quote age markers, flicker rate.
    Constant-time; uses only L1 bid/ask if present.
    """
    # compute dt from last update (observed time)
    dt_s = 0.0
    if m.last_update_ms > 0.0 and recv_ms > m.last_update_ms:
        dt_s = (recv_ms - m.last_update_ms) / 1000.0
    m.last_update_ms = recv_ms

    # grab L1
    bid_px = bids[0].px if bids else 0.0
    bid_sz = bids[0].sz if bids else 0.0
    ask_px = asks[0].px if asks else 0.0
    ask_sz = asks[0].sz if asks else 0.0

    # --- quote age + flicker (price changes only) ---
    bid_changed = (bid_px > 0.0 and bid_px != m.bid_px)
    ask_changed = (ask_px > 0.0 and ask_px != m.ask_px)

    if m.bid_last_change_ms <= 0.0 and bid_px > 0.0:
        m.bid_last_change_ms = recv_ms
    if m.ask_last_change_ms <= 0.0 and ask_px > 0.0:
        m.ask_last_change_ms = recv_ms

    if bid_changed:
        m.bid_last_change_ms = recv_ms
    if ask_changed:
        m.ask_last_change_ms = recv_ms

    # flicker inst: changes/sec (1 change over dt) else 0
    # tau picks how "sticky" we want the regime detector to be
    tau_flicker = 2.0
    if dt_s > 0.0:
        m.bid_flicker_ema = _ewma_dt(m.bid_flicker_ema, (1.0 / dt_s) if bid_changed else 0.0, dt_s, tau_flicker)
        m.ask_flicker_ema = _ewma_dt(m.ask_flicker_ema, (1.0 / dt_s) if ask_changed else 0.0, dt_s, tau_flicker)

    # --- depletion/refill: only meaningful if price is same queue ---
    # If price changed, treat as new queue and reset baseline.
    tau_dep = 2.0

    # BID: depletion when size shrinks
    if bid_px > 0.0:
        if (not bid_changed) and m.bid_px == bid_px and dt_s > 0.0:
            dsz = bid_sz - m.bid_sz
            dep = max(0.0, -dsz) / dt_s
            ref = max(0.0,  dsz) / dt_s
            m.bid_dep_ema = _ewma_dt(m.bid_dep_ema, dep, dt_s, tau_dep)
            m.bid_refill_ema = _ewma_dt(m.bid_refill_ema, ref, dt_s, tau_dep)
        # baseline
        m.bid_px = bid_px
        m.bid_sz = bid_sz
    else:
        m.bid_px = 0.0
        m.bid_sz = 0.0

    # ASK
    if ask_px > 0.0:
        if (not ask_changed) and m.ask_px == ask_px and dt_s > 0.0:
            dsz = ask_sz - m.ask_sz
            dep = max(0.0, -dsz) / dt_s
            ref = max(0.0,  dsz) / dt_s
            m.ask_dep_ema = _ewma_dt(m.ask_dep_ema, dep, dt_s, tau_dep)
            m.ask_refill_ema = _ewma_dt(m.ask_refill_ema, ref, dt_s, tau_dep)
        m.ask_px = ask_px
        m.ask_sz = ask_sz
    else:
        m.ask_px = 0.0
        m.ask_sz = 0.0

    # --- microprice vs mid (normalized) ---
    if bid_px > 0.0 and ask_px > 0.0:
        spread = ask_px - bid_px
        mid = 0.5 * (bid_px + ask_px)
        m.mid = mid
        m.spread = spread
        denom = bid_sz + ask_sz
        if denom > 0.0:
            micro = (ask_px * bid_sz + bid_px * ask_sz) / denom
        else:
            micro = mid
        m.micro = micro
        m.micro_bias = 0.0 if spread <= 0.0 else (micro - mid) / spread
    else:
        m.mid = 0.0
        m.spread = 0.0
        m.micro = 0.0
        m.micro_bias = 0.0

    # --- maker danger score (O(1)) ---
    try:
        m.danger_bid, _ = danger_score_bid(
            dep_ema=m.bid_dep_ema,
            flicker_ema=m.bid_flicker_ema,
            spread=m.spread,
            micro_bias=m.micro_bias,
        )
        m.danger_ask, _ = danger_score_ask(
            dep_ema=m.ask_dep_ema,
            flicker_ema=m.ask_flicker_ema,
            spread=m.spread,
            micro_bias=m.micro_bias,
        )
    except Exception:
        # keep last values if import fails / during dev
        pass


def _now_ms() -> float:
    return time.time() * 1000.0


def build_subscribe_message(asset_ids: List[str]) -> Dict[str, Any]:
    """Build the minimal subscription payload for the CLOB market WS."""
    return {
        "assets_ids": asset_ids,
        "type": "market",
    }


async def _app_ping_loop(ws: websockets.WebSocketClientProtocol, every_s: float) -> None:
    """Send application-level PING messages (CLOB WS does not use WS ping frames)."""
    try:
        while True:
            await ws.send("PING")
            await asyncio.sleep(every_s)
    except asyncio.CancelledError:
        return
    except Exception:
        # Let the main stream loop handle reconnect.
        return


def _canon_mid_spread(state: AppState) -> tuple[float, float]:
    """
    Canonical mid/spread in YES-probability space.

    Mirror invariant:
      YES mid = 1 - NO mid
      spread is identical
    """
    # Prefer YES if present
    if state.book.yes_bids and state.book.yes_asks:
        bid = float(state.book.yes_bids[0].px)
        ask = float(state.book.yes_asks[0].px)
        sp = ask - bid
        mid = 0.5 * (bid + ask)
        return mid, sp if sp > 0.0 else 0.0

    # Fallback to NO (mirror-map into YES space)
    if state.book.no_bids and state.book.no_asks:
        bid = float(state.book.no_bids[0].px)
        ask = float(state.book.no_asks[0].px)
        sp = ask - bid
        mid_no = 0.5 * (bid + ask)
        mid_yes = 1.0 - mid_no
        return mid_yes, sp if sp > 0.0 else 0.0

    return 0.0, 0.0


def _update_canon_metrics(state: AppState, recv_ms: float) -> None:
    """
    Update canonical mid/spread + mid velocity EWMA + touch-cross risk.
    O(1), called on every book snapshot apply.
    """
    mid, spread = _canon_mid_spread(state)
    c = state.book.canon
    c.mid = mid
    c.spread = spread

    # velocity EWMA
    dt_s = 0.0
    if c.mid_prev_ms > 0.0 and recv_ms > c.mid_prev_ms:
        dt_s = (recv_ms - c.mid_prev_ms) / 1000.0

    if dt_s > 0.0 and c.mid_prev > 0.0 and mid > 0.0:
        mid_vel_inst = abs(mid - c.mid_prev) / dt_s  # price units / s
        # tau=1.5s is a good default for "touch cross" risk
        c.mid_vel_ema = _ewma_dt(c.mid_vel_ema, mid_vel_inst, dt_s, tau_s=1.5)
    elif c.mid_vel_ema <= 0.0:
        c.mid_vel_ema = 0.0

    c.mid_prev = mid
    c.mid_prev_ms = recv_ms

    # touch-cross risk: (vel * tau)/spread, squashed to [0,1]
    sp = max(1e-9, spread)
    intensity = (c.mid_vel_ema * 1.5) / sp if mid > 0.0 else 0.0
    c.touch_cross_risk = 0.0 if intensity <= 0.0 else (intensity / (1.0 + intensity))


def _align_reset(a: AlignState) -> None:
    a.pending = False
    a.impulse_ms = 0.0
    a.dir = 0
    a.mid0 = 0.0
    a.spread0 = 0.0
    a.expires_ms = 0.0

    a.resp_last_ms = 0.0
    a.resp_ema_ms = 0.0

    a.n_impulses = 0
    a.n_matched = 0
    a.n_missed = 0

    a.last_resp_update_ms = 0.0
    a.resp_ema_t_ms = 0.0


def _maybe_match_align(state: AppState, recv_ms: float) -> None:
    a = state.align
    if not a.pending:
        return

    # Expire
    if recv_ms > a.expires_ms:
        a.pending = False
        a.n_missed += 1
        return

    b = state.book.canon
    mid = float(b.mid)

    # If canon mid isn't valid, we cannot match (don't consume the pending impulse)
    if mid <= 0.0 or a.mid0 <= 0.0 or a.dir == 0:
        return

    # --- threshold in probability space ---
    # Base tick-ish threshold: 0.005 catches the first meaningful movement.
    # Also require at least half the *armed* spread (prevents matching on noise).
    # Clamp to avoid "pinned markets" producing huge thresholds.
    base = 0.005
    half_sp0 = 0.5 * max(0.0, float(a.spread0))
    thr = max(base, half_sp0)
    thr = min(thr, 0.02)  # safety clamp

    d = mid - a.mid0
    if a.dir * d < thr:
        return

    # Matched
    resp = recv_ms - a.impulse_ms
    if resp < 0.0:
        resp = 0.0

    a.resp_last_ms = resp

    # dt-aware EWMA (tau ~ 3s)
    last_t = float(a.resp_ema_t_ms)
    if a.resp_ema_ms <= 0.0 or last_t <= 0.0:
        a.resp_ema_ms = resp
    else:
        dt_s = max(0.0, (recv_ms - last_t) / 1000.0)
        a.resp_ema_ms = _ewma_dt(a.resp_ema_ms, resp, dt_s, tau_s=3.0)

    a.resp_ema_t_ms = recv_ms
    a.last_resp_update_ms = recv_ms

    a.pending = False
    a.n_matched += 1


def _to_levels_best_first(levels: Any, n: int) -> List[OrderbookLevel]:
    """Convert raw levels (list[dict]) into L1..Ln (best-first) OrderbookLevel list."""
    if not isinstance(levels, list) or not levels:
        return []

    # Reference code uses bids[-1]/asks[-1] as best.
    # So we take last N and reverse so index 0 is best.
    tail = levels[-n:]
    out: List[OrderbookLevel] = []
    for lvl in reversed(tail):
        try:
            out.append(OrderbookLevel(px=float(lvl["price"]), sz=float(lvl["size"])))
        except Exception:
            # If any level is malformed, skip just that level.
            continue
    return out


def _is_ping_pong(raw: Any) -> bool:
    return isinstance(raw, str) and raw in ("PING", "PONG")


def _iter_messages(raw: Any) -> Iterable[Dict[str, Any]]:
    """Normalize ws payloads into an iterable of dict messages."""
    if isinstance(raw, dict):
        yield raw
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                yield item


def _coerce_epoch_ms(ts: Any) -> float:
    """
    Polymarket CLOB messages usually carry epoch milliseconds, but we harden:
      - numeric strings -> float
      - seconds epoch -> ms epoch (10-digit magnitude)
    """
    if ts is None:
        return 0.0

    try:
        v = float(ts)  # handles int/float/str
    except Exception:
        return 0.0

    # Heuristic: if it's in seconds (≈ 1e9..1e10), convert to ms.
    # If it's already ms (≈ 1e12..1e13), leave it.
    if 1e9 <= v < 1e11:
        v *= 1000.0

    return v


def _parse_book_msg(msg: Dict[str, Any]) -> Optional[Tuple[str, float, Any, Any]]:
    """Return (asset_id, event_ts_ms, bids, asks) if msg is a book snapshot."""
    if msg.get("event_type") != "book":
        return None

    asset_id = msg.get("asset_id")
    if not isinstance(asset_id, str) or not asset_id:
        return None

    ts_ms = _coerce_epoch_ms(msg.get("timestamp"))

    bids = msg.get("bids")
    asks = msg.get("asks")
    return (asset_id, ts_ms, bids, asks)


def _apply_book_snapshot(
    state: AppState,
    *,
    asset_id: str,
    bids: Any,
    asks: Any,
    yes_asset_id: str,
    no_asset_id: str,
    max_levels: int,
    recv_ms: float,
    event_ms: float,
) -> None:
    """Apply one book snapshot to the bounded in-memory orderbook state."""
    b = _to_levels_best_first(bids, max_levels)
    a = _to_levels_best_first(asks, max_levels)

    if asset_id == yes_asset_id:
        state.book.yes_bids = b
        state.book.yes_asks = a
        _update_l1_metrics(state.book.metrics.yes, state.book.yes_bids, state.book.yes_asks, recv_ms)
    elif asset_id == no_asset_id:
        state.book.no_bids = b
        state.book.no_asks = a
        _update_l1_metrics(state.book.metrics.no, state.book.no_bids, state.book.no_asks, recv_ms)
    else:
        return

    _update_canon_metrics(state, recv_ms)
    _maybe_match_align(state, recv_ms)

    state.book.updates += 1
    state.book.pulse = "*" if (state.book.updates % 2 == 0) else "."

    if event_ms > 0.0:
        lag = recv_ms - event_ms
        if lag < 0.0:
            lag = 0.0
        state.book.lag_raw_ms = lag
        state.book.lag_ms = max(0.0, lag - state.diag.clock_offset_ms)
        state.book.last_change_ms = lag   # keep this if you're currently using it
    else:
        state.book.lag_raw_ms = 0.0
        state.book.lag_ms = 0.0
        state.book.last_change_ms = 0.0


async def polymarket_clob_task(
    state: AppState,
    logger: AsyncJsonlLogger,
    *,
    yes_asset_id: str,
    no_asset_id: str,
    max_levels: int = 5,
) -> None:
    """Connect → subscribe → parse → apply-to-book → log."""
    asset_ids = [yes_asset_id, no_asset_id]

    backoff = 1.0
    max_backoff = 30.0

    while True:
        try:
            async with websockets.connect(
                PM_CLOB_WS_URL,
                ping_interval=None,
                ping_timeout=None,
            ) as ws:
                await ws.send(json.dumps(build_subscribe_message(asset_ids)))

                logger.log(
                    {
                        "ts_local_ms": _now_ms(),
                        "source": "polymarket_clob",
                        "type": "subscribe",
                        "assets_ids": asset_ids,
                        "max_levels": max_levels,
                    }
                )

                ping_task = asyncio.create_task(_app_ping_loop(ws, PM_CLOB_PING_EVERY_S))
                sample_left = 5

                try:
                    async for raw in ws:
                        recv_ms = _now_ms()

                        if _is_ping_pong(raw):
                            continue

                        # Log a few raw samples per connection (debugging / schema drift detection)
                        if sample_left > 0:
                            logger.log(
                                {
                                    "ts_local_ms": recv_ms,
                                    "source": "polymarket_clob",
                                    "type": "raw_sample",
                                    "payload": raw,
                                }
                            )
                            sample_left -= 1

                        t0 = time.perf_counter()

                        try:
                            msg0 = json.loads(raw)
                        except Exception:
                            continue

                        for msg in _iter_messages(msg0):
                            parsed = _parse_book_msg(msg)
                            if parsed is None:
                                continue

                            asset_id, event_ms, bids, asks = parsed
                            _apply_book_snapshot(
                                state,
                                asset_id=asset_id,
                                bids=bids,
                                asks=asks,
                                yes_asset_id=yes_asset_id,
                                no_asset_id=no_asset_id,
                                max_levels=max_levels,
                                recv_ms=recv_ms,
                                event_ms=event_ms,
                            )

                            # Minimal semantic logging (no full depth).
                            yb0 = state.book.yes_bids[0].px if state.book.yes_bids else 0.0
                            ya0 = state.book.yes_asks[0].px if state.book.yes_asks else 0.0
                            nb0 = state.book.no_bids[0].px if state.book.no_bids else 0.0
                            na0 = state.book.no_asks[0].px if state.book.no_asks else 0.0

                            logger.log(
                                {
                                    "ts_local_ms": recv_ms,
                                    "source": "polymarket_clob",
                                    "type": "book",
                                    "asset_id": asset_id,
                                    "event_ms": event_ms,
                                    "lag_raw_ms": (recv_ms - event_ms) if event_ms > 0.0 else None,
                                    "lag_ms": max(0.0, recv_ms - event_ms - state.diag.clock_offset_ms) if event_ms > 0.0 else None,
                                    "l1": {
                                        "yes_bid": yb0,
                                        "yes_ask": ya0,
                                        "no_bid": nb0,
                                        "no_ask": na0,
                                    },
                                    "updates": state.book.updates,
                                }
                            )

                        dt_ms = (time.perf_counter() - t0) * 1000.0
                        state.diag.clob_apply_ms = dt_ms

                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)

            backoff = 1.0

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logger.log(
                {
                    "ts_local_ms": _now_ms(),
                    "source": "polymarket_clob",
                    "type": "error",
                    "error": repr(e),
                    "retry_in_s": backoff,
                }
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, max_backoff)


async def polymarket_clob_autoresolve_task(
    state: AppState,
    logger: AsyncJsonlLogger,
    *,
    binance_symbol: str,
    max_levels: int = 5,
) -> None:
    """
    Supervisor task:
      - figures out the current 15m bucket
      - resolves the corresponding Polymarket market slug via Gamma API
      - starts the CLOB WS ingestion task for that market's YES/NO asset_ids
      - on 15m roll, cancels and restarts with the new market

    This keeps the WS ingestion pure (parse→apply→log) and puts market resolution
    outside the WS callbacks.
    """
    coin = PM_COIN_BY_BINANCE.get(binance_symbol)
    if not coin:
        raise RuntimeError(f"No PM coin mapping for binance_symbol={binance_symbol!r}")

    current_bucket_ms: int = 0
    clob_task: asyncio.Task | None = None

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                now_ms = int(time.time() * 1000)
                bucket_ms = int(bucket_start_ms(now_ms, TF_15M_MS))

                # Only resolve on bucket change (or first run)
                if bucket_ms != current_bucket_ms:
                    current_bucket_ms = bucket_ms
                    start_s = int(bucket_ms // 1000)
                    slug = build_15m_slug(coin, start_s)

                    meta = await resolve_market_by_slug(session, slug)

                    logger.log(
                        {
                            "ts_local_ms": _now_ms(),
                            "source": "polymarket_clob",
                            "type": "market_resolve",
                            "binance_symbol": binance_symbol,
                            "coin": coin,
                            "bucket_ms": bucket_ms,
                            "slug": slug,
                            "resolved": meta is not None,
                        }
                    )

                    if meta is None:
                        # Market might not exist yet; keep trying quickly until it appears.
                        await asyncio.sleep(1.0)
                        continue

                    # Update state with resolved metadata (UI can show it)
                    state.book.market_slug = meta.slug
                    state.book.question = meta.question
                    state.book.market_id = meta.market_id
                    state.book.yes_asset_id = meta.yes_asset_id
                    state.book.no_asset_id = meta.no_asset_id
                    state.book.market_start_ms = float(meta.start_ms)
                    state.book.market_end_ms = float(meta.end_ms)

                    # ✅ reset align latency stats on instrument boundary
                    _align_reset(state.align)

                    # Reset book levels for the new market
                    state.book.yes_bids = []
                    state.book.yes_asks = []
                    state.book.no_bids = []
                    state.book.no_asks = []
                    state.book.updates = 0
                    state.book.last_change_ms = 0.0
                    state.book.pulse = "."

                    logger.log(
                        {
                            "ts_local_ms": _now_ms(),
                            "source": "polymarket_clob",
                            "type": "market_start",
                            "slug": meta.slug,
                            "market_id": meta.market_id,
                            "yes_asset_id": meta.yes_asset_id,
                            "no_asset_id": meta.no_asset_id,
                            "outcomes": list(meta.outcomes),
                        }
                    )

                    # Restart the live CLOB ingestion for this market
                    if clob_task is not None:
                        clob_task.cancel()
                        await asyncio.gather(clob_task, return_exceptions=True)

                    clob_task = asyncio.create_task(
                        polymarket_clob_task(
                            state,
                            logger,
                            yes_asset_id=meta.yes_asset_id,
                            no_asset_id=meta.no_asset_id,
                            max_levels=max_levels,
                        )
                    )

                # Poll cadence: we only need to catch the boundary quickly.
                # 0.25s is fine and cheap.
                await asyncio.sleep(0.25)

            except asyncio.CancelledError:
                if clob_task is not None:
                    clob_task.cancel()
                    await asyncio.gather(clob_task, return_exceptions=True)
                raise

            except Exception as e:
                logger.log(
                    {
                        "ts_local_ms": _now_ms(),
                        "source": "polymarket_clob",
                        "type": "supervisor_error",
                        "error": repr(e),
                    }
                )
                await asyncio.sleep(1.0)
