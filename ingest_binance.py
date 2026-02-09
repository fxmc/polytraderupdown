"""
Binance ingestion task.

Connects to the Binance WS trade stream and updates:
- state.driver (last, d_last, lag_ms, strike)
- state.tape_driver (burst tape newest-first)
- raw logger (each inbound payload)
"""

from __future__ import annotations

import json
import math
import time
from typing import Any, Dict, Optional, Tuple

import websockets

from candles import (
    CandleSeries,
    TF_1M_MS,
    TF_5M_MS,
    TF_15M_MS,
    atr2_from_last3,
    bucket_start_ms,
    update_series_with_trade,
)
from raw_logger import AsyncJsonlLogger
from state import AppState, now_ms, update_tape_on_trade
from momentum import SecPriceBuffer, mom_pct, mom_points, update_sec_price
from mom_zscore import MomentumZConfig, MomentumZTracker
from config import (
    BINANCE_STREAM_SUFFIX,
    BINANCE_WS_BASE,
    VOL_R_CLIP_BY_SYMBOL,
    VOL_DRIVER,
    VOL_BLEND_W,
    FV_USE_DRIFT,
    DRIFT_DRIVER,
    DRIFT_BLEND_W,
)
from volatility import VolStack, sigma_over_seconds
from fair_value import digital_prob_normal_points, digital_prob_lognormal, digital_prob_lognormal_drift


# -----------------------------
# URL + parsing
# -----------------------------

def _stream_name(symbol: str) -> str:
    """Return Binance stream name for a symbol (lowercase)."""
    return f"{symbol.lower()}{BINANCE_STREAM_SUFFIX}"


def _ws_url(symbol: str) -> str:
    """Return the Binance WS URL for a symbol trade stream."""
    return f"{BINANCE_WS_BASE}/{_stream_name(symbol)}"


def _parse_trade_msg(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a Binance trade message into a small dict; return None if not a trade."""
    if "p" not in msg:
        return None

    price = float(msg["p"])
    trade_ts_ms = float(msg.get("T", 0.0))
    event_ts_ms = float(msg.get("E", 0.0))

    return {
        "price": price,
        "trade_ts_ms": trade_ts_ms,
        "event_ts_ms": event_ts_ms,
        "trade_id": msg.get("t"),
        "qty": float(msg.get("q", 0.0)) if "q" in msg else 0.0,
        "is_buyer_maker": bool(msg.get("m", False)),
    }


# -----------------------------
# Driver updates (per trade)
# -----------------------------

def _compute_trade_time_ms(trade_ts_ms: float) -> float:
    return trade_ts_ms if trade_ts_ms > 0.0 else now_ms()


def _compute_lag_ms(recv_ms: float, trade_ts_ms: float, clock_offset_ms: float) -> Tuple[float, float]:
    lag_raw_ms = max(0.0, recv_ms - trade_ts_ms) if trade_ts_ms > 0 else 0.0
    lag_ms = max(0.0, lag_raw_ms - clock_offset_ms)
    return lag_raw_ms, lag_ms


def _update_diag_timestamps(state: AppState, recv_ms: float, trade_ms: float) -> None:
    state.diag.binance_last_rx_ms = recv_ms
    state.diag.binance_last_trade_ms = trade_ms


def _update_driver_last(state: AppState, price: float, lag_raw_ms: float, lag_ms: float) -> None:
    prev = state.driver.last
    state.driver.last = price
    state.driver.d_last = price - prev if prev != 0.0 else 0.0
    state.driver.lag_raw_ms = lag_raw_ms
    state.driver.lag_ms = lag_ms


def _ensure_driver_strike_initialized(state: AppState, trade_ms: float, price: float) -> None:
    if state.driver.strike != 0.0:
        return

    state.driver.strike = price
    ws = bucket_start_ms(trade_ms, TF_15M_MS)
    state.driver.win_start_ms = ws
    state.driver.expiry_ms = ws + TF_15M_MS


# -----------------------------
# Momentum
# -----------------------------

def _update_momentum(state: AppState, mom_buf: SecPriceBuffer, trade_ms: float, price: float) -> bool:
    """Update momentum buffers/fields. Returns True on new second boundary."""
    new_sec = update_sec_price(mom_buf, trade_ms, price)

    state.driver.mom_5s = mom_points(mom_buf, 5)
    state.driver.mom_10s = mom_points(mom_buf, 10)
    state.driver.mom_15s = mom_points(mom_buf, 15)
    state.driver.mom_30s = mom_points(mom_buf, 30)
    state.driver.mom_1m = mom_points(mom_buf, 60)
    state.driver.mom_5m = mom_points(mom_buf, 300)

    state.driver.mom_5s_pct = mom_pct(mom_buf, 5)
    state.driver.mom_10s_pct = mom_pct(mom_buf, 10)
    state.driver.mom_15s_pct = mom_pct(mom_buf, 15)
    state.driver.mom_30s_pct = mom_pct(mom_buf, 30)
    state.driver.mom_1m_pct = mom_pct(mom_buf, 60)
    state.driver.mom_5m_pct = mom_pct(mom_buf, 300)

    return new_sec


def _update_momentum_zscores(state: AppState, mom_z_fast: MomentumZTracker, mom_z_slow: MomentumZTracker) -> None:
    mom_by_h = {
        5: state.driver.mom_5s,
        10: state.driver.mom_10s,
        15: state.driver.mom_15s,
        30: state.driver.mom_30s,
        60: state.driver.mom_1m,
    }

    z_fast = mom_z_fast.update(mom_by_h)
    z_slow = mom_z_slow.update(mom_by_h)

    state.driver.mom_z_5s = float(z_fast.get(5, 0.0))
    state.driver.mom_z_10s = float(z_fast.get(10, 0.0))
    state.driver.mom_z_15s = float(z_fast.get(15, 0.0))
    state.driver.mom_z_30s = float(z_fast.get(30, 0.0))
    state.driver.mom_z_1m = float(z_fast.get(60, 0.0))

    state.driver.mom_z_combo_fast = mom_z_fast.combo(z_fast)
    state.driver.mom_z_combo_slow = mom_z_slow.combo(z_slow)


# -----------------------------
# Model step (1Hz) — exact golden behavior
# -----------------------------

def _compute_tte_s(state: AppState, trade_ms: float) -> float:
    expiry_ms = state.driver.expiry_ms
    tte_s = max(0.0, (expiry_ms - trade_ms) / 1000.0) if expiry_ms > 0 else 0.0
    state.driver.tte_s = tte_s
    return tte_s


def _publish_display_vols(state: AppState, v: Dict[str, Any]) -> Tuple[float, float, float]:
    sigma15_30 = sigma_over_seconds(v["v30"], 900.0)
    sigma15_60 = sigma_over_seconds(v["v60"], 900.0)
    sigma15_300 = sigma_over_seconds(v["v300"], 900.0)

    state.driver.vol_30s = 100.0 * sigma15_30
    state.driver.vol_1m = 100.0 * sigma15_60
    state.driver.vol_5m = 100.0 * sigma15_300

    return sigma15_30, sigma15_60, sigma15_300


def _choose_v_drive(v: Dict[str, Any]) -> Tuple[float, float, float]:
    v60 = float(v.get("v60", 0.0) or 0.0)
    vslow = float(v.get("v_slow", 0.0) or 0.0)

    if VOL_DRIVER == "rv60":
        v_drive = v60 if v60 > 0.0 else vslow
    elif VOL_DRIVER == "ewma_slow":
        v_drive = vslow
    else:  # "blend"
        w = float(VOL_BLEND_W)
        # blend only once rv60 has some signal; otherwise fall back to vslow
        v_drive = (w * v60 + (1.0 - w) * vslow) if v60 > 0.0 else vslow

    return float(v_drive), v60, vslow


def _publish_sigma_fields(state: AppState, sigma_raw: float) -> None:
    # Exact golden truth: no quiet logic; keep these fields as raw.
    state.driver.sigma_rem_pct_raw = 100.0 * sigma_raw
    state.driver.quiet_binance = False
    state.driver.sigma_rem_pct_eff = 100.0 * sigma_raw
    state.driver.sigma_rem_pct = state.driver.sigma_rem_pct_eff
    state.driver.sigma_eff = sigma_raw


def _publish_fv_no_drift(state: AppState, price: float, sigma_raw: float) -> float:
    p_yes = digital_prob_lognormal(price, state.driver.strike, sigma_raw)
    state.driver.prob_yes = p_yes
    state.driver.fv_yes = p_yes
    state.driver.fv_no = 1.0 - p_yes
    return p_yes


def _publish_normal_points_diag(state: AppState, price: float, sigma_raw: float) -> float:
    sigma_pts_rem = price * sigma_raw
    p_yes_norm = digital_prob_normal_points(price, state.driver.strike, sigma_pts_rem)
    state.driver.prob_yes_norm = p_yes_norm
    return p_yes_norm


def _compute_mu_hat(v: Dict[str, Any]) -> Tuple[float, float, float]:
    mu60 = v.get("mu60", 0.0)
    mu_slow = v.get("mu_slow", 0.0)

    if DRIFT_DRIVER == "mu60":
        mu_hat = mu60
    elif DRIFT_DRIVER == "ewma_slow":
        mu_hat = mu_slow
    else:  # "blend"
        w = float(DRIFT_BLEND_W)
        mu_hat = (w * mu60 + (1.0 - w) * mu_slow) if mu60 != 0.0 else mu_slow

    return float(mu60), float(mu_slow), float(mu_hat)


def _publish_drift_diag_fields(state: AppState, v_drive: float, mu_hat: float, tte_s: float) -> float:
    sigma_1s_raw = math.sqrt(v_drive) if v_drive > 0.0 else 0.0
    mu_over_sigma = (mu_hat / sigma_1s_raw) if sigma_1s_raw > 1e-12 else 0.0
    state.driver.mu_over_sigma = mu_over_sigma

    mu_T = mu_hat * tte_s
    state.driver.mu_hat_per_s = mu_hat
    state.driver.mu_T = mu_T
    return mu_T


def _publish_fv_with_optional_drift(
    state: AppState,
    price: float,
    sigma_raw: float,
    mu_T: float,
) -> Tuple[float, float]:
    if FV_USE_DRIFT:
        p_yes = digital_prob_lognormal_drift(price, state.driver.strike, sigma_raw, mu_T)
    else:
        p_yes = digital_prob_lognormal(price, state.driver.strike, sigma_raw)

    p_yes_nd = digital_prob_lognormal(price, state.driver.strike, sigma_raw)

    state.driver.p_yes_nd = p_yes_nd
    state.driver.fv_yes_nd = p_yes_nd
    state.driver.fv_no_nd = 1.0 - p_yes_nd

    state.driver.prob_yes = p_yes
    state.driver.fv_yes = p_yes
    state.driver.fv_no = 1.0 - p_yes

    return p_yes, p_yes_nd


def _propagate_fv_to_book(state: AppState) -> None:
    state.book.fv_yes = state.driver.fv_yes
    state.book.fv_no = state.driver.fv_no
    state.book.fv_yes_nd = state.driver.fv_yes_nd
    state.book.fv_no_nd = state.driver.fv_no_nd


def _publish_no_drift_baseline_again(state: AppState, price: float, sigma_raw: float) -> None:
    # Preserve your golden truth redundancy exactly (harmless, but we keep it).
    p_yes_nd = digital_prob_lognormal(price, state.driver.strike, sigma_raw)
    state.driver.p_yes_nd = p_yes_nd


def _maybe_arm_alignment_impulse(state: AppState, recv_ms: float) -> None:
    a = state.align
    can_arm = (not a.pending) and (recv_ms - a.last_impulse_ms >= 1000.0)
    if not can_arm:
        return

    thr_pts = 10
    sec_move = float(state.tape_driver.sec_move_px)
    imp = abs(sec_move)

    if imp < thr_pts:
        return

    mid0 = state.book.canon.mid
    sp0 = state.book.canon.spread

    # only arm if we have a meaningful two-sided mid
    if (mid0 is None) or (sp0 is None) or not (mid0 > 0.0):
        return

    a.pending = True
    a.impulse_ms = recv_ms
    a.last_impulse_ms = recv_ms  # AlignState must have this
    a.dir = 1 if sec_move > 0.0 else -1
    a.mid0 = float(mid0)
    a.spread0 = float(sp0)
    a.expires_ms = recv_ms + 4000.0
    a.n_impulses += 1


def _fv_ok_reason(tte_s: float, v_drive: float) -> Tuple[bool, str]:
    fv_ok = (tte_s is not None) and (tte_s > 0.0) and (v_drive is not None) and (v_drive > 0.0)
    fv_reason = "ok" if fv_ok else ("warmup" if (v_drive is None or v_drive <= 0.0) else "bad_tte")
    return fv_ok, fv_reason


def _log_metrics_1hz(
    logger: AsyncJsonlLogger,
    state: AppState,
    *,
    recv_ms: float,
    symbol: str,
    trade_ms: float,
    price: float,
    r_clip: float,
    tte_s: float,
    v: Dict[str, Any],
    v60: float,
    vslow: float,
    v_drive: float,
    sigma_raw: float,
    p_yes: float,
    p_yes_norm: float,
    mu60: float,
    mu_slow: float,
    mu_hat: float,
    mu_T: float,
    fv_ok: bool,
    fv_reason: str,
) -> None:
    logger.log(
        {
            "ts_local_ms": recv_ms,
            "market_slug": state.book.market_slug,
            "source": "metrics",
            "type": "fv_vol_1s",
            "symbol": symbol,
            "trade_ms": trade_ms,
            "price": price,
            "strike": state.driver.strike,
            "tte_s": tte_s,
            "vol_driver": VOL_DRIVER,
            "r_clip": r_clip,
            "v30": v["v30"],
            "v60": v60,
            "v300": v["v300"],
            "v_fast": v["v_fast"],
            "v_slow": vslow,
            "v_drive": v_drive,
            "sigma_rem_pct": state.driver.sigma_rem_pct,
            "vol15_30_pct": state.driver.vol_30s,
            "vol15_60_pct": state.driver.vol_1m,
            "vol15_300_pct": state.driver.vol_5m,
            "p_yes": p_yes,
            "p_yes_norm": p_yes_norm,
            "mu60": mu60,
            "mu_slow": mu_slow,
            "mu_hat": mu_hat,
            "mu_T": mu_T,
            "FV_USE_DRIFT": FV_USE_DRIFT,
            "DRIFT_DRIVER": DRIFT_DRIVER,
            "fv_ok": fv_ok,
            "fv_reason": fv_reason,
            "sigma_raw": sigma_raw,
            "sigma_eff": sigma_raw,
        }
    )


def _model_step_1hz(
    state: AppState,
    logger: AsyncJsonlLogger,
    symbol: str,
    vol: VolStack,
    r_clip: float,
    *,
    recv_ms: float,
    trade_ms: float,
    price: float,
) -> None:
    """
    Exact golden truth model path:
    - sigma_raw drives everything (no floors / quiet logic)
    - drift can be applied via FV_USE_DRIFT
    - metrics emitted exactly like golden truth
    """
    tte_s = _compute_tte_s(state, trade_ms)

    v = vol.update_price_1s(price)  # v30/v60/v300/v_fast/v_slow
    _publish_display_vols(state, v)

    v_drive, v60, vslow = _choose_v_drive(v)

    sigma_raw = sigma_over_seconds(v_drive, tte_s)  # (no extra guards; preserve behavior)
    _publish_sigma_fields(state, sigma_raw)

    _publish_fv_no_drift(state, price, sigma_raw)
    p_yes_norm = _publish_normal_points_diag(state, price, sigma_raw)

    mu60, mu_slow, mu_hat = _compute_mu_hat(v)
    mu_T = _publish_drift_diag_fields(state, v_drive, mu_hat, tte_s)

    p_yes, _p_yes_nd = _publish_fv_with_optional_drift(state, price, sigma_raw, mu_T)
    _propagate_fv_to_book(state)
    _publish_no_drift_baseline_again(state, price, sigma_raw)

    _maybe_arm_alignment_impulse(state, recv_ms)

    fv_ok, fv_reason = _fv_ok_reason(tte_s, v_drive)

    _log_metrics_1hz(
        logger,
        state,
        recv_ms=recv_ms,
        symbol=symbol,
        trade_ms=trade_ms,
        price=price,
        r_clip=r_clip,
        tte_s=tte_s,
        v=v,
        v60=v60,
        vslow=vslow,
        v_drive=v_drive,
        sigma_raw=sigma_raw,
        p_yes=p_yes,
        p_yes_norm=p_yes_norm,
        mu60=mu60,
        mu_slow=mu_slow,
        mu_hat=mu_hat,
        mu_T=mu_T,
        fv_ok=fv_ok,
        fv_reason=fv_reason,
    )


# -----------------------------
# Candles + 15m roll
# -----------------------------

def _update_candles(cs_1m: CandleSeries, cs_5m: CandleSeries, cs_15m: CandleSeries, trade_ms: float, price: float) -> None:
    update_series_with_trade(cs_1m, trade_ms, price)
    update_series_with_trade(cs_5m, trade_ms, price)
    update_series_with_trade(cs_15m, trade_ms, price)


def _maybe_roll_15m(
    state: AppState,
    cs_1m: CandleSeries,
    cs_5m: CandleSeries,
    cs_15m: CandleSeries,
    *,
    trade_ms: float,
    price: float,
    last_roll_15m_start: int,
) -> int:
    roll_15m_start = bucket_start_ms(trade_ms, TF_15M_MS)

    if last_roll_15m_start == 0:
        return roll_15m_start

    if roll_15m_start == last_roll_15m_start:
        return last_roll_15m_start

    # new 15m bucket -> roll
    state.driver.strike = price
    state.driver.win_start_ms = roll_15m_start
    state.driver.expiry_ms = roll_15m_start + TF_15M_MS

    a1 = atr2_from_last3(cs_1m.closed)
    a5 = atr2_from_last3(cs_5m.closed)
    a15 = atr2_from_last3(cs_15m.closed)

    if a1 > 0.0:
        state.driver.atr_1m = a1
    if a5 > 0.0:
        state.driver.atr_5m = a5
    if a15 > 0.0:
        state.driver.atr_15m = a15

    return roll_15m_start


# -----------------------------
# Logging helpers
# -----------------------------

def _log_trade_raw(
    logger: AsyncJsonlLogger,
    *,
    recv_ms: float,
    symbol: str,
    trade_ms: float,
    price: float,
    qty: float,
    is_buyer_maker: bool,
    lag_raw_ms: float,
    lag_ms: float,
) -> None:
    logger.log(
        {
            "ts_local_ms": recv_ms,
            "source": "binance",
            "type": "trade",
            "symbol": symbol,
            "event_ms": float(trade_ms),
            "price": float(price),
            "qty": float(qty),
            "is_buyer_maker": bool(is_buyer_maker),
            "lag_raw_ms": lag_raw_ms,
            "lag_ms": lag_ms,
        }
    )


def _log_stream_error(logger: AsyncJsonlLogger, *, symbol: str, err: Exception) -> None:
    try:
        logger.log(
            {
                "ts_local_ms": now_ms(),
                "source": "binance",
                "type": "error",
                "symbol": symbol,
                "err": repr(err),
            }
        )
    except Exception:
        pass


def _log_model_error(logger: AsyncJsonlLogger, *, recv_ms: float, symbol: str, err: Exception) -> None:
    try:
        logger.log(
            {
                "ts_local_ms": recv_ms,
                "source": "binance",
                "type": "fv_error",
                "symbol": symbol,
                "err": repr(err),
            }
        )
    except Exception:
        pass


# -----------------------------
# Main task
# -----------------------------

async def binance_ws_task(state: AppState, logger: AsyncJsonlLogger, symbol: str) -> None:
    """Run Binance WS ingestion and update driver state + tape (state-only mutation)."""
    url = _ws_url(symbol)
    state.driver.symbol = symbol

    cs_1m = CandleSeries(tf_ms=TF_1M_MS)
    cs_5m = CandleSeries(tf_ms=TF_5M_MS)
    cs_15m = CandleSeries(tf_ms=TF_15M_MS)

    mom_buf = SecPriceBuffer(max_secs=300)
    mom_horizons = [5, 10, 15, 30, 60]

    mom_cfg_fast = MomentumZConfig(horizons_s=mom_horizons, lookback=30, min_count=20)
    mom_cfg_slow = MomentumZConfig(horizons_s=mom_horizons, lookback=120, min_count=60)
    mom_z_fast = MomentumZTracker(cfg=mom_cfg_fast)
    mom_z_slow = MomentumZTracker(cfg=mom_cfg_slow)

    r_clip = float(VOL_R_CLIP_BY_SYMBOL.get(symbol, 0.006))
    vol = VolStack(r_clip=r_clip)

    last_roll_15m_start: int = 0

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as binance_socket:
                async for raw in binance_socket:
                    t0 = time.perf_counter()
                    recv_ms = now_ms()

                    msg = json.loads(raw)
                    parsed = _parse_trade_msg(msg)
                    if parsed is None:
                        continue

                    price = float(parsed["price"])
                    qty = float(parsed["qty"])
                    is_buyer_maker = bool(parsed["is_buyer_maker"])
                    trade_ts_ms = float(parsed["trade_ts_ms"])
                    trade_ms = _compute_trade_time_ms(trade_ts_ms)

                    _update_diag_timestamps(state, recv_ms, trade_ms)
                    lag_raw_ms, lag_ms = _compute_lag_ms(recv_ms, trade_ts_ms, state.diag.clock_offset_ms)

                    _update_driver_last(state, price, lag_raw_ms, lag_ms)
                    _ensure_driver_strike_initialized(state, trade_ms, price)

                    new_sec = _update_momentum(state, mom_buf, trade_ms, price)
                    if new_sec:
                        _update_momentum_zscores(state, mom_z_fast, mom_z_slow)
                        try:
                            _model_step_1hz(state, logger, symbol, vol, r_clip, recv_ms=recv_ms, trade_ms=trade_ms, price=price)
                        except Exception as e:
                            _log_model_error(logger, recv_ms=recv_ms, symbol=symbol, err=e)

                    _update_candles(cs_1m, cs_5m, cs_15m, trade_ms, price)
                    last_roll_15m_start = _maybe_roll_15m(state, cs_1m,cs_5m, cs_15m, trade_ms=trade_ms, price=price, last_roll_15m_start=last_roll_15m_start)

                    update_tape_on_trade(state.tape_driver, trade_ms, state.driver.last, state.driver.d_last, lag_raw_ms, lag_ms, qty, is_buyer_maker,)

                    _log_trade_raw(logger, recv_ms=recv_ms, symbol=symbol, trade_ms=trade_ms, price=price, qty=qty, is_buyer_maker=is_buyer_maker, lag_raw_ms=lag_raw_ms, lag_ms=lag_ms)

                    state.diag.binance_apply_ms = (time.perf_counter() - t0) * 1000.0

        except Exception as e:
            _log_stream_error(logger, symbol=symbol, err=e)
            await _backoff_sleep()


async def _backoff_sleep() -> None:
    """Small reconnect backoff to avoid hot-looping on failures."""
    await asyncio_sleep_s(1.0)


async def asyncio_sleep_s(s: float) -> None:
    """Async sleep wrapper to avoid inline awaits in the reconnect path."""
    import asyncio
    await asyncio.sleep(s)
