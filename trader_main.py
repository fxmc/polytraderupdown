"""
Trader app entrypoint (separate process from dashboard).

Mirrors dashboard patterns:
- argparse CLI style
- run_context semantics (run_id keyed to BTC 15m market start, rolling every 15m)
- Gamma resolve via slug (15m UpDown canonical format)

This app:
- resolves current 15m market (coin derived from symbol)
- derives condition_id (required for user channel subscription)
- starts user_ws_task (authenticated)
- runs prompt_toolkit UI (trader_ui.py)
"""

from __future__ import annotations

import os
import argparse
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from config import BINANCE_SYMBOL_DEFAULT, UI_HZ, PM_GET_MARKET_FROM_SLUG_URL
from run_context import RunContext, bucket_start_ms_from_serverish_clock
from config import CLOB_HOST  # if you have it; otherwise use "https://clob.polymarket.com"
from resolve_polymarket_market import resolve_market_by_slug, build_15m_slug, MarketMeta
from py_clob_client.client import ClobClient
from trader_state import TraderState, scenario_pnl, mark_to_market_pnl, check_stuck_trades
from user_ws_task import user_ws_task, ConnStatus
from trader_ui import build_trader_app
from market_ws_task_trader import market_ws_task_trader, ConnStatus as MarketConnStatus


LOGGER = logging.getLogger(__name__)


def derive_user_ws_auth(*, host: str, chain_id: int, signature_type: int, funder: str, private_key: str) -> dict[str, str]:
    client = ClobClient(host, key=private_key, chain_id=chain_id, signature_type=signature_type, funder=funder)
    creds = client.create_or_derive_api_creds()  # returns dict with apiKey/secret/passphrase
    api_key = creds.api_key
    secret = creds.api_secret
    passphrase = creds.api_passphrase
    if not api_key or not secret or not passphrase:
        raise RuntimeError(f"Bad derived api creds: {creds!r}")
    return {"apiKey": api_key, "secret": secret, "passphrase": passphrase}


def _coin_from_symbol(symbol: str) -> str:
    # Mirrors dashboard tendency: symbol like BTCUSDT -> "btc"
    return symbol[:3].lower()


async def _resolve_condition_id_by_slug(session: aiohttp.ClientSession, slug: str) -> Optional[str]:
    """
    Gamma slug endpoint returns a JSON object for the market.
    We need the *condition ID* string (bytes32 hex) for user-channel subscription.

    This function:
      - GETs /markets/slug/<slug>
      - tries common key variants for condition id
    """
    url = f"{PM_GET_MARKET_FROM_SLUG_URL}/{slug}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    # Common key variants seen across Polymarket/Gamma payloads in the wild.
    for k in ("conditionId", "condition_id", "conditionID", "condition"):
        v = data.get(k)
        if isinstance(v, str) and v.startswith("0x") and len(v) >= 10:
            return v

    # Some payloads might nest or encode it differently; last-resort: try to find any 0x.. string field.
    for k, v in data.items():
        if isinstance(v, str) and v.startswith("0x") and len(v) >= 10 and "cond" in k.lower():
            return v

    return None


async def _resolve_current_market(symbol: str) -> tuple[MarketMeta, str]:
    """
    Resolve the current BTC 15m UpDown market:
      - compute current 15m bucket start (UTC seconds)
      - build slug: "{coin}-updown-15m-{start_time_utc_s}"
      - resolve MarketMeta via resolve_market_by_slug
      - also fetch condition_id (bytes32 hex) from Gamma payload
    """
    coin = _coin_from_symbol(symbol)

    # Current 15m bucket, in ms, computed using the same helper used by dashboard run_context.
    start_ms = bucket_start_ms_from_serverish_clock(
        last_clob_event_ms=0,
        clock_offset_ms=0.0,
        now_local_ms=int(time.time() * 1000.0),
    )
    start_s = int(start_ms // 1000)

    slug = build_15m_slug(coin, start_s)

    async with aiohttp.ClientSession() as session:
        meta = await resolve_market_by_slug(session, slug)
        if meta is None:
            raise RuntimeError(f"Could not resolve market by slug: {slug}")

        cond = await _resolve_condition_id_by_slug(session, slug)
        if not cond:
            raise RuntimeError(
                f"Resolved slug={slug} but could not extract condition_id from Gamma response. "
                f"(Need bytes32 hex for user WS subscription.)"
            )

    return meta, cond


async def _run_ctx_roll_task(run_ctx: RunContext) -> None:
    """
    Mirrors dashboard run_dir_roll_task logic, but without requiring book state.
    We recompute the current 15m bucket from local time and roll when it changes.
    """
    last_run_id = run_ctx.run_id
    while True:
        await asyncio.sleep(0.25)
        start_ms = bucket_start_ms_from_serverish_clock(
            last_clob_event_ms=0,
            clock_offset_ms=0.0,
            now_local_ms=int(time.time() * 1000.0),
        )
        changed = run_ctx.set_market_start_ms(int(start_ms))
        if not changed:
            continue
        if run_ctx.run_id != last_run_id:
            last_run_id = run_ctx.run_id
            # UI/logger will pick up run_ctx.run_dir as needed.
            # Keep this task intentionally minimal.
            LOGGER.info("RunContext rolled -> %s", run_ctx.run_dir)


async def run_app(
    *,
    symbol: str,
    condition_id: str | None = None,
) -> None:
    """
    Create and run the trader application.
    Mirrors dashboard run_app pattern: build state, ensure run_ctx, start tasks, run UI.
    """
    # --- state ---
    st = TraderState()
    user_status = ConnStatus()
    mkt_status = MarketConnStatus()

    # --- run context (same semantics) ---
    run_ctx = RunContext(base_dir="logs")
    # Initialize immediately to current 15m bucket (may roll)
    start_ms = bucket_start_ms_from_serverish_clock(
        last_clob_event_ms=0,
        clock_offset_ms=0.0,
        now_local_ms=int(time.time() * 1000.0),
    )
    run_ctx.set_market_start_ms(int(start_ms))
    run_ctx.ensure()

    # --- resolve market ---
    if condition_id is None:
        meta, condition_id = await _resolve_current_market(symbol)
    else:
        try:
            # still resolve meta for UI + run_ctx anchor
            meta, _ = await _resolve_current_market(symbol)
        except Exception as e:
            st.recent_events.appendleft(f"SYSTEM: startup exception: {e!r}")
            raise

    asset_map = {
        meta.yes_asset_id: "Up",
        meta.no_asset_id: "Down",
    }

    # Anchor run_ctx to Gamma eventStartTime (source-of-truth) if available
    if meta.start_ms > 0:
        run_ctx.set_market_start_ms(int(meta.start_ms))

    # st.recent_events.appendleft(f"Resolved market: slug={meta.slug}")
    # st.recent_events.appendleft(f"question: {meta.question}")
    # st.recent_events.appendleft(f"condition_id: {condition_id}")
    # st.recent_events.appendleft(f"assets: Up={meta.yes_asset_id} Down={meta.no_asset_id}")

    # --- background tasks ---
    stop_evt = asyncio.Event()

    tasks: list[asyncio.Task] = []
    tasks.append(asyncio.create_task(_run_ctx_roll_task(run_ctx)))

    host = CLOB_HOST  # should be "https://clob.polymarket.com"
    chain_id = 137
    signature_type = 1  # matches your current executor (Magic/Proxy)
    private_key = os.getenv("POLYMARKET_SECRET_KEY", "").strip()
    funder = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip()

    if not private_key:
        raise RuntimeError("Missing env POLYMARKET_SECRET_KEY")
    if not funder:
        raise RuntimeError("Missing env POLYMARKET_FUNDER_ADDRESS")

    auth = derive_user_ws_auth(
        host=host,
        chain_id=chain_id,
        signature_type=signature_type,
        funder=funder,
        private_key=private_key,
    )

    # User WS (truth for orders/trades)
    tasks.append(
        asyncio.create_task(
            user_ws_task(
                state=st,
                conn_status=user_status,
                markets=[condition_id],
                api_key=auth["apiKey"],
                secret=auth["secret"],
                passphrase=auth["passphrase"],
                stop_event=stop_evt,
                url_base="wss://ws-subscriptions-clob.polymarket.com",
            )
        )
    )

    tasks.append(
        asyncio.create_task(
            market_ws_task_trader(
                state=st,
                conn_status=mkt_status,
                asset_id_to_outcome=asset_map,
                stop_event=stop_evt,
                url_base="wss://ws-subscriptions-clob.polymarket.com",
            )
        )
    )

    # --- watchdog tick (barf on stuck trades) ---
    async def watchdog_task() -> None:
        while True:
            await asyncio.sleep(0.5)
            check_stuck_trades(st, int(time.time()), timeout_s=30)

    tasks.append(asyncio.create_task(watchdog_task()))

    # --- UI ---
    app = build_trader_app(
        trader_state=st,
        user_conn_status=user_status,
        market_conn_status=mkt_status,
        run_ctx=run_ctx,
        market_meta=meta,
        ui_hz=UI_HZ,
        stop_event=stop_evt,
    )

    try:
        await app.run_async()
    finally:
        st.recent_events.appendleft("SYSTEM: shutting down -> cancelling tasks")
        stop_evt.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Polymarket trader UI (separate process).")
    p.add_argument(
        "--symbol",
        default=BINANCE_SYMBOL_DEFAULT,
        help=f"Symbol/coin hint (default: {BINANCE_SYMBOL_DEFAULT}). Used to resolve current 15m market.",
    )
    p.add_argument(
        "--condition-id",
        default=None,
        help="Optional override condition_id (bytes32 hex). If omitted, we resolve current 15m slug via Gamma.",
    )

    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    asyncio.run(run_app(symbol=args.symbol, condition_id=args.condition_id))


if __name__ == "__main__":
    import os
    main()
