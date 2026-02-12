"""
Minimal prompt_toolkit UI for the Trader app.

Panels:
- Header: market question + run_dir + user WS status
- Left: recent events / barfs
- Right: inventory + avg price + scenario P&L + MTM (if marks available)
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

from prompt_toolkit.application import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Layout
from prompt_toolkit.layout.containers import HSplit, VSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style

from run_context import RunContext
from resolve_polymarket_market import MarketMeta

from trader_state import TraderState, scenario_pnl, mark_to_market_pnl, dollar_exposure
from user_ws_task import ConnStatus
from market_ws_task_trader import ConnStatus as MarketConnStatus


def _fmt_pos_line(outcome: str, shares: Decimal, avg: Decimal, realized: Decimal) -> str:
    return f"{outcome:>4}  sh={shares:>10}  avg={avg:>8}  rPNL={realized:>10}"


def build_trader_app(
    *,
    trader_state: TraderState,
    user_conn_status: ConnStatus,
    market_conn_status: MarketConnStatus,   # NEW
    run_ctx: RunContext,
    market_meta: MarketMeta,
    ui_hz: float,
    stop_event: asyncio.Event,
) -> Application:
    kb = KeyBindings()

    @kb.add("q")
    def _(event) -> None:
        stop_event.set()
        event.app.exit()

    header_ctl = FormattedTextControl(text="")
    events_ctl = FormattedTextControl(text="")
    inv_ctl = FormattedTextControl(text="")

    header_win = Window(content=header_ctl, height=5)
    events_win = Window(content=events_ctl, wrap_lines=True)
    inv_win = Window(content=inv_ctl, width=60, wrap_lines=False)

    root = HSplit(
        [
            header_win,
            VSplit([events_win, inv_win]),
            Window(
                height=1,
                content=FormattedTextControl(
                    text="Keys: q=quit   (Order-entry panel comes next; this is read-only plumbing + ledger.)"
                ),
            ),
        ]
    )

    style = Style.from_dict({"": ""})

    app = Application(
        layout=Layout(root),
        key_bindings=kb,
        style=style,
        full_screen=True,
        refresh_interval=None,  # we drive refresh
    )

    async def _refresh_loop() -> None:
        dt = 1.0 / max(1.0, float(ui_hz))
        while not stop_event.is_set():
            _render_header(header_ctl, user_conn_status, market_conn_status, run_ctx, market_meta)
            _render_events(events_ctl, trader_state)
            _render_inventory(inv_ctl, trader_state)
            app.invalidate()
            await asyncio.sleep(dt)

    # Schedule background refresh task immediately (safe before run_async)
    app.create_background_task(_refresh_loop())

    return app


def _render_header(
    header_ctl: FormattedTextControl,
    user_status: ConnStatus,
    mkt_status: MarketConnStatus,
    run_ctx: RunContext,
    meta: MarketMeta,
) -> None:
    now = int(time.time())

    # --- user ws staleness ---
    if user_status.last_msg_ts:
        stale_user = now - int(user_status.last_msg_ts)
        stale_user_s = f"{stale_user}s"
    else:
        stale_user_s = "n/a"

    # --- market ws staleness ---
    if mkt_status.last_msg_ts:
        stale_mkt = now - int(mkt_status.last_msg_ts)
        stale_mkt_s = f"{stale_mkt}s"
    else:
        stale_mkt_s = "n/a"

    line1 = (meta.question or meta.slug)[:200]

    line2 = (
        f"run_dir={run_ctx.run_dir} | "
        f"user_ws={'UP' if user_status.connected else 'DOWN'} ({stale_user_s}) | {user_status.text} | "
        f"mkt_ws={'UP' if mkt_status.connected else 'DOWN'} ({stale_mkt_s}) | {mkt_status.text}"
    )

    line3 = f"assets: Up={meta.yes_asset_id}"
    line4 = f"        Down={meta.no_asset_id}"

    header_ctl.text = "\n".join([line1, line2, line3, line4])


def _render_events(events_ctl: FormattedTextControl, st: TraderState) -> None:
    # Show last N events, newest first
    lines = []
    for i, s in enumerate(list(st.recent_events)[:120]):
        lines.append(s)
    if not lines:
        lines = ["(no events yet)"]
    events_ctl.text = "\n".join(lines)


def _render_inventory(inv_ctl: FormattedTextControl, st: TraderState) -> None:
    up = st.positions.get("Up")
    down = st.positions.get("Down")

    up_sh = up.shares if up else Decimal("0")
    dn_sh = down.shares if down else Decimal("0")

    up_avg = up.avg_price if up else Decimal("0")
    dn_avg = down.avg_price if down else Decimal("0")

    up_r = up.realized_pnl if up else Decimal("0")
    dn_r = down.realized_pnl if down else Decimal("0")

    pnl_up, pnl_down = scenario_pnl(st)
    mtm = mark_to_market_pnl(st)

    q_up = st.quotes.get("Up")
    q_dn = st.quotes.get("Down")

    def qline(name: str, q):
        if not q or q.mid is None:
            return f"{name}: bid=? ask=? mid=?"
        age = int(time.time()) - int(q.ts)
        return f"{name}: bid={q.bid} ask={q.ask} mid={q.mid}  ({age}s)"

    exp_up, exp_dn = dollar_exposure(st)

    lines = [
        "INVENTORY / P&L",
        "-" * 58,
        _fmt_pos_line("Up", up_sh, up_avg, up_r),
        _fmt_pos_line("Down", dn_sh, dn_avg, dn_r),
        "",
        qline("Up", q_up),
        qline("Down", q_dn),
        "",
        f"$ exposure Up={exp_up}  Down={exp_dn}",
        "",
        f"cash={st.cash}",
        "",
        f"Scenario P&L if Up settles  : {pnl_up}",
        f"Scenario P&L if Down settles: {pnl_down}",
        "",
        f"MTM P&L                     : {mtm}",
        "",
        "Notes:",
        "- Fills are applied at MATCHED (idempotent).",
        "- If a trade gets stuck (no CONFIRMED/MINED), you'll see BARF alerts.",
    ]
    inv_ctl.text = "\n".join(lines)
