# validate_tracked_fills.py
import json
import math
from pathlib import Path

F = Path("tracked_trader_fills.jsonl")

# If you want the MTM check to match the header precisely,
# feed these from a captured PlotSnap at the same time.
LAST_MID_YES = float("nan")  # TODO: set to last YES mid
LAST_MID_NO  = float("nan")  # TODO: set to last NO mid

pos = {"YES": 0.0, "NO": 0.0}
cash = {"YES": 0.0, "NO": 0.0}
buy_qty = {"YES": 0.0, "NO": 0.0}
buy_notional = {"YES": 0.0, "NO": 0.0}

# Optional: if you know the YES/NO token_ids, lock them.
YES_TOKEN_ID = None  # int or None
NO_TOKEN_ID = None   # int or None

def map_token(token_id: int) -> str:
    # Match plot_process behavior: first-seen NO, second YES (unless overrides)
    # For offline sanity, prefer explicit IDs if you have them.
    nonlocal_map = map_token.__dict__.setdefault("tokmap", {})
    if YES_TOKEN_ID is not None:
        nonlocal_map[YES_TOKEN_ID] = "YES"
    if NO_TOKEN_ID is not None:
        nonlocal_map[NO_TOKEN_ID] = "NO"

    if token_id in nonlocal_map:
        return nonlocal_map[token_id]

    vals = set(nonlocal_map.values())
    if "NO" not in vals:
        nonlocal_map[token_id] = "NO"
        return "NO"
    nonlocal_map[token_id] = "YES"
    return "YES"

for line in F.read_text(encoding="utf-8").splitlines():
    o = json.loads(line)
    assert o["type"] == "tracked_fill"

    token = map_token(int(o["token_id"]))
    side = str(o["side"]).upper()
    sz = float(o["size"])
    px = float(o["price"])

    if sz <= 0 or not math.isfinite(px):
        continue

    if side == "BUY":
        pos[token] += sz
        cash[token] += sz * px
        buy_qty[token] += sz
        buy_notional[token] += sz * px
    else:
        pos[token] -= sz
        cash[token] -= sz * px

net_spent = cash["YES"] + cash["NO"]

avg_yes = buy_notional["YES"] / buy_qty["YES"] if buy_qty["YES"] > 0 else float("nan")
avg_no  = buy_notional["NO"]  / buy_qty["NO"]  if buy_qty["NO"]  > 0 else float("nan")

pnl_if_yes = pos["YES"] * 1.0 - net_spent
pnl_if_no  = pos["NO"]  * 1.0 - net_spent

value = pos["YES"] * LAST_MID_YES + pos["NO"] * LAST_MID_NO
pnl_mtm = value - net_spent

print("pos:", pos)
print("cash(outflow):", cash, "net_spent:", net_spent)
print("avg_buy:", {"YES": avg_yes, "NO": avg_no})
print("pnl_mtm:", pnl_mtm)
print("pnl_if_yes:", pnl_if_yes, "pnl_if_no:", pnl_if_no)
