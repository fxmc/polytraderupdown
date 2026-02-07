"""
taker_ev_min.py — Polymarket EV Lab v3.0 (bare minimum)

Goal:
- Detect impulses on fv_vol_1s.price (driver returns) per slug
- Regime gate using book_1s near t0
- Execute taker fills using polymarket_clob.book L1 (ask in, bid out)
- Gap-aware via entry/exit slippage caps
- Output trade-level rows + minimal summary + slip diagnostics

Run:
  python taker_ev_min.py --metrics ./metrics.jsonl --clob ./polymarket_clob.jsonl --out ./out

Notes:
- This is intentionally minimal: one slug, one tau.
- No bucketing yet. No UI. No perfection.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

import polars as pl


FV_TYPE = "fv_vol_1s"
BOOK1S_TYPE = "book_1s"
CLOB_BOOK_TYPE = "book"


@dataclass(frozen=True)
class Params:
    W_sec: int = 60
    k: float = 4
    tau_ms: int = 200
    entry_slip_max_ms: int = 500
    exit_slip_max_ms: int = 500
    warmup_reason: str = "warmup"


def read_jsonl(path: str) -> pl.DataFrame:
    return pl.read_ndjson(path, infer_schema_length=20_000)


def ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def slug_window_bounds(slug: str) -> tuple[int, int]:
    # slug format: btc-updown-15m-<START_S>
    start_s = int(slug.rsplit("-", 1)[-1])
    start_ms = start_s * 1000
    end_ms = start_ms + 900_000
    return start_ms, end_ms


import json

def find_first_bad_line(path, max_lines=None):
    with open(path, "rb") as f:
        for i, raw in enumerate(f, start=1):
            if max_lines and i > max_lines:
                print("No bad line found in first", max_lines, "lines")
                return None

            s = raw.decode("utf-8", errors="replace")

            if s.strip() == "":
                print(f"BAD (blank/whitespace) line {i}: repr={repr(s[:80])}")
                return i

            try:
                json.loads(s)
            except Exception as e:
                print(f"BAD line {i}: {e}")
                print("Preview repr:", repr(s[:200].replace("\n", "\\n")))
                return i

    print("No bad lines found (file is valid NDJSON).")
    return None


# find_first_bad_line(r"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_20260202_082832\polymarket_clob.jsonl")
# find_first_bad_line(r"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_20260202_082832\metrics.jsonl")




def main() -> None:
    # ap = argparse.ArgumentParser()
    # ap.add_argument("--metrics", required=True)
    # ap.add_argument("--clob", required=True)
    # ap.add_argument("--out", default="./out")
    # args = ap.parse_args()

    date_str = "20260202_082832"
    args = argparse.Namespace(
        clob=fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\polymarket_clob.jsonl",
        metrics=fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\metrics.jsonl",
        out=r"..\data"
    )

    p = Params()
    ensure_out_dir(args.out)

    clob = read_jsonl(args.clob)
    metrics = read_jsonl(args.metrics)

    # -----------------------------
    # Extract streams + canonicalize
    # -----------------------------
    fv = (
        metrics.filter(pl.col("type") == FV_TYPE)
        .with_columns(
            [
                pl.col("trade_ms").cast(pl.Int64, strict=False),
                pl.col("ts_local_ms").cast(pl.Int64, strict=False),
                pl.col("price").cast(pl.Float64, strict=False),
                pl.col("tte_s").cast(pl.Float64, strict=False),
                pl.col("p_yes").cast(pl.Float64, strict=False),
                pl.col("market_slug").cast(pl.Utf8, strict=False),
                pl.col("fv_reason").cast(pl.Utf8, strict=False),
                pl.col("fv_ok").cast(pl.Boolean, strict=False),
            ]
        )
        .filter(pl.col("market_slug") != "")  # hygiene
    )

    b1 = (
        metrics.filter(pl.col("type") == BOOK1S_TYPE)
        .with_columns(
            [
                pl.col("ts_local_ms").cast(pl.Int64, strict=False),
                pl.col("market_slug").cast(pl.Utf8, strict=False),
            ]
        )
    )

    cb = (
        clob.filter(pl.col("type") == CLOB_BOOK_TYPE)
        .with_columns(
            [
                pl.col("ts_local_ms").cast(pl.Int64, strict=False),
                pl.col("market_slug").cast(pl.Utf8, strict=False),
                # flatten l1 for join/selection simplicity
                pl.col("l1").struct.field("yes_bid").cast(pl.Float64, strict=False).alias("yes_bid"),
                pl.col("l1").struct.field("yes_ask").cast(pl.Float64, strict=False).alias("yes_ask"),
                pl.col("l1").struct.field("no_bid").cast(pl.Float64, strict=False).alias("no_bid"),
                pl.col("l1").struct.field("no_ask").cast(pl.Float64, strict=False).alias("no_ask"),
            ]
        )
        .select(["market_slug", "ts_local_ms", "yes_bid", "yes_ask", "no_bid", "no_ask"])
    )

    # -----------------------------
    # Pick ONE slug (overlap)
    # -----------------------------
    fv_slugs = set(fv.select("market_slug").unique().to_series().to_list())
    cb_slugs = set(cb.select("market_slug").unique().to_series().to_list())
    overlap = sorted(list(fv_slugs & cb_slugs))
    if not overlap:
        raise RuntimeError("No slug overlap between fv_vol_1s and clob.book")

    slug = overlap[0]
    w0, w1 = slug_window_bounds(slug)

    print(f"Using slug: {slug}")
    print(f"Window: [{w0}, {w1})  tau_ms={p.tau_ms}  k={p.k}  W_sec={p.W_sec}")
    print(f"Slip caps: entry<= {p.entry_slip_max_ms}ms, exit<= {p.exit_slip_max_ms}ms")

    fv_s = fv.filter(pl.col("market_slug") == slug).sort("trade_ms")
    b1_s = b1.filter(pl.col("market_slug") == slug).sort("ts_local_ms")
    cb_s = cb.filter(pl.col("market_slug") == slug).sort("ts_local_ms")

    # -----------------------------
    # Warmup trim (per slug)
    # -----------------------------
    fv_s = fv_s.with_row_index("row_id")
    first_good = (
        fv_s.filter(pl.col("fv_ok") | (pl.col("fv_reason") != p.warmup_reason))
        .select(pl.col("row_id").min().alias("min_id"))
        .item()
    )
    if first_good is None:
        raise RuntimeError("No non-warmup fv rows found for slug")

    fv_s = fv_s.filter(pl.col("row_id") >= int(first_good)).drop("row_id")

    # -----------------------------
    # Compute driver returns + rolling sigma
    # -----------------------------
    # returns in log space
    fv_s = fv_s.with_columns(
        [
            pl.col("price").log().diff().alias("log_ret"),
        ]
    )

    # Rolling window size in rows: fv is 1s cadence by design, so W_sec rows.
    W = p.W_sec
    fv_s = fv_s.with_columns(
        [
            pl.col("log_ret").rolling_std(window_size=W, min_periods=W).alias("sigma"),
        ]
    ).with_columns(
        [
            (pl.col("log_ret").abs() / pl.col("sigma")).alias("z"),
            pl.when(pl.col("log_ret") > 0).then(pl.lit(1)).otherwise(pl.lit(-1)).alias("dir"),
            pl.col("trade_ms").alias("t0_ms"),
        ]
    )

    # Filter impulses: requires sigma non-null and trigger z > k
    impulses = fv_s.filter(pl.col("sigma").is_not_null() & (pl.col("z") > p.k))

    # Window overlap filter
    impulses = impulses.filter((pl.col("t0_ms") >= w0) & (pl.col("t0_ms") < w1))

    n_imp_raw = impulses.height
    print(f"Impulses in-window (pre-regime): {n_imp_raw}")

    if n_imp_raw == 0:
        # Write a small diagnostic and exit cleanly
        impulses.select(["t0_ms", "price", "log_ret", "sigma", "z", "dir"]).write_csv(
            os.path.join(args.out, "impulses_min_EMPTY.csv")
        )
        raise RuntimeError("No impulses detected for this slug with current k/W.")

    # -----------------------------
    # Regime gate at/near t0
    # Use backward asof: last known regime snapshot at or before t0.
    # -----------------------------
    impulses = impulses.sort("t0_ms")
    b1_gate = (
        b1_s.select(
            [
                pl.col("ts_local_ms"),
                pl.col("market_slug"),
                pl.col("regime").struct.field("has_book").alias("has_book"),
                pl.col("regime").struct.field("is_two_sided").alias("is_two_sided"),
                pl.col("regime").struct.field("is_pinned").alias("is_pinned"),
                pl.col("regime").struct.field("no_trade_zone").alias("no_trade_zone"),
            ]
        )
        .sort("ts_local_ms")
    )

    impulses_g = impulses.join_asof(
        b1_gate,
        left_on="t0_ms",
        right_on="ts_local_ms",
        by="market_slug",
        strategy="backward",
    )

    impulses_g = impulses_g.filter(
        (pl.col("has_book") == True)
        & (pl.col("is_two_sided") == True)
        & (pl.col("is_pinned") == False)
        & (pl.col("no_trade_zone") == False)
    )

    n_pass_regime = impulses_g.height
    print(f"Impulses passing regime: {n_pass_regime} ({n_pass_regime/n_imp_raw:.2%})")

    if n_pass_regime == 0:
        impulses_g.write_csv(os.path.join(args.out, "impulses_min_REGIME_EMPTY.csv"))
        raise RuntimeError("All impulses filtered out by regime gating.")

    # -----------------------------
    # Entry join (forward): first CLOB book at/after t0
    # -----------------------------
    cb_s = cb_s.sort("ts_local_ms")

    entry = impulses_g.join_asof(
        cb_s,
        left_on="t0_ms",
        right_on="ts_local_ms",
        by="market_slug",
        strategy="forward",
        suffix="_entry",
    ).with_columns(
        [
            (pl.col("ts_local_ms_entry") - pl.col("t0_ms")).alias("entry_slip_ms"),
            (pl.col("t0_ms") + pl.lit(p.tau_ms)).alias("t1_ms"),
        ]
    )

    # Gate entry slippage
    entry = entry.filter((pl.col("entry_slip_ms") >= 0) & (pl.col("entry_slip_ms") <= p.entry_slip_max_ms))

    n_entry = entry.height
    print(f"Entry rows after slip gate: {n_entry} ({n_entry/n_pass_regime:.2%})")
    if n_entry == 0:
        entry.write_csv(os.path.join(args.out, "trades_min_ENTRY_EMPTY.csv"))
        raise RuntimeError("No entries found within entry_slip_max_ms.")

    # Compute entry_px (ask) based on direction
    entry = entry.with_columns(
        [
            pl.when(pl.col("dir") == 1).then(pl.col("yes_ask")).otherwise(pl.col("no_ask")).alias("entry_px"),
        ]
    ).filter(pl.col("entry_px").is_not_null() & (pl.col("entry_px") > 0))

    # -----------------------------
    # Exit join (forward): first CLOB book at/after t1 = t0 + tau
    # -----------------------------
    exit_ = entry.join_asof(
        cb_s,
        left_on="t1_ms",
        right_on="ts_local_ms",
        by="market_slug",
        strategy="forward",
        suffix="_exit",
    ).with_columns(
        [
            (pl.col("ts_local_ms_exit") - pl.col("t1_ms")).alias("exit_slip_ms"),
        ]
    )

    # Gate exit slippage
    exit_ = exit_.filter((pl.col("exit_slip_ms") >= 0) & (pl.col("exit_slip_ms") <= p.exit_slip_max_ms))

    # Compute exit_px (bid) based on direction
    exit_ = exit_.with_columns(
        [
            pl.when(pl.col("dir") == 1).then(pl.col("yes_bid_exit")).otherwise(pl.col("no_bid_exit")).alias("exit_px"),
        ]
    ).filter(pl.col("exit_px").is_not_null() & (pl.col("exit_px") > 0))

    # PnL
    trades = exit_.with_columns(
        [
            (pl.col("exit_px") - pl.col("entry_px")).alias("pnl"),
            pl.lit(p.tau_ms).alias("tau_ms"),
        ]
    )

    n_trades = trades.height
    print(f"Trades after exit + slip gates: {n_trades} ({n_trades/n_entry:.2%})")

    # -----------------------------
    # Write trade-level output (minimal columns)
    # -----------------------------
    trades_out = trades.select(
        [
            "market_slug",
            "t0_ms",
            "t1_ms",
            "dir",
            "z",
            "tte_s",
            "p_yes",
            "entry_px",
            "exit_px",
            "pnl",
            "entry_slip_ms",
            "exit_slip_ms",
            "tau_ms",
        ]
    )
    trades_out.write_csv(os.path.join(args.out, "trades_min.csv"))

    # -----------------------------
    # Summary
    # -----------------------------
    summary = trades_out.select(
        [
            pl.len().alias("n"),
            pl.col("pnl").mean().alias("mean_pnl"),
            pl.col("pnl").median().alias("median_pnl"),
            (pl.col("pnl") > 0).mean().alias("winrate"),
            pl.col("pnl").quantile(0.10, interpolation="nearest").alias("p10"),
            pl.col("pnl").quantile(0.90, interpolation="nearest").alias("p90"),
        ]
    )
    summary.write_csv(os.path.join(args.out, "summary_min.csv"))

    # -----------------------------
    # Slip diagnostics
    # -----------------------------
    diag = trades_out.select(
        [
            pl.col("entry_slip_ms").quantile(0.50).alias("entry_slip_p50"),
            pl.col("entry_slip_ms").quantile(0.90).alias("entry_slip_p90"),
            pl.col("entry_slip_ms").quantile(0.99).alias("entry_slip_p99"),
            pl.col("exit_slip_ms").quantile(0.50).alias("exit_slip_p50"),
            pl.col("exit_slip_ms").quantile(0.90).alias("exit_slip_p90"),
            pl.col("exit_slip_ms").quantile(0.99).alias("exit_slip_p99"),
        ]
    )
    diag.write_csv(os.path.join(args.out, "diag_slip_min.csv"))

    print("\n✅ Done.")
    print(f"Wrote: {os.path.join(args.out, 'trades_min.csv')}")
    print(f"Wrote: {os.path.join(args.out, 'summary_min.csv')}")
    print(f"Wrote: {os.path.join(args.out, 'diag_slip_min.csv')}")


if __name__ == "__main__":
    main()
