"""
taker_ev_allslugs_min.py — Polymarket EV Lab v3.0 (bare minimum, all slugs)

Next step:
- SAME exact logic as taker_ev_min.py
- BUT run across ALL overlapping slugs (still one tau, one k, one W)
- Output:
    * trades_allslugs.csv
    * summary_allslugs.csv
    * diag_counts_allslugs.csv
    * diag_slip_allslugs.csv

Run:
  python taker_ev_allslugs_min.py --metrics ./metrics.jsonl --clob ./polymarket_clob.jsonl --out ./out

Optional knobs:
  --k 4.0 --W 60 --tau 200 --entry_slip 500 --exit_slip 500
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import List, Tuple

import polars as pl


FV_TYPE = "fv_vol_1s"
BOOK1S_TYPE = "book_1s"
CLOB_BOOK_TYPE = "book"


@dataclass(frozen=True)
class Params:
    W_sec: int = 60
    k: float = 4.0
    tau_ms: int = 200
    entry_slip_max_ms: int = 500
    exit_slip_max_ms: int = 500
    warmup_reason: str = "warmup"


def read_jsonl(path: str) -> pl.DataFrame:
    return pl.read_ndjson(path, infer_schema_length=20_000)


def ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def slug_window_bounds(slug: str) -> tuple[int, int]:
    start_s = int(slug.rsplit("-", 1)[-1])
    start_ms = start_s * 1000
    end_ms = start_ms + 900_000
    return start_ms, end_ms


def warmup_trim_per_slug(fv_s: pl.DataFrame, warmup_reason: str) -> pl.DataFrame:
    # Drop rows until first row with fv_ok==True OR fv_reason!="warmup"
    fv_s = fv_s.with_row_index("row_id")
    first_good = (
        fv_s.filter(pl.col("fv_ok") | (pl.col("fv_reason") != warmup_reason))
        .select(pl.col("row_id").min().alias("min_id"))
        .item()
    )
    if first_good is None:
        return fv_s.head(0).drop("row_id")
    return fv_s.filter(pl.col("row_id") >= int(first_good)).drop("row_id")


def compute_impulses_for_slug(fv_s: pl.DataFrame, p: Params, slug: str) -> pl.DataFrame:
    w0, w1 = slug_window_bounds(slug)

    fv_s = fv_s.sort(["market_slug", "trade_ms"])

    # returns + rolling sigma (1s cadence assumed)
    W = p.W_sec
    fv_s = (
        fv_s.with_columns(
            [
                pl.col("price").log().diff().alias("log_ret"),
            ]
        )
        .with_columns(
            [
                # Polars >=1.21: min_samples
                pl.col("log_ret").rolling_std(window_size=W, min_samples=W).alias("sigma"),
            ]
        )
        .with_columns(
            [
                (pl.col("log_ret").abs() / pl.col("sigma")).alias("z"),
                pl.when(pl.col("log_ret") > 0).then(pl.lit(1)).otherwise(pl.lit(-1)).alias("dir"),
                pl.col("trade_ms").alias("t0_ms"),
            ]
        )
    )

    impulses = fv_s.filter(pl.col("sigma").is_not_null() & (pl.col("z") > p.k))
    impulses = impulses.filter((pl.col("t0_ms") >= w0) & (pl.col("t0_ms") < w1))
    return impulses


def main() -> None:
    # ap = argparse.ArgumentParser()
    # ap.add_argument("--metrics", required=True)
    # ap.add_argument("--clob", required=True)
    # ap.add_argument("--out", default="./out")
    # args = ap.parse_args()

    # date_str = "20260202_082832"
    date_str = "20260202_183354"
    args = argparse.Namespace(
        clob=fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\polymarket_clob.jsonl",
        metrics=fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\metrics.jsonl",
        out=r"..\data"
    )

    p = Params(
        W_sec=60,
        k=4.0,
        tau_ms=2000,
        entry_slip_max_ms=500,
        exit_slip_max_ms=500,
    )

    ensure_out_dir(args.out)

    metrics = read_jsonl(args.metrics)
    clob = read_jsonl(args.clob)

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
        .filter(pl.col("market_slug") != "")
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
                pl.col("l1").struct.field("yes_bid").cast(pl.Float64, strict=False).alias("yes_bid"),
                pl.col("l1").struct.field("yes_ask").cast(pl.Float64, strict=False).alias("yes_ask"),
                pl.col("l1").struct.field("no_bid").cast(pl.Float64, strict=False).alias("no_bid"),
                pl.col("l1").struct.field("no_ask").cast(pl.Float64, strict=False).alias("no_ask"),
            ]
        )
        .select(["market_slug", "ts_local_ms", "yes_bid", "yes_ask", "no_bid", "no_ask"])
    )

    # -----------------------------
    # Overlap slugs
    # -----------------------------
    fv_slugs = set(fv.select("market_slug").unique().to_series().to_list())
    cb_slugs = set(cb.select("market_slug").unique().to_series().to_list())
    overlap = sorted(list(fv_slugs & cb_slugs))
    if not overlap:
        raise RuntimeError("No slug overlap between fv_vol_1s and clob.book")

    print(f"Overlap slugs: {len(overlap)}")
    print(f"Params: k={p.k} W={p.W_sec}s tau={p.tau_ms}ms entry_slip<={p.entry_slip_max_ms} exit_slip<={p.exit_slip_max_ms}")

    trades_list: List[pl.DataFrame] = []
    diag_rows: List[pl.DataFrame] = []

    # Pre-sort right sides for join_asof (group sorted)
    b1_gate_all = (
        b1.select(
            [
                pl.col("market_slug"),
                pl.col("ts_local_ms"),
                pl.col("regime").struct.field("has_book").alias("has_book"),
                pl.col("regime").struct.field("is_two_sided").alias("is_two_sided"),
                pl.col("regime").struct.field("is_pinned").alias("is_pinned"),
                pl.col("regime").struct.field("no_trade_zone").alias("no_trade_zone"),
            ]
        )
        .filter(pl.col("market_slug") != "")
        .sort(["market_slug", "ts_local_ms"])
    )

    cb_all = cb.sort(["market_slug", "ts_local_ms"])

    # -----------------------------
    # Loop slugs
    # -----------------------------
    for slug in overlap:
        w0, w1 = slug_window_bounds(slug)

        fv_s = fv.filter(pl.col("market_slug") == slug)
        if fv_s.height == 0:
            continue

        fv_s = warmup_trim_per_slug(fv_s, p.warmup_reason)
        n_after_trim = fv_s.height
        if n_after_trim < p.W_sec + 2:
            # Not enough rows for rolling sigma
            diag_rows.append(
                pl.DataFrame(
                    {
                        "market_slug": [slug],
                        "n_after_trim": [n_after_trim],
                        "n_impulses": [0],
                        "n_pass_regime": [0],
                        "n_entry": [0],
                        "n_trades": [0],
                        "note": ["too_few_rows_for_sigma"],
                    }
                )
            )
            continue

        impulses = compute_impulses_for_slug(fv_s, p, slug)
        n_imp = impulses.height
        if n_imp == 0:
            diag_rows.append(
                pl.DataFrame(
                    {
                        "market_slug": [slug],
                        "n_after_trim": [n_after_trim],
                        "n_impulses": [0],
                        "n_pass_regime": [0],
                        "n_entry": [0],
                        "n_trades": [0],
                        "note": ["no_impulses"],
                    }
                )
            )
            continue

        # Regime gate (backward asof on book_1s)
        impulses = impulses.sort(["market_slug", "t0_ms"])
        impulses_g = impulses.join_asof(
            b1_gate_all,
            left_on="t0_ms",
            right_on="ts_local_ms",
            by="market_slug",
            strategy="backward",
        ).filter(
            (pl.col("has_book") == True)
            & (pl.col("is_two_sided") == True)
            & (pl.col("is_pinned") == False)
            & (pl.col("no_trade_zone") == False)
        )
        n_pass = impulses_g.height
        if n_pass == 0:
            diag_rows.append(
                pl.DataFrame(
                    {
                        "market_slug": [slug],
                        "n_after_trim": [n_after_trim],
                        "n_impulses": [n_imp],
                        "n_pass_regime": [0],
                        "n_entry": [0],
                        "n_trades": [0],
                        "note": ["all_filtered_by_regime"],
                    }
                )
            )
            continue

        # Entry join (forward)
        entry = impulses_g.join_asof(
            cb_all,
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

        entry = entry.filter(
            (pl.col("entry_slip_ms") >= 0) & (pl.col("entry_slip_ms") <= p.entry_slip_max_ms)
        )

        # Entry px (ask), drop null/<=0
        entry = entry.with_columns(
            [
                pl.when(pl.col("dir") == 1).then(pl.col("yes_ask")).otherwise(pl.col("no_ask")).alias("entry_px"),
            ]
        ).filter(pl.col("entry_px").is_not_null() & (pl.col("entry_px") > 0))

        n_entry = entry.height
        if n_entry == 0:
            diag_rows.append(
                pl.DataFrame(
                    {
                        "market_slug": [slug],
                        "n_after_trim": [n_after_trim],
                        "n_impulses": [n_imp],
                        "n_pass_regime": [n_pass],
                        "n_entry": [0],
                        "n_trades": [0],
                        "note": ["no_entries_after_slip_or_null_px"],
                    }
                )
            )
            continue

        # Exit join (forward at t1)
        exit_ = entry.join_asof(
            cb_all,
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

        exit_ = exit_.filter((pl.col("exit_slip_ms") >= 0) & (pl.col("exit_slip_ms") <= p.exit_slip_max_ms))

        # Exit px (bid), drop null/<=0
        exit_ = exit_.with_columns(
            [
                pl.when(pl.col("dir") == 1)
                .then(pl.col("yes_bid_exit"))
                .otherwise(pl.col("no_bid_exit"))
                .alias("exit_px"),
            ]
        ).filter(pl.col("exit_px").is_not_null() & (pl.col("exit_px") > 0))

        trades = exit_.with_columns(
            [
                (pl.col("exit_px") - pl.col("entry_px")).alias("pnl"),
                pl.lit(p.tau_ms).alias("tau_ms"),
            ]
        )

        n_trades = trades.height

        diag_rows.append(
            pl.DataFrame(
                {
                    "market_slug": [slug],
                    "n_after_trim": [n_after_trim],
                    "n_impulses": [n_imp],
                    "n_pass_regime": [n_pass],
                    "n_entry": [n_entry],
                    "n_trades": [n_trades],
                    "note": [""],
                }
            )
        )

        if n_trades > 0:
            trades_list.append(
                trades.select(
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
            )

    # -----------------------------
    # Combine + write outputs
    # -----------------------------
    diag = pl.concat(diag_rows, how="vertical") if diag_rows else pl.DataFrame()
    diag.write_csv(os.path.join(args.out, f"diag_counts_allslugs_{int(p.tau_ms)}.csv"))

    if not trades_list:
        print("No trades produced across all slugs with current parameters.")
        print(f"Wrote diagnostics: {os.path.join(args.out, f'diag_counts_allslugs_{int(p.tau_ms)}.csv')}")
        return

    trades_all = pl.concat(trades_list, how="vertical")

    trades_all = trades_all.with_columns(
        pl.when(pl.col("tte_s") < 60)
        .then(pl.lit("tte_lt_60"))
        .otherwise(pl.lit("tte_ge_60"))
        .alias("tte_bucket")
    )

    trades_all.write_csv(os.path.join(args.out, f"trades_allslugs_{int(p.tau_ms)}.csv"))

    trades_dir = trades_all.filter(
        (pl.col("tte_s") >= 60) &
        (pl.col("tau_ms").is_in([1000, 2000]))
    )
    summary_dir = (
        trades_dir
        .group_by(["tau_ms", "dir"])
        .agg(
            [
                pl.len().alias("n"),
                pl.col("pnl").mean().alias("mean_pnl"),
                (pl.col("pnl") > 0).mean().alias("winrate"),
            ]
        )
        .sort(["tau_ms", "dir"])
        .with_columns(
            pl.when(pl.col("dir") == 1)
            .then(pl.lit("UP → buy YES"))
            .otherwise(pl.lit("DOWN → buy NO"))
            .alias("direction")
        )
    )

    summary_dir.write_csv(
        os.path.join(args.out, f"summary_directional_tte_ge_60_{int(p.tau_ms)}.csv")
    )


    # summary = trades_all.select(
    #     [
    #         pl.len().alias("n"),
    #         pl.col("pnl").mean().alias("mean_pnl"),
    #         pl.col("pnl").median().alias("median_pnl"),
    #         (pl.col("pnl") > 0).mean().alias("winrate"),
    #         pl.col("pnl").quantile(0.10, interpolation="nearest").alias("p10"),
    #         pl.col("pnl").quantile(0.90, interpolation="nearest").alias("p90"),
    #     ]
    # )

    summary = trades_all.group_by(["tau_ms", "tte_bucket"]).agg(
        [
            pl.len().alias("n"),
            pl.col("pnl").mean().alias("mean_pnl"),
            (pl.col("pnl") > 0).mean().alias("winrate"),
        ]
    )

    summary.write_csv(os.path.join(args.out, f"summary_allslugs_{int(p.tau_ms)}.csv"))

    slip_diag = trades_all.select(
        [
            pl.col("entry_slip_ms").quantile(0.50).alias("entry_slip_p50"),
            pl.col("entry_slip_ms").quantile(0.90).alias("entry_slip_p90"),
            pl.col("entry_slip_ms").quantile(0.99).alias("entry_slip_p99"),
            pl.col("exit_slip_ms").quantile(0.50).alias("exit_slip_p50"),
            pl.col("exit_slip_ms").quantile(0.90).alias("exit_slip_p90"),
            pl.col("exit_slip_ms").quantile(0.99).alias("exit_slip_p99"),
        ]
    )
    slip_diag.write_csv(os.path.join(args.out, f"diag_slip_allslugs_{int(p.tau_ms)}.csv"))

    print("\n✅ Done.")
    print(f"Wrote: {os.path.join(args.out, f'diag_counts_allslugs_{int(p.tau_ms)}.csv')}")
    print(f"Wrote: {os.path.join(args.out, f'trades_allslugs_{int(p.tau_ms)}.csv')}")
    print(f"Wrote: {os.path.join(args.out, f'summary_allslugs_{int(p.tau_ms)}.csv')}")
    print(f"Wrote: {os.path.join(args.out, f'diag_slip_allslugs_{int(p.tau_ms)}.csv')}")


if __name__ == "__main__":
    main()
