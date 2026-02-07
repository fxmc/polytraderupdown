"""
taker_ev_down_spread_filter.py

Purpose (minimal, decisive):
- Focus on the discovered edge: DOWN impulses (dir=-1 => buy NO)
- Evaluate tau in {1000,2000} ms (configurable)
- Filter to tte_s >= 60 (avoid expiry convexity)
- Add a NO-side entry spread sanity filter: (no_ask - no_bid) <= S_max
- Compare unfiltered vs filtered summaries

Run:
  python taker_ev_down_spread_filter.py --metrics ..\\data\\metrics.jsonl --clob ..\\data\\polymarket_clob.jsonl --out ..\\data --k 4.0 --W 60 --taus 1000,2000 --Smax 0.02
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import List

import polars as pl


FV_TYPE = "fv_vol_1s"
BOOK1S_TYPE = "book_1s"
CLOB_BOOK_TYPE = "book"


@dataclass(frozen=True)
class Params:
    W_sec: int = 60
    k: float = 4.0
    taus_ms: tuple[int, ...] = (1000, 2000)
    entry_slip_max_ms: int = 500
    exit_slip_max_ms: int = 500
    warmup_reason: str = "warmup"
    tte_min_s: float = 60.0
    smax: float = 0.02  # NO spread max at entry


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

    W = p.W_sec
    fv_s = (
        fv_s.with_columns(
            [
                pl.col("price").log().diff().alias("log_ret"),
            ]
        )
        .with_columns(
            [
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


def summarize(df: pl.DataFrame, by_cols: list[str]) -> pl.DataFrame:
    return (
        df.group_by(by_cols)
        .agg(
            [
                pl.len().alias("n"),
                pl.col("pnl").mean().alias("mean_pnl"),
                pl.col("pnl").median().alias("median_pnl"),
                (pl.col("pnl") > 0).mean().alias("winrate"),
                pl.col("pnl").quantile(0.10, interpolation="nearest").alias("p10"),
                pl.col("pnl").quantile(0.90, interpolation="nearest").alias("p90"),
            ]
        )
        .sort(by_cols)
    )


def main() -> None:
    # ap = argparse.ArgumentParser()
    # ap.add_argument("--metrics", required=True)
    # ap.add_argument("--clob", required=True)
    # ap.add_argument("--out", default="./out")
    #
    # ap.add_argument("--k", type=float, default=4.0)
    # ap.add_argument("--W", type=int, default=60)
    # ap.add_argument("--taus", type=str, default="1000,2000")
    # ap.add_argument("--entry_slip", type=int, default=500)
    # ap.add_argument("--exit_slip", type=int, default=500)
    # ap.add_argument("--tte_min", type=float, default=60.0)
    # ap.add_argument("--Smax", type=float, default=0.02)
    #
    # args = ap.parse_args()

    date_str = '20260202_082832'
    # date_str = '20260202_183354'
    args = argparse.Namespace(
        metrics=fr'..\logs\run_{date_str}\metrics.jsonl',
        clob=fr'..\logs\run_{date_str}\polymarket_clob.jsonl',
        out=r'..\data',
        k=4.0,
        W=60,
        taus="1000,2000",
        entry_slip=500,
        exit_slip=500,
        tte_min=60.0,
        Smax=0.02
    )

    taus = tuple(int(x.strip()) for x in args.taus.split(",") if x.strip())
    p = Params(
        W_sec=args.W,
        k=args.k,
        taus_ms=taus,
        entry_slip_max_ms=args.entry_slip,
        exit_slip_max_ms=args.exit_slip,
        tte_min_s=args.tte_min,
        smax=args.Smax,
    )

    ensure_out_dir(args.out)

    metrics = read_jsonl(args.metrics)
    clob = read_jsonl(args.clob)

    fv = (
        metrics.filter(pl.col("type") == FV_TYPE)
        .with_columns(
            [
                pl.col("trade_ms").cast(pl.Int64, strict=False),
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

    b1_gate_all = (
        metrics.filter(pl.col("type") == BOOK1S_TYPE)
        .with_columns(
            [
                pl.col("ts_local_ms").cast(pl.Int64, strict=False),
                pl.col("market_slug").cast(pl.Utf8, strict=False),
            ]
        )
        .select(
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

    cb_all = (
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
        .sort(["market_slug", "ts_local_ms"])
    )

    # overlap slugs
    overlap = sorted(
        list(
            set(fv.select("market_slug").unique().to_series().to_list())
            & set(cb_all.select("market_slug").unique().to_series().to_list())
        )
    )
    if not overlap:
        raise RuntimeError("No slug overlap between fv_vol_1s and clob.book")

    print(f"Overlap slugs: {len(overlap)}")
    print(f"Params: k={p.k} W={p.W_sec}s taus={p.taus_ms} tte_min={p.tte_min_s}s Smax={p.smax}")

    trades_all: List[pl.DataFrame] = []

    for slug in overlap:
        fv_s = fv.filter(pl.col("market_slug") == slug)
        if fv_s.height < p.W_sec + 2:
            continue

        fv_s = warmup_trim_per_slug(fv_s, p.warmup_reason)
        if fv_s.height < p.W_sec + 2:
            continue

        impulses = compute_impulses_for_slug(fv_s, p, slug)
        if impulses.height == 0:
            continue

        # focus only on DOWN impulses (dir=-1) and tte>=min
        impulses = impulses.filter((pl.col("dir") == -1) & (pl.col("tte_s") >= p.tte_min_s))
        if impulses.height == 0:
            continue

        # regime gate
        impulses = impulses.sort(["market_slug", "t0_ms"])
        impulses = impulses.join_asof(
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
        if impulses.height == 0:
            continue

        # entry join
        entry = impulses.join_asof(
            cb_all,
            left_on="t0_ms",
            right_on="ts_local_ms",
            by="market_slug",
            strategy="forward",
            suffix="_entry",
        ).with_columns(
            [
                (pl.col("ts_local_ms_entry") - pl.col("t0_ms")).alias("entry_slip_ms"),
                (pl.col("no_ask") - pl.col("no_bid")).alias("no_spread_entry"),
                pl.col("no_ask").alias("entry_px"),  # for dir=-1 we always buy NO at ask
            ]
        )

        # gate entry slip and nulls
        entry = entry.filter(
            (pl.col("entry_slip_ms") >= 0)
            & (pl.col("entry_slip_ms") <= p.entry_slip_max_ms)
            & pl.col("entry_px").is_not_null()
            & (pl.col("entry_px") > 0)
            & pl.col("no_bid").is_not_null()
            & (pl.col("no_bid") > 0)
            & pl.col("no_spread_entry").is_not_null()
        )
        if entry.height == 0:
            continue

        # build exits for each tau, concat
        for tau in p.taus_ms:
            t1 = entry.with_columns((pl.col("t0_ms") + pl.lit(tau)).alias("t1_ms"))

            ex = t1.join_asof(
                cb_all,
                left_on="t1_ms",
                right_on="ts_local_ms",
                by="market_slug",
                strategy="forward",
                suffix="_exit",
            ).with_columns(
                [
                    (pl.col("ts_local_ms_exit") - pl.col("t1_ms")).alias("exit_slip_ms"),
                    pl.col("no_bid_exit").alias("exit_px"),  # dir=-1: sell NO at bid
                    pl.lit(tau).alias("tau_ms"),
                ]
            )

            ex = ex.filter(
                (pl.col("exit_slip_ms") >= 0)
                & (pl.col("exit_slip_ms") <= p.exit_slip_max_ms)
                & pl.col("exit_px").is_not_null()
                & (pl.col("exit_px") > 0)
            )

            if ex.height == 0:
                continue

            ex = ex.with_columns((pl.col("exit_px") - pl.col("entry_px")).alias("pnl"))

            trades_all.append(
                ex.select(
                    [
                        "market_slug",
                        "t0_ms",
                        "t1_ms",
                        "tau_ms",
                        "tte_s",
                        "z",
                        "entry_px",
                        "exit_px",
                        "pnl",
                        "entry_slip_ms",
                        "exit_slip_ms",
                        "no_spread_entry",
                        "no_bid",
                        "no_ask",
                    ]
                )
            )

    if not trades_all:
        print("No trades produced.")
        return

    trades = pl.concat(trades_all, how="vertical")

    # Unfiltered summary (baseline)
    summary_unf = summarize(trades, ["tau_ms"])
    summary_unf.write_csv(os.path.join(args.out, "summary_down_unfiltered.csv"))

    # Filtered by entry NO spread
    trades_f = trades.filter(pl.col("no_spread_entry") <= p.smax)
    summary_f = summarize(trades_f, ["tau_ms"])
    summary_f.write_csv(os.path.join(args.out, "summary_down_filtered.csv"))

    trades_f.write_csv(os.path.join(args.out, "trades_down_filtered.csv"))

    print("\n✅ Done.")
    print(f"Wrote: {os.path.join(args.out, 'summary_down_unfiltered.csv')}")
    print(f"Wrote: {os.path.join(args.out, 'summary_down_filtered.csv')}")
    print(f"Wrote: {os.path.join(args.out, 'trades_down_filtered.csv')}")


if __name__ == "__main__":
    main()
