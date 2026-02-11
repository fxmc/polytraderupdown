"""
taker_ev_down_volsplit.py

One job (minimal, decisive):
- Use the SAME pipeline you already use (DOWN only, tte>=60, regime gated, CLOB execution)
- Compute trades for taus (default: 1000,2000)
- Split by impulse strength z_bucket (weak/strong)
- Split by driver volatility v_drive bucket (low/high) computed on the *impulse row*
- Output: summary_down_z_vol_bucket.csv

Run:
  python taker_ev_down_volsplit.py ^
    --metrics ..\data\metrics.jsonl ^
    --clob ..\data\polymarket_clob.jsonl ^
    --out ..\data ^
    --k 4.0 --W 60 --taus 1000,2000 --tte_min 60 --Smax 999 --vol_field v_drive --vol_split median
Notes:
- Smax is optional; set huge to effectively disable spread filtering (as above).
- vol_field can be v_drive, v_fast, sigma_rem_pct, etc (must exist in fv_vol_1s rows).
- vol_split: median (default) or p75 or fixed:<value> (e.g. fixed:0.35)
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
    taus_ms: tuple[int, ...] = (1000, 2000)
    entry_slip_max_ms: int = 500
    exit_slip_max_ms: int = 500
    warmup_reason: str = "warmup"
    tte_min_s: float = 60.0
    smax: float = 999.0
    vol_field: str = "v_drive"
    vol_split: str = "median"  # "median" | "p75" | "fixed:<value>"


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


def add_z_bucket(df: pl.DataFrame, k: float) -> pl.DataFrame:
    return df.with_columns(
        pl.when((pl.col("z") >= k) & (pl.col("z") < (k + 1.0)))
        .then(pl.lit("weak"))
        .when(pl.col("z") >= (k + 1.0))
        .then(pl.lit("strong"))
        .otherwise(pl.lit("other"))
        .alias("z_bucket")
    )


def parse_vol_threshold(vol_split: str, values: pl.Series) -> float:
    s = vol_split.strip().lower()
    if s == "median":
        return float(values.median())
    if s == "p75":
        return float(values.quantile(0.75, interpolation="nearest"))
    if s.startswith("fixed:"):
        return float(s.split(":", 1)[1])
    raise ValueError("vol_split must be median|p75|fixed:<value>")


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
    # ap.add_argument("--Smax", type=float, default=999.0)
    #
    # ap.add_argument("--vol_field", type=str, default="v_drive")
    # ap.add_argument("--vol_split", type=str, default="median")
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
        Smax=999.0,
        vol_field="v_drive",
        vol_split="median"
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
        vol_field=args.vol_field,
        vol_split=args.vol_split,
    )

    ensure_out_dir(args.out)

    metrics = read_jsonl(args.metrics)
    clob = read_jsonl(args.clob)

    # fv rows
    fv = (
        metrics.filter(pl.col("type") == FV_TYPE)
        .with_columns(
            [
                pl.col("trade_ms").cast(pl.Int64, strict=False),
                pl.col("price").cast(pl.Float64, strict=False),
                pl.col("tte_s").cast(pl.Float64, strict=False),
                pl.col("market_slug").cast(pl.Utf8, strict=False),
                pl.col("fv_reason").cast(pl.Utf8, strict=False),
                pl.col("fv_ok").cast(pl.Boolean, strict=False),
                pl.col(p.vol_field).cast(pl.Float64, strict=False).alias(p.vol_field),
            ]
        )
        .filter(pl.col("market_slug") != "")
    )

    # regime gate
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

    # clob book
    cb_all = (
        clob.filter(pl.col("type") == CLOB_BOOK_TYPE)
        .with_columns(
            [
                pl.col("ts_local_ms").cast(pl.Int64, strict=False),
                pl.col("market_slug").cast(pl.Utf8, strict=False),
                pl.col("l1").struct.field("no_bid").cast(pl.Float64, strict=False).alias("no_bid"),
                pl.col("l1").struct.field("no_ask").cast(pl.Float64, strict=False).alias("no_ask"),
            ]
        )
        .select(["market_slug", "ts_local_ms", "no_bid", "no_ask"])
        .sort(["market_slug", "ts_local_ms"])
    )

    overlap = sorted(
        list(
            set(fv.select("market_slug").unique().to_series().to_list())
            & set(cb_all.select("market_slug").unique().to_series().to_list())
        )
    )
    if not overlap:
        raise RuntimeError("No slug overlap between fv_vol_1s and clob.book")

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

        # DOWN only + away from expiry
        impulses = impulses.filter((pl.col("dir") == -1) & (pl.col("tte_s") >= p.tte_min_s))
        if impulses.height == 0:
            continue

        # Regime gate
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

        # Entry (forward)
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
                pl.col("no_ask").alias("entry_px"),
            ]
        )

        entry = entry.filter(
            (pl.col("entry_slip_ms") >= 0)
            & (pl.col("entry_slip_ms") <= p.entry_slip_max_ms)
            & pl.col("entry_px").is_not_null()
            & (pl.col("entry_px") > 0)
            & pl.col("no_bid").is_not_null()
            & (pl.col("no_bid") > 0)
            & pl.col("no_spread_entry").is_not_null()
            & (pl.col("no_spread_entry") <= p.smax)
        )
        if entry.height == 0:
            continue

        # exits per tau
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
                    pl.col("no_bid_exit").alias("exit_px"),
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
                        "tau_ms",
                        "tte_s",
                        "z",
                        p.vol_field,
                        "pnl",
                    ]
                )
            )

    if not trades_all:
        print("No trades produced.")
        return

    trades = pl.concat(trades_all, how="vertical")
    trades = add_z_bucket(trades, p.k).filter(pl.col("z_bucket").is_in(["weak", "strong"]))

    # Determine vol threshold across all trades (impulse-time vol)
    vol_values = trades.select(pl.col(p.vol_field)).to_series().drop_nulls()
    if len(vol_values) == 0:
        raise RuntimeError(f"No non-null values for vol_field={p.vol_field}")

    thr = parse_vol_threshold(p.vol_split, vol_values)
    print(f"Vol threshold ({p.vol_split}) for {p.vol_field}: {thr}")

    trades = trades.with_columns(
        pl.when(pl.col(p.vol_field) >= pl.lit(thr))
        .then(pl.lit("high"))
        .otherwise(pl.lit("low"))
        .alias("vol_bucket")
    )

    summary = (
        trades.group_by(["tau_ms", "z_bucket", "vol_bucket"])
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
        .sort(["tau_ms", "z_bucket", "vol_bucket"])
    )

    out_path = os.path.join(args.out, "summary_down_z_vol_bucket.csv")
    summary.write_csv(out_path)
    print(f"✅ Wrote: {out_path}")


if __name__ == "__main__":
    main()
