"""
maker_ev_regime_min.py

Minimal Maker EV Tier-1 (fill-optimistic) conditioned on taker EV sign.

- Impulses from metrics fv_vol_1s.price returns (per slug window).
- Regime gating from metrics book_1s.regime.*
- Execution prices from polymarket_clob type=book l1.no_{bid,ask}
- Focus: DOWN impulses only (dir=-1), tte_s >= 60
- Taker regime signal: rolling mean of taker_pnl at tau_ref (default 1000ms)
- Maker pnl: sell NO at ask at t0, buy back at bid at t0+tau

Outputs:
  - impulses_with_regime.csv
  - summary_maker_by_regime.csv
  - trades_maker_by_regime.csv

Run:
  python maker_ev_regime_min.py --metrics ..\\data\\metrics.jsonl --clob ..\\data\\polymarket_clob.jsonl --out ..\\data
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
    tau_ref_ms: int = 1000
    roll_M: int = 20
    entry_slip_max_ms: int = 500
    exit_slip_max_ms: int = 500
    tte_min_s: float = 60.0
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

    imp = fv_s.filter(pl.col("sigma").is_not_null() & (pl.col("z") > p.k))
    imp = imp.filter((pl.col("t0_ms") >= w0) & (pl.col("t0_ms") < w1))
    return imp


def add_z_bucket(df: pl.DataFrame, k: float) -> pl.DataFrame:
    return df.with_columns(
        pl.when((pl.col("z") >= k) & (pl.col("z") < (k + 1.0)))
        .then(pl.lit("weak"))
        .when(pl.col("z") >= (k + 1.0))
        .then(pl.lit("strong"))
        .otherwise(pl.lit("other"))
        .alias("z_bucket")
    )


def summarize(df: pl.DataFrame, by_cols: list[str], pnl_col: str) -> pl.DataFrame:
    return (
        df.group_by(by_cols)
        .agg(
            [
                pl.len().alias("n"),
                pl.col(pnl_col).mean().alias("mean_pnl"),
                pl.col(pnl_col).median().alias("median_pnl"),
                (pl.col(pnl_col) > 0).mean().alias("winrate"),
                pl.col(pnl_col).quantile(0.10, interpolation="nearest").alias("p10"),
                pl.col(pnl_col).quantile(0.90, interpolation="nearest").alias("p90"),
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
    # ap.add_argument("--tau_ref", type=int, default=1000)
    # ap.add_argument("--roll_M", type=int, default=20)
    # ap.add_argument("--entry_slip", type=int, default=500)
    # ap.add_argument("--exit_slip", type=int, default=500)
    # ap.add_argument("--tte_min", type=float, default=60.0)
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
        tau_ref=1000,
        roll_M=20,
        entry_slip=500,
        exit_slip=500,
        tte_min=60.0,
    )

    taus = tuple(int(x.strip()) for x in args.taus.split(",") if x.strip())
    p = Params(
        W_sec=args.W,
        k=args.k,
        taus_ms=taus,
        tau_ref_ms=args.tau_ref,
        roll_M=args.roll_M,
        entry_slip_max_ms=args.entry_slip,
        exit_slip_max_ms=args.exit_slip,
        tte_min_s=args.tte_min,
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

    cb = (
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
            & set(cb.select("market_slug").unique().to_series().to_list())
        )
    )
    if not overlap:
        raise RuntimeError("No slug overlap between fv_vol_1s and clob.book")

    impulses_all: List[pl.DataFrame] = []

    for slug in overlap:
        fv_s = fv.filter(pl.col("market_slug") == slug)
        if fv_s.height < p.W_sec + 2:
            continue

        fv_s = warmup_trim_per_slug(fv_s, p.warmup_reason)
        if fv_s.height < p.W_sec + 2:
            continue

        imp = compute_impulses_for_slug(fv_s, p, slug)
        if imp.height == 0:
            continue

        # DOWN only + away from expiry
        imp = imp.filter((pl.col("dir") == -1) & (pl.col("tte_s") >= p.tte_min_s))
        if imp.height == 0:
            continue

        # Regime gate at t0
        imp = imp.sort(["market_slug", "t0_ms"])
        imp = (
            imp.join_asof(
                b1_gate_all,
                left_on="t0_ms",
                right_on="ts_local_ms",
                by="market_slug",
                strategy="backward",
            )
            .filter(
                (pl.col("has_book") == True)
                & (pl.col("is_two_sided") == True)
                & (pl.col("is_pinned") == False)
                & (pl.col("no_trade_zone") == False)
            )
        )
        if imp.height == 0:
            continue

        # Entry prices at t0 (forward CLOB)
        imp = (
            imp.join_asof(
                cb,
                left_on="t0_ms",
                right_on="ts_local_ms",
                by="market_slug",
                strategy="forward",
                suffix="_entry",
            )
            .with_columns(
                [
                    (pl.col("ts_local_ms_entry") - pl.col("t0_ms")).alias("entry_slip_ms"),
                    pl.col("no_ask").alias("entry_no_ask"),
                    pl.col("no_bid").alias("entry_no_bid"),
                ]
            )
            .filter(
                (pl.col("entry_slip_ms") >= 0)
                & (pl.col("entry_slip_ms") <= p.entry_slip_max_ms)
                & pl.col("entry_no_ask").is_not_null()
                & pl.col("entry_no_bid").is_not_null()
            )
        )
        if imp.height == 0:
            continue

        impulses_all.append(
            imp.select(
                [
                    "market_slug",
                    "t0_ms",
                    "tte_s",
                    "z",
                    "entry_no_ask",
                    "entry_no_bid",
                ]
            )
        )

    if not impulses_all:
        print("No impulses survived gates.")
        return

    impulses = pl.concat(impulses_all, how="vertical").sort("t0_ms")
    impulses = add_z_bucket(impulses, p.k).filter(pl.col("z_bucket").is_in(["weak", "strong"]))

    # --- TAKER regime signal (tau_ref): buy NO @ ask, sell @ bid after tau_ref
    impulses_ref = impulses.with_columns((pl.col("t0_ms") + pl.lit(p.tau_ref_ms)).alias("t1_ref_ms"))

    ref_exit = (
        impulses_ref.join_asof(
            cb,
            left_on="t1_ref_ms",
            right_on="ts_local_ms",
            by="market_slug",
            strategy="forward",
        )
        .with_columns(
            [
                pl.col("ts_local_ms").alias("exit_ref_ts_ms"),
                pl.col("no_bid").alias("exit_ref_no_bid"),
            ]
        )
        .with_columns(
            [
                (pl.col("exit_ref_ts_ms") - pl.col("t1_ref_ms")).alias("exit_ref_slip_ms"),
            ]
        )
        .filter(
            (pl.col("exit_ref_slip_ms") >= 0)
            & (pl.col("exit_ref_slip_ms") <= p.exit_slip_max_ms)
            & pl.col("exit_ref_no_bid").is_not_null()
        )
        .with_columns(
            [
                (pl.col("exit_ref_no_bid") - pl.col("entry_no_ask")).alias("taker_pnl_ref"),
            ]
        )
        .sort("t0_ms")
    )

    # rolling regime on taker pnl (global)
    ref_exit = ref_exit.with_columns(
        pl.col("taker_pnl_ref")
        .rolling_mean(window_size=p.roll_M, min_samples=p.roll_M)
        .alias("taker_pnl_ref_roll")
    ).with_columns(
        pl.when(pl.col("taker_pnl_ref_roll").is_null())
        .then(pl.lit("UNK"))
        .when(pl.col("taker_pnl_ref_roll") < 0)
        .then(pl.lit("NEG"))
        .otherwise(pl.lit("POS"))
        .alias("taker_regime")
    )

    # save impulses with regime label (debuggable contract)
    ref_exit.select(
        [
            "market_slug",
            "t0_ms",
            "tte_s",
            "z",
            "z_bucket",
            "taker_pnl_ref",
            "taker_pnl_ref_roll",
            "taker_regime",
        ]
    ).write_csv(os.path.join(args.out, "impulses_with_regime.csv"))

    # --- MAKER Tier-1 trades: sell NO @ ask, buy back @ bid after tau
    maker_trades: List[pl.DataFrame] = []

    for tau in p.taus_ms:
        t = ref_exit.with_columns((pl.col("t0_ms") + pl.lit(tau)).alias("t1_ms")).with_columns(
            pl.lit(tau).alias("tau_ms")
        )

        ex = (
            t.join_asof(
                cb,
                left_on="t1_ms",
                right_on="ts_local_ms",
                by="market_slug",
                strategy="forward",
                suffix="_exit",
            )
            .with_columns(
                [
                    (pl.col("ts_local_ms_exit") - pl.col("t1_ms")).alias("exit_slip_ms"),
                    pl.col("no_bid_exit").alias("exit_no_bid"),
                ]
            )
            .filter(
                (pl.col("exit_slip_ms") >= 0)
                & (pl.col("exit_slip_ms") <= p.exit_slip_max_ms)
                & pl.col("exit_no_bid").is_not_null()
            )
            .with_columns(
                [
                    # maker sells at ask, buys back at bid
                    (pl.col("entry_no_ask") - pl.col("exit_no_bid")).alias("maker_pnl"),
                ]
            )
            .select(
                [
                    "market_slug",
                    "t0_ms",
                    "tte_s",
                    "z",
                    "z_bucket",
                    "taker_regime",
                    "tau_ms",
                    "maker_pnl",
                    "entry_no_ask",
                    "exit_no_bid",
                ]
            )
        )

        if ex.height:
            maker_trades.append(ex)

    if not maker_trades:
        print("No maker trades produced.")
        return

    maker = pl.concat(maker_trades, how="vertical")

    maker.write_csv(os.path.join(args.out, "trades_maker_by_regime.csv"))

    # Summary: maker pnl by taker regime (and z bucket for extra resolution)
    summary = summarize(maker, ["tau_ms", "taker_regime", "z_bucket"], "maker_pnl")
    summary.write_csv(os.path.join(args.out, "summary_maker_by_regime.csv"))

    print("✅ Done.")
    print(f"Wrote: {os.path.join(args.out, 'impulses_with_regime.csv')}")
    print(f"Wrote: {os.path.join(args.out, 'trades_maker_by_regime.csv')}")
    print(f"Wrote: {os.path.join(args.out, 'summary_maker_by_regime.csv')}")


if __name__ == "__main__":
    main()
