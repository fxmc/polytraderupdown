# r"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_20260201_221712\metrics.jsonl"
# check_metrics.py
# Usage:
#   python check_metrics.py path\to\metrics.jsonl

# check_metrics.py
# Usage:
#   python check_metrics.py path\to\metrics.jsonl

import sys
import polars as pl


def pct_true(expr: pl.Expr) -> pl.Expr:
    # mean(bool) works; cast to Int8 for explicitness
    return (expr.cast(pl.Int8).mean() * 100.0).alias("pct")


def main(path: str) -> None:
    # For large jsonl, always use scan_ndjson (LazyFrame) + collect at the end.
    lf = pl.scan_ndjson(path)

    # ---- book_1s micro-tests ----
    book = lf.filter(pl.col("type") == "book_1s")

    # NOTE: these are nested struct fields
    no_trade = pl.col("regime").struct.field("no_trade_zone")
    has_book = pl.col("regime").struct.field("has_book")
    canon_mid = pl.col("canon").struct.field("mid")

    out = book.select([
        pct_true(no_trade).alias("no_trade_zone_true_pct"),
        pct_true(canon_mid.is_null()).alias("canon_mid_null_pct"),
        pl.len().alias("n_book_rows"),
    ]).collect()

    print(f"% no_trade_zone true (book_1s): {out['no_trade_zone_true_pct'][0]:.2f}%")
    print(f"% canon.mid null     (book_1s): {out['canon_mid_null_pct'][0]:.2f}%")

    out2 = (
        book
        .filter(has_book == True)
        .select([
            pct_true(no_trade).alias("no_trade_zone_true_pct"),
            pct_true(canon_mid.is_null()).alias("canon_mid_null_pct"),
            pl.len().alias("n_book_rows"),
        ])
        .collect()
    )

    print(f"% no_trade_zone true | has_book==true: {out2['no_trade_zone_true_pct'][0]:.2f}%")
    print(f"% canon.mid null     | has_book==true: {out2['canon_mid_null_pct'][0]:.2f}%")

    # ---- fv_vol_1s micro-test ----
    fv = lf.filter(pl.col("type") == "fv_vol_1s")
    fv_ok = pl.col("fv_ok")

    out3 = fv.select([
        pct_true(fv_ok == False).alias("fv_ok_false_pct"),
        pl.len().alias("n_fv_rows"),
    ]).collect()

    print(f"% fv_ok false        (fv_vol_1s): {out3['fv_ok_false_pct'][0]:.2f}%")

    # ---- Market breakdown (book_1s) ----
    by_mkt = (
        book
        .group_by("market_id")
        .agg([
            (no_trade.cast(pl.Int8).mean() * 100).alias("no_trade_pct"),
            (canon_mid.is_null().cast(pl.Int8).mean() * 100).alias("mid_null_pct"),
            pl.len().alias("n_rows"),
            pl.col("market_slug").drop_nulls().first().alias("market_slug"),
        ])
        .sort("n_rows", descending=True)
        .collect()
    )

    print(by_mkt.head(10))

    # ---- “Dodgy row” trap: one-sided-ish row where canon.mid is null but no_trade_zone is false ----
    # (This is exactly the kind of line you pasted.)
    dodgy = (
        book
        .filter(
            (has_book == True)
            & canon_mid.is_null()
            & (no_trade == False)
        )
        .select([
            "run_id", "seq", "ts_local_ms", "market_slug", "market_id",
            pl.col("clob_l1"),
            pl.col("regime"),
            pl.col("canon"),
        ])
        .head(25)
        .collect()
    )

    if dodgy.height == 0:
        print("dodgy rows (has_book && mid_null && !no_trade_zone): none ✅")
    else:
        print("dodgy rows (has_book && mid_null && !no_trade_zone):")
        print(dodgy)


if __name__ == "__main__":
    main(r"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_20260201_230555\metrics.jsonl")
