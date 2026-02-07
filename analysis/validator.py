"""
validate_inputs.py — Polymarket EV Lab v3.0
Polars-based, crash-first validator for:
  - metrics.jsonl (fv_vol_1s + book_1s)
  - polymarket_clob.jsonl (book)

Design:
  * No schema guessing: require explicit column paths.
  * Crash-first: any missing required field/dtype => raise with context.
  * Nulls in execution L1 are allowed (drop later), but the fields must exist.
  * Prints per-stream schema and per-slug diagnostics; optionally writes schema to ./out/.

Run:
  python validate_inputs.py --metrics ./metrics.jsonl --clob ./polymarket_clob.jsonl --out ./out
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

import polars as pl


# -----------------------------
# Config / Truth Table
# -----------------------------

FV_TYPE = "fv_vol_1s"
BOOK1S_TYPE = "book_1s"
CLOB_BOOK_TYPE = "book"

REQUIRED_FV_TOPLEVEL: Tuple[str, ...] = (
    "type",
    "trade_ms",
    "ts_local_ms",
    "symbol",
    "price",
    "market_slug",
    "fv_ok",
    "fv_reason",
    "tte_s",
)

REQUIRED_BOOK1S_TOPLEVEL: Tuple[str, ...] = (
    "type",
    "ts_local_ms",
    "market_slug",
    "regime",  # struct with required fields below
)

REQUIRED_BOOK1S_REGIME_FIELDS: Tuple[str, ...] = (
    "has_book",
    "is_two_sided",
    "is_pinned",
    "no_trade_zone",
)

REQUIRED_CLOB_TOPLEVEL: Tuple[str, ...] = (
    "type",
    "ts_local_ms",
    "market_slug",
    "l1",  # struct with required fields below
)

REQUIRED_CLOB_L1_FIELDS: Tuple[str, ...] = (
    "yes_bid",
    "yes_ask",
    "no_bid",
    "no_ask",
)


# -----------------------------
# Errors
# -----------------------------

class ValidationError(RuntimeError):
    pass


def _fail(msg: str) -> None:
    raise ValidationError(msg)


# -----------------------------
# IO helpers
# -----------------------------

def read_jsonl(path: str) -> pl.DataFrame:
    if not os.path.exists(path):
        _fail(f"File not found: {path}")
    # Polars json reader handles JSON lines.
    # infer_schema_length controls how many rows are scanned for schema.
    return pl.read_ndjson(path, infer_schema_length=20_000)


def ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def write_schema(df: pl.DataFrame, out_path: str) -> None:
    schema_dict = {k: str(v) for k, v in df.schema.items()}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(schema_dict, f, indent=2, sort_keys=True)


# -----------------------------
# Schema / field asserts
# -----------------------------

def assert_cols_exist(df: pl.DataFrame, cols: Sequence[str], ctx: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        _fail(f"[{ctx}] Missing required top-level columns: {missing}")


def assert_struct_fields_exist(
    df: pl.DataFrame,
    struct_col: str,
    fields: Sequence[str],
    ctx: str,
) -> None:
    if struct_col not in df.columns:
        _fail(f"[{ctx}] Missing required struct column: {struct_col}")

    dtype = df.schema.get(struct_col)
    if not isinstance(dtype, pl.Struct):
        _fail(f"[{ctx}] Column '{struct_col}' must be Struct, got: {dtype}")

    struct_fields = {f.name for f in dtype.fields}
    missing = [f for f in fields if f not in struct_fields]
    if missing:
        _fail(f"[{ctx}] Struct '{struct_col}' missing fields: {missing}. Has: {sorted(struct_fields)}")


def assert_castable_int64(df: pl.DataFrame, col: str, ctx: str) -> None:
    try:
        _ = df.select(pl.col(col).cast(pl.Int64, strict=False))
    except Exception as e:
        _fail(f"[{ctx}] Column '{col}' is not castable to Int64: {e}")


def assert_castable_float64(df: pl.DataFrame, col: str, ctx: str) -> None:
    try:
        _ = df.select(pl.col(col).cast(pl.Float64, strict=False))
    except Exception as e:
        _fail(f"[{ctx}] Column '{col}' is not castable to Float64: {e}")


def assert_boolish(df: pl.DataFrame, col: str, ctx: str) -> None:
    # Accept bool or 0/1 or "true"/"false" if castable.
    try:
        _ = df.select(pl.col(col).cast(pl.Boolean, strict=False))
    except Exception as e:
        _fail(f"[{ctx}] Column '{col}' is not castable to Boolean: {e}")


def assert_stringish(df: pl.DataFrame, col: str, ctx: str) -> None:
    try:
        _ = df.select(pl.col(col).cast(pl.Utf8, strict=False))
    except Exception as e:
        _fail(f"[{ctx}] Column '{col}' is not castable to Utf8: {e}")


# -----------------------------
# Slug parsing / window checks
# -----------------------------

def parse_window_start_s_from_slug(slug: str) -> int:
    # Required format: btc-updown-15m-<WINDOW_START_S>
    try:
        suffix = slug.rsplit("-", 1)[-1]
        return int(suffix)
    except Exception as e:
        _fail(f"[slug] Cannot parse window start seconds from slug '{slug}': {e}")
    return 0  # unreachable


def add_window_bounds(df: pl.DataFrame, slug_col: str = "market_slug") -> pl.DataFrame:
    # Adds window_start_ms and window_end_ms based on slug suffix.
    # Crash if any slug is unparsable.
    slugs = df.select(pl.col(slug_col)).unique().to_series().to_list()
    # Validate parse for all slugs (crash-first).
    _ = [parse_window_start_s_from_slug(s) for s in slugs]

    # Now add via expression (split by last '-')
    return df.with_columns(
        pl.col(slug_col)
        .str.split_exact("-", 3)  # not reliable if slug contains more hyphens; we use rsplit below
    ).drop(slug_col)  # placeholder: we'll implement robustly below


def add_window_bounds_robust(df: pl.DataFrame, slug_col: str = "market_slug") -> pl.DataFrame:
    # Robustly parse last '-' segment as seconds using regex capture.
    # Still crash-first via explicit check above.
    slugs = df.select(pl.col(slug_col)).unique().to_series().to_list()
    _ = [parse_window_start_s_from_slug(s) for s in slugs]

    # Use regex to capture trailing digits.
    # If no match, result is null (but we already ensured parseability).
    start_s = pl.col(slug_col).str.extract(r"(\d+)$", 1).cast(pl.Int64)
    start_ms = (start_s * 1000).alias("window_start_ms")
    end_ms = (start_ms + pl.lit(900_000)).alias("window_end_ms")
    return df.with_columns([start_ms, end_ms])


# -----------------------------
# Stream-specific validation
# -----------------------------

@dataclass(frozen=True)
class ValidateConfig:
    warmup_reason: str = "warmup"
    write_schema: bool = True
    write_diagnostics: bool = True


def validate_metrics_fv(df: pl.DataFrame) -> pl.DataFrame:
    ctx = "metrics:fv_vol_1s"
    assert_cols_exist(df, REQUIRED_FV_TOPLEVEL, ctx)

    # Type checks (castability, not strict typing)
    assert_stringish(df, "type", ctx)
    assert_castable_int64(df, "trade_ms", ctx)
    assert_castable_int64(df, "ts_local_ms", ctx)
    assert_stringish(df, "symbol", ctx)
    assert_castable_float64(df, "price", ctx)
    assert_stringish(df, "market_slug", ctx)
    assert_boolish(df, "fv_ok", ctx)
    assert_stringish(df, "fv_reason", ctx)
    assert_castable_float64(df, "tte_s", ctx)

    # Filter to correct type and crash if empty
    fv = df.filter(pl.col("type") == FV_TYPE)
    if fv.height == 0:
        _fail(f"[{ctx}] No rows with type='{FV_TYPE}'")

    # Canonicalize dtypes (non-strict; nulls allowed, but we will report)
    fv = fv.with_columns(
        [
            pl.col("trade_ms").cast(pl.Int64, strict=False),
            pl.col("ts_local_ms").cast(pl.Int64, strict=False),
            pl.col("price").cast(pl.Float64, strict=False),
            pl.col("tte_s").cast(pl.Float64, strict=False),
            pl.col("market_slug").cast(pl.Utf8, strict=False),
            pl.col("symbol").cast(pl.Utf8, strict=False),
            pl.col("fv_reason").cast(pl.Utf8, strict=False),
            pl.col("fv_ok").cast(pl.Boolean, strict=False),
        ]
    )

    # Slug parsing sanity (crash-first)
    _ = [parse_window_start_s_from_slug(s) for s in fv.select("market_slug").unique().to_series().to_list()]

    return fv


def validate_metrics_book1s(df: pl.DataFrame) -> pl.DataFrame:
    ctx = "metrics:book_1s"
    assert_cols_exist(df, REQUIRED_BOOK1S_TOPLEVEL, ctx)
    assert_struct_fields_exist(df, "regime", REQUIRED_BOOK1S_REGIME_FIELDS, ctx)

    assert_stringish(df, "type", ctx)
    assert_castable_int64(df, "ts_local_ms", ctx)
    assert_stringish(df, "market_slug", ctx)

    b = df.filter(pl.col("type") == BOOK1S_TYPE)
    if b.height == 0:
        _fail(f"[{ctx}] No rows with type='{BOOK1S_TYPE}'")

    b = b.with_columns(
        [
            pl.col("ts_local_ms").cast(pl.Int64, strict=False),
            pl.col("market_slug").cast(pl.Utf8, strict=False),
        ]
    )

    # Ensure regime fields are boolean-castable
    for f in REQUIRED_BOOK1S_REGIME_FIELDS:
        assert_boolish(b.select(pl.col("regime").struct.field(f).alias(f)), f, f"{ctx}:regime.{f}")

    # --- Quarantine non-tradable / unscoped rows (allowed to exist, but must not poison slug invariants)
    has_book = pl.col("regime").struct.field("has_book").cast(pl.Boolean, strict=False)

    b_tradable = b.filter(has_book & (pl.col("market_slug") != ""))
    n_bad = b.height - b_tradable.height
    if n_bad > 0:
        # Don't crash — but make it loud.
        print(f"[{ctx}] WARN: quarantined {n_bad} book_1s rows with has_book==False and/or empty market_slug")

    # Slug parsing sanity
    _ = [parse_window_start_s_from_slug(s) for s in b_tradable.select("market_slug").unique().to_series().to_list()]
    return b


def validate_clob_book(df: pl.DataFrame) -> pl.DataFrame:
    ctx = "clob:book"
    assert_cols_exist(df, REQUIRED_CLOB_TOPLEVEL, ctx)
    assert_struct_fields_exist(df, "l1", REQUIRED_CLOB_L1_FIELDS, ctx)

    assert_stringish(df, "type", ctx)
    assert_castable_int64(df, "ts_local_ms", ctx)
    assert_stringish(df, "market_slug", ctx)

    c = df.filter(pl.col("type") == CLOB_BOOK_TYPE)
    if c.height == 0:
        _fail(f"[{ctx}] No rows with type='{CLOB_BOOK_TYPE}'")

    c = c.with_columns(
        [
            pl.col("ts_local_ms").cast(pl.Int64, strict=False),
            pl.col("market_slug").cast(pl.Utf8, strict=False),
        ]
    )

    # L1 fields must exist; nulls allowed, but must be float-castable
    for f in REQUIRED_CLOB_L1_FIELDS:
        _ = c.select(pl.col("l1").struct.field(f).cast(pl.Float64, strict=False).alias(f))

    # Slug parsing sanity
    _ = [parse_window_start_s_from_slug(s) for s in c.select("market_slug").unique().to_series().to_list()]
    return c


# -----------------------------
# Diagnostics
# -----------------------------

def null_rate_expr(col: str) -> pl.Expr:
    return (pl.col(col).is_null().mean()).alias(f"null_rate__{col}")


def stream_overview(df: pl.DataFrame, name: str, out_dir: Optional[str]) -> None:
    print(f"\n=== {name} ===")
    print(f"rows: {df.height:,}  cols: {len(df.columns)}")
    # Print a compact schema
    for k, v in df.schema.items():
        print(f"  {k}: {v}")
    if out_dir:
        write_schema(df, os.path.join(out_dir, f"schema__{name}.json"))


def diagnostics_per_slug_fv(fv: pl.DataFrame, out_dir: Optional[str], warmup_reason: str) -> None:
    # Basic per-slug counts and warmup trim boundary location
    diag = (
        fv.group_by("market_slug")
        .agg(
            [
                pl.len().alias("n_fv_rows"),
                pl.col("trade_ms").min().alias("trade_ms_min"),
                pl.col("trade_ms").max().alias("trade_ms_max"),
                pl.col("tte_s").min().alias("tte_s_min"),
                pl.col("tte_s").max().alias("tte_s_max"),
                (pl.col("fv_ok") | (pl.col("fv_reason") != warmup_reason)).sum().alias("n_nonwarmup_rows"),
                null_rate_expr("trade_ms"),
                null_rate_expr("price"),
                null_rate_expr("tte_s"),
                null_rate_expr("fv_ok"),
                null_rate_expr("fv_reason"),
            ]
        )
        .sort("n_fv_rows", descending=True)
    )

    print("\n--- FV per-slug diagnostics (top 20 by rows) ---")
    print(diag.head(20))

    if out_dir:
        diag.write_csv(os.path.join(out_dir, "diag__fv_per_slug.csv"))


def diagnostics_per_slug_book1s(b: pl.DataFrame, out_dir: Optional[str]) -> None:
    # Count and regime true-rates
    diag = (
        b.with_columns(
            [
                pl.col("regime").struct.field("has_book").cast(pl.Int64, strict=False).alias("has_book_i"),
                pl.col("regime").struct.field("is_two_sided").cast(pl.Int64, strict=False).alias("is_two_sided_i"),
                pl.col("regime").struct.field("is_pinned").cast(pl.Int64, strict=False).alias("is_pinned_i"),
                pl.col("regime").struct.field("no_trade_zone").cast(pl.Int64, strict=False).alias("no_trade_zone_i"),
            ]
        )
        .group_by("market_slug")
        .agg(
            [
                pl.len().alias("n_book1s_rows"),
                pl.col("ts_local_ms").min().alias("ts_ms_min"),
                pl.col("ts_local_ms").max().alias("ts_ms_max"),
                pl.mean("has_book_i").alias("rate_has_book"),
                pl.mean("is_two_sided_i").alias("rate_two_sided"),
                pl.mean("is_pinned_i").alias("rate_pinned"),
                pl.mean("no_trade_zone_i").alias("rate_no_trade_zone"),
            ]
        )
        .sort("n_book1s_rows", descending=True)
    )

    print("\n--- book_1s per-slug diagnostics (top 20 by rows) ---")
    print(diag.head(20))

    if out_dir:
        diag.write_csv(os.path.join(out_dir, "diag__book1s_per_slug.csv"))


def diagnostics_per_slug_clob(c: pl.DataFrame, out_dir: Optional[str]) -> None:
    # Null rates for L1 fields
    diag = (
        c.with_columns(
            [
                pl.col("l1").struct.field("yes_bid").cast(pl.Float64, strict=False).alias("yes_bid"),
                pl.col("l1").struct.field("yes_ask").cast(pl.Float64, strict=False).alias("yes_ask"),
                pl.col("l1").struct.field("no_bid").cast(pl.Float64, strict=False).alias("no_bid"),
                pl.col("l1").struct.field("no_ask").cast(pl.Float64, strict=False).alias("no_ask"),
            ]
        )
        .group_by("market_slug")
        .agg(
            [
                pl.len().alias("n_clob_rows"),
                pl.col("ts_local_ms").min().alias("ts_ms_min"),
                pl.col("ts_local_ms").max().alias("ts_ms_max"),
                (pl.col("yes_bid").is_null().mean()).alias("null_yes_bid"),
                (pl.col("yes_ask").is_null().mean()).alias("null_yes_ask"),
                (pl.col("no_bid").is_null().mean()).alias("null_no_bid"),
                (pl.col("no_ask").is_null().mean()).alias("null_no_ask"),
            ]
        )
        .sort("n_clob_rows", descending=True)
    )

    print("\n--- CLOB per-slug diagnostics (top 20 by rows) ---")
    print(diag.head(20))

    if out_dir:
        diag.write_csv(os.path.join(out_dir, "diag__clob_per_slug.csv"))


def diagnostics_cross_stream_slug_overlap(
    fv: pl.DataFrame,
    b: pl.DataFrame,
    c: pl.DataFrame,
    out_dir: Optional[str],
) -> None:
    fv_slugs = set(fv.select("market_slug").unique().to_series().to_list())
    b_slugs = set(b.select("market_slug").unique().to_series().to_list())
    c_slugs = set(c.select("market_slug").unique().to_series().to_list())

    overlap = fv_slugs & b_slugs & c_slugs
    only_fv = fv_slugs - overlap
    only_b = b_slugs - overlap
    only_c = c_slugs - overlap

    print("\n=== SLUG OVERLAP ===")
    print(f"fv slugs:   {len(fv_slugs):,}")
    print(f"book1s slugs:{len(b_slugs):,}")
    print(f"clob slugs: {len(c_slugs):,}")
    print(f"overlap:    {len(overlap):,}")
    if only_fv:
        print(f"slugs missing from book1s/clob (show up to 10): {sorted(list(only_fv))[:10]}")
    if only_b:
        print(f"slugs only in book1s (show up to 10): {sorted(list(only_b))[:10]}")
    if only_c:
        print(f"slugs only in clob (show up to 10): {sorted(list(only_c))[:10]}")

    if out_dir:
        with open(os.path.join(out_dir, "diag__slug_overlap.json"), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "n_fv_slugs": len(fv_slugs),
                    "n_book1s_slugs": len(b_slugs),
                    "n_clob_slugs": len(c_slugs),
                    "n_overlap": len(overlap),
                    "only_fv": sorted(list(only_fv))[:200],
                    "only_book1s": sorted(list(only_b))[:200],
                    "only_clob": sorted(list(only_c))[:200],
                },
                f,
                indent=2,
                sort_keys=True,
            )


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    date_str = "20260202_082832"
    APP_PATH = fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\app.jsonl"
    BINANCE_PATH = fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\binance.jsonl"
    MET_PATH = fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\metrics.jsonl"
    PM_PATH = fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\polymarket_clob.jsonl"
    CH_PATH = fr"C:\Users\ccodi\Documents\prog\python\polytraderupdown\logs\run_{date_str}\polymarket_rtds.jsonl"
    OUT_DIR = r"./data"
    NO_WRITE = True

    out_dir = OUT_DIR
    if out_dir and not NO_WRITE:
        ensure_out_dir(out_dir)

    # Read
    metrics = read_jsonl(MET_PATH)
    clob = read_jsonl(PM_PATH)

    # Quick stream overviews (raw)
    stream_overview(metrics, "metrics_raw", out_dir if (out_dir and not NO_WRITE) else None)
    stream_overview(clob, "clob_raw", out_dir if (out_dir and not NO_WRITE) else None)

    # Validate substreams
    fv = validate_metrics_fv(metrics)
    b1 = validate_metrics_book1s(metrics)
    cb = validate_clob_book(clob)

    # Overviews (filtered)
    stream_overview(fv, "metrics_fv_vol_1s", out_dir if (out_dir and not NO_WRITE) else None)
    stream_overview(b1, "metrics_book_1s", out_dir if (out_dir and not NO_WRITE) else None)
    stream_overview(cb, "clob_book", out_dir if (out_dir and not NO_WRITE) else None)

    # Diagnostics
    diagnostics_cross_stream_slug_overlap(
        fv=fv, b=b1, c=cb, out_dir=(out_dir if (out_dir and not NO_WRITE) else None)
    )
    diagnostics_per_slug_fv(fv, out_dir if (out_dir and not NO_WRITE) else None, warmup_reason="warmup")
    diagnostics_per_slug_book1s(b1, out_dir if (out_dir and not NO_WRITE) else None)
    diagnostics_per_slug_clob(cb, out_dir if (out_dir and not NO_WRITE) else None)

    print("\n✅ Validation passed (contract satisfied).")


if __name__ == "__main__":
    try:
        main()
    except ValidationError as e:
        print(f"\n❌ VALIDATION FAILED: {e}")
        raise
