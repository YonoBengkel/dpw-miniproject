"""Benchmark Pandas vs Polars (eager dan lazy) pada transformasi yang identik."""
import argparse
import statistics
import time
from pathlib import Path

import pandas as pd
import polars as pl


def amplify(src: str, dst: str, factor: int) -> str:
    """Perbesar dataset secara sintetis agar pengukuran waktu bermakna."""
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    big = pd.concat([df] * factor, ignore_index=True)
    big["order_id"] = big["order_id"] + "-" + big.index.astype(str)
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    big.to_csv(dst, index=False)
    return dst


def bench_pandas(path):
    df = pd.read_csv(path)
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
    return df.groupby("product_category", dropna=False)["net_amount"].sum()


def bench_polars_eager(path):
    df = pl.read_csv(path)
    df = df.with_columns(
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    ).with_columns(
        (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
    )
    return df.group_by("product_category").agg(pl.col("net_amount").sum())


def bench_polars_lazy(path):
    return (
        pl.scan_csv(path)
        .with_columns(
            (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        )
        .with_columns(
            (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
        )
        .group_by("product_category")
        .agg(pl.col("net_amount").sum())
        .collect()
    )


def timeit(fn, path, repeat):
    fn(path)                                   # warm-up, tidak diukur
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn(path)
        samples.append(time.perf_counter() - t0)
    return samples


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/orders.csv")
    ap.add_argument("--factor", type=int, default=5000)
    ap.add_argument("--repeat", type=int, default=7)
    args = ap.parse_args()

    path = amplify(args.source, "data/bench/orders_big.csv", args.factor)
    n_rows = sum(1 for _ in open(path, encoding="utf-8")) - 1
    print(f"dataset benchmark : {path} ({n_rows:,} baris, "
          f"hasil amplifikasi sintetis x{args.factor})")
    print(f"repetisi          : {args.repeat} (median dilaporkan, warm-up dibuang)")
    print()

    for name, fn in [("pandas", bench_pandas),
                     ("polars-eager", bench_polars_eager),
                     ("polars-lazy", bench_polars_lazy)]:
        s = timeit(fn, path, args.repeat)
        print(f"{name:14s} median={statistics.median(s)*1000:8.2f} ms  "
              f"min={min(s)*1000:8.2f} ms  max={max(s)*1000:8.2f} ms")


if __name__ == "__main__":
    main()
