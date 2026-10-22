"""Eksperimen data skew: satu customer_id mendominasi distribusi grup."""
import argparse
import statistics
import time
from pathlib import Path

import polars as pl
import pandas as pd


def build_datasets(src, out_dir, factor, skew_ratio):
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    uniform = pd.concat([df] * factor, ignore_index=True)
    uniform["order_id"] = uniform["order_id"] + "-" + uniform.index.astype(str)
    uniform_path = out / "orders_uniform.csv"
    uniform.to_csv(uniform_path, index=False)

    skewed = uniform.copy()
    n_hot = int(len(skewed) * skew_ratio)
    skewed.loc[:n_hot, "customer_id"] = "C001"      # satu pelanggan dominan
    skewed_path = out / "orders_skewed.csv"
    skewed.to_csv(skewed_path, index=False)
    return uniform_path, skewed_path


def group_by_customer(path):
    return (
        pl.scan_csv(path)
        .with_columns(
            (pl.col("quantity") * pl.col("unit_price")
             * (1 - pl.col("discount_pct"))).alias("net_amount"))
        .group_by("customer_id")
        .agg(pl.col("net_amount").sum(), pl.len().alias("n_rows"))
        .collect()
    )


def timeit(path, repeat):
    group_by_customer(path)
    s = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        group_by_customer(path)
        s.append(time.perf_counter() - t0)
    return statistics.median(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/orders.csv")
    ap.add_argument("--out-dir", default="data/bench")
    ap.add_argument("--factor", type=int, default=5000)
    ap.add_argument("--skew-ratio", type=float, default=0.8)
    ap.add_argument("--repeat", type=int, default=5)
    args = ap.parse_args()

    uniform_path, skewed_path = build_datasets(
        args.source, args.out_dir, args.factor, args.skew_ratio)

    for label, path in [("uniform", uniform_path), ("skewed", skewed_path)]:
        result = group_by_customer(path).sort("n_rows", descending=True)
        top = result.row(0, named=True)
        total = result["n_rows"].sum()
        print(f"[{label}] grup={result.height}  total_baris={total:,}")
        print(f"          grup terbesar: {top['customer_id']} "
              f"= {top['n_rows']:,} baris ({top['n_rows']/total*100:.1f}% dari total)")
        print(f"          median waktu group_by: {timeit(path, args.repeat)*1000:.2f} ms")
        print()

    print("Catatan: ini mengukur distribusi dan waktu agregasi pada satu mesin.")
    print("Jangan menyebut terjadi OOM kecuali proses benar-benar gagal kehabisan memori.")


if __name__ == "__main__":
    main()
