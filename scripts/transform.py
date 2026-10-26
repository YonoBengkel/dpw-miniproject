"""Transformasi Polars: gabungkan record valid dari jalur batch dan streaming.

Kedua jalur sudah divalidasi kontrak yang sama. Skrip ini menyeragamkan tipe data,
membuang duplikat per order_id, menghitung gross_amount dan net_amount, lalu menulis
hasilnya sebagai Parquet yang dimuat ke warehouse oleh load_duckdb.py.
"""
import argparse
from pathlib import Path

import polars as pl

KOLOM = ["order_id", "customer_id", "order_ts", "product_category", "quantity",
         "unit_price", "discount_pct", "payment_method", "status"]


def seragamkan(lf: pl.LazyFrame, sumber: str) -> pl.LazyFrame:
    """Samakan tipe kolom agar batch (datetime) dan streaming (teks) bisa digabung."""
    ts = pl.col("order_ts")
    if lf.collect_schema()["order_ts"] == pl.String:
        ts = ts.str.to_datetime(time_unit="us", time_zone="UTC")
    return lf.select(KOLOM).with_columns(
        ts.dt.convert_time_zone("UTC").alias("order_ts"),
        pl.col("quantity").cast(pl.Int64),
        pl.col("unit_price").cast(pl.Float64),
        pl.col("discount_pct").cast(pl.Float64),
        pl.lit(sumber).alias("sumber"),
    )


def transform(batch: pl.LazyFrame, stream: pl.LazyFrame | None) -> pl.LazyFrame:
    """Gabung, dedup per order_id (versi streaming menang karena datang belakangan),
    lalu hitung metrik. Semuanya lazy; eksekusi terjadi saat collect()."""
    bagian = [seragamkan(batch, "batch")]
    if stream is not None:
        bagian.append(seragamkan(stream, "stream"))
    return (
        pl.concat(bagian, how="vertical")
        .unique(subset="order_id", keep="last", maintain_order=True)
        .with_columns(
            (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        )
        .with_columns(
            (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
            pl.col("order_ts").dt.date().alias("order_date"),
        )
        .sort("order_id")
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", default="data/valid/valid_orders.parquet")
    ap.add_argument("--stream", default="data/stream/valid_events.jsonl")
    ap.add_argument("--out", default="data/transformed/orders_transformed.parquet")
    args = ap.parse_args()

    batch = pl.scan_parquet(args.batch)
    stream_path = Path(args.stream)
    punya_stream = stream_path.exists() and stream_path.stat().st_size > 0
    stream = pl.scan_ndjson(stream_path) if punya_stream else None
    if not punya_stream:
        print(f"[catatan] {stream_path} tidak ada atau kosong; hanya jalur batch yang dipakai")

    n_batch = batch.select(pl.len()).collect().item()
    n_stream = stream.select(pl.len()).collect().item() if stream is not None else 0

    hasil = transform(batch, stream).collect()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    hasil.write_parquet(args.out)

    print(f"record valid dari batch    : {n_batch}")
    print(f"record valid dari streaming: {n_stream}")
    print(f"gabungan sebelum dedup     : {n_batch + n_stream}")
    print(f"setelah dedup per order_id : {hasil.height}")
    print(f"total net_amount           : {hasil['net_amount'].sum():.2f}")
    print(f"output                     : {args.out}")
    print()
    # dicetak lewat pandas: tabel Polars memakai karakter garis kotak yang gagal
    # ditampilkan oleh konsol Windows berencoding cp1252
    contoh = hasil.select("order_id", "order_ts", "quantity", "unit_price",
                          "discount_pct", "gross_amount", "net_amount", "sumber").head(5)
    print(contoh.to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
