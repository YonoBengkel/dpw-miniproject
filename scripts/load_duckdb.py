"""Memuat hasil transformasi Polars (record valid batch + streaming) ke DuckDB."""
import argparse
from pathlib import Path

import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/transformed/orders_transformed.parquet")
    ap.add_argument("--db", default="warehouse/warehouse.duckdb")
    args = ap.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.orders AS
        SELECT * FROM read_parquet('{Path(args.source).as_posix()}');
    """)

    n = con.execute("SELECT COUNT(*) FROM raw.orders").fetchone()[0]
    print(f"raw.orders dimuat: {n} baris")
    print(con.execute("""
        SELECT status, COUNT(*) AS n
        FROM raw.orders GROUP BY status ORDER BY n DESC
    """).fetchdf().to_string(index=False))

    # bukti bahwa data cacat tidak masuk warehouse
    bad = con.execute("""
        SELECT COUNT(*) FROM raw.orders
        WHERE quantity <= 0 OR unit_price < 0
           OR discount_pct < 0 OR discount_pct > 1
           OR customer_id IS NULL OR customer_id = ''
           OR status NOT IN ('paid','shipped','cancelled','refunded')
    """).fetchone()[0]
    print(f"\nrecord melanggar kontrak di warehouse: {bad} (harus 0)")
    con.close()


if __name__ == "__main__":
    main()
