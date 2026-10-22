"""Menjalankan kelima pertanyaan analitik wajib dan mencetak hasilnya."""
import argparse

import duckdb

PERTANYAAN = [
    ("1. Nilai penjualan bersih per hari", """
        SELECT order_date, ROUND(SUM(net_sales), 2) AS net_sales
        FROM mart_daily_sales
        GROUP BY order_date
        ORDER BY order_date
    """),
    ("2. Kategori produk dengan penjualan bersih tertinggi", """
        SELECT product_category, ROUND(SUM(net_sales), 2) AS net_sales
        FROM mart_daily_sales
        GROUP BY product_category
        ORDER BY net_sales DESC
    """),
    ("3. Jumlah dan nilai pesanan menurut status", """
        SELECT status,
               COUNT(*) AS order_count,
               ROUND(SUM(net_amount), 2) AS total_net_amount
        FROM fct_orders
        GROUP BY status
        ORDER BY order_count DESC
    """),
    ("4. Metode pembayaran paling sering digunakan", """
        SELECT payment_method, COUNT(*) AS order_count
        FROM fct_orders
        GROUP BY payment_method
        ORDER BY order_count DESC
    """),
    ("5. Total penjualan bersih yang diakui (paid dan shipped)", """
        SELECT ROUND(SUM(net_sales), 2) AS net_sales_diakui
        FROM mart_daily_sales
    """),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse/warehouse.duckdb")
    args = ap.parse_args()

    # read_only agar berkas tidak terkunci bila dbt atau proses lain sedang memakainya
    con = duckdb.connect(args.db, read_only=True)
    for judul, sql in PERTANYAAN:
        print("=" * 70)
        print(judul)
        print("-" * 70)
        print(con.execute(sql).fetchdf().to_string(index=False))
        print()
    con.close()
    print("Catatan: pertanyaan nomor 5 pada brief soal record yang ditolak kontrak")
    print("dijawab dari data/rejected/rejected_orders.csv, bukan dari warehouse,")
    print("karena record cacat memang tidak pernah dimuat ke dalamnya.")


if __name__ == "__main__":
    main()
