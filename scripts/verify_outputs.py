"""Periksa ulang keluaran pipeline dan tulis berkas bukti ke evidence/checks/.

Setiap pemeriksaan membaca hasil nyata (Parquet, CSV, DuckDB) lalu menulis ringkasan
yang bisa dibuka penilai: baca-ulang raw Parquet, jumlah order unik per lapisan,
cek perhitungan transformasi, validasi agregasi harian, contoh join star schema,
kasus validasi, dan spesifikasi lingkungan benchmark.
"""
import ctypes
import os
import platform
import sys
from pathlib import Path

import duckdb
import pandas as pd

OUT = Path("evidence/checks")
DB = "warehouse/warehouse.duckdb"
hasil = []


def catat(nama, lulus, keterangan):
    hasil.append((nama, "LULUS" if lulus else "GAGAL", keterangan))


def ram_gb():
    class Mem(ctypes.Structure):
        _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
    m = Mem(); m.dwLength = ctypes.sizeof(Mem)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
    return m.ullTotalPhys / 1024 ** 3


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    raw = pd.read_parquet("data/raw/orders.parquet")
    valid = pd.read_parquet("data/valid/valid_orders.parquet")
    rejected = pd.read_csv("data/rejected/rejected_orders.csv")
    trans = pd.read_parquet("data/transformed/orders_transformed.parquet")
    con = duckdb.connect(DB, read_only=True)

    # 1. raw Parquet dapat dibaca kembali
    (OUT / "raw_readback.txt").write_text(
        f"berkas : data/raw/orders.parquet ({Path('data/raw/orders.parquet').stat().st_size} byte)\n"
        f"bentuk : {raw.shape[0]} baris x {raw.shape[1]} kolom\n"
        f"kolom  : {list(raw.columns)}\n\n{raw.head(5).to_string(index=False)}\n",
        encoding="utf-8")
    catat("raw Parquet terbaca ulang", len(raw) == 36, f"{len(raw)} baris")

    # 2. order unik per lapisan dan ringkasan jumlah record
    fct = con.execute("select * from fct_orders").fetchdf()
    lapisan = [
        ("raw (batch_ingest)", raw), ("valid (validate)", valid),
        ("transformed (transform)", trans),
        ("warehouse raw.orders", con.execute("select * from raw.orders").fetchdf()),
        ("dbt fct_orders", fct),
    ]
    baris = [f"{n:26s} baris={len(df):3d}  order_id unik={df['order_id'].nunique():3d}  "
             f"duplikat={len(df) - df['order_id'].nunique()}" for n, df in lapisan]
    baris += ["", f"ringkasan: dibaca 36 | valid {len(valid)} | ditolak {len(rejected)} | "
                  f"ditambahkan ke warehouse {len(fct)}"]
    (OUT / "idempotensi_order_unik.txt").write_text("\n".join(baris) + "\n", encoding="utf-8")
    tanpa_dup = all(len(df) == df["order_id"].nunique() for _, df in lapisan)
    catat("tidak ada order_id ganda di setiap lapisan", tanpa_dup, "raw 36, lapisan lain 30")

    # 3. cek perhitungan transformasi
    cek = trans[["order_id", "quantity", "unit_price", "discount_pct",
                 "gross_amount", "net_amount"]].copy()
    cek["gross_manual"] = cek["quantity"] * cek["unit_price"]
    cek["net_manual"] = cek["gross_manual"] * (1 - cek["discount_pct"])
    cek["cocok"] = ((cek["gross_manual"] - cek["gross_amount"]).abs() < 1e-9) & \
                   ((cek["net_manual"] - cek["net_amount"]).abs() < 1e-9)
    cek.to_csv(OUT / "transformation_check.csv", index=False)
    tipe = "\n".join(f"{k}: {v}" for k, v in trans.dtypes.astype(str).items())
    (OUT / "transformation_schema.txt").write_text(tipe + "\n", encoding="utf-8")
    catat("gross dan net amount sesuai hitung manual", bool(cek["cocok"].all()),
          f"{int(cek['cocok'].sum())}/{len(cek)} baris cocok")

    # 4. validasi agregasi harian terhadap data transaksi
    akui = trans[trans["status"].isin(["paid", "shipped"])]
    manual = akui.groupby(akui["order_date"].astype(str))["net_amount"].sum().round(2)
    mart = con.execute("select cast(order_date as varchar) d, round(sum(net_sales),2) s "
                       "from mart_daily_sales group by 1").fetchdf().set_index("d")["s"]
    agg = pd.DataFrame({"manual_dari_transaksi": manual, "mart_daily_sales": mart})
    agg["selisih"] = (agg["manual_dari_transaksi"] - agg["mart_daily_sales"]).round(2)
    agg.index.name = "order_date"
    agg.to_csv(OUT / "agregasi_harian_check.csv")
    catat("mart_daily_sales sama dengan hitungan manual", bool((agg["selisih"] == 0).all()),
          f"total {manual.sum():.2f}")

    # 5. contoh join star schema
    join = con.execute("""
        select f.order_id, f.order_date, c.customer_id, c.total_orders as order_pelanggan,
               p.product_category, f.net_amount
        from fct_orders f
        join dim_customer c using (customer_id)
        join dim_product_category p using (product_category)
        order by f.order_id
    """).fetchdf()
    yatim = con.execute("""select count(*) from fct_orders f
        left join dim_customer c using (customer_id)
        where c.customer_id is null""").fetchone()[0]
    (OUT / "star_schema_join.txt").write_text(
        "fct_orders JOIN dim_customer JOIN dim_product_category\n"
        f"baris hasil join : {len(join)} (fct_orders {len(fct)})\n"
        f"fakta tanpa dimensi pelanggan : {yatim}\n\n"
        "grain fct_orders : satu baris per order_id\n"
        "key              : customer_id -> dim_customer, product_category -> dim_product_category\n\n"
        f"{join.head(10).to_string(index=False)}\n", encoding="utf-8")
    catat("join fakta-dimensi utuh", len(join) == len(fct) and yatim == 0, f"{len(join)} baris")

    # 6. kasus validasi valid dan invalid
    kasus = pd.concat([
        valid.head(5).assign(status_validasi="valid", alasan="-")[
            ["order_id", "status_validasi", "alasan"]],
        rejected.assign(status_validasi="ditolak").rename(columns={"error_reason": "alasan"})[
            ["order_id", "status_validasi", "alasan"]],
    ])
    kasus.to_csv(OUT / "validation_cases.csv", index=False)
    catat("6 record ditolak dengan alasan berbeda", rejected["error_reason"].nunique() == 6,
          f"{len(rejected)} ditolak")

    # 7. lingkungan benchmark
    import polars, pyarrow, pydantic
    (OUT / "environment.txt").write_text(
        f"OS        : {platform.platform()}\n"
        f"Prosesor  : {platform.processor()}\n"
        f"Core log. : {os.cpu_count()}\n"
        f"RAM       : {ram_gb():.1f} GB\n"
        f"Python    : {sys.version.split()[0]}\n"
        f"pandas {pd.__version__} | polars {polars.__version__} | pyarrow {pyarrow.__version__} | "
        f"pydantic {pydantic.__version__} | duckdb {duckdb.__version__}\n", encoding="utf-8")
    con.close()

    ringkas = pd.DataFrame(hasil, columns=["pemeriksaan", "hasil", "keterangan"])
    ringkas.to_csv(OUT / "ringkasan_pemeriksaan.csv", index=False)
    print(ringkas.to_string(index=False))
    print(f"\nberkas bukti ditulis ke {OUT}/")
    sys.exit(0 if (ringkas["hasil"] == "LULUS").all() else 1)


if __name__ == "__main__":
    main()
