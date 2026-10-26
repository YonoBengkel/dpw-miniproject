"""Tes kecil untuk scripts/transform.py. Jalankan: python tests/test_transform.py"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from transform import transform  # noqa: E402


def baris(order_id, ts, qty, price, disc):
    return {"order_id": order_id, "customer_id": "C1", "order_ts": ts,
            "product_category": "Books", "quantity": qty, "unit_price": price,
            "discount_pct": disc, "payment_method": "card", "status": "paid"}


def test_gabung_dedup_dan_metrik():
    ts = datetime(2026, 9, 1, 9, 15, tzinfo=timezone.utc)
    batch = pl.LazyFrame([baris("O1", ts, 2, 100.0, 0.1), baris("O2", ts, 1, 50.0, 0.0)])
    # O2 juga datang dari streaming dengan timestamp berupa teks
    stream = pl.LazyFrame([baris("O2", "2026-09-01T09:15:00Z", 1, 50.0, 0.0),
                           baris("O3", "2026-09-02T10:00:00Z", 3, 10.0, 0.5)])

    hasil = transform(batch, stream).collect()

    assert hasil["order_id"].to_list() == ["O1", "O2", "O3"], "duplikat O2 harus dibuang"
    o1 = hasil.filter(pl.col("order_id") == "O1").row(0, named=True)
    assert o1["gross_amount"] == 200.0
    assert abs(o1["net_amount"] - 180.0) < 1e-9, "diskon 0,1 berarti 10 persen"
    assert hasil.schema["order_ts"] == pl.Datetime("us", "UTC"), "timestamp harus seragam UTC"
    assert hasil.filter(pl.col("order_id") == "O2")["sumber"].item() == "stream"


def test_tanpa_streaming():
    ts = datetime(2026, 9, 1, tzinfo=timezone.utc)
    hasil = transform(pl.LazyFrame([baris("O1", ts, 1, 10.0, 0.0)]), None).collect()
    assert hasil.height == 1 and hasil["sumber"].item() == "batch"


if __name__ == "__main__":
    test_gabung_dedup_dan_metrik()
    test_tanpa_streaming()
    print("semua tes transform lulus")
