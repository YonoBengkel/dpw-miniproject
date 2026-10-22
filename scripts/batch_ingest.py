"""Batch ingestion: CSV sumber disimulasikan sebagai REST API yang dipaginasi."""
import argparse
import json
import random
import time
from pathlib import Path

import pandas as pd


class TransientError(Exception):
    """Mewakili error sementara seperti timeout atau HTTP 429/5xx."""


def fetch_page(df, page, page_size, fail_rate, rng):
    """Adapter sumber. Pada API nyata, isi fungsi ini dengan requests.get()."""
    if rng.random() < fail_rate:
        raise TransientError(f"simulasi kegagalan sementara pada page={page}")
    start = page * page_size
    return df.iloc[start:start + page_size].copy()


def fetch_page_with_retry(df, page, page_size, fail_rate, rng, max_retry=4):
    """Retry dengan exponential backoff + jitter, persis pola Modul Data Ingestion."""
    for attempt in range(1, max_retry + 1):
        try:
            return fetch_page(df, page, page_size, fail_rate, rng)
        except TransientError as exc:
            if attempt == max_retry:
                raise
            delay = (2 ** (attempt - 1)) + rng.uniform(0, 0.5)
            print(f"  [retry] {exc} -> percobaan {attempt}/{max_retry}, "
                  f"tunggu {delay:.2f}s")
            time.sleep(delay)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/orders.csv")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--page-size", type=int, default=10)
    ap.add_argument("--checkpoint", default="data/watermark.json")
    ap.add_argument("--fail-rate", type=float, default=0.0,
                    help="peluang kegagalan sementara per halaman, untuk uji retry")
    ap.add_argument("--lookback-minutes", type=int, default=0,
                    help="mundurkan watermark untuk menangkap data terlambat")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    source = pd.read_csv(args.source, dtype=str, keep_default_na=False)

    # --- 1. Baca watermark lama -------------------------------------------
    checkpoint = Path(args.checkpoint)
    last_ts = None
    if checkpoint.exists():
        last_ts = json.loads(checkpoint.read_text()).get("watermark")
    print(f"watermark awal : {last_ts}")

    # --- 2. Tarik seluruh halaman -----------------------------------------
    pages, page = [], 0
    while True:
        chunk = fetch_page_with_retry(source, page, args.page_size,
                                      args.fail_rate, rng)
        if chunk.empty:
            break
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk)
        page += 1

    incoming = pd.concat(pages, ignore_index=True) if pages else source.iloc[0:0].copy()
    n_read = len(incoming)

    # --- 3. Normalisasi waktu, pisahkan yang tidak dapat diparse ----------
    incoming["order_ts_parsed"] = pd.to_datetime(
        incoming["order_ts"], errors="coerce", utc=True, format="ISO8601")
    unparsed = incoming[incoming["order_ts_parsed"].isna()]
    if not unparsed.empty:
        print(f"  [catatan] {len(unparsed)} record memiliki order_ts tidak valid "
              f"dan tetap diteruskan ke raw layer untuk divalidasi kontrak: "
              f"{list(unparsed['order_id'])}")

    # --- 4. Filter inkremental berdasarkan watermark ----------------------
    if last_ts:
        bound = pd.Timestamp(last_ts)
        if args.lookback_minutes:
            bound -= pd.Timedelta(minutes=args.lookback_minutes)
        is_new = incoming["order_ts_parsed"] > bound
        # record ber-timestamp rusak tidak boleh hilang diam-diam
        is_new = is_new | incoming["order_ts_parsed"].isna()
        incoming = incoming[is_new]
    n_new = len(incoming)

    # --- 5. Gabungkan ke raw layer dan deduplikasi berdasarkan order_id ---
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "orders.parquet"

    before = 0
    if target.exists():
        old = pd.read_parquet(target)
        before = len(old)
        merged = pd.concat([old, incoming], ignore_index=True)
    else:
        merged = incoming
    merged = merged.drop_duplicates("order_id", keep="last")
    merged = merged.sort_values("order_id")

    # --- 6. Tulis, lalu VERIFIKASI sebelum menyentuh watermark ------------
    merged.drop(columns=["order_ts_parsed"]).to_parquet(target, index=False)
    verify = pd.read_parquet(target)
    if len(verify) != len(merged):
        raise RuntimeError("verifikasi gagal: jumlah baris Parquet tidak sesuai")

    # --- 7. Baru sekarang watermark boleh maju ----------------------------
    valid_ts = merged["order_ts_parsed"].dropna() if "order_ts_parsed" in merged else []
    if len(valid_ts):
        new_watermark = valid_ts.max().isoformat()
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        checkpoint.write_text(json.dumps({"watermark": new_watermark}, indent=2))
    else:
        new_watermark = last_ts

    print("-" * 58)
    print(f"record dibaca      : {n_read}")
    print(f"record baru        : {n_new}")
    print(f"duplikat diabaikan : {before + n_new - len(merged)}")
    print(f"total di raw layer : {len(merged)}")
    print(f"watermark akhir    : {new_watermark}")
    print(f"output             : {target}")


if __name__ == "__main__":
    main()
