"""Mengonsumsi event order, memvalidasi kontrak, dan menulis sink idempoten."""
import argparse
import json
from pathlib import Path

from kafka import KafkaConsumer
from pydantic import ValidationError

from contract import Order


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--group", default="orders-consumer")
    ap.add_argument("--out-dir", default="data/stream")
    ap.add_argument("--timeout-ms", type=int, default=15000)
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    valid_path = out / "valid_events.jsonl"
    invalid_path = out / "invalid_events.jsonl"

    # sink idempoten: dikunci berdasarkan order_id, sehingga event yang sama
    # dikirim dua kali tetap menghasilkan satu baris bisnis
    sink = {}
    if valid_path.exists():
        for line in valid_path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            sink[row["order_id"]] = row

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers="localhost:9092",
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,           # commit manual setelah sink berhasil
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=args.timeout_ms,
    )

    n_msg = n_valid = n_invalid = n_dup = 0
    with invalid_path.open("a", encoding="utf-8") as bad:
        for msg in consumer:
            n_msg += 1
            print(f"[CONSUMER] topic={msg.topic} partition={msg.partition} "
                  f"offset={msg.offset} key={msg.key.decode() if msg.key else None}")
            try:
                order = Order(**msg.value)
            except ValidationError as exc:
                n_invalid += 1
                bad.write(json.dumps({
                    "order_id": msg.value.get("order_id"),
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "error_reason": "; ".join(
                        f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}"
                        for e in exc.errors()),
                    "payload": msg.value,
                }, default=str) + "\n")
                consumer.commit()
                continue

            row = json.loads(order.model_dump_json())
            if row["order_id"] in sink:
                n_dup += 1
                print(f"           duplikat {row['order_id']} diabaikan oleh sink")
            else:
                n_valid += 1
            sink[row["order_id"]] = row     # upsert, bukan append
            consumer.commit()               # offset maju setelah sink berhasil

    with valid_path.open("w", encoding="utf-8") as fh:
        for row in sink.values():
            fh.write(json.dumps(row, default=str) + "\n")

    consumer.close()
    print("-" * 58)
    print(f"pesan diterima      : {n_msg}")
    print(f"valid (baru)        : {n_valid}")
    print(f"duplikat diabaikan  : {n_dup}")
    print(f"invalid             : {n_invalid}")
    print(f"baris unik di sink  : {len(sink)}")


if __name__ == "__main__":
    main()
