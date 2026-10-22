"""Menerbitkan event order dari CSV ke topic Kafka."""
import argparse
import json

import pandas as pd
from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/orders.csv")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--limit", type=int, default=36)
    ap.add_argument("--duplicate", type=int, default=0,
                    help="kirim ulang N event pertama untuk uji idempotensi")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    df = pd.read_csv(args.source, dtype=str, keep_default_na=False)
    events = df.head(args.limit).to_dict("records")
    if args.duplicate:
        events += events[:args.duplicate]

    for event in events:
        # key = order_id agar seluruh event satu order masuk ke partisi yang sama
        future = producer.send(args.topic, key=event["order_id"], value=event)
        meta = future.get(timeout=10)
        print(f"[PRODUCER] {event['order_id']} -> partition={meta.partition} "
              f"offset={meta.offset}")

    producer.flush()
    print(f"[PRODUCER] selesai, {len(events)} event terkirim "
          f"({args.duplicate} di antaranya duplikat sengaja)")


if __name__ == "__main__":
    main()
