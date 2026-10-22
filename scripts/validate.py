"""Validasi raw layer terhadap kontrak: pisahkan record valid dan rejected."""
import argparse
import json
from pathlib import Path

import pandas as pd
from pydantic import ValidationError

from contract import Order


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/raw/orders.parquet")
    ap.add_argument("--valid-out", default="data/valid/valid_orders.parquet")
    ap.add_argument("--rejected-out", default="data/rejected/rejected_orders.csv")
    ap.add_argument("--fail-fast", action="store_true",
                    help="hentikan batch pada pelanggaran pertama")
    args = ap.parse_args()

    df = pd.read_parquet(args.source)
    records = df.astype(object).where(pd.notna(df), None).to_dict("records")

    valid_rows, rejected_rows = [], []
    for raw in records:
        try:
            order = Order(**raw)
        except ValidationError as exc:
            reasons = "; ".join(
                f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
                for err in exc.errors()
            )
            rejected_rows.append({
                "order_id": raw.get("order_id"),
                "error_reason": reasons,
                "payload": json.dumps(raw, default=str),
            })
            if args.fail_fast:
                print(f"[FAIL-FAST] batch dihentikan pada {raw.get('order_id')}: {reasons}")
                raise SystemExit(1)
            continue
        valid_rows.append(order.model_dump())

    Path(args.valid_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.rejected_out).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(valid_rows).to_parquet(args.valid_out, index=False)
    pd.DataFrame(rejected_rows).to_csv(args.rejected_out, index=False)

    total = len(records)
    print(f"total record   : {total}")
    print(f"valid          : {len(valid_rows)}")
    print(f"rejected       : {len(rejected_rows)}")
    print(f"tingkat validitas: {len(valid_rows) / total * 100:.2f}%")
    print()
    for row in rejected_rows:
        print(f"  {row['order_id']}: {row['error_reason']}")


if __name__ == "__main__":
    main()
