import argparse
import json
import time
import uuid
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
VERSION = "payment_snapshot_v1"
NUMERIC = [
    "purchase_hour", "purchase_weekday", "purchase_month", "is_weekend",
    "payment_total", "max_installments", "uses_credit_card",
    "uses_boleto", "uses_voucher", "uses_debit_card",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    if args.limit < 1 or args.interval < 0:
        parser.error("limit must be positive; interval cannot be negative")

    metadata = json.loads(
        (ROOT / "data/processed/feature_metadata.json").read_text(
            encoding="utf-8"
        )
    )
    if metadata["feature_version"] != VERSION:
        raise ValueError("Feature version mismatch")

    data = pd.read_csv(ROOT / "data/processed/order_features.csv")
    data = (
        data.loc[data["split"].eq("validation")]
        .sample(frac=1, random_state=42)
        .head(args.limit)
    )

    producer = KafkaProducer(
        bootstrap_servers="127.0.0.1:9092",
        acks="all",
        retries=5,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, allow_nan=False
        ).encode("utf-8"),
    )

    run_id = uuid.uuid4().hex[:12]
    print(f"PRODUCER CONNECTED | run={run_id}", flush=True)
    print("Historical feature replay; labels are not transmitted.", flush=True)

    try:
        for index, row in enumerate(data.to_dict("records"), start=1):
            event = {
                "event_id": f"{run_id}-{index}",
                "feature_version": VERSION,
                "order_id": row["order_id"],
                "customer_state": (
                    "UNKNOWN" if pd.isna(row["customer_state"])
                    else str(row["customer_state"])
                ),
                "emitted_at": time.time(),
                **{
                    name: None if pd.isna(row[name]) else float(row[name])
                    for name in NUMERIC
                },
            }
            receipt = producer.send(
                "ordershield.orders",
                key=event["event_id"],
                value=event,
            ).get(timeout=30)

            print(
                f"SENT {index}/{len(data)} | "
                f"order={event['order_id']} | offset={receipt.offset}",
                flush=True,
            )
            time.sleep(args.interval)
        producer.flush(timeout=30)
        print("ALL EVENTS ACKNOWLEDGED BY KAFKA", flush=True)
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
