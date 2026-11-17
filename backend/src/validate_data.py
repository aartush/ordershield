import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("OrderShield-Validation")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def main():
    raw = ROOT / "data" / "raw"
    output = ROOT / "outputs"
    output.mkdir(parents=True, exist_ok=True)

    required = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv",
    ]

    missing = [name for name in required if not (raw / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing CSVs: {missing}")

    tables = {}
    report = {"tables": {}}

    for name in required:
        df = (
            spark.read
            .option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(str(raw / name))
        )
        tables[name] = df
        count = df.count()
        report["tables"][name] = {
            "rows": count,
            "columns": len(df.columns),
        }
        print(f"{name}: {count:,} rows | {len(df.columns)} columns")

    orders = tables["olist_orders_dataset.csv"].cache()

    print("\nORDER STATUS DISTRIBUTION")
    statuses = orders.groupBy("order_status").count().orderBy(F.desc("count"))
    statuses.show(truncate=False)
    report["order_statuses"] = [row.asDict() for row in statuses.collect()]

    print("\nMISSING VALUES IN ORDERS")
    missing_values = orders.select([
        F.sum(
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c)) == ""),
                1,
            ).otherwise(0)
        ).alias(c)
        for c in orders.columns
    ]).first().asDict()

    for column, count in missing_values.items():
        print(f"{column}: {count:,}")

    report["orders_missing_values"] = missing_values
    report["duplicate_order_ids"] = (
        orders.filter(F.col("order_id").isNotNull())
        .groupBy("order_id").count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"\nDuplicate order IDs: {report['duplicate_order_ids']}")

    destination = output / "data_validation.json"
    destination.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nVALIDATION COMPLETE\nReport: {destination}")


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
