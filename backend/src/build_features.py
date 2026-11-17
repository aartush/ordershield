import csv
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
VERSION = "payment_snapshot_v1"

NUMERIC = [
    "purchase_hour",
    "purchase_weekday",
    "purchase_month",
    "is_weekend",
    "payment_total",
    "max_installments",
    "uses_credit_card",
    "uses_boleto",
    "uses_voucher",
    "uses_debit_card",
]
CATEGORICAL = ["customer_state"]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def read_csv(spark, name):
    path = ROOT / "data" / "raw" / name
    if not path.is_file():
        raise FileNotFoundError(path)
    return spark.read.option("header", True).csv(str(path))


def assert_unique(frame, column, name):
    if frame.filter(F.col(column).isNull()).limit(1).count():
        raise ValueError(f"{name}: missing {column}")

    if (
        frame.groupBy(column).count()
        .filter(F.col("count") > 1)
        .limit(1).count()
    ):
        raise ValueError(f"{name}: duplicate {column}")


def main(spark):
    orders = read_csv(spark, "olist_orders_dataset.csv")
    customers = read_csv(spark, "olist_customers_dataset.csv")
    payments = read_csv(spark, "olist_order_payments_dataset.csv")

    assert_unique(orders, "order_id", "Orders")
    assert_unique(customers, "customer_id", "Customers")

    payments = payments.select(
        "order_id",
        "payment_type",
        F.col("payment_value").cast("double").alias("payment_value"),
        F.col("payment_installments").cast("double").alias(
            "payment_installments"
        ),
    )

    invalid_payments = payments.filter(
        F.col("payment_value").isNull()
        | F.col("payment_installments").isNull()
        | (F.col("payment_value") < 0)
        | (F.col("payment_installments") < 0)
    )
    if invalid_payments.limit(1).count():
        raise ValueError("Invalid payment values/installments detected")

    payment_features = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_total"),
        F.max("payment_installments").alias("max_installments"),
        *[
            F.max(
                F.when(F.col("payment_type") == kind, 1).otherwise(0)
            ).alias(f"uses_{kind}")
            for kind in ["credit_card", "boleto", "voucher", "debit_card"]
        ],
    )

    resolved = orders.filter(
        F.col("order_status").isin(
            "delivered", "canceled", "unavailable"
        )
    )

    features = (
        resolved
        .join(
            customers.select("customer_id", "customer_state"),
            "customer_id",
            "left",
        )
        .join(payment_features, "order_id", "left")
        .withColumn(
            "purchase_time",
            F.to_timestamp("order_purchase_timestamp"),
        )
        .withColumn(
            "label",
            F.when(F.col("order_status") == "delivered", 0).otherwise(1),
        )
        .withColumn(
            "customer_state",
            F.coalesce("customer_state", F.lit("UNKNOWN")),
        )
        .withColumn("purchase_hour", F.hour("purchase_time"))
        .withColumn("purchase_weekday", F.dayofweek("purchase_time"))
        .withColumn("purchase_month", F.month("purchase_time"))
        .withColumn(
            "is_weekend",
            F.when(F.col("purchase_weekday").isin(1, 7), 1).otherwise(0),
        )
        .withColumn(
            "split",
            F.when(
                F.col("purchase_time")
                < F.lit("2018-04-01").cast("timestamp"),
                "train",
            )
            .when(
                F.col("purchase_time")
                < F.lit("2018-07-01").cast("timestamp"),
                "validation",
            )
            .otherwise("test"),
        )
        .select(
            "order_id",
            "purchase_time",
            *CATEGORICAL,
            *NUMERIC,
            "label",
            "split",
        )
        .cache()
    )

    if features.filter(F.col("purchase_time").isNull()).limit(1).count():
        raise ValueError("Invalid purchase timestamps detected")

    if features.count() != resolved.count():
        raise ValueError("Joins changed the number of orders")

    # Leave missing payment values empty. Training-only imputation handles them.
    # No missing-payment indicator is provided to the classifier.
    missing_payments = features.filter(
        F.col("payment_total").isNull()
    ).count()

    summary = (
        features.groupBy("split", "label")
        .count()
        .orderBy("split", "label")
    )

    print("\nLABEL DISTRIBUTION BY TIME SPLIT")
    summary.show(truncate=False)

    print("\nFEATURE PREVIEW")
    features.show(5, truncate=False)
    print(f"\nOrders without payment records: {missing_payments}")

    output = ROOT / "data" / "processed"
    output.mkdir(parents=True, exist_ok=True)
    destination = output / "order_features.csv"

    temporary = destination.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(features.columns)
        for row in features.toLocalIterator():
            writer.writerow(list(row))
    temporary.replace(destination)

    metadata = {
        "feature_version": VERSION,
        "target": "canceled_or_unavailable_vs_delivered",
        "scoring_stage": "payment_details_available",
        "categorical_features": CATEGORICAL,
        "numeric_features": NUMERIC,
        "excluded_from_model": [
            "order_id", "purchase_time", "label", "split",
        ],
        "split_boundaries": {
            "validation_start": "2018-04-01",
            "test_start": "2018-07-01",
        },
        "split_counts": [r.asDict() for r in summary.collect()],
        "orders_without_payments": missing_payments,
        "limitations": [
            "Payment features come from an untimestamped historical "
            "snapshot; availability at scoring time is assumed, "
            "not verified.",
            "Historical snapshot lacks outcome-resolution timestamps; "
            "time splitting cannot fully reconstruct historical "
            "label availability.",
            "Item features are excluded because missing item records "
            "strongly correlate with the outcome.",
            "The previous baseline test set has already been inspected; "
            "it is not a completely unseen final holdout.",
        ],
    }

    (output / "feature_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    print(f"\nFEATURES READY: {destination}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("OrderShield-Features")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        main(spark)
    finally:
        spark.stop()
