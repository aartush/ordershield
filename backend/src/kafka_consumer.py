import json
import os
import sqlite3
import sys
import time
from functools import reduce
from pathlib import Path

import pyspark
from kafka import KafkaProducer
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F, types as T

ROOT = Path(__file__).resolve().parents[1]
VERSION = "payment_snapshot_v1"
LIVE = ROOT / "outputs/live"
LIVE.mkdir(parents=True, exist_ok=True)

NUMERIC = [
    "purchase_hour", "purchase_weekday", "purchase_month", "is_weekend",
    "payment_total", "max_installments", "uses_credit_card",
    "uses_boleto", "uses_voucher", "uses_debit_card",
]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

native = ROOT / "tools/hadoop-3.4.1"
if os.name == "nt" and (native / "bin/hadoop.dll").exists():
    os.environ["HADOOP_HOME"] = str(native)
    os.environ["PATH"] = (
        str(native / "bin") + os.pathsep + os.environ.get("PATH", "")
    )


def status(active, **details):
    payload = {
        "active": active,
        "heartbeat": time.time(),
        "feature_version": VERSION,
        **details,
    }
    temporary = LIVE / "status.tmp"
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    temporary.replace(LIVE / "status.json")


def main():
    report = json.loads(
        (ROOT / f"outputs/{VERSION}/model_metrics.json").read_text(
            encoding="utf-8"
        )
    )
    if report["feature_version"] != VERSION:
        raise ValueError("Model version mismatch")
    threshold = float(report["threshold"])

    # Connector must match the installed Spark and Scala binary versions.
    package = (
        "org.apache.spark:spark-sql-kafka-0-10_2.13:"
        + pyspark.__version__
    )

    status(False, message="Starting Spark and loading Kafka connector")
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("OrderShield-Kafka-Streaming")
        .config("spark.jars.packages", package)
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    producer = None
    query = None

    try:
        model = PipelineModel.load(str(ROOT / "models" / VERSION))
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:9092",
            acks="all",
            retries=5,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, allow_nan=False
            ).encode("utf-8"),
        )

        with sqlite3.connect(LIVE / "predictions.db") as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    event_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    risk_score REAL NOT NULL,
                    alert INTEGER NOT NULL,
                    emitted_at REAL NOT NULL,
                    scored_at REAL NOT NULL,
                    source_partition INTEGER NOT NULL,
                    source_offset INTEGER NOT NULL,
                    batch_id INTEGER NOT NULL
                )
            """)

        schema = T.StructType([
            T.StructField("event_id", T.StringType()),
            T.StructField("feature_version", T.StringType()),
            T.StructField("order_id", T.StringType()),
            T.StructField("customer_state", T.StringType()),
            T.StructField("emitted_at", T.DoubleType()),
            *[
                T.StructField(name, T.DoubleType())
                for name in NUMERIC
            ],
        ])

        def process_batch(batch, batch_id):
            parsed = batch.select(
                F.col("partition").alias("source_partition"),
                F.col("offset").alias("source_offset"),
                F.from_json(
                    F.col("value").cast("string"), schema
                ).alias("event"),
            ).select("source_partition", "source_offset", "event.*").cache()

            try:
                if parsed.isEmpty():
                    return

                required = [
                    "event_id", "feature_version", "order_id",
                    "customer_state", "emitted_at",
                ]
                invalid = reduce(
                    lambda a, b: a | b,
                    [F.col(name).isNull() for name in required],
                ) | (F.col("feature_version") != VERSION)

                if parsed.filter(invalid).limit(1).count():
                    raise ValueError(
                        "Invalid event/schema version. Stream stopped "
                        "without advancing this batch's checkpoint."
                    )

                rows = (
                    model.transform(parsed)
                    .select(
                        "event_id", "order_id", "emitted_at",
                        "source_partition", "source_offset",
                        vector_to_array("probability")[1].alias("risk_score"),
                    )
                    .collect()
                )

                records = []
                for row in rows:
                    event = {
                        **row.asDict(),
                        "alert": int(row["risk_score"] >= threshold),
                        "scored_at": time.time(),
                        "batch_id": int(batch_id),
                        "feature_version": VERSION,
                        "threshold": threshold,
                    }
                    producer.send(
                        "ordershield.predictions",
                        key=event["event_id"],
                        value=event,
                    ).get(timeout=30)
                    records.append((
                        event["event_id"], event["order_id"],
                        event["risk_score"], event["alert"],
                        event["emitted_at"], event["scored_at"],
                        event["source_partition"], event["source_offset"],
                        event["batch_id"],
                    ))

                # Stable event IDs prevent duplicate dashboard records on retry.
                with sqlite3.connect(
                    LIVE / "predictions.db", timeout=30
                ) as db:
                    db.executemany(
                        "INSERT OR IGNORE INTO predictions "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        records,
                    )

                print(
                    f"BATCH {batch_id} | scored={len(records)} | "
                    f"alerts={sum(row[3] for row in records)}",
                    flush=True,
                )
            finally:
                parsed.unpersist()

        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "127.0.0.1:9092")
            .option("subscribe", "ordershield.orders")
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "100")
            .option("failOnDataLoss", "true")
            .load()
        )

        query = (
            source.writeStream
            .queryName("ordershield_live")
            .foreachBatch(process_batch)
            .option(
                "checkpointLocation",
                str(ROOT / "checkpoints" / VERSION),
            )
            .trigger(processingTime="5 seconds")
            .start()
        )

        print("STREAM READY | waiting for Kafka order events", flush=True)

        while query.isActive:
            progress = query.lastProgress or {}
            status(
                True,
                message=query.status.get("message", ""),
                query_id=str(query.id),
                batch_id=progress.get("batchId"),
                last_input_rows=progress.get("numInputRows", 0),
            )
            query.awaitTermination(5)

        if query.exception():
            raise RuntimeError(str(query.exception()))

    except KeyboardInterrupt:
        print("Stopping stream...", flush=True)
    except Exception as error:
        status(False, message=str(error))
        raise
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            status(False, message="Consumer stopped; inspect terminal for errors")
            if producer is not None:
                producer.close(timeout=10)
            spark.stop()


if __name__ == "__main__":
    main()
