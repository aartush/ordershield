import asyncio
import json
import logging
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time

from contextlib import asynccontextmanager
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sklearn.metrics import precision_recall_curve

ROOT = Path(__file__).resolve().parent
VERSION = "payment_snapshot_v1"
OUTPUT = ROOT / "outputs" / VERSION
LIVE = ROOT / "outputs/live"

NUMERIC = [
    "purchase_hour", "purchase_weekday", "purchase_month", "is_weekend",
    "payment_total", "max_installments", "uses_credit_card",
    "uses_boleto", "uses_voucher", "uses_debit_card",
]
METHODS = ["credit_card", "boleto", "voucher", "debit_card"]
STATES = sorted(
    "AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN "
    "RS RO RR SC SP SE TO".split()
)

FILES = {
    "orders": ROOT / "data/raw/olist_orders_dataset.csv",
    "customers": ROOT / "data/raw/olist_customers_dataset.csv",
    "features": ROOT / "data/processed/order_features.csv",
    "metadata": ROOT / "data/processed/feature_metadata.json",
    "report": OUTPUT / "model_metrics.json",
    "predictions": OUTPUT / "validation_predictions.csv",
    "model": ROOT / "models" / VERSION / "metadata",
}

SPARK = None
MODEL = None
MODEL_SIGNATURE = None
MODEL_LOCK = threading.Lock()
REPLAY_LOCK = threading.Lock()
REPLAY = None
LOGGER = logging.getLogger("ordershield")

NOTE = "Class-weighted risk scores are not calibrated cancellation probabilities."

LIMITATIONS = [
    "Current predictive performance is limited.",
    "Payment availability at scoring time is assumed; source timestamps are absent.",
    "Historical outcome-resolution timestamps are unavailable.",
    "Validation selected the model and threshold.",
    "The earlier baseline test set was inspected; it is not a pristine holdout.",
    "Scenario changes show associations, not causal effects.",
    "Brazil 2016â€“2018 data does not establish present-day Indian COD/RTO performance.",
    "Spark runs locally; multi-node scalability has not been benchmarked.",
]


@asynccontextmanager
async def lifespan(app):
    yield
    with MODEL_LOCK:
        if SPARK is not None:
            SPARK.stop()


app = FastAPI(
    title="OrderShield API",
    version="1.0.0",
    description="Research API for Spark analytics, model inference and Kafka streaming.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3301", "http://127.0.0.1:3301", "http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


@app.exception_handler(FileNotFoundError)
async def missing_file(request, error):
    return JSONResponse(
        status_code=503,
        content={"detail": "A required project file is missing.", "file": str(error)},
    )


def records(frame):
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def signature():
    missing = [name for name, path in FILES.items() if not path.exists()]
    if missing:
        raise HTTPException(503, detail={"missing_artifacts": missing})
    return tuple((name, path.stat().st_mtime_ns) for name, path in FILES.items())


@lru_cache(maxsize=1)
def load_data(stamp):
    orders = pd.read_csv(
        FILES["orders"], parse_dates=["order_purchase_timestamp"]
    )
    customers = pd.read_csv(
        FILES["customers"], usecols=["customer_id", "customer_state"]
    )
    orders = orders.merge(
        customers, on="customer_id", how="left", validate="many_to_one"
    )
    features = pd.read_csv(
        FILES["features"], parse_dates=["purchase_time"]
    )
    report = json.loads(FILES["report"].read_text(encoding="utf-8"))
    metadata = json.loads(FILES["metadata"].read_text(encoding="utf-8"))

    if any(
        item.get("feature_version") != VERSION
        for item in [report, metadata]
    ):
        raise HTTPException(409, "Feature/model version mismatch.")

    predictions = pd.read_csv(FILES["predictions"])
    samples = features.loc[features["split"].eq("validation")].merge(
        predictions[["order_id", "risk_score"]],
        on="order_id",
        validate="one_to_one",
    )
    if len(samples) != len(predictions):
        raise HTTPException(409, "Validation features and predictions do not match.")

    samples["alert"] = samples["risk_score"].ge(report["threshold"])
    samples["case"] = np.select(
        [
            samples["label"].eq(1) & samples["alert"],
            samples["label"].eq(1) & ~samples["alert"],
            samples["label"].eq(0) & samples["alert"],
        ],
        ["detected_risk", "missed_risk", "false_alert"],
        default="correctly_below_threshold",
    )
    return orders, features, report, samples


def data():
    return load_data(signature())


class OrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    customer_state: Literal[
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
        "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
        "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
    ] = "SP"
    purchase_date: date = date(2018, 6, 15)
    purchase_hour: int = Field(default=12, ge=0, le=23)
    payment_total: float = Field(default=150, ge=0, le=100_000_000)
    max_installments: int = Field(default=1, ge=0, le=100)
    payment_methods: list[
        Literal["credit_card", "boleto", "voucher", "debit_card"]
    ] = Field(default_factory=lambda: ["credit_card"], min_length=1, max_length=4)

    def features(self):
        weekday = (self.purchase_date.weekday() + 1) % 7 + 1
        return {
            "customer_state": self.customer_state,
            "purchase_hour": float(self.purchase_hour),
            "purchase_weekday": float(weekday),
            "purchase_month": float(self.purchase_date.month),
            "is_weekend": float(weekday in [1, 7]),
            "payment_total": self.payment_total,
            "max_installments": float(self.max_installments),
            **{
                "uses_" + method: float(method in self.payment_methods)
                for method in METHODS
            },
        }


class CompareInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    orders: list[OrderInput] = Field(min_length=2, max_length=10)


class ReplayInput(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    limit: int = Field(default=30, ge=1, le=200)
    interval: float = Field(default=1, ge=0.1, le=5)


def score_orders(orders):
    global SPARK, MODEL, MODEL_SIGNATURE

    from pyspark.sql import SparkSession, functions as F
    from pyspark.ml import PipelineModel
    from pyspark.ml.functions import vector_to_array

    started = time.perf_counter()

    # Protect the cached Spark session and serialize demonstration inference.
    with MODEL_LOCK:
        current_signature = signature()
        report = load_data(current_signature)[2]

        if MODEL is not None and MODEL_SIGNATURE != current_signature:
            raise HTTPException(
                409, "Project artifacts changed. Restart the API to reload the model."
            )

        try:
            if MODEL is None:
                os.environ["PYSPARK_PYTHON"] = sys.executable
                os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
                native = ROOT / "tools/hadoop-3.4.1"
                if os.name == "nt" and (native / "bin/hadoop.dll").exists():
                    os.environ["HADOOP_HOME"] = str(native)
                    os.environ["PATH"] = (
                        str(native / "bin") + os.pathsep
                        + os.environ.get("PATH", "")
                    )

                SPARK = (
                    SparkSession.builder.master("local[2]")
                    .appName("OrderShield-API")
                    .config("spark.sql.shuffle.partitions", "4")
                    .getOrCreate()
                )
                SPARK.sparkContext.setLogLevel("ERROR")
                MODEL = PipelineModel.load(str(ROOT / "models" / VERSION))
                MODEL_SIGNATURE = current_signature

            frame = None
            feature_rows = [order.features() for order in orders]

            for index, row in enumerate(feature_rows):
                columns = [
                    F.lit(index).alias("position"),
                    F.lit(row["customer_state"]).alias("customer_state"),
                    *[
                        F.lit(float(row[name])).alias(name)
                        for name in NUMERIC
                    ],
                ]
                part = SPARK.range(1).select(*columns)
                frame = part if frame is None else frame.unionByName(part)

            scored = (
                MODEL.transform(frame)
                .select("position", vector_to_array("probability")[1].alias("score"))
                .orderBy("position").collect()
            )
        except Exception as error:
            LOGGER.exception("Spark inference failed")
            raise HTTPException(
                503, "Spark inference failed. Check the API terminal and native setup."
            ) from error

    threshold = float(report["threshold"])
    results = []
    for order, row, features in zip(orders, scored, feature_rows):
        warnings = []
        if not 2016 <= order.purchase_date.year <= 2018:
            warnings.append("Input date is outside the dataset years.")

        results.append({
            "risk_score": float(row["score"]),
            "alert": bool(row["score"] >= threshold),
            "decision": (
                "review_suggested" if row["score"] >= threshold
                else "below_threshold"
            ),
            "threshold": threshold,
            "features": features,
            "input": order.model_dump(mode="json"),
            "warnings": warnings,
        })

    return {
        "model": report["selected_model"],
        "feature_version": VERSION,
        "score_note": NOTE,
        "total_call_ms": round((time.perf_counter() - started) * 1000, 2),
        "results": results,
    }


def live_snapshot(limit=50):
    now = time.time()
    try:
        status = json.loads((LIVE / "status.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        status = {}

    heartbeat = status.get("heartbeat")
    age = now - float(heartbeat) if heartbeat else None
    result = {
        "server_time": now,
        "consumer_active": bool(status.get("active") and age is not None and age < 20),
        "heartbeat_age_seconds": round(age, 1) if age is not None else None,
        "consumer": status,
        "total_events": 0,
        "alerts": 0,
        "scored_last_60_seconds": 0,
        "latest": [],
        "database_available": False,
    }

    database = LIVE / "predictions.db"
    if not database.exists():
        return result

    try:
        with sqlite3.connect(
            database.resolve().as_uri() + "?mode=ro", uri=True, timeout=5
        ) as connection:
            total, alerts, recent = connection.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(alert), 0),
                       COALESCE(SUM(scored_at >= ?), 0)
                FROM predictions
                """,
                (now - 60,),
            ).fetchone()

            connection.row_factory = sqlite3.Row
            latest = connection.execute(
                """
                SELECT * FROM predictions
                ORDER BY scored_at DESC, source_offset DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        result.update({
            "total_events": total,
            "alerts": alerts,
            "scored_last_60_seconds": recent,
            "latest": [
                {
                    **dict(row),
                    "alert": bool(row["alert"]),
                    "queue_to_result_seconds": round(
                        max(0, row["scored_at"] - row["emitted_at"]), 3
                    ),
                }
                for row in latest
            ],
            "database_available": True,
        })
    except sqlite3.Error:
        result["database_error"] = "Database unavailable or schema not ready."
    return result


@app.get("/health", tags=["System"])
def health():
    try:
        with socket.create_connection(("127.0.0.1", 9092), timeout=0.5):
            kafka_tcp = True
    except OSError:
        kafka_tcp = False

    missing = [name for name, path in FILES.items() if not path.exists()]
    return {
        "service": "OrderShield API",
        "status": "running",
        "artifacts_available": not missing,
        "missing_artifacts": missing,
        "spark_model_loaded": MODEL is not None,
        "kafka_tcp_reachable": kafka_tcp,
        "note": "TCP reachability alone does not verify Kafka protocol health.",
    }


@app.get("/api/options", tags=["Project"])
def options():
    return {
        "states": STATES,
        "payment_methods": METHODS,
        "currency": "BRL",
        "feature_version": VERSION,
        "default_order": OrderInput().model_dump(mode="json"),
        "sample_categories": [
            "detected_risk", "missed_risk", "false_alert",
            "correctly_below_threshold",
        ],
    }


@app.get("/api/stats", tags=["Analytics"])
def stats(state: str | None = None):
    orders, features, report, samples = data()
    if state is not None and state not in STATES:
        raise HTTPException(422, "Invalid Brazilian state code.")

    view = orders.loc[orders.customer_state.eq(state)] if state else orders
    risky = view.order_status.isin(["canceled", "unavailable"])

    monthly = (
        view.groupby(view.order_purchase_timestamp.dt.to_period("M"))
        .size().reset_index(name="orders")
    )
    monthly.columns = ["month", "orders"]
    monthly["month"] = monthly["month"].astype(str)

    locations = (
        view.assign(risky=risky).groupby("customer_state")
        .agg(orders=("order_id", "size"), risky=("risky", "sum"))
        .reset_index()
    )
    locations["risk_share"] = locations["risky"] / locations["orders"]

    return {
        "filter": {"state": state},
        "total_orders": len(view),
        "delivered": int(view.order_status.eq("delivered").sum()),
        "risky": int(risky.sum()),
        "risk_share": float(risky.mean()) if len(view) else 0,
        "statuses": {
            str(k): int(v) for k, v in view.order_status.value_counts().items()
        },
        "monthly": records(monthly),
        "states": records(locations),
        "model_cohort": {
            "orders": len(features),
            "risky": int(features.label.sum()),
            "split_counts": records(
                features.groupby(["split", "label"]).size()
                .reset_index(name="orders")
            ),
        },
    }


@app.get("/api/overview", tags=["Project"])
def overview():
    orders, features, report, samples = data()
    return {
        "name": "OrderShield",
        "subtitle": "E-Commerce Order Risk Research",
        "historical_orders": len(orders),
        "resolved_orders": len(features),
        "risky_orders": int(features.label.sum()),
        "selected_model": report["selected_model"],
        "threshold": report["threshold"],
        "validation_metrics": report["validation_results"][report["selected_model"]],
        "live": live_snapshot(5),
        "score_note": NOTE,
        "limitations": LIMITATIONS,
    }


@app.get("/api/samples", tags=["Samples"])
def sample_list(
    category: Literal[
        "detected_risk", "missed_risk", "false_alert",
        "correctly_below_threshold"
    ] | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    samples = data()[3]
    if category:
        samples = samples.loc[samples["case"].eq(category)]
    samples = samples.sort_values("risk_score", ascending=False)
    return {
        "total": len(samples),
        "offset": offset,
        "limit": limit,
        "items": records(samples.iloc[offset:offset + limit]),
        "note": "Labels and cases are historical validation evidence.",
    }


@app.get("/api/samples/{order_id}", tags=["Samples"])
def sample_detail(order_id: str):
    samples = data()[3]
    found = samples.loc[samples.order_id.eq(order_id)]
    if found.empty:
        raise HTTPException(404, "Validation order not found.")
    row = found.iloc[0]

    return {
        "sample": records(found)[0],
        "playground_input": {
            "customer_state": row.customer_state,
            "purchase_date": row.purchase_time.date().isoformat(),
            "purchase_hour": int(row.purchase_hour),
            "payment_total": (
                None if pd.isna(row.payment_total) else float(row.payment_total)
            ),
            "max_installments": (
                None if pd.isna(row.max_installments) else int(row.max_installments)
            ),
            "payment_methods": [
                method for method in METHODS if row["uses_" + method] == 1
            ],
        },
        "note": "Missing payment values need explicit user input in the playground.",
    }


@app.get("/api/model", tags=["Evaluation"])
def model_report():
    return {**data()[2], "limitations": LIMITATIONS, "score_note": NOTE}


@app.get("/api/model/evaluation", tags=["Evaluation"])
def evaluation(threshold: float | None = Query(None, ge=0, le=1)):
    orders, features, report, samples = data()
    threshold = float(report["threshold"]) if threshold is None else threshold
    actual = samples.label.eq(1)
    predicted = samples.risk_score.ge(threshold)

    tp = int((actual & predicted).sum())
    fp = int((~actual & predicted).sum())
    fn = int((actual & ~predicted).sum())
    tn = int((~actual & ~predicted).sum())
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)

    p, r, _ = precision_recall_curve(samples.label, samples.risk_score)
    indices = np.unique(np.linspace(0, len(p) - 1, min(len(p), 400), dtype=int))

    return {
        "dataset": "validation",
        "threshold": threshold,
        "saved_threshold": report["threshold"],
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / max(precision + recall, 1e-12),
        "alerts": tp + fp,
        "alert_rate": (tp + fp) / len(samples),
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
        "class_prevalence": float(actual.mean()),
        "pr_curve": [
            {"recall": float(r[i]), "precision": float(p[i])}
            for i in indices
        ],
        "curve_note": "Chart points are downsampled; saved metrics use full data.",
        "note": "Exploration only; this endpoint does not modify saved settings.",
    }


@app.post("/api/predict", tags=["Inference"])
def predict(order: OrderInput):
    result = score_orders([order])
    prediction = result.pop("results")[0]
    return {**result, **prediction}


@app.post("/api/compare", tags=["Inference"])
def compare(payload: CompareInput):
    result = score_orders(payload.orders)
    result["difference_second_minus_first"] = (
        result["results"][1]["risk_score"] - result["results"][0]["risk_score"]
    )
    return result


@app.get("/api/live", tags=["Streaming"])
def live(limit: int = Query(50, ge=1, le=500)):
    return live_snapshot(limit)


@app.get("/api/live/events", tags=["Streaming"])
async def live_events(request: Request):
    async def events():
        while not await request.is_disconnected():
            snapshot = await asyncio.to_thread(live_snapshot, 50)
            yield (
                "event: snapshot\n"
                "data: " + json.dumps(snapshot, allow_nan=False) + "\n\n"
            )
            await asyncio.sleep(3)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/replay", tags=["Streaming"], status_code=202)
def start_replay(payload: ReplayInput):
    global REPLAY
    script = ROOT / "src/kafka_producer.py"
    if not script.exists():
        raise HTTPException(503, "Producer script is missing.")

    with REPLAY_LOCK:
        if REPLAY is not None and REPLAY.poll() is None:
            raise HTTPException(409, "An API-started replay is already running.")

        LIVE.mkdir(parents=True, exist_ok=True)
        with (LIVE / "api-producer.log").open("ab") as log:
            REPLAY = subprocess.Popen(
                [
                    sys.executable, str(script),
                    "--limit", str(payload.limit),
                    "--interval", str(payload.interval),
                ],
                cwd=str(ROOT),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        return {
            "status": "started",
            "pid": REPLAY.pid,
            "limit": payload.limit,
            "note": "Started does not mean published; check replay status and live counters.",
        }


@app.get("/api/replay", tags=["Streaming"])
def replay_status():
    with REPLAY_LOCK:
        if REPLAY is None:
            return {"status": "not_started_by_this_api_process"}
        code = REPLAY.poll()
        return {
            "status": (
                "running" if code is None
                else "completed" if code == 0 else "failed"
            ),
            "pid": REPLAY.pid,
            "exit_code": code,
            "log_file": "outputs/live/api-producer.log",
        }


@app.get("/api/content", tags=["Project"])
def content():
    stages = [
        ("sources", "CSV Sources", "Validate relational data, keys and missing values."),
        ("etl", "Spark ETL", "Aggregate payments before joining orders and customers."),
        ("features", "Feature Engineering", "Build order-level location, timing and payment features."),
        ("training", "MLlib Training", "Fit preprocessing and class-weighted LR/RF on training data."),
        ("validation", "Validation", "Select by average precision and choose a validation-F1 threshold."),
        ("inference", "Saved Inference", "Reuse the fitted pipeline for scenarios and Kafka events."),
    ]
    return {
        "workflow": {
            "nodes": [
                {"id": key, "title": title, "description": description}
                for key, title, description in stages
            ],
            "edges": [
                {"source": stages[i][0], "target": stages[i + 1][0]}
                for i in range(len(stages) - 1)
            ],
        },
        "streaming_workflow": [
            "Historical feature producer", "Kafka orders topic",
            "Spark Structured Streaming", "Saved MLlib model",
            "Kafka predictions topic + SQLite", "FastAPI live feed",
        ],
        "applications": [
            {
                "title": "Review prioritization",
                "user": "E-commerce operations team",
                "action": "Review flagged orders before lower-priority orders.",
                "measure": "Precision and useful detections within review capacity.",
            },
            {
                "title": "Workload planning",
                "user": "Operations manager",
                "action": "Explore thresholds against available review capacity.",
                "measure": "Alerts per day, recall and false-alert workload.",
            },
            {
                "title": "Historical investigation",
                "user": "Business analyst",
                "action": "Explore state, payment and timing associations.",
                "measure": "Patterns requiring further investigation, not causal proof.",
            },
        ],
        "business_note": "Reduced cancellations or costs have not been demonstrated.",
        "target": {"positive": ["canceled", "unavailable"], "negative": ["delivered"]},
        "split": {
            "training": "Before 2018-04-01",
            "validation": "2018-04-01 through 2018-06-30",
            "test": "2018-07-01 onward; previously inspected for the baseline",
        },
        "features": ["customer_state"] + NUMERIC,
        "limitations": LIMITATIONS,
        "score_note": NOTE,
    }

