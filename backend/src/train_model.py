import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.functions import vector_to_array

ROOT = Path(__file__).resolve().parents[1]

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def score(model, frame):
    return (
        model.transform(frame)
        .select(
            "order_id",
            "label",
            vector_to_array("probability")[1].alias("risk_score"),
        )
        .toPandas()
        .sort_values("order_id")
        .reset_index(drop=True)
    )


def choose_threshold(frame):
    precision, recall, thresholds = precision_recall_curve(
        frame["label"], frame["risk_score"]
    )
    if not len(thresholds):
        raise ValueError("No validation thresholds available")

    f1 = (
        2 * precision[:-1] * recall[:-1]
        / np.maximum(precision[:-1] + recall[:-1], 1e-12)
    )
    return float(thresholds[np.argmax(f1)])


def evaluate(frame, threshold):
    actual = frame["label"].astype(int)
    predicted = (frame["risk_score"] >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        actual, predicted, labels=[0, 1]
    ).ravel()

    prevalence = float(actual.mean())
    ap = float(
        average_precision_score(actual, frame["risk_score"])
    )

    return {
        "orders": len(frame),
        "risk_orders": int(actual.sum()),
        "risk_prevalence": prevalence,
        "average_precision": ap,
        "ap_lift_over_prevalence": ap / prevalence,
        "roc_auc": float(
            roc_auc_score(actual, frame["risk_score"])
        ),
        "threshold": threshold,
        "precision": float(
            precision_score(actual, predicted, zero_division=0)
        ),
        "recall": float(
            recall_score(actual, predicted, zero_division=0)
        ),
        "f1": float(
            f1_score(actual, predicted, zero_division=0)
        ),
        "alert_rate": float(predicted.mean()),
        "confusion_matrix": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
    }


def build_pipeline(classifier, numeric):
    imputed = [f"{name}_imputed" for name in numeric]

    return Pipeline(stages=[
        Imputer(
            strategy="median",
            inputCols=numeric,
            outputCols=imputed,
        ),
        StringIndexer(
            inputCol="customer_state",
            outputCol="state_index",
            handleInvalid="keep",
        ),
        OneHotEncoder(
            inputCols=["state_index"],
            outputCols=["state_vector"],
            dropLast=False,
        ),
        VectorAssembler(
            inputCols=imputed + ["state_vector"],
            outputCol="features",
        ),
        classifier,
    ])


def main(spark, evaluate_test):
    processed = ROOT / "data" / "processed"
    metadata = json.loads(
        (processed / "feature_metadata.json").read_text(
            encoding="utf-8"
        )
    )
    version = metadata["feature_version"]
    numeric = metadata["numeric_features"]

    if version != "payment_snapshot_v1":
        raise ValueError("Run the updated build_features.py first")

    data = (
        spark.read
        .option("header", True)
        .csv(str(processed / "order_features.csv"))
    )

    required = {
        "order_id", "customer_state", "label", "split", *numeric,
    }
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"Missing feature columns: {sorted(missing)}")

    data = data.select(
        "order_id",
        F.coalesce(
            F.col("customer_state"), F.lit("UNKNOWN")
        ).alias("customer_state"),
        *[
            F.col(name).cast("double").alias(name)
            for name in numeric
        ],
        F.col("label").cast("double").alias("label"),
        "split",
    )

    train = data.filter(F.col("split") == "train").cache()
    validation = data.filter(
        F.col("split") == "validation"
    ).cache()

    for name, frame in [
        ("train", train),
        ("validation", validation),
    ]:
        labels = {
            row["label"]
            for row in frame.select("label").distinct().collect()
        }
        if labels != {0.0, 1.0}:
            raise ValueError(f"{name} must contain labels 0 and 1")

    counts = {
        int(row["label"]): row["count"]
        for row in train.groupBy("label").count().collect()
    }
    positive_weight = counts[0] / counts[1]

    weighted_train = train.withColumn(
        "class_weight",
        F.when(
            F.col("label") == 1, positive_weight
        ).otherwise(1.0),
    )

    classifiers = {
        "logistic_regression": LogisticRegression(
            maxIter=100,
            regParam=0.1,
            weightCol="class_weight",
        ),
        "random_forest": RandomForestClassifier(
            numTrees=80,
            maxDepth=6,
            minInstancesPerNode=20,
            seed=42,
            weightCol="class_weight",
        ),
    }

    results = {}
    winner = None
    winner_ap = -1.0

    for name, classifier in classifiers.items():
        print(f"\nTRAINING: {name}", flush=True)

        model = build_pipeline(classifier, numeric).fit(
            weighted_train
        )
        validation_scores = score(model, validation)
        threshold = choose_threshold(validation_scores)
        metrics = evaluate(validation_scores, threshold)

        results[name] = metrics
        print(json.dumps(metrics, indent=2), flush=True)

        if metrics["average_precision"] > winner_ap:
            winner_ap = metrics["average_precision"]
            winner = (
                name, model, threshold, validation_scores,
            )

    name, model, threshold, validation_scores = winner

    # Versioned paths preserve the original baseline artifacts.
    output = ROOT / "outputs" / version
    model_path = ROOT / "models" / version
    output.mkdir(parents=True, exist_ok=True)
    model_path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        "feature_version": version,
        "selected_model": name,
        "selection_metric": "validation_average_precision",
        "threshold_objective": "maximum_validation_f1",
        "threshold": threshold,
        "validation_results": results,
        "model_path": str(model_path),
        "score_note": (
            "Class-weighted scores are not calibrated "
            "cancellation probabilities."
        ),
        "limitations": metadata["limitations"],
        "test_evaluated": False,
    }

    validation_scores["predicted_risk"] = (
        validation_scores["risk_score"] >= threshold
    ).astype(int)
    validation_scores.to_csv(
        output / "validation_predictions.csv", index=False
    )
    write_json(output / "model_metrics.json", report)

    print("\nSAVING SELECTED MODEL...", flush=True)
    model.write().overwrite().save(str(model_path))

    # Verify that the persisted pipeline produces the same scores.
    loaded = PipelineModel.load(str(model_path))
    sample = validation.orderBy("order_id").limit(10)
    before = score(model, sample)
    after = score(loaded, sample)

    if (
        before["order_id"].tolist() != after["order_id"].tolist()
        or not np.allclose(
            before["risk_score"],
            after["risk_score"],
            rtol=1e-9,
            atol=1e-12,
        )
    ):
        raise RuntimeError("Saved model reload verification failed")

    report["reload_verified"] = True

    print("MODEL SAVE + RELOAD VERIFIED")
    print(f"\nSELECTED MODEL: {name}")
    print(f"THRESHOLD: {threshold:.6f}")

    if evaluate_test:
        test = data.filter(F.col("split") == "test")
        labels = {
            row["label"]
            for row in test.select("label").distinct().collect()
        }
        if labels != {0.0, 1.0}:
            raise ValueError("Test must contain labels 0 and 1")

        test_scores = score(loaded, test)
        report["test_results"] = evaluate(
            test_scores, threshold
        )
        report["test_evaluated"] = True

        test_scores["predicted_risk"] = (
            test_scores["risk_score"] >= threshold
        ).astype(int)
        test_scores.to_csv(
            output / "test_predictions.csv", index=False
        )

        print("\nTEST RESULTS")
        print(json.dumps(report["test_results"], indent=2))
    else:
        print("Test evaluation deferred.")

    write_json(output / "model_metrics.json", report)
    print(f"\nRESULTS: {output}")
    print("TRAINING COMPLETE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--evaluate-test",
        action="store_true",
        help="Evaluate test data after selecting the model on validation.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("OrderShield-Training")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
            "2",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        main(spark, args.evaluate_test)
    finally:
        spark.stop()
