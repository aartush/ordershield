# OrderShield

**E-commerce order-risk research prototype — ranking canceled or unavailable orders for human review.**

Tushar Sharma and Aayush Juneja
M.Tech (1st Year), Netaji Subhas University of Technology (NSUT), New Delhi
tushar.sharma.pg26@nsut.ac.in · ayush.juneja.pg26@nsut.ac.in

OrderShield is an academic project for the Big Data Analytics course. It builds an
end-to-end pipeline over the public Olist Brazilian e-commerce dataset: Spark
validation and feature engineering, class-weighted Spark MLlib training, a saved
inference pipeline, a FastAPI service, a real Kafka + Spark Structured Streaming
demonstration path, and a Next.js research interface.

The honest headline: **the engineering pipeline works end to end, but predictive
quality is weak.** On the validation split the selected Random Forest reaches
average precision 0.00787, which is 1.89× the class prevalence. At the selected
threshold it catches 7 of 82 risky orders with 256 false alerts. The model is a
ranking aid for human reviewers, not an automated decision system.

---

## Table of contents

1. [What the project does](#1-what-the-project-does)
2. [Problem and research question](#2-problem-and-research-question)
3. [Dataset](#3-dataset)
4. [Repository structure](#4-repository-structure)
5. [Results](#5-results)
6. [How the model is built](#6-how-the-model-is-built)
7. [Running the project](#7-running-the-project)
8. [API reference](#8-api-reference)
9. [Frontend](#9-frontend)
10. [Limitations and responsible use](#10-limitations-and-responsible-use)
11. [Team](#11-team)
12. [Acknowledgements and licence](#12-acknowledgements-and-licence)

---

## 1. What the project does

- Validates nine Olist CSV tables (row counts, status distribution, missing values,
  duplicate keys).
- Aggregates payment records **before** joining, so each order stays one training row.
- Builds 11 order-level features (10 numeric, 1 categorical) in PySpark.
- Splits chronologically: train before 2018-04-01, validation Apr–Jun 2018,
  test from 2018-07-01.
- Trains class-weighted Logistic Regression and Random Forest inside a Spark MLlib
  `Pipeline`, and saves the whole fitted pipeline.
- Selects the model by validation **average precision**; selects the alert
  threshold by maximum validation F1.
- Serves analytics, scenario inference and threshold exploration through FastAPI.
- Replays historical feature rows through Kafka; a Spark Structured Streaming
  consumer scores them with the same saved model and writes to Kafka and SQLite.
- Exposes the results in a 10-route Next.js interface.

## 2. Problem and research question

Operations teams cannot inspect every order, and canceled/unavailable orders are
rare (1.26% of resolved orders in this dataset) but expensive. A useful model does
not remove the work — it changes the **order** in which the work is done.

> Can historical order, customer-location, timing and payment-snapshot features
> rank canceled or unavailable orders above delivered orders well enough to
> support human review prioritization?

**Target.** Positive = `canceled` or `unavailable`. Negative = `delivered`.
Unresolved statuses (`shipped`, `invoiced`, `processing`, `created`, `approved`)
are excluded because the snapshot gives no evidence of their final outcome.

**Not RTO prediction.** The dataset has no verified Return-to-Origin label. Naming
the target RTO would be a different, unverified claim.

## 3. Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(Brazil, 2016–2018). The repository ships the complete archive at
`backend/data/olist.zip` (≈43 MB, nine CSVs), and extracts only the two files the
API needs at request time:

| Table | Rows | Role in this project |
|---|---:|---|
| `olist_orders_dataset` | 99,441 | Order status, purchase timestamp — drives the target |
| `olist_customers_dataset` | 99,441 | Customer state, joined on `customer_id` |
| `olist_order_payments_dataset` | 103,886 | Payment value, installments, method — aggregated per order |
| `olist_order_items_dataset` | 112,650 | Not used in the model |
| `olist_order_reviews_dataset` | 99,224 | Not used |
| `olist_products_dataset` | 32,951 | Not used |
| `olist_sellers_dataset` | 3,095 | Not used |
| `olist_geolocation_dataset` | 1,000,163 | Not used |
| `product_category_name_translation` | 71 | Not used |

Order status counts: delivered 96,478; shipped 1,107; canceled 625;
unavailable 609; invoiced 314; processing 301; created 5; approved 2.
Resolved cohort = 97,712 orders, of which 1,234 are positive (1.2629%).

To rebuild from scratch, extract the archive into `backend/data/raw/`:

```powershell
cd backend\data
Expand-Archive -Path olist.zip -DestinationPath raw -Force
```

## 4. Repository structure

```text
OrderShield/
├── .gitattributes                     keeps model/data files byte-exact
├── .gitignore
├── README.md                          this file
├── backend/
│   ├── main.py                        FastAPI service (15 routes)
│   ├── app.py                         optional Streamlit dashboard
│   ├── live_view.py                   optional Streamlit live view
│   ├── start.ps1                      launcher: api | consumer | producer | dashboard | kafka
│   ├── compose.kafka.yaml             single-broker Kafka 4.1.2 in KRaft mode
│   ├── requirements-environment.txt   pinned Python dependencies
│   ├── README.md                      full working and demo guide
│   ├── LEARNING_GUIDE.md              line-by-line walkthrough of the pipeline
│   ├── src/
│   │   ├── validate_data.py           nine-table validation report
│   │   ├── build_features.py          payment aggregation, joins, features, split
│   │   ├── train_model.py             pipeline, class weights, selection, threshold
│   │   ├── kafka_producer.py          historical feature replay
│   │   └── kafka_consumer.py          Structured Streaming scoring
│   ├── data/
│   │   ├── olist.zip                  full Olist archive
│   │   ├── raw/                       olist_orders + olist_customers CSVs
│   │   └── processed/                 order_features.csv + feature_metadata.json
│   ├── models/payment_snapshot_v1/    saved Spark ML PipelineModel
│   ├── outputs/
│   │   ├── payment_snapshot_v1/       model_metrics.json, validation_predictions.csv
│   │   ├── data_validation.json       raw-table validation report
│   │   ├── model_metrics.json         LEGACY prior-model metrics (see §5)
│   │   └── test_predictions.csv       legacy prior-model test scores
│   ├── handoff/                       JSON snapshots used by the interface and tests
│   └── tools/hadoop-3.4.1/            Windows winutils/Hadoop DLLs for Spark
└── frontend/
    ├── app/                           10 routes (App Router)
    ├── components/                    layout, live monitor, playground, charts
    ├── lib/                           typed API client and navigation config
    ├── hooks/  stores/                React Query hooks, Zustand settings
    ├── tests/                         Vitest + Testing Library
    ├── package.json                   Next.js 16, React 19
    └── README.md                      frontend-specific guide
```

Runtime folders (`backend/outputs/live/`, `backend/checkpoints/`, `.venv/`,
`node_modules/`, `.next/`) are recreated by the tools and are git-ignored.
`.gitattributes` marks the saved model, data and bundled tools as binary, so Git
cannot rewrite their line endings and silently corrupt the Spark pipeline.

## 5. Results

All figures below are **validation** results from
`backend/outputs/payment_snapshot_v1/model_metrics.json` on 19,728 orders with
82 positives. Selection used validation average precision; the threshold was
chosen to maximize validation F1.

| Metric | Logistic Regression | Random Forest (selected) |
|---|---:|---:|
| Average precision (AP) | 0.006977 | **0.007871** |
| AP lift over prevalence | 1.679× | **1.894×** |
| ROC-AUC | 0.62459 | **0.63655** |
| Precision at threshold | 1.391% | 2.662% |
| Recall at threshold | 18.293% | 8.537% |
| F1 at threshold | 0.02586 | 0.04058 |
| Alert rate | 5.464% | 1.333% |
| Threshold | 0.55673 | 0.56881 |
| Confusion (TN / FP / FN / TP) | 18,583 / 1,063 / 67 / 15 | 19,390 / 256 / 75 / 7 |

Reading the selected model honestly:

- It ranks better than random: AP 0.00787 against a prevalence baseline of 0.00416.
- At the selected threshold it flags 263 orders, of which 7 are truly risky.
  256 are false alerts, and 75 of the 82 risky orders are missed.
- Logistic Regression trades far more alerts for higher recall; neither model is
  production-ready.

**These are not calibrated probabilities.** Scores come from a class-weighted
model and are ranking positions, not the probability that an order will be canceled.

**The current model has no test result.** `test_evaluated` is `false`. The dated
test split was already inspected during earlier work, so it is not a pristine
holdout. `backend/outputs/model_metrics.json` and `test_predictions.csv` hold the
**older, unversioned prior-model** numbers (test AP 0.016611, ROC-AUC 0.57239) and
are kept only for provenance. Do not mix them with the table above.

`reload_verified` is `true`: the saved pipeline was reloaded and reproduced its
validation predictions.

## 6. How the model is built

**Feature engineering** (`backend/src/build_features.py`):

1. `groupBy("order_id")` on payments — sum value, max installments, one indicator
   per method (`credit_card`, `boleto`, `voucher`, `debit_card`).
2. LEFT JOIN customers (state) and the aggregated payments onto resolved orders.
3. Guard: the join must not change the order count (line 147–148 raises otherwise).
4. Derive `purchase_hour`, `purchase_weekday`, `purchase_month`, `is_weekend`;
   label `delivered = 0`, `canceled/unavailable = 1`.
5. Chronological split at 2018-04-01 and 2018-07-01.

**Training** (`backend/src/train_model.py`): a single `Pipeline` of
`Imputer(median)` → `StringIndexer(state)` → `OneHotEncoder` → `VectorAssembler`
→ classifier. Positive rows are weighted by `negatives / positives` ≈ 65.43 in the
training split. Logistic Regression: `maxIter=100`, `regParam=0.1`.
Random Forest: `numTrees=80`, `maxDepth=6`, `minInstancesPerNode=20`, `seed=42`.
Because preprocessing is inside the pipeline, training and inference use identical
fitted transforms.

**Environment:** PySpark 4.0.3, `local[2]`, 4 shuffle partitions.

**Streaming** (`src/kafka_consumer.py`): 5-second processing-time trigger,
`maxOffsetsPerTrigger=100`, `startingOffsets=earliest`, `failOnDataLoss=true`,
checkpointed offsets, and `INSERT OR IGNORE` into SQLite keyed on `event_id` so
at-least-once delivery cannot duplicate rows.

## 7. Running the project

### Prerequisites

- Windows 10/11 with PowerShell (the launcher scripts are PowerShell)
- Docker Desktop with Linux containers (Kafka)
- Java 17 (Spark)
- Python 3.10–3.12
- Node.js 20.9+ (22 LTS recommended)
- ~1 GB free disk for dependencies

### One-time setup

```powershell
# From the repository root
cd backend
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-environment.txt
cd ..\frontend
Copy-Item .env.example .env.local
npm ci
```

### Start the services (five terminals)

```powershell
# 1 — Kafka
cd backend
docker compose -p ordershield -f compose.kafka.yaml up -d --wait

# 2 — API  (http://127.0.0.1:8000/docs)
cd backend
.\start.ps1 api

# 3 — Streaming consumer (wait for "STREAM READY")
cd backend
.\start.ps1 consumer

# 4 — Frontend (http://localhost:3000)
cd frontend
npm run dev

# 5 — Replay historical events (when demonstrating streaming)
cd backend
.\start.ps1 producer -Limit 30
```

`start.ps1` sets `PYSPARK_PYTHON`, `HADOOP_HOME` and `PATH` so Spark finds the
bundled Windows Hadoop DLLs. Run every command in a separate terminal and stop
each with `Ctrl+C`.

**Batch rebuild** (optional, regenerates features and the model):

```powershell
cd backend
.\.venv\Scripts\python.exe src\validate_data.py
.\.venv\Scripts\python.exe src\build_features.py
.\.venv\Scripts\python.exe src\train_model.py
```

`train_model.py` evaluates the test split only when `--evaluate-test` is passed;
the shipped artifacts were produced **without** it.

### Ports

| Port | Service |
|---:|---|
| 8000 | FastAPI |
| 9092 | Kafka (loopback only) |
| 3000 | Next.js |

The frontend pins 3000 in `package.json`. The backend's CORS list allows
`localhost`/`127.0.0.1` on both **3000 and 3301**, so an override still works,
but 3000 is the documented default throughout this repository.

## 8. API reference

Interactive docs at <http://127.0.0.1:8000/docs>. There is **no authentication**;
this is a localhost research prototype.

| Method | Route | Purpose |
|---|---|---|
| GET | `/health` | API, artifact and Kafka TCP checks |
| GET | `/api/overview`, `/api/stats`, `/api/samples` | Historical analytics and validation samples |
| GET | `/api/samples/{order_id}` | One stored validation sample |
| GET | `/api/model`, `/api/model/evaluation` | Saved metrics and an exploratory threshold curve |
| POST | `/api/predict` | Single-order Spark inference |
| POST | `/api/compare` | Two-scenario comparison (2–10 orders) |
| GET | `/api/live`, `/api/live/events` | Live snapshot and SSE stream (every 3 s) |
| POST/GET | `/api/replay` | Start and inspect the demo producer |
| GET | `/api/content`, `/api/options` | Workflow explanations and input options |

Example:

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/predict `
  -ContentType application/json -Body '{
    "customer_state":"SP","purchase_date":"2018-06-15","purchase_hour":12,
    "payment_total":150,"max_installments":1,"payment_methods":["credit_card"]}'
```

The first inference call starts Spark lazily and can take up to ~180 seconds.

## 9. Frontend

Ten routes: Overview, Applications, Dataset, Workflow, How It Works, Samples,
Playground, Performance, Live Stream, Documentation. It includes A/B scenario
inference, debounced threshold exploration, historical sample inspection, SSE
live monitoring and producer replay. Built with Next.js 16, React 19, TanStack
Query, Zustand, Tailwind 4 and Recharts.

```powershell
cd frontend
npm run typecheck
npm run lint
npm test
```

See `frontend/README.md` for details.

## 10. Limitations and responsible use

Stated plainly, because they are research-scope limits rather than bugs:

- **Weak predictive performance.** Precision 2.66% and recall 8.54% at the selected
  threshold mean most alerts are false and most risky orders are missed.
- **Validation selected both the model and the threshold**, so the reported metrics
  are optimistic for threshold choice. There is no untouched test estimate.
- **Assumed payment availability.** The payment snapshot carries no availability
  timestamp; the project assumes those fields exist at scoring time.
- **Labels cannot be fully reconstructed.** The snapshot lacks outcome-resolution
  timestamps.
- **Items excluded.** Item features were dropped because missing item records
  correlate strongly with the outcome.
- **No calibration.** Scores are class-weighted ranking scores, not probabilities.
- **Brazil 2016–2018.** This does not establish present-day market, Indian COD or
  RTO behaviour, and there is no verified RTO label.
- **Not benchmarked.** No throughput, latency-percentile, CPU or memory measurement
  exists. Spark runs on `local[2]`; Kafka is a single broker. The `448–453 s`
  queue-to-result values in `handoff/live.json` are five events from one captured
  run, not an average or a percentile.
- **Streaming is a replay.** The Kafka path carries historical feature rows, not a
  live merchant feed, and nothing retrains.
- **Security.** No authentication, TLS, audit logging or PII retention policy.
  Kafka listeners are PLAINTEXT and bound to loopback.
- **No cost or business impact is measured.** No automatic cancellation or customer
  penalty is implemented; humans review the queue.

## 11. Team

| Name | Programme | Email |
|---|---|---|
| Tushar Sharma | M.Tech, 1st Year, NSUT | tushar.sharma.pg26@nsut.ac.in |
| Aayush Juneja | M.Tech, 1st Year, NSUT | ayush.juneja.pg26@nsut.ac.in |

Course: Big Data Analytics, M.Tech (1st Year), Netaji Subhas University of
Technology, New Delhi.

## 12. Acknowledgements and licence

Dataset: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
used under its published Kaggle terms.

Built on open-source software: [Apache Spark](https://spark.apache.org/docs/)
(Spark SQL, MLlib, Structured Streaming), [Apache Kafka](https://kafka.apache.org/documentation/),
[scikit-learn](https://scikit-learn.org/stable/modules/model_evaluation.html),
[FastAPI](https://fastapi.tiangolo.com/), [Next.js](https://nextjs.org/docs),
[React](https://react.dev/), [Tailwind CSS](https://tailwindcss.com/) and
[Recharts](https://recharts.org/). The Windows build uses the
`hadoop-3.4.1` winutils bundle in `backend/tools/` so Spark can write to local
disk; that bundle is redistributed under the Apache License 2.0.

The project code is submitted for academic assessment. No licence is granted for
redistribution beyond coursework use; contact the authors for any other use.
