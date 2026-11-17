# OrderShield — Working & Demo Guide

Tushar Sharma and Aayush Juneja — M.Tech (1st Year), NSUT, New Delhi.

This guide is scoped to the `backend/` folder. See the repository root
[`README.md`](../README.md) for the project overview, results table, and the
five-terminal startup sequence.

OrderShield demonstrates an end-to-end e-commerce order-risk pipeline using
PySpark, Spark MLlib, Apache Kafka, Spark Structured Streaming, FastAPI and Next.js.

## 1. What the project does

The project:
- Reads historical Olist CSV files.
- Validates data, keys, missing values and order statuses.
- Uses Spark to join tables and build order-level features.
- Trains and evaluates Spark MLlib classifiers.
- Saves the selected model together with fitted preprocessing.
- Scores hypothetical orders through FastAPI.
- Publishes historical features as Kafka events.
- Processes those events using Spark Structured Streaming.
- Writes predictions to Kafka and a local SQLite database.
- Exposes analytics and live results to the frontend.

Target:
- Positive: canceled or unavailable
- Negative: delivered
- Other unresolved statuses are excluded from training.

This is order cancellation/unavailability research, not verified RTO prediction.

## 2. Project structure

OrderShield/
  backend/
    main.py
    start.ps1
    requirements-environment.txt
    compose.kafka.yaml
    src/
      validate_data.py
      build_features.py
      train_model.py
      kafka_producer.py
      kafka_consumer.py
    data/
      olist.zip
      raw/               olist_orders + olist_customers CSVs
      processed/         order_features.csv + feature_metadata.json
    models/
      payment_snapshot_v1/
    outputs/
      payment_snapshot_v1/
      data_validation.json
      model_metrics.json       legacy prior-model metrics
    handoff/                   JSON snapshots for the API and frontend tests
    tools/                     Windows Hadoop winutils bundle
    app.py
    live_view.py
  frontend/
    package.json
    .env.example
    ...

`checkpoints/`, `outputs/live/`, `.venv/`, `node_modules/` and `.next/` are created
at runtime and are git-ignored.

The Streamlit files are an optional earlier dashboard.
Next.js is the main frontend.

## 3. Prerequisites

- Docker Desktop running with Linux containers
- Java 17
- Working backend virtual environment
- Node.js/npm compatible with frontend/package.json
- Olist data and saved model already generated

Check:

```powershell
docker info --format '{{.OSType}}'
java -version
node --version
npm.cmd --version
```

Docker should report:

```text
linux
```

## 4. Everyday startup

Open separate PowerShell terminals for the long-running services.

### Terminal 1 — Start Kafka

```powershell
cd "C:\Projects\OrderShield"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 kafka
```

Kafka runs in Docker in the background.
This terminal can be reused after startup completes.

### Terminal 2 — Start FastAPI

```powershell
cd "C:\Projects\OrderShield"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 api
```

Keep this terminal running.

API:
http://127.0.0.1:8000

Interactive API documentation:
http://127.0.0.1:8000/docs

### Terminal 3 — Start Spark streaming consumer

```powershell
cd "C:\Projects\OrderShield"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 consumer
```

Wait for:

```text
STREAM READY | waiting for Kafka order events
```

Keep this terminal running.

### Terminal 4 — Start Next.js

```powershell
cd "C:\Projects\OrderShield\frontend"
npm.cmd run dev
```

Open:
http://localhost:3000

The frontend pins port 3000 in `package.json`, so `npm run dev` serves
http://localhost:3000. The backend's CORSMiddleware allows both 3000 and 3301
on `localhost` and `127.0.0.1`, so either port works if you override it.

### Frontend configuration

frontend/.env.local must contain:

```dotenv
NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000
```

Restart Next.js after changing this file.

If frontend dependencies are missing:

```powershell
cd "C:\Projects\OrderShield\frontend"

if (Test-Path "package-lock.json") {
    npm.cmd ci
} else {
    npm.cmd install
}
```

## 5. Batch-processing workflow

Historical CSVs
→ Spark validation
→ payment aggregation and customer join
→ order-level features
→ chronological split
→ Spark MLlib training
→ validation-based model selection
→ saved pipeline

The current model uses:
- Customer state
- Purchase hour
- Purchase weekday
- Purchase month
- Weekend indicator
- Payment total
- Maximum installments
- Credit-card, boleto, voucher and debit-card indicators

Payment records are aggregated before joining to avoid multiplying order rows.

Preprocessing is fitted only on training data:
- Median imputation
- State indexing
- One-hot encoding
- Feature-vector assembly

Classifiers:
- Logistic Regression
- Random Forest

Model selection:
- Highest validation average precision

Alert threshold:
- Maximum validation F1 for the selected model

Splits:
- Train: before April 1, 2018
- Validation: April–June 2018
- Test: July 2018 onward

## 6. Rebuilding the batch pipeline

Normal demonstrations do not require retraining.

Run these only when intentionally rebuilding the data/model:

```powershell
cd "C:\Projects\OrderShield\backend"

.\.venv\Scripts\python.exe src\validate_data.py
.\.venv\Scripts\python.exe src\build_features.py
.\.venv\Scripts\python.exe src\train_model.py
```

Run each step only after the previous step succeeds.

Stop API, streaming consumer and dashboards before retraining.
Restart them afterward to load the new artifacts.

Do not replace a model underneath an existing streaming checkpoint without
planning a separate model-version/checkpoint migration.

Important outputs:
- data/processed/order_features.csv
- data/processed/feature_metadata.json
- models/payment_snapshot_v1/
- outputs/payment_snapshot_v1/model_metrics.json
- outputs/payment_snapshot_v1/validation_predictions.csv

## 7. API prediction workflow

Frontend form
→ POST /api/predict
→ request validation
→ feature construction
→ saved Spark pipeline
→ model score
→ threshold comparison
→ JSON response
→ frontend result

Spark starts lazily on the first API prediction, so the first request can take longer.

The API uses one worker to reuse a single cached Spark session.
It does not retrain the model during prediction.

### Prediction test

Run in another PowerShell terminal while the API is running:

```powershell
$body = @{
    customer_state = "SP"
    purchase_date = "2018-06-15"
    purchase_hour = 12
    payment_total = 150
    max_installments = 1
    payment_methods = @("credit_card")
} | ConvertTo-Json

Invoke-RestMethod `
    -Uri "http://127.0.0.1:8000/api/predict" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body `
    -TimeoutSec 180 |
    ConvertTo-Json -Depth 8
```

The response includes:
- Selected model
- Feature version
- Model risk score
- Alert threshold
- Review decision
- Actual feature inputs
- Request duration
- Warnings

The score is not a calibrated cancellation probability.

## 8. Actual Kafka streaming workflow

Historical feature producer
→ ordershield.orders
→ Spark Structured Streaming
→ saved MLlib pipeline
→ ordershield.predictions
→ SQLite
→ FastAPI live endpoints
→ frontend

Kafka transports the events.
Spark reads them in micro-batches and performs inference.
The model does not retrain for every event.

### Publish a demonstration batch

With Kafka and the consumer running:

```powershell
cd "C:\Projects\OrderShield"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 producer -Limit 30
```

Producer output shows:
- Run identifier
- Order identifiers
- Kafka offsets
- Broker acknowledgements

Consumer output shows:

```text
BATCH ... | scored=... | alerts=...
```

Each producer run generates new event IDs.
The same historical order can therefore appear in multiple demonstration runs.

The producer does not transmit historical outcome labels.

## 9. Live outputs

Database:
backend/outputs/live/predictions.db

Consumer heartbeat:
backend/outputs/live/status.json

API-started producer log:
backend/outputs/live/api-producer.log

SQLite does not need a separate server.
The consumer writes it and FastAPI reads it.

### Read live API results

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/live?limit=5" |
    ConvertTo-Json -Depth 10
```

Important fields:
- consumer_active
- heartbeat_age_seconds
- total_events
- alerts
- scored_last_60_seconds
- latest

Old saved rows do not mean the consumer is currently active.

### Read database counts

```powershell
cd "C:\Projects\OrderShield"

.\backend\.venv\Scripts\python.exe -c "import sqlite3; c=sqlite3.connect('file:backend/outputs/live/predictions.db?mode=ro',uri=True); print(c.execute('SELECT COUNT(*), MAX(source_offset) FROM predictions').fetchone()); c.close()"
```

Zero alerts can be a valid result when all sampled scores are below the threshold.

## 10. SSE live updates

Endpoint:

```text
GET /api/live/events
```

The server sends a named event:

```text
snapshot
```

The frontend should use:

```javascript
source.addEventListener("snapshot", handler);
```

Each message is a full snapshot.
Replace the displayed snapshot rather than appending duplicate rows.

Close EventSource when leaving the page.

Queue-to-result duration includes:
- Kafka waiting
- Micro-batch scheduling
- Inference and output processing

It is not model-only inference latency.

## 11. API reference

| Endpoint | Purpose |
|---|---|
| GET /health | API, artifact and Kafka TCP checks |
| GET /api/options | States, methods and default input |
| GET /api/overview | Project summary |
| GET /api/stats | Historical analytics |
| GET /api/stats?state=SP | State-filtered analytics |
| GET /api/samples | Paginated validation samples |
| GET /api/samples/{order_id} | Sample details |
| GET /api/model | Saved model report |
| GET /api/model/evaluation | Validation threshold experiment |
| POST /api/predict | Single-order Spark inference |
| POST /api/compare | Multiple-scenario comparison |
| GET /api/live | Live snapshot |
| GET /api/live/events | SSE snapshots |
| POST /api/replay | Launch demonstration producer |
| GET /api/replay | API-started producer status |
| GET /api/content | Workflow and project explanations |

POST /api/replay returning 202 means launched, not successfully published.
Check replay status and live counts separately.

Threshold exploration does not change saved model settings.

## 12. Checkpoint recovery test

Checkpoint directory:
backend/checkpoints/payment_snapshot_v1

Do not delete this folder during a normal restart.

Verification:
1. Wait for the current producer run to finish.
2. Wait for the consumer to catch up.
3. Record the database count and last source offset.
4. Stop the consumer with Ctrl+C.
5. Restart the same consumer with the same checkpoint.
6. Confirm it resumes rather than rescoring the entire topic.
7. Publish five new events.
8. Verify five additional unique event records and new offsets.

A stable database count alone does not prove checkpoint recovery because
SQLite also deduplicates by event_id. Inspect consumer progress as well.

Kafka prediction output is at least once.
A retry can produce duplicate Kafka prediction messages.
SQLite deduplicates dashboard records using event_id.

## 13. Teacher demonstration sequence

1. Overview:
   Explain the research question and intended users.

2. Applications:
   Explain manual-review prioritization and review-capacity planning.

3. Dataset:
   Show status imbalance and the relational joins.

4. Workflow:
   Explain batch processing and Kafka streaming separately.

5. Playground:
   Submit an order and show actual API/Spark inference.

6. Performance:
   Show precision, recall, missed risks and false alerts.
   Move the experimental threshold to explain the trade-off.

7. Live Stream:
   Publish 10–30 events.
   Show producer offsets, consumer batch output and growing UI counters.

8. Documentation:
   Explain limitations and future improvements.

Suggested explanation:

“This project integrates PySpark batch processing, Spark MLlib inference,
Kafka event transport and Spark Structured Streaming. FastAPI exposes the
results to a Next.js interface. Historical orders simulate incoming events,
while Kafka transport and Spark processing are real.”

## 14. What is useful about the project?

Intended applications:
- Prioritize manual order reviews.
- Explore review workload versus risk detection.
- Investigate historical operational patterns.

Engineering value:
- Reproducible data processing
- Consistent training/inference transformations
- Model persistence and reload
- Event-based ingestion
- Streaming inference
- API/frontend integration
- Transparent evaluation

Reduced cancellations, business savings and production effectiveness
have not been demonstrated.

## 15. Limitations

- Current predictive performance is weak.
- The target is canceled/unavailable, not verified RTO.
- Scores are not calibrated probabilities.
- Payment availability at scoring time is assumed.
- Payment and outcome-resolution timestamps are insufficient for a full
  historical point-in-time reconstruction.
- Validation selected the model and threshold.
- The original baseline test set was already inspected.
- Scenario changes show associations, not causal effects.
- Data is from Brazil in 2016–2018.
- Indian COD/RTO and present-day performance are unverified.
- Spark runs in local mode, not a benchmarked multi-node cluster.

## 16. Troubleshooting

### Docker command works but engine connection fails

Open Docker Desktop and wait for the engine to start.

```powershell
docker info --format '{{.OSType}}'
```

### Kafka status/logs

```powershell
cd "C:\Projects\OrderShield\backend"
docker compose -p ordershield -f compose.kafka.yaml ps
docker compose -p ordershield -f compose.kafka.yaml logs --tail 60
```

Windows clients use:
127.0.0.1:9092

The external advertised listener must also use 127.0.0.1.

### Backend missing Python packages

```powershell
cd "C:\Projects\OrderShield\backend"
.\.venv\Scripts\python.exe -m pip install --no-cache-dir -r requirements-environment.txt
```

### NativeIO / Hadoop DLL error

Run services through backend/start.ps1.

Check:
backend/tools/hadoop-3.4.1/bin/hadoop.dll
backend/tools/hadoop-3.4.1/bin/winutils.exe

### Frontend CORS error

Confirm backend allows the exact frontend origin:
http://localhost:3000

Restart FastAPI after modifying CORS.

### Prediction loading for a long time

The first call initializes Spark.
Inspect the API terminal.
Do not repeatedly submit the same request while initialization is pending.

### Live count is not increasing

Check:
- Docker/Kafka is running
- Consumer terminal is running
- Consumer heartbeat is fresh
- Producer acknowledged the events
- Consumer has no exception

### File-in-use error during moving or cleanup

Stop API, consumer and dashboards first.
Do not manually delete SQLite db/wal/shm files while processes are running.

## 17. Shutdown

- Frontend: Ctrl+C
- API: Ctrl+C
- Consumer: Ctrl+C

Stop Kafka without deleting data:

```powershell
cd "C:\Projects\OrderShield\backend"
docker compose -p ordershield -f compose.kafka.yaml stop
```

Do not use `down -v` unless intentionally deleting Kafka storage.

## 18. Verification status

Previously observed:
- Dataset validation completed.
- Model training, saving and reloading completed.
- Actual FastAPI/Spark prediction returned successfully.
- Kafka publishing and 80 saved streaming predictions were observed.

After folder reorganization:
- Recheck API health and prediction.
- Recheck the frontend on port 3000.
- Recheck live streaming.
- Complete checkpoint recovery verification.

This guide describes expected behavior; it does not claim those final
post-reorganization checks have already passed.
