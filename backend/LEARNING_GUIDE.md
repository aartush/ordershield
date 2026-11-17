# OrderShield — Learning & Viva Guide

## How to study this guide

This guide explains the project, its components, design decisions and limitations.

Study in this order:
1. Problem and intended application
2. Dataset and target
3. Spark batch processing
4. Feature engineering
5. Model training and evaluation
6. Kafka and Structured Streaming
7. FastAPI and frontend
8. Demonstration and viva questions

Do not memorize only commands. Understand what enters each stage,
what happens there and what comes out.

---

## 1. Explain the project in 30 seconds

“OrderShield is an e-commerce order-risk research prototype. It uses
PySpark to process historical order data and Spark MLlib to train a model
that scores cancellation or unavailability risk. Kafka transports simulated
incoming order events, and Spark Structured Streaming scores them using
the saved model. FastAPI exposes analytics and predictions to a Next.js
frontend.”

Important:
The incoming events are historical feature replays.
The Kafka transport and Spark processing are actual working components.

---

## 2. What problem does it address?

An e-commerce operations team may not have enough time to manually review
every order.

A risk-scoring system could help prioritize which orders deserve attention.

Intended applications:
- Manual review prioritization
- Review-capacity planning
- Historical operational investigation

Example:
A team receives 1,000 orders but can review only 50.
A useful model could rank orders to help decide which 50 to inspect.

Our project demonstrates the engineering workflow.
It has not proven that its current model can make that prioritization effective.

Reduced cancellations, financial savings and production usefulness
have not been established.

---

## 3. What is the prediction target?

The target is binary:

- label = 1: canceled or unavailable
- label = 0: delivered

Other statuses are excluded from training:
- shipped
- invoiced
- processing
- created
- approved

Why exclude them?
Their final outcome is unresolved in the snapshot.
Treating them as successful would assign labels without evidence.

Why not call this RTO prediction?
RTO means Return to Origin.
Canceled/unavailable is not the same event as a shipped parcel returning.
The dataset does not establish a suitable verified RTO label for this model.

---

## 4. Dataset

Source:
Brazilian E-Commerce Public Dataset by Olist

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Historical period:
2016–2018

Observed source counts:
- Orders: 99,441
- Customers: 99,441
- Order items: 112,650
- Payments: 103,886
- Reviews: 99,224
- Products: 32,951
- Sellers: 3,095
- Geolocation: 1,000,163
- Category translation: 71

These are counts from the earlier validation run.
Check current validation output if source files change.

### Order statuses observed

- delivered: 96,478
- canceled: 625
- unavailable: 609
- other unresolved statuses: 1,729

Resolved model cohort:
96,478 + 625 + 609 = 97,712 orders

Positive cases:
625 + 609 = 1,234

Positive share:
Approximately 1.26% of the resolved cohort.

---

## 5. Why are there multiple tables?

Relational datasets split entities into different tables.

Examples:
- Orders describe purchases and order status.
- Customers describe customer location.
- Payments describe payment records.
- Items describe products within orders.

Join keys:
- Orders → customers: customer_id
- Orders → payments: order_id

The current model uses:
- Orders
- Customers
- Aggregated payments

The existence of nine CSVs does not mean all nine feed the model.

---

## 6. Why aggregate before joining?

One order can have multiple payment records.

Suppose:
- One order has 2 items.
- The same order has 3 payments.

Joining raw items and payments directly by order_id can produce 6 rows:
2 × 3.

This can inflate sums and duplicate order information.

Our approach:
1. Aggregate payments to one row per order.
2. Join the aggregated result with orders.
3. Join customer state.
4. Verify that joins did not change the expected order count.

This is protection against join fan-out.

---

## 7. What is Apache Spark?

Apache Spark is a data-processing engine that supports distributed computation.

It provides:
- DataFrames
- SQL
- Aggregations and joins
- Machine-learning pipelines
- Streaming processing

Our demonstration uses local mode:

local[2]

This means Spark runs locally with two worker threads.
It does not mean two physical machines.

We have not benchmarked a multi-node cluster.

---

## 8. What is PySpark?

PySpark is the Python API for Apache Spark.

We write transformations in Python.
Spark's execution engine performs the underlying processing.

In this project, PySpark:
- Reads CSV files
- Filters resolved orders
- Groups payment records
- Joins tables
- Builds feature columns
- Trains MLlib pipelines
- Reads Kafka events
- Performs model inference

PySpark is not a separate database.
It is an interface to Spark.

---

## 9. Spark DataFrame versus pandas DataFrame

Spark DataFrame:
- Uses Spark's execution engine
- Can process data across partitions and machines
- Supports lazy transformations
- Supports distributed joins and aggregations

pandas DataFrame:
- Primarily operates in one Python process
- Convenient for smaller analysis and API response preparation

Our project uses both:
- Spark for ETL, MLlib and streaming inference
- pandas for manageable result analysis and API response preparation

Using pandas in supporting steps does not mean all processing is distributed.

---

## 10. Lazy evaluation

Spark often builds an execution plan before running computation.

Transformations describe operations:
- select
- filter
- withColumn
- groupBy
- join

Actions trigger computation:
- count
- collect
- show
- writes

Example:
Filtering a DataFrame describes the desired subset.
Calling count asks Spark to execute the plan and return a result.

Why useful?
Spark can optimize the plan before execution.

---

## 11. Partitions, driver and execution

Driver:
- Runs the application logic
- Builds plans
- Coordinates execution

Executors:
- Execute Spark tasks and hold processing data

Partitions:
- Pieces of a distributed dataset

In local mode, these roles execute on one computer.
Do not describe this demonstration as a tested multi-machine deployment.

---

## 12. What is a shuffle?

A shuffle moves data between partitions.

Common causes:
- groupBy
- Some joins
- Global sorting

Example:
To sum all payment values for one order, records for that order
must be brought together.

Shuffles can involve network, memory and disk overhead.

Our local configuration uses a small shuffle-partition count
because the demonstration dataset and machine are modest.

---

## 13. Batch-processing pipeline

Historical CSVs
→ validation
→ resolved-order filtering
→ payment aggregation
→ customer/payment joins
→ feature engineering
→ chronological split
→ model training
→ validation
→ saved pipeline

Batch means processing a finite historical dataset.
It is not the same as continuously reading Kafka events.

---

## 14. Features used by the current model

### Customer state
Brazilian state code, such as SP or RJ.

### Purchase hour
Hour of day: 0–23.

### Purchase weekday
Spark dayofweek convention:
- Sunday = 1
- Monday = 2
- ...
- Saturday = 7

### Purchase month
Month: 1–12.

### Weekend indicator
1 for Saturday/Sunday, otherwise 0.

### Payment total
Sum of payment values for the order.

### Maximum installments
Maximum installment count across its payment records.

### Payment-method indicators
Separate binary columns for:
- credit_card
- boleto
- voucher
- debit_card

Multiple indicators may be 1 because an order may have multiple payment methods.

Currency is BRL, not INR.

---

## 15. What is feature engineering?

Feature engineering converts available raw attributes into model inputs.

Examples:
- Timestamp → purchase hour
- Timestamp → weekday
- Weekday → weekend indicator
- Payment rows → total payment
- Payment types → binary indicators

A useful feature must be available at the intended prediction stage.

A column that predicts the outcome well is not automatically legitimate.

---

## 16. Data leakage

Data leakage happens when training or prediction uses information
that would not legitimately be available at scoring time.

Obvious examples for an early order-risk model:
- Final order status as an input
- Actual customer delivery timestamp
- Reviews written after delivery

These can reveal the outcome rather than predict it.

In our project:
- Final status creates the label only.
- Actual delivery timestamps and reviews are excluded from predictors.
- IDs are excluded from the feature vector.

Remaining limitation:
Payment records have no event timestamps, so payment availability at the
chosen scoring stage is assumed, not verified.

A chronological split alone does not eliminate every possible leakage source.

---

## 17. Why exclude item features?

The inspected snapshot showed:
603 of 609 unavailable orders had no item records.

A model could learn that recording pattern rather than an operational signal
available when the order was placed.

Therefore, item features were excluded from the current model.

This does not mean item/product features can never be useful.
They require a defensible availability definition and data-quality analysis.

---

## 18. Preprocessing pipeline

The Spark pipeline contains:

1. Median imputation
2. State indexing
3. One-hot encoding
4. Vector assembly
5. Classifier

### Median imputation
Replaces missing numeric values with medians learned from training data.

### String indexing
Maps state strings to categorical indices.

### One-hot encoding
Represents categorical states using indicator-vector positions.

Why not use the state index as an ordinary number?
That could imply an artificial numeric ordering between states.

### Vector assembler
Combines numeric and encoded categorical columns into one feature vector.

All fitted preprocessing is learned from training data only.

---

## 19. Models

### Logistic Regression
A linear classification model that combines features into a score
through a logistic formulation.

Useful as:
- A baseline
- A relatively simple comparator

### Random Forest
An ensemble of decision trees.

Useful for:
- Nonlinear patterns
- Feature interactions

The selected payment-feature model was Random Forest based on
validation average precision.

Do not claim Random Forest is universally best.
It won this specific comparison under the chosen setup.

---

## 20. Why class weighting?

Risky orders are rare.

Without suitable handling, learning can be dominated by the majority class.

The project assigns greater weight to positive training cases:
negative training count / positive training count.

This increases their contribution to the training objective.

Class weighting:
- Does not create new observations
- Does not guarantee high recall
- Does not solve weak features
- Does not guarantee calibrated probabilities

---

## 21. Chronological splitting

Training:
Before April 1, 2018

Validation:
April 1 through June 30, 2018

Test:
July 1, 2018 onward

Observed counts:

| Split | Delivered | Risky |
|---|---:|---:|
| Train | 64,322 | 983 |
| Validation | 19,646 | 82 |
| Test | 12,510 | 169 |

Why chronological?
It approximates training on older purchases and predicting later purchases.

Limitation:
Outcome-resolution timestamps are absent.
We cannot fully reconstruct exactly when historical labels became available.

---

## 22. Training, validation and test roles

Training:
Fit preprocessing and model parameters.

Validation:
Choose the model and decision threshold.

Test:
Estimate performance after choices are fixed.

Our project:
- Selected the model by validation average precision.
- Selected its threshold by validation F1.
- Previously inspected the original baseline test results.

Therefore, the original test set is no longer a pristine unseen final holdout.

Validation results are selection results, not an unbiased final estimate.

---

## 23. Classification outcomes

True Positive:
Risky order correctly flagged.

False Positive:
Delivered order incorrectly flagged.

False Negative:
Risky order missed.

True Negative:
Delivered order correctly below threshold.

Operational interpretation:
- False positives create unnecessary review work.
- False negatives leave risky orders undetected.

A useful system must consider both.

---

## 24. Evaluation metrics

Precision = TP / (TP + FP)

Question:
Of all alerts, how many were actually risky?

Recall = TP / (TP + FN)

Question:
Of all risky orders, how many were detected?

F1 = 2 × precision × recall / (precision + recall)

Question:
How does this threshold balance precision and recall?

ROC-AUC:
Measures ranking discrimination across thresholds using
true-positive versus false-positive rates.

Average precision:
Summarizes precision–recall ranking behavior.
It is not identical to trapezoidal PR-AUC.

Alert rate:
Fraction of orders flagged for review.

---

## 25. Why accuracy is misleading here

Approximately 98.74% of resolved orders are delivered.

A classifier that predicts “delivered” for every order would achieve
approximately 98.74% accuracy while detecting zero risky orders.

Therefore:
High accuracy alone is insufficient.

We inspect:
- Average precision
- Precision
- Recall
- Confusion matrix
- Review workload

---

## 26. Actual payment-model validation results

Observed Random Forest validation results:

- Orders: 19,728
- Risky orders: 82
- Average precision: approximately 0.00787
- ROC-AUC: approximately 0.63655
- Precision: approximately 2.66%
- Recall: approximately 8.54%
- F1: approximately 0.04058
- Threshold: approximately 0.568813

Confusion matrix:
- TN: 19,390
- FP: 256
- FN: 75
- TP: 7

Interpretation:
The model detected 7 of 82 risky orders and generated 256 false alerts.

The engineering pipeline works, but predictive performance is weak.

These results describe the earlier observed model version.
Current model_metrics.json is authoritative after retraining.

---

## 27. Model score versus probability

The API returns risk_score on a 0–1 scale.

Because the classifier was class-weighted and not calibrated,
the score must not be described as an actual cancellation probability.

Example:
score = 0.54 does not establish a 54% chance of cancellation.

Use:
“Model risk score: 0.54.”

Do not use:
“Cancellation probability: 54%.”

---

## 28. Decision threshold

The threshold converts a score into a review decision.

If:
risk_score >= threshold

Then:
review suggested

Otherwise:
below threshold

Below threshold does not guarantee delivery.

Changing the threshold changes:
- Precision
- Recall
- Number of alerts
- Number of missed risks

It does not retrain the model.

The performance-page slider is exploratory and does not modify saved settings.

---

## 29. Model saving and loading

The saved Spark pipeline contains preprocessing and classifier together.

Why useful?
Inference uses the same fitted transformations as training.

Otherwise:
- Category mappings might differ
- Missing-value handling might differ
- Feature ordering might differ

We verified saving and reloading by comparing prediction scores
before and after persistence.

---

## 30. What is Kafka?

Apache Kafka is an event-streaming platform.

In our project:
- Producer publishes order events.
- Broker stores them in a topic.
- Spark reads those events.
- Prediction events are published to another topic.

Kafka is not our prediction model.
Kafka does not decide whether an order is risky.

---

## 31. Kafka concepts

Producer:
Publishes events.

Broker:
Kafka server that stores and serves topic records.

Topic:
Named stream of records.

Partition:
An ordered log within a topic.

Offset:
A record's position within one partition.

Consumer:
Reads topic records.

Consumer group:
A coordination mechanism for consumers that share partition consumption.

Important distinction:
Spark Structured Streaming tracks source progress using its checkpoint.
Do not equate Spark progress with manually managed Python consumer commits.

---

## 32. Kafka configuration in this project

Broker address for Windows clients:
127.0.0.1:9092

Topics:
- ordershield.orders
- ordershield.predictions

Local topics use:
- One partition
- Replication factor 1

The broker runs in KRaft combined mode:
broker + controller.

No separate ZooKeeper service is used.

This is a local demonstration setup, not a highly available Kafka cluster.

---

## 33. Why advertised listeners matter

A Kafka client first connects to a bootstrap address.

The broker then returns metadata containing addresses for further connections.

If the broker advertises an address the client cannot reach,
bootstrap may succeed while subsequent operations fail.

We observed:
- 127.0.0.1 bootstrap worked.
- The broker advertised localhost.
- Subsequent client connections timed out.

Using 127.0.0.1 consistently for the external advertised listener fixed it.

---

## 34. What does the producer send?

The producer reads validation feature rows.

It publishes:
- Unique event ID
- Feature version
- Order ID
- Customer state
- Emission timestamp
- Numeric model features

It does not publish:
- Historical outcome label as a model input

Each run creates new event IDs.
The same historical order can be replayed in multiple demonstration runs.

An acknowledgement means Kafka accepted the event.
It does not prove Spark has already scored it.

---

## 35. What is Structured Streaming?

Spark Structured Streaming is a streaming-processing API built around
DataFrame-style operations.

Our consumer:
1. Reads Kafka records
2. Parses JSON using a schema
3. Checks required fields and feature version
4. Applies the saved model
5. Publishes prediction events
6. Writes deduplicated dashboard records

The source is continuous, but this implementation processes it
in micro-batches.

---

## 36. What is a micro-batch?

A micro-batch is a finite group of records processed as one streaming step.

Our trigger interval is approximately five seconds.

This is a scheduling interval, not a guarantee that every result arrives
within exactly five seconds.

Actual delay depends on:
- Startup
- Kafka backlog
- Spark scheduling
- Model processing
- Output writes

A batch ID identifies a processing batch.
Kafka offset identifies a record position in a partition.

They are different concepts.

---

## 37. foreachBatch

foreachBatch lets us apply custom processing to each micro-batch DataFrame.

In this project it:
- Runs model inference
- Sends prediction events through the Python Kafka producer
- Writes results into SQLite

The model computation remains Spark MLlib inference.

Publishing output using the Python producer is an implementation choice;
it is not the Spark native Kafka sink.

---

## 38. Prediction outputs

Kafka output:
ordershield.predictions

Local output:
outputs/live/predictions.db

SQLite stores:
- Event ID
- Order ID
- Risk score
- Alert
- Emission time
- Scoring time
- Source partition
- Source offset
- Batch ID

SQLite is an embedded database.
It does not require a separate database-server process.

---

## 39. Delivery guarantees and deduplication

Kafka prediction output is at least once.

If a batch retries after some predictions were published,
duplicate prediction messages can appear in the output topic.

SQLite uses event_id as a primary key and INSERT OR IGNORE.

This prevents repeated dashboard records for the same event ID.

It does not make the entire Kafka + SQLite pipeline an atomic
exactly-once transaction.

Do not claim end-to-end exactly-once delivery.

---

## 40. Checkpointing

Spark checkpoints store streaming progress and metadata.

Our checkpoint directory is:
backend/checkpoints/payment_snapshot_v1

With the same valid checkpoint, the consumer should resume from its
recorded progress.

Do not delete the checkpoint for an ordinary restart.

Verification requires:
- Recording previous progress
- Restarting the consumer
- Inspecting resumed processing
- Publishing new events
- Confirming new offsets are processed

A stable SQLite count alone is insufficient proof because SQLite
also deduplicates repeated event IDs.

---

## 41. Event time and processing time

Our Kafka events contain:
- emitted_at: when the replay producer sent the event
- scored_at: when scoring/output handling produced the prediction record

The historical purchase timestamp is older and serves feature construction.

Queue-to-result delay:
scored_at - emitted_at

This includes waiting and scheduling.
It is not a pure model inference benchmark.

---

## 42. FastAPI's role

FastAPI provides an HTTP interface to the project.

It exposes:
- Analytics
- Samples
- Model reports
- Threshold evaluation
- Spark inference
- Live streaming snapshots
- Replay controls
- Project content

It does not replace Spark.
It connects frontend requests to existing processing and artifacts.

The API starts Spark lazily on the first prediction request.

One API worker is used to reuse one cached Spark session.

---

## 43. Request validation

Pydantic validates prediction inputs.

Examples:
- Purchase hour must be 0–23.
- Payment total cannot be negative.
- State must be a supported Brazilian state code.
- At least one payment method is required.
- Unknown extra fields are rejected.

Invalid inputs produce an API validation error rather than being
silently accepted.

---

## 44. CORS

CORS controls which browser origins can read API responses.

An origin includes:
- Protocol
- Host
- Port

These are different origins:
- http://localhost:3301
- http://localhost:3000
- http://127.0.0.1:3301

Our frontend uses port 3000 (pinned in `package.json`).
The backend must allow the exact frontend origin; it also allows 3301.

CORS is not authentication.

---

## 45. SSE and live updates

Server-Sent Events provide server-to-browser updates over HTTP.

Endpoint:
GET /api/live/events

Named event:
snapshot

Each message contains the current full live snapshot.

The frontend:
- Opens EventSource
- Listens for snapshot events
- Replaces displayed server state
- Closes the connection when no longer needed

SSE transports updates from API to browser.
It is separate from Kafka's internal event transport.

---

## 46. Next.js frontend

The frontend is the presentation and interaction layer.

It should:
- Display actual API data
- Explain the architecture
- Allow sample exploration
- Submit prediction requests
- Compare scenarios
- Show evaluation trade-offs
- Display live stream progress

Animations explain the system.
They must not pretend that a stopped service is running.

The browser does not directly execute Spark jobs.
It requests inference from FastAPI.

---

## 47. Entire system in one explanation

Batch:
CSV → Spark ETL → features → MLlib training → saved model

Interactive inference:
Browser → FastAPI → saved Spark pipeline → JSON → browser

Streaming:
Historical feature producer → Kafka → Spark Structured Streaming
→ saved model → Kafka predictions + SQLite → FastAPI → browser

Training and inference are separate processes.

Streaming inference does not automatically update model parameters.

---

## 48. Why the model may be weak

Possible reasons:
- Limited predictive signal in available features
- Rare positive cases
- Different positive prevalence across periods
- Missing useful operational context
- Snapshot-data limitations

Do not “fix” weak results by introducing outcome leakage.

Potential future work:
- Verified scoring-time feature availability
- Better timestamped operational features
- Independent evaluation
- Carefully evaluated model improvements
- Calibration if probability estimates are needed
- Review-capacity or cost-based threshold selection

These are future directions, not completed improvements.

---

## 49. What can we honestly claim?

We can demonstrate:
- Spark batch processing
- MLlib training and saved inference
- Kafka publishing and offsets
- Structured Streaming micro-batches
- API integration
- Stored prediction outputs
- An interactive frontend, after local verification

We cannot claim:
- Proven financial savings
- Production-ready prediction quality
- Verified RTO prediction
- Calibrated cancellation probabilities
- Tested multi-node scalability
- End-to-end exactly-once output
- Current-world generalization
- Completed recovery verification without observing the test

---

## 50. Teacher demonstration

1. Explain the business problem.
2. Show the dataset and class imbalance.
3. Explain the batch diagram.
4. Show a real sample and its features.
5. Submit a fresh API prediction.
6. Explain the score and threshold.
7. Show the confusion matrix and limitations.
8. Publish a Kafka demonstration batch.
9. Show producer offsets and consumer batch output.
10. Show prediction counters increasing in the UI.

Do not train the model live unless specifically required.
Use the existing saved model for a reliable presentation.

---

## 51. Common viva questions

### Why use Spark for this dataset?
To demonstrate distributed-processing APIs and integrated ML/streaming
engineering. This local dataset alone does not require or prove
large-cluster performance.

### Is PySpark the same as Kafka?
No. PySpark processes data; Kafka transports and stores events.

### Does Kafka predict the result?
No. The saved MLlib model predicts.

### Is the model retrained for every event?
No. The saved model is reused for inference.

### Why do multiple events share one batch?
Structured Streaming groups records into micro-batches.

### Why exclude reviews?
They are post-outcome information for the intended early scoring stage.

### Why save preprocessing with the model?
To keep training and inference transformations consistent.

### Why not call the score a probability?
Class weighting and lack of calibration mean the score is not a verified
real-world cancellation probability.

### Why does the same order appear again?
Each producer run replays historical inputs with new event IDs.

### Does zero alerts mean streaming failed?
No. All sample scores may be below the threshold.

### Does producer completion mean scoring completed?
No. Kafka acknowledgement and downstream consumer processing are separate.

### Why SQLite?
It is a lightweight local store suitable for this demonstration.
It avoids running a separate database server.

### What would production need?
Stronger data and evaluation, security, robust operations, monitoring,
capacity testing and clearly tested failure/recovery behavior.

---

## 52. Commands to understand and practice

All commands below start from:
C:\Projects\OrderShield

Start Kafka:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 kafka
```

Start API in another terminal:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 api
```

Start consumer in another terminal:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 consumer
```

Publish 10 events:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\backend\start.ps1 producer -Limit 10
```

Start frontend in another terminal:

```powershell
cd "C:\Projects\OrderShield\frontend"
npm.cmd run dev
```

Frontend:
http://localhost:3000

API documentation:
http://127.0.0.1:8000/docs

Read live output:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/live?limit=3" |
    ConvertTo-Json -Depth 10
```

---

## 53. Questions you should answer without notes

- What exactly is the label?
- Which tables feed the current model?
- Why aggregate payments before joining?
- Which features are excluded and why?
- What is fitted only on training data?
- What does class weighting do?
- Why is accuracy misleading?
- How are model and threshold selected?
- Why is the score not a probability?
- What does Kafka contribute?
- What is a partition offset?
- What does a micro-batch do?
- What is stored in the checkpoint?
- What does FastAPI do?
- What does the frontend do?
- Which claims remain unproven?

---

## 54. Prompt to give ChatGPT with this file

Act as my patient project tutor and viva examiner.

I need to understand this OrderShield project well enough to explain
it confidently to my teacher.

Use this guide and any attached source code as your evidence.
Do not invent implementation details.
If the guide and current source code differ, inspect the code and
explain the difference.

Teach me in Hinglish, keeping technical terms in English.

Start with the big picture, then teach:
1. Problem and application
2. Dataset and labels
3. Spark and PySpark
4. Joins and feature engineering
5. Model pipeline
6. Class imbalance and evaluation
7. Kafka
8. Structured Streaming
9. Checkpointing and output guarantees
10. FastAPI
11. Next.js integration
12. Demo and viva

For each topic:
- Explain in simple language.
- Connect it to this project's implementation.
- Give one small example.
- Explain important code when source files are attached.
- Tell me what I should say to the teacher.
- Ask me two short open-ended questions.
- Correct misunderstandings before moving on.

Do not dump every topic in one answer.
Teach one topic at a time.

Separate:
- Implemented behavior
- Observed test evidence
- Assumptions
- Limitations
- Future work

Never make weak model results sound strong.
Help me defend the engineering honestly.

After teaching, conduct a viva practice session with increasingly
difficult questions and help improve my answers.
