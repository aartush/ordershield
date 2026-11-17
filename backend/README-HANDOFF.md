# OrderShield Backend Handoff

Tushar Sharma and Aayush Juneja — M.Tech (1st Year), NSUT, New Delhi.

This file documents the API contract that the Next.js frontend consumes. The
JSON files in `handoff/` are captured examples used by the frontend test
fixtures; `handoff/openapi.json` is the authoritative schema.

## Project layout

OrderShield/
  backend/main.py
  backend/src/
  backend/data/
  backend/models/
  backend/outputs/
  backend/handoff/
  backend/tools/
  frontend/

`backend/main.py` resolves `ROOT` to the `backend/` directory, so data, model and
output paths resolve inside `backend/`. Do not move `main.py` or change those
paths.

## Run the backend

```powershell
cd backend
.\.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1
```

API: http://127.0.0.1:8000
Swagger: http://127.0.0.1:8000/docs

## Frontend connection

NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000

Run Next.js on port 3000. CORS allows:
http://localhost:3000
http://127.0.0.1:3000
http://localhost:3301
http://127.0.0.1:3301

## Contract

`handoff/openapi.json` is authoritative.
The other handoff JSON files are captured examples, not runtime fixtures.

GET /api/live/events is SSE.
Listen for the named "snapshot" event using addEventListener.
Each message contains a full live snapshot, not an incremental delta.

POST /api/replay launches the existing Kafka producer.
GET /api/replay reports that API process's latest replay status.
A 202 response means started, not successfully published.

The frontend must not claim that Kafka or the consumer is running
based solely on old counters or an API response from a previous run.

## Scientific scope

Target: canceled/unavailable vs delivered, not verified RTO.
Risk scores are not calibrated cancellation probabilities.
Payment availability at scoring time is assumed.
Current predictive quality is limited.
Validation selected the model and threshold.
The earlier baseline test set has already been inspected.

Spark uses local mode. Kafka transports historical feature replay.
Real-time ingestion does not improve model accuracy by itself.

## Frontend status

The Next.js frontend is included in `frontend/` and is complete. It consumes the
endpoints and schema described above; no endpoint is mocked or fabricated.
