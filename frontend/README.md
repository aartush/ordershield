# OrderShield frontend

Tushar Sharma and Aayush Juneja — M.Tech (1st Year), NSUT, New Delhi.

This guide covers the Next.js interface only. See the repository root
[`README.md`](../README.md) for the backend, pipeline and results.

A local Next.js research workspace for the OrderShield FastAPI service.
Ten routes: Overview, Applications, Dataset, Workflow, How It Works, Samples,
Playground, Performance, Live Stream and Documentation. Includes responsive
navigation, presentation cues, motion settings, A/B inference, exploratory
thresholds, historical sample inspection, SSE monitoring and producer replay.

## Requirements

Node.js 20.9+ (22 LTS recommended), npm, and the `backend/` service in this repository.
The lockfile pins the installed versions. `npm ci` reproduces this installation.
Google Fonts are fetched at build time through next/font/google; allow access to
Google's font servers during build. Bangers is selective display typography;
Montserrat is the interface font and JetBrains Mono is used for technical text.

## Windows installation

Clone the repository, then in PowerShell:

```powershell
cd <repository>\frontend
Copy-Item .env.example .env.local
npm ci
npm run dev
```

Open http://localhost:3000. Port 3000 is explicit: a busy port causes failure
rather than silently switching to a CORS-incompatible port. Stop the other
process first. `.env.local` is git-ignored, so run the `Copy-Item` step after
cloning.

Start the API in a separate PowerShell terminal:

```powershell
cd <repository>\backend
.\.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1
```

The API imports `main.py` from inside `backend/`, and resolves its data, model
and output paths relative to that folder.

`NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000` is supplied in `.env.example`.
Restart Next.js after changing `.env.local`; production builds embed public env
values and must be rebuilt after changes. Keep the backend's data, saved model,
native Hadoop tools and checkpoints where they are.

Production:

```powershell
npm run build
npm start
```

## Check the integration

1. Open http://127.0.0.1:8000/health and /docs. Confirm required artifacts exist.
2. Overview should show actual API counts; disconnected screens show errors,
   never captured response fallback data.
3. Playground: select single scenario and predict the defaults. The first call
   starts Spark lazily; allow up to 180 seconds. Compare sends
   `{ "orders": [scenarioA, scenarioB] }`. Edit a payment afterward: old results
   must be visibly stale. A browser round trip and backend total call duration
   are displayed separately, neither described as model-only latency.
4. Samples: inspect an order and load it into A. Missing payment amounts or
   installments remain empty, requiring explicit input. Scores are sorted
   descending by the server; filters and pagination are server-side.
5. Performance: move the threshold. Requests debounce for 300 ms, superseded
   reads abort, and the saved model threshold is never changed.
6. Start Kafka and the Spark consumer with `backend\start.ps1 kafka` and
   `backend\start.ps1 consumer`. Wait for `STREAM READY`. On Live Stream, verify
   a fresh heartbeat rather than trusting saved counts.
7. Set a small replay, e.g. 5 events / 1 second. Start it, then observe producer
   running/completed/failed status separately from unique scored counts.
   202 means the producer started; consumer catch-up may take longer.
8. Confirm new named `snapshot` events every ~3 seconds in browser Network.
   Each full snapshot replaces the previous one. Navigate away to close SSE.
9. Check the mobile drawer, keyboard navigation, presentation sequence,
   diagrams in vertical mobile layout and OS/manual reduced motion.

## Troubleshooting

- Backend unavailable: confirm the API terminal is running and the URL is exact.
  No response at 127.0.0.1:8000 here is expected until you start your local API.
- CORS: only localhost:3000 and 127.0.0.1:3000 are allowed by the supplied backend.
  Keep port 3000. Do not use a LAN hostname without backend authorization/config.
- 503: inspect API logs for missing files, native Spark setup or inference errors.
- 409 replay: another replay process is running; inspect status before retrying.
- SSE reconnecting/stale: check API availability, browser Network and proxy
  buffering. Old saved predictions do not prove the consumer is running.
- Empty live database: start the actual consumer, then replay features. Producer
  completion and database count are different observations.
- First inference timeout: check the API terminal before manually retrying.
  Aborting the browser request does not guarantee backend Spark work is canceled.
- Requests: read timeout 20 seconds; inference timeout 180 seconds, configured in
  lib/config.ts. GET requests may retry once. POSTs do not retry automatically.

## Development checks

```powershell
npm run typecheck
npm run lint
npm test
npm run build
```

Tests use the handoff's captured JSON explicitly in `tests/fixtures/`.
Fixtures are imported only by tests, never by application runtime code.
See VALIDATION.md for checks completed during delivery and remaining local work.

## Scientific scope

Target: canceled/unavailable versus delivered, not verified RTO. Scores are
uncalibrated. Payment availability is assumed. Validation selected both model
and threshold. Earlier baseline test data was inspected. Predictive quality is
limited. Scenario changes are associations, not causal evidence. Financial
savings are unverified. Spark is local; Kafka transports historical feature
replay and streaming does not improve accuracy automatically.
