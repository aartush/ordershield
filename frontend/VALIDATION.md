# Validation record

What was checked on the project as published in this repository.

## Automated checks

- Strict TypeScript check (`npm run typecheck`) — passes with no errors.
- ESLint (`npm run lint`) — no errors or warnings.
- Production build (`npm run build`) — succeeds.
- 12 Vitest/jsdom integration tests — all pass. They cover:
  1. FastAPI field-error array formatting.
  2. A failed POST is not retried.
  3. Comparison uses `orders`, keeps the submitted snapshot, marks edited results stale.
  4. A failed inference preserves inputs.
  5. Sample transfer preserves null payments and blocks incomplete submission.
  6. SSE snapshot replacement, a single connection, and listener cleanup.
  7. Replay conflict handling with no duplicate POST.
  8. Diagram stepping, batch/stream toggle and reset with reduced motion.
  9. Threshold debounce, cancellation of obsolete requests, preserved slider position.
  10. Producer running → completed while the consumer stays stopped/stale.
  11. Producer running → failed while the consumer stays stopped/stale.
  12. Backend-disconnected view renders an error without substituting captured data.

- Backend: all JSON artifacts parse; every Python module compiles.
- Backend: `GET /health`, `/api/model`, `/api/stats`, `/api/options`, `/api/content`,
  `/api/live`, `/api/model/evaluation` and `/api/samples` all return HTTP 200.
- Backend: `POST /api/predict` performs real Spark inference and returns a
  `risk_score`, `threshold` and `alert` decision.
- Saved model: reloads successfully and is byte-identical to the trained artifact.

## Evidence boundaries

Scores are class-weighted ranking scores, not calibrated probabilities. All
reported metrics are validation metrics; validation selected both the model and
the threshold, so they are optimistic for threshold choice. The current model has
no test-set evaluation (`test_evaluated` is `false`), and the earlier baseline test
period was already inspected.

Spark runs in local mode. Kafka transports a historical replay, not a live
merchant feed, and nothing retrains. No throughput, latency-percentile, CPU or
memory benchmark exists, so none is claimed.

Captured JSON in `backend/handoff/` is imported by the frontend tests only; there
is no runtime mock mode. Live counters come from `backend/outputs/live/`, which is
created at runtime and excluded from version control.

## Local checks worth doing on your machine

Ten direct routes and refresh, mobile drawer, fonts and spacing, keyboard
interactions, presentation mode, OS-level reduced motion, first inference, A/B
inference, sample transfer, threshold exploration, live SSE snapshots, replay
start/status, fresh versus stale heartbeat, and reconnection after an API restart.

Producer completion does not prove consumer catch-up, and saved predictions do not
establish that the consumer is running now.
