# SPEC: Task 4.5 - Adversarial Concurrency Stress, Graceful Shutdown & Production Hardening

## GOAL
Harden PulseQueue for production deployment by adding static SPA serving, graceful SIGTERM/SIGINT worker drainage, an optional single-container worker boot flag (`EMBED_WORKER=true`), and an adversarial concurrency stress suite executed by Agent Gamma.

## ACCEPTANCE CRITERIA
1. Production Static Pipeline & Embedded Worker Toggle (`server.js` & `package.json`):
   - When `NODE_ENV === 'production'`, `server.js` serves static assets from `client/dist` and registers Express 5 wildcard fallback `app.get('/{*splat}', ...)` strictly after `/api/*` routes.
   - When `process.env.EMBED_WORKER === 'true'`, `server.js` initializes the BullMQ worker inside the process and drains it cleanly during `closeServer()` / `SIGTERM` / `SIGINT`.
   - Update root `package.json` with:
     - `"build": "npm install && cd client && npm install --include=dev && npm run build"`
     - `"start": "node server.js"`
     - `"worker": "node workers/batchWorker.js"`
2. Graceful Shutdown (`workers/batchWorker.js` & `server.js`):
   - Register `SIGTERM` and `SIGINT` handlers that call `await worker.close()` (waiting for active jobs to finish rather than corrupting state), close Redis connections cleanly, and exit `0`.
3. Adversarial Concurrency & Resilience Suite (`tests/adversarial-stress-4.5.test.js` — Agent Gamma):
   - **Test 1 — High-Concurrency Burst (30 Simultaneous Jobs):** Fire 30 `POST /api/jobs/batch` requests concurrently via `Promise.all` across two tenants (15 for User A, 15 for User B). Verify all 30 return HTTP 202 in under 500ms, the worker processes all 30 to `completed` with concurrency cap `5` respected, and zero cross-tenant socket events leak between User A and User B.
   - **Test 2 — Production SPA & API Routing Check:** Boot server with `NODE_ENV=production` and verify `GET /` and `GET /mission-control/live` return HTTP 200 HTML while `GET /api/jobs` enforces JWT authentication (HTTP 401 without token).
   - **Test 3 — Graceful Worker Drain:** Enqueue an active job, invoke `worker.close()`, and verify the in-flight job finishes cleanly (`completed`) before the worker resolves shutdown.

## CONSTRAINTS
- Agent Alpha handles `server.js`, `workers/batchWorker.js`, and `package.json`.
- Agent Gamma creates and runs `tests/adversarial-stress-4.5.test.js` without altering production code.
- Adhere to `AGENTS.md` Rules 1–7.

## BLAST RADIUS
MEDIUM (Touches production startup/shutdown lifecycle and concurrency boundaries).