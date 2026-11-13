# SPEC: Task 4.4 - Decoupled Real-Time Mission Control UI

## GOAL
Build a decoupled React + Vite Mission Control dashboard inside `/client` that authenticates users, establishes an authenticated Socket.io connection, dispatches batch jobs, animates live progress bars from 0% to 100% in real time without polling, and displays quarantined Dead-Letter Queue (DLQ) failures.

## ACCEPTANCE CRITERIA
1. Client Scaffolding (`client/`):
   - Vite + React setup inside `/client` with `socket.io-client` installed.
   - `vite.config.js` configured to proxy `/api` and `/socket.io` (with `ws: true`) to `http://localhost:5000`.
2. Authentication & Socket Lifecycle (`client/src/context/AuthContext.jsx` & `client/src/hooks/usePulseSocket.js`):
   - `AuthContext` manages JWT persistence (`localStorage`), login/register (`POST /api/auth/login`, `POST /api/auth/register`), and an authenticated `authFetch` helper.
   - `usePulseSocket` custom hook connects to Socket.io passing `auth: { token }`, tracks connection status badge (`Connected` vs `Disconnected`), and registers listeners for `job:progress`, `job:completed`, and `job:failed`.
3. Job Dispatcher Panel (`client/src/components/JobDispatcher.jsx`):
   - Allows selecting `jobType` (`BULK_INVOICE_SYNC`, `WEBHOOK_FANOUT`, `LEDGER_RECONCILE`), setting `itemCount` (1–100), and toggling optional chaos simulation checkboxes (`Simulate Transient Failure`, `Simulate Fatal Poison-Pill`).
   - Dispatches `POST /api/jobs/batch` and optimistically inserts the queued job (`0%`, `waiting`) into state immediately upon receiving HTTP 202.
4. Live Progress Feed & History (`client/src/components/LiveJobFeed.jsx`):
   - Hydrates initial jobs via `GET /api/jobs` on mount/reconnect.
   - Updates progress bars (`0%` $\rightarrow$ `100%`), status pills (`waiting`, `active`, `completed`, `failed`), and completion timestamps instantaneously as WebSocket events arrive.
5. Dead-Letter Queue (DLQ) Inspector (`client/src/components/DlqInspector.jsx`):
   - Fetches quarantined failures from `GET /api/jobs/dlq` and auto-refreshes whenever a `job:failed` event with `isDeadLetter: true` arrives over WebSockets.
   - Displays `jobId`, `jobType`, `attemptsMade`, `failedReason`, and `failedAt`.
6. Production Build Verification:
   - `npm run build` inside `/client` compiles cleanly with exit code 0.

## CONSTRAINTS
- Strict file scope: Confined exclusively to `/client` (and `session-notes.md` / verification test script).
- Agent Beta is FORBIDDEN from editing any backend files (`server.js`, `controllers/`, `routes/`, `socket/`, `workers/`, `queues/`).
- Adhere to `AGENTS.md` Rules 1–7.

## STOPPING CONDITIONS
- Stop if Vite proxy fails to forward WebSocket upgrades (`/socket.io`).
- Stop if the UI relies on `setInterval` HTTP polling instead of WebSocket events for job progress.

## BLAST RADIUS
LOW (Isolated frontend client workspace).

## ADVERSARIAL VERIFICATION STEPS
1. Run `npm run build` inside `/client` -> verify exit code 0 and `client/dist/index.html` generated.
2. Run an automated end-to-end verification script against the live server + worker confirming:
   - User registration/login returns valid JWT.
   - Enqueuing a batch job via `POST /api/jobs/batch` hydrates via `GET /api/jobs`, updates live over Socket.io to `100%`, and lands in `completed`.
   - Enqueuing a fatal poison-pill job surfaces in `GET /api/jobs/dlq` after emitting `isDeadLetter: true` over WebSockets.