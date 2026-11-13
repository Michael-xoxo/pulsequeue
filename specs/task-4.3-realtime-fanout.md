# SPEC: Task 4.3 - Real-Time Event Fan-Out & Tenant-Isolated Socket Broadcasts

## GOAL
Bridge background BullMQ worker lifecycle events across process boundaries into the Socket.io server using Redis Pub/Sub (or BullMQ QueueEvents), pushing real-time progress, completion, and failure notifications strictly to `tenant:<userId>` rooms.

## ACCEPTANCE CRITERIA
1. Cross-Process Event Bridge (`socket/queueBridge.js` & `socket/socketHandler.js`):
   - Uses BullMQ `QueueEvents('batch-processor', { connection })` (with a dedicated duplicated `ioredis` connection, `maxRetriesPerRequest: null`) OR an explicit Redis Pub/Sub channel (`pulsequeue:events`).
   - Note: Because BullMQ `progress`, `completed`, and `failed` events in `QueueEvents` only carry `{ jobId, data/returnvalue/failedReason }` by default, ensure the user's `userId` is resolvable without slow per-tick DB lookups (e.g., either embed `userId` in the worker's published Redis Pub/Sub message or resolve via `batchQueue.getJob(jobId)` / lightweight Redis mapping).
   - Emits three standardized events strictly to `io.to(\`tenant:${userId}\`)`:
     - `job:progress` $\rightarrow$ `{ jobId, jobType, progress, timestamp }`
     - `job:completed` $\rightarrow$ `{ jobId, jobType, result, timestamp }`
     - `job:failed` $\rightarrow$ `{ jobId, jobType, failedReason, attemptsMade, isDeadLetter, timestamp }`
2. Rule 5 Tenant Room Enforcement:
   - Never invoke `io.emit(...)`. Every broadcast must target `io.to(\`tenant:${userId}\`).emit(...)`.
3. Server & Worker Integration (`server.js`, `workers/batchWorker.js`):
   - `server.js` initializes the `queueBridge(io)` on startup and closes it gracefully on shutdown.
   - `GET /api/jobs` (List Recent Jobs): Add an endpoint returning the authenticated user's recent jobs from BullMQ (`getJobs(['waiting', 'active', 'completed', 'failed'])` filtered by `job.data.userId === req.user._id.toString()`) so reconnecting clients can hydrate initial state before live socket events take over.

## CONSTRAINTS
- Strict file scope: Confined to `socket/`, `workers/`, `controllers/`, `routes/`, `server.js`, `tests/`.
- Zero frontend modifications.
- Adhere to `AGENTS.md` Rules 1–7.

## STOPPING CONDITIONS
- Stop if any event is emitted globally (`io.emit`) instead of room-scoped (`io.to('tenant:...')`).
- Stop if Redis connection sharing causes blocking command collisions on Upstash.

## BLAST RADIUS
MEDIUM (Wires real-time cross-process event streaming into the WebSocket layer).

## ADVERSARIAL VERIFICATION STEPS
1. Real-Time Progress Stream: Connect User A via WebSocket, enqueue `{ itemCount: 4 }`, and assert User A receives sequential `job:progress` socket frames (`25`, `50`, `75`, `100`) followed by `job:completed` without making any HTTP polling requests.
2. Zero Cross-Tenant Leakage: Connect User B via WebSocket simultaneously while User A runs a batch job -> assert User B receives 0 socket events (`job:progress`, `job:completed`, `job:failed`).
3. Real-Time DLQ Alert: Enqueue `{ itemCount: 2, simulateFatalFail: true }` as User A -> assert User A receives `job:failed` with `isDeadLetter: true` once retries exhaust.
4. State Hydration Endpoint: Query `GET /api/jobs` as User A -> returns User A's jobs; query as User B -> returns none of User A's jobs.