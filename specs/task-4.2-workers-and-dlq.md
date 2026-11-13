# SPEC: Task 4.2 - Asynchronous BullMQ Workers, Exponential Backoff & Dead-Letter Queue

## GOAL
Implement an isolated BullMQ background worker capable of processing batch jobs in incremental chunks, reporting real-time progress, retrying transient errors with exponential backoff, and persisting permanently failed jobs into a MongoDB Dead-Letter Queue (DLQ).

## ACCEPTANCE CRITERIA
1. Queue Retry Policy (`queues/batchQueue.js`):
   - Configure `defaultJobOptions`:
     - `attempts: 3`
     - `backoff: { type: 'exponential', delay: 1000 }` (1s, 2s, 4s delays)
     - `removeOnComplete: 100`
     - `removeOnFail: false` (keep failed jobs inspectable in Redis while also logging to MongoDB DLQ)
2. Dead-Letter Queue Schema (`models/FailedJob.js`):
   - Fields: `jobId` (String, required, unique), `userId` (ObjectId, ref: 'User', required), `jobType` (String), `payload` (Object), `failedReason` (String), `stacktrace` ([String]), `attemptsMade` (Number), `failedAt` (Date, default: Date.now).
3. Isolated Worker Engine (`workers/batchWorker.js`):
   - Instantiates `new Worker('batch-processor', processorFn, { connection, concurrency: 5 })`.
   - Note: BullMQ Workers require a dedicated `ioredis` connection (or `connection.duplicate()`) with `maxRetriesPerRequest: null`.
   - Stepwise Processing:
     - Iterates through `itemCount` chunks, updating progress via `await job.updateProgress(percent)` at each step (e.g., 20%, 40%, 60%, 80%, 100%).
     - Simulates realistic async work (e.g., 50ms per item).
     - Simulated Chaos Hook: If `job.data.simulateTransientFail === true` and `job.attemptsMade < 2`, throw `new Error("Simulated transient downstream timeout")` to prove retry recovery.
     - If `job.data.simulateFatalFail === true`, throw `new Error("Simulated fatal poison-pill payload")` on every attempt until retries exhaust.
   - Worker Event Listeners:
     - `worker.on('failed', async (job, err) => ...)`: When `job.attemptsMade >= job.opts.attempts`, persist the exhausted job into the `FailedJob` MongoDB collection (idempotently via `findOneAndUpdate` on `jobId`).
4. DLQ Inspection Endpoint (`controllers/batchController.js` & `routes/batchRoutes.js`):
   - `GET /api/jobs/dlq`: Protected by `authGuard`. Returns all `FailedJob` records scoped strictly to `req.user._id`, sorted by `failedAt: -1`.
   - Note: Mount `/dlq` route BEFORE `/:id` in `batchRoutes.js` so Express does not treat `"dlq"` as a job `:id` parameter.
5. Package Script (`package.json`):
   - Add `"worker": "node workers/batchWorker.js"` script.

## CONSTRAINTS
- Strict file scope: Confined to `queues/`, `models/`, `workers/`, `controllers/`, `routes/`, `package.json`, `tests/`.
- Zero frontend modifications.
- Adhere to `AGENTS.md` Rules 1–7.

## STOPPING CONDITIONS
- Stop if Worker shares a blocking Redis connection instance instead of duplicating/isolating it.
- Stop if `/api/jobs/dlq` is shadowed by `/api/jobs/:id`.

## BLAST RADIUS
MEDIUM (Introduces background consumer processes and failure persistence).

## ADVERSARIAL VERIFICATION STEPS
1. Happy Path: Enqueue `{ itemCount: 5 }` -> Worker processes chunks, updates progress to 100, and transitions state to `completed`.
2. Transient Recovery: Enqueue `{ itemCount: 3, simulateTransientFail: true }` -> Fails on attempt 1, backs off, retries automatically, and succeeds on attempt 2 (`state === 'completed'`).
3. Poison-Pill DLQ Quarantine: Enqueue `{ itemCount: 3, simulateFatalFail: true }` -> Exhausts all 3 attempts, transitions to `failed`, and writes a quarantined document to `FailedJob` in MongoDB.
4. Tenant-Scoped DLQ API: Query `GET /api/jobs/dlq` as User A -> returns User A's quarantined job; query as User B -> returns empty array `[]`.