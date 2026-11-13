# Session Notes — PulseQueue

## Task 4.1: Redis Connection, Queue Producer, and Authenticated WebSocket Handshake

### Architectural Decision Made
- **Stateless JWT Verification & Non-Blocking Deterministic Queue Producer (`controllers/batchController.js`, `middleware/authGuard.js`, `socket/socketHandler.js`)**:
  - Unified JWT verification across HTTP (`authGuard`) and WebSocket handshakes (`io.use`) so authenticated sockets are deterministically bound to `socket.user` and automatically joined to isolated rooms (`tenant:<userId>`), strictly enforcing Rule 5.
  - Pre-assigned deterministic UUID-based job IDs (`batch-<uuid>`) in `POST /api/jobs/batch` and coordinated in-flight enqueue promises with `GET /api/jobs/:id` so HTTP 202 responses return in `< 100ms` (Rule 4 Producer-Consumer Isolation) while still guaranteeing read-after-write consistency over high-latency remote Redis connections.

### Friction Point / Bug Encountered
- **Upstash Redis TLS Protocol Detection & WAN Round-Trip Latency (`config/redis.js`)**:
  - BullMQ requires `maxRetriesPerRequest: null` on the `ioredis` connection instance, and Upstash cloud instances require TLS negotiation (`rediss://` or `tls` socket options) and exhibit ~220ms WAN round-trip latency per command.
  - Resolved by configuring `config/redis.js` with `maxRetriesPerRequest: null` and automatic TLS detection when `rediss://` or an `.upstash.io` host is present in `REDIS_URL`, alongside in-flight promise coordination in `batchController.js`.

## Task 4.2: Asynchronous BullMQ Workers, Exponential Backoff & Dead-Letter Queue

### Architectural Decision Made
- **Dedicated Worker Redis Connection Duplication & Idempotent MongoDB DLQ Quarantine (`workers/batchWorker.js`, `models/FailedJob.js`, `routes/batchRoutes.js`)**:
  - Used `redisConnection.duplicate({ maxRetriesPerRequest: null })` for the BullMQ `Worker` consumer so blocking `BZPOPMIN` operations on the worker never contend with non-blocking producer commands on the Express HTTP server.
  - Persisted permanently exhausted jobs (`job.attemptsMade >= job.opts.attempts`) into the MongoDB `FailedJob` Dead-Letter Queue collection idempotently via `findOneAndUpdate({ jobId }, ..., { upsert: true })` while retaining `removeOnFail: false` in Redis for dual-layer inspectability, and mounted `GET /api/jobs/dlq` prior to `GET /api/jobs/:id` to prevent route parameter shadowing.

### Friction Point / Bug Encountered
- **BullMQ v6 `job.attemptsMade` Pre- vs. Post-Execution Lifecycle (`workers/batchWorker.js`)**:
  - In BullMQ v6, `job.attemptsMade` is incremented inside `moveToCompleted` / `moveToFailed` *after* the processor function returns or throws. Consequently, inside `processorFn(job)`, `job.attemptsMade` is `0` on Attempt 1 and `1` on Attempt 2, whereas inside `worker.on('failed')` it has already been incremented to `1`, `2`, and `3`.
  - Resolved by computing `const currentAttempt = (job.attemptsMade ?? 0) + 1` inside `processorFn` (`currentAttempt < 2`) so transient chaos jobs fail on Attempt 1 and recover on Attempt 2 (`attemptsMade === 2` upon completion), while checking `job.attemptsMade >= maxAttempts` inside `worker.on('failed')` for DLQ quarantine after Attempt 3.

## Task 4.3: Real-Time Event Fan-Out & Tenant-Isolated Socket Broadcasts

### Architectural Decision Made
- **Dedicated Redis Pub/Sub Event Bridge with Embedded Tenant Context (`socket/queueBridge.js`, `workers/batchWorker.js`)**:
  - Bridged background worker lifecycle transitions (`job:progress`, `job:completed`, `job:failed`) to Socket.io via a dedicated Redis Pub/Sub channel (`pulsequeue:events`) using an isolated subscriber connection (`redisConnection.duplicate({ maxRetriesPerRequest: null })`).
  - Embedded `userId` and `jobType` directly into the worker's published Pub/Sub payloads and pipelined `job.updateProgress(percent)` alongside `redisConnection.publish(...)` so `queueBridge` routes events strictly to `io.to(\`tenant:${userId}\`)` (Rule 5) with zero extra Redis/MongoDB lookups on receive and guaranteed sequential delivery (`25% -> 50% -> 75% -> 100% -> completed`).

### Friction Point / Bug Encountered
- **Redis Subscriber Mode Connection Exclusivity & Event Ordering Across WAN (`socket/queueBridge.js`)**:
  - Once an `ioredis` connection enters `SUBSCRIBE` mode, it cannot issue standard Redis commands (`PING`, `EVALSHA`, `HGETALL`) without throwing a subscriber-mode error, and using BullMQ `QueueEvents` alone would require a secondary `batchQueue.getJob(jobId)` WAN round-trip per progress tick to resolve `userId`, risking out-of-order arrival between `job:progress(100)` and `job:completed`.
  - Resolved by isolating a dedicated `subConnection` inside `socket/queueBridge.js` awaited during `startServer()` and publishing pre-enriched tenant payloads directly from `workers/batchWorker.js`.

## Task 4.4: Decoupled Real-Time Mission Control UI

### Architectural Decision Made
- **Zero-Polling Event-Driven React State Architecture (`client/src/hooks/usePulseSocket.js`, `client/src/App.jsx`, `client/src/components/DlqInspector.jsx`)**:
  - Hydrated initial job state once via `GET /api/jobs` on mount/reconnect, inserted newly dispatched jobs optimistically upon receiving HTTP 202 from `POST /api/jobs/batch`, and drove all progress bar transitions (`0% -> 100%`), status pills, and DLQ quarantine refreshes (`refreshSignal` on `isDeadLetter: true`) purely from Socket.io events (`job:progress`, `job:completed`, `job:failed`) with zero `setInterval` polling.

### Friction Point / Bug Encountered
- **Socket.io Client Reconnection Callback Staleness & Vite WebSocket Proxy Upgrade (`client/vite.config.js`, `client/src/hooks/usePulseSocket.js`)**:
  - In React, binding Socket.io event listeners inside `useEffect` with changing state callbacks can trigger unnecessary socket disconnects/reconnects on every render, and Vite's dev proxy drops Socket.io WebSocket upgrade frames unless `ws: true` is explicitly enabled on `/socket.io`.
  - Resolved by storing event callbacks in a mutable `useRef` inside `usePulseSocket` so the Socket.io connection lifecycle depends strictly on `token`, and configuring `/socket.io` with `ws: true` and `changeOrigin: true` in `client/vite.config.js`.
