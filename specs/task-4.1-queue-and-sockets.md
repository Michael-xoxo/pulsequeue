# SPEC: Task 4.1 - Redis Connection, Queue Producer, and Authenticated WebSocket Handshake

## GOAL
Establish the unified Redis connection client using ioredis, construct the HTTP + WebSocket server pairing, implement JWT handshake authentication for WebSockets, and build the first background job queue (`batchProcessorQueue`) with an Express producer endpoint.

## ACCEPTANCE CRITERIA
1. Redis & Queue Infrastructure (`config/redis.js` & `queues/batchQueue.js`):
   - Initializes an `ioredis` connection instance with maxRetriesPerRequest: null (required by BullMQ).
   - Exports `batchQueue` instance (`new Queue('batch-processor', { connection })`).
2. Real-Time Socket Server (`server.js` & `socket/socketHandler.js`):
   - Express server wrapped with Node `http.createServer(app)`.
   - Socket.io attached with CORS support.
   - Handshake Middleware: Inspects `socket.handshake.auth.token` or `socket.handshake.headers.authorization`.
     - Verifies JWT via `JWT_SECRET`.
     - Attaches `socket.user = decoded`.
     - Automatically joins room `tenant:${decoded._id}`.
     - Unauthenticated connections are rejected with Error("Authentication error").
3. Producer Route (`controllers/batchController.js` & `routes/batchRoutes.js`):
   - `POST /api/jobs/batch`:
     - Protected by `authGuard`.
     - Accepts `{ jobType: "BULK_INVOICE_SYNC", itemCount: Number }`.
     - Validates `itemCount` (integer between 1 and 100).
     - Pushes job to `batchQueue` with `{ userId: req.user._id, jobType, itemCount }`.
     - Returns HTTP 202 Accepted with `{ jobId: job.id, status: "queued", message: "Job successfully enqueued" }`.
4. Job Status Route (`GET /api/jobs/:id`):
   - Inspects `batchQueue.getJob(id)`.
   - Returns current state: `waiting`, `active`, `completed`, or `failed`, along with job progress.

## CONSTRAINTS
- Strict file scope: Confined to backend (`config/`, `queues/`, `socket/`, `controllers/`, `routes/`, `server.js`).
- Zero frontend modifications in this phase.
- Adhere to `AGENTS.md` Rules 1–7.

## STOPPING CONDITIONS
- Stop if Redis connection throws TLS or handshake rejections.
- Stop if unauthenticated WebSockets are allowed to connect.

## BLAST RADIUS
HIGH (Establishes the foundational event and queue architecture).

## ADVERSARIAL VERIFICATION STEPS
1. Verify Redis connection ping returns PONG.
2. Attempt WebSocket connection without a token -> verify connection is rejected with HTTP 401 / Authentication error.
3. Authenticate User A, connect WebSocket with JWT -> verify user connects and joins room `tenant:<UserA_ID>`.
4. Dispatch `POST /api/jobs/batch` with `{ itemCount: 10 }` -> verify response is HTTP 202 with valid `jobId`.
5. Query `GET /api/jobs/:jobId` -> verify job exists in queue state `waiting` or `active`.