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
