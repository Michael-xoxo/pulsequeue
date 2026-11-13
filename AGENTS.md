# SYSTEM RULES & SUB-AGENT TOPOLOGY

RULE 1 — SPECIFICATION-DRIVEN ISOLATION
All tasks must follow an explicit spec in `/specs/`. Sub-agents are strictly isolated to their declared directory domains. Cross-domain alterations are forbidden.

RULE 2 — CONTRACT CONVERGENCE
Backend and Frontend interact solely through verified data schemas and socket event contracts defined in `/specs/contracts/`.

RULE 3 — RED ZONE HALT
Halt and request user clearance before executing changes touching Redis connection options, queue retry policies, database schemas, or WebSocket authentication handshakes.

RULE 4 — PRODUCER-CONSUMER ISOLATION
Express route handlers must never perform CPU-intensive tasks, batch iterations, or disk file operations. They validate the payload, push to BullMQ, and immediately respond with HTTP 202 Accepted.

RULE 5 — TENANT-ISOLATED REAL-TIME BROADCASTS
All WebSocket events must be emitted strictly to user-scoped rooms (`tenant:<userId>`). Emitting sensitive job progress to global sockets (`io.emit`) is strictly forbidden.

RULE 6 — EVIDENCE-BASED COMPLETION
A task is verified only through executable proof: exact files modified, diff summary, and execution command outputs demonstrating both success and failure handling.

RULE 7 — SESSION LESSONS LOG
At session wrap-up, append to session-notes.md:
- One architectural decision made
- One friction point / bug encountered