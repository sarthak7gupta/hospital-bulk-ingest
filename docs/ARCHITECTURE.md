# Architecture & Technical Design Document

This document details the architectural layout, core subsystems, and key engineering trade-offs of the `hospital-bulk-ingest` service.

---

## 1. System Overview & Core Abstractions

The system is structured around five primary domain abstractions:

1. [`BatchRepository`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/repositories/batch_repo.py): Central repository managing batch records, TTL eviction, and WebSocket pub/sub.
2. [`DirectoryClient`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/services/directory_client.py): Downstream HTTP client with retry logic, rate limit backoff, and failure classification.
3. [`BulkProcessor`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/services/processor.py): Orchestrates ingestion, concurrency throttling, reconciliation, and activation.
4. [`Settings`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/config.py): Environment-driven, validated runtime configuration via `pydantic-settings`.
5. [`CSVParser`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/services/csv_parser.py): High-performance CSV sanitizer and validator.

---

## 2. Layered Architecture

```
                                  Client Request
                                        │
┌───────────────────────────────────────▼───────────────────────────────────────┐
│ Middlewares (Execution: Outermost → Innermost)                                │
│  1. RequestIdMiddleware         (attaches X-Request-ID to all responses/logs) │
│  2. RequestBodyLimitMiddleware  (streaming 413 check before memory spooling)  │
│  3. AuthAndRateLimitMiddleware  (timing-safe API key & per-IP rate limiting)  │
│  4. CORSMiddleware              (configurable allowed origins & credentials)  │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
┌───────────────────────────────────────▼───────────────────────────────────────┐
│ Routing & Ingestion Endpoints                                                 │
│  • POST /hospitals/bulk       → Synchronous full ingest (returns 200)         │
│  • POST /hospitals/bulk/jobs  → Asynchronous enqueue (returns 202)            │
│  • POST /hospitals/bulk/validate → Dry-run schema validation                  │
│  • GET  /hospitals/bulk/{id}  → Polling status snapshot                       │
│  • WS   /hospitals/bulk/{id}/ws → Real-time event stream                      │
│  • POST /hospitals/bulk/{id}/resume → Safe idempotent retry                  │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
┌───────────────────────────────────────▼───────────────────────────────────────┐
│ CSV Parser & Validation Service                                               │
│  • UTF-8 / BOM auto-detection via `utf-8-sig`                                 │
│  • ASCII control character stripping via `str.translate`                      │
│  • Header validation: rejects duplicates, empty names, missing columns        │
│  • Field boundaries: 1024-char cap, phone format regex, max 20 rows           │
└───────────────────────────────────────┬───────────────────────────────────────┘
                                        │
┌───────────────────────────────────────▼───────────────────────────────────────┐
│ Bulk Processor Engine                                                         │
│  • asyncio.Semaphore throttling (default 10 concurrent HTTP requests)         │
│  • Per-row isolated processing: partial failures do not crash entire batch    │
│  • Failure reconciliation: detects ambiguous 500/timeouts and recovers IDs    │
│  • Two-phase atomic activation: PATCH /hospitals/batch/{id}/activate          │
└───────────────────────┬───────────────────────────────┬───────────────────────┘
                        │                               │
┌───────────────────────▼──────────────┐ ┌──────────────▼───────────────────────┐
│ In-Memory Batch Repository           │ │ Directory HTTP Client (aiohttp)       │
│  • Shallow dataclass cloning         │ │  • Connection pool reuse (100 total)  │
│  • O(1) / O(K) eviction via          │ │  • Fast orjson serialization          │
│    OrderedDict for TTL expiry        │ │  • Custom tenacity wait with          │
│  • Isolated per-batch asyncio queues │ │    Retry-After backoff capped at 30s  │
│  • BatchRepositoryProtocol for scale │ │  • Ambiguous error classification     │
└──────────────────────────────────────┘ └───────────────────────────────────────┘
```

---

## 3. Key Design Decisions

### 3.1 Synchronous vs. Asynchronous Ingestion
- **Requirement**: Maintain backward-compatible schema for `POST /hospitals/bulk` while supporting non-blocking background jobs for large/slow batches.
- **Design Decision**:
  - `POST /hospitals/bulk`: Synchronous endpoint returning `200 OK` with full batch details.
  - `POST /hospitals/bulk/jobs`: Returns `202 Accepted` immediately with `status_url` and `ws_url`. Enqueues execution on `app.state.background_tasks` registry.
  - Both endpoints execute identical underlying `BulkProcessor.process_bulk()` logic.

### 3.2 Error Classification & Reconciliation (Avoiding Duplicates)
- **Problem**: In distributed systems, network timeouts, HTTP 500s, or connection drops mid-response are ambiguous—the downstream API might have already created the hospital record. Naive retries produce duplicates.
- **Design Decision**:
  - **Safe to retry**: Connection-establishment drops, HTTP 429 (rate-limited), HTTP 502/503/504.
  - **Ambiguous (never blindly retried)**: HTTP 500, unparseable 200 responses, read timeouts. These raise `DirectoryAmbiguousError`.
  - **Reconciliation Phase**: If any row encounters an ambiguous outcome, the processor calls `GET /hospitals/batch/{batch_id}` once. Remote records are matched against CSV rows using a normalized multiset `(name, address, phone)` key. Successfully reconciled rows are marked `CREATED` with remote IDs.

### 3.3 Middleware Ordering & Outer Request ID Context
- **Problem**: Starlette executes middlewares in reverse order of registration (`app.add_middleware()`). If `RequestIdMiddleware` is added first, downstream middlewares (body limits, authentication, rate limits) abort requests before `RequestIdMiddleware` attaches the `X-Request-ID` header.
- **Design Decision**:
  - Registered `RequestIdMiddleware` last so it runs outermost.
  - Every error response (401, 413, 429, 500) and log line contains the request ID.

### 3.4 In-Memory Repository Performance & Multi-Pod Scalability
- **Problem**: `copy.deepcopy` on batch snapshots was CPU-heavy. Full dictionary scans for TTL eviction took $O(N)$ time.
- **Design Decision**:
  - **Shallow Cloner**: Implemented `_clone_batch()` copying only dataclass fields and the row list, dropping copying latency.
  - **$O(1)$/$O(K)$ Eviction**: Terminal batches (`COMPLETED`, `FAILED`, etc.) are tracked in an `OrderedDict[UUID, datetime]`. Eviction checks terminate at the first unexpired entry.
  - **Multi-Pod Scale**: Extracted [`BatchRepositoryProtocol`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/repositories/batch_repo.py#L75) allowing future drop-in backends (e.g., Redis Hashes + Pub/Sub) without changing domain services.

### 3.5 Real-Time WebSocket Streaming & Guardrails
- **Problem**: Client disconnects during streaming created task churn or leaked open coroutines. Slow consumers exhausted server memory.
- **Design Decision**:
  - Held a persistent `recv_task` across message deliveries rather than spawning/cancelling tasks per loop iteration.
  - Enforced per-batch subscriber cap (default: 10) and total subscriber cap (default: 100), rejecting excess subscribers with WebSocket close code `1013 (Try Again Later)`.
  - Suppressed `WebSocketDisconnect` during disconnect cleanups to silence unretrieved task warnings.

### 3.6 Graceful Shutdown Drain
- **Problem**: Server redeployments or container SIGTERMs hard-cancelled in-flight batch operations immediately.
- **Design Decision**:
  - Application lifespan waits up to `shutdown_drain_timeout_seconds` (default: 10.0s) for active background tasks to complete naturally before cancelling pending tasks.

---

## 4. Things Intentionally Left Out (Non-Goals & Exclusions)

To maintain a lean, robust microservice without accidental complexity, several architectural alternatives were evaluated and intentionally excluded:

### 4.1 Persistent Database (PostgreSQL / SQLite / MongoDB)
- **What was omitted**: Adding relational ORMs (SQLAlchemy, Tortoise), migrations (Alembic), or document stores for batch storage.
- **Why omitted**:
  - The service is an ingestion proxy/pipeline whose system of record is the external Hospital Directory API.
  - Adding a database introduces schema migrations, connection pooling contention, credentials rotation, and operational failure modes disproportionate to batch orchestration.
  - Keeps the microservice completely stateless and cloud-native for zero-downtime container updates.
- **Alternative implemented**: In-memory repository with async locking, bounded memory caps, shallow dataclass cloning, and $O(1)$/$O(K)$ TTL eviction. The [`BatchRepositoryProtocol`](file:///Users/sarthak7gupta/batcave/paribus_assignment/app/repositories/batch_repo.py#L75) interface allows dropping in Redis or PostgreSQL without touching domain logic if multi-pod durability is needed.

### 4.2 External Distributed Task Queue (Celery / RabbitMQ / Redis Streams / Kafka)
- **What was omitted**: Dedicated message brokers, distributed workers, and external consumer processes.
- **Why omitted**:
  - For batches capped at 20 rows (processing duration ~1-5 seconds), external message brokers add substantial infrastructure cost, serialize/deserialize overhead, separate process monitoring, and operational complexity.
- **Alternative implemented**: Native `asyncio.create_task` with strong references maintained in `app.state.background_tasks`, bounded concurrency semaphores, client disconnect cleanup, and graceful lifecycle shutdown drain.

### 4.3 Automatic Remote Deletion / Rollback on Partial Failures
- **What was omitted**: Attempting to issue compensating `DELETE` requests against created hospitals when a batch partially fails.
- **Why omitted**:
  - Downstream Hospital Directory API lacks atomic batch transactions (no single rollback endpoint).
  - Deleting already created records over a flaky network risks creating cascading secondary failures, deletes records while in-flight requests may still resolve, and destroys audit logs.
  - Prevents human inspection and safe resumption.
- **Alternative implemented**: Clear two-phase isolation: partial batches remain unactivated (`batch_activated: false`). Failed rows are marked with precise error strings, and an idempotent `POST /hospitals/bulk/{id}/resume` endpoint allows retrying only uncreated rows without duplicating existing ones.

### 4.4 Blind Unbounded Retries of Non-Idempotent HTTP POSTs
- **What was omitted**: Retrying all failed `POST /hospitals` requests on any HTTP 5xx or connection reset.
- **Why omitted**:
  - An HTTP 500 or network timeout often occurs *after* the downstream server processed the write. Blind retries create duplicate hospital entities and corrupt downstream batch state.
- **Alternative implemented**: Strict error classification. Safe transient errors (HTTP 429, 502, 503, 504) retry with exponential backoff and `Retry-After` enforcement. Ambiguous outcomes (HTTP 500, read timeouts) trigger a single-pass multiset reconciliation (`GET /hospitals/batch/{batch_id}`) to safely recover generated IDs.

### 4.5 Disk-Spooling Multi-Part File Uploads
- **What was omitted**: Streaming uploads to temporary disk files (`tempfile`, SpooledTemporaryFile) before parsing.
- **Why omitted**:
  - Payloads are strictly capped at 20 CSV rows and 1MB. Disk I/O introduces filesystem bottlenecks, inode leaks, container disk filling risks, and permission issues in read-only container filesystems.
- **Alternative implemented**: In-memory streaming inspection via `RequestBodyLimitMiddleware` (rejecting bodies > 1MB with HTTP 413 before buffering) followed by pure in-memory `utf-8-sig` decoding and row sanitation.

### 4.6 Complex Distributed Locking (Redlock / ZooKeeper)
- **What was omitted**: Distributed lock coordinators for synchronizing batch executions across multiple hosts.
- **Why omitted**:
  - Each batch upload generates a cryptographically random UUID (`creation_batch_id`). Batches are naturally partitioned and never compete for the same row keys.
- **Alternative implemented**: Lightweight intra-process `asyncio.Lock` around batch state mutations, combined with atomic `try_mark_processing` test-and-set guards to prevent concurrent resume executions.
