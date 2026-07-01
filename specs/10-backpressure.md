---
id: "10"
title: Backpressure & Flow Control
status: todo
phase: 5
depends_on: ["09"]
requirements: [FR-47, FR-48, FR-49, NFR-10]
---

# Spec 10 · Backpressure & Flow Control

## What
Protect the broker from being overwhelmed by fast producers or exhausted by slow
consumers. Add bounded per-partition queues, producer throttling, and slow-
consumer isolation so a stalled consumer cannot crash the broker or starve others.

## Why
Without backpressure, a fast producer fills the broker's heap until OOM. A stalled
consumer that never commits offsets must not prevent the broker from serving other
clients. This spec makes the system safe under load.

## What to build

### `broker/`
- `BackpressureController` — per-partition `ArrayBlockingQueue<PendingPublish>`;
  `acquire()` blocks the producer's virtual thread when the queue is full (natural
  backpressure via blocking IO — no separate throttle thread needed)
- Queue capacity configurable via `BrokerConfig` (`PUBLISH_QUEUE_CAPACITY`, default 1000)
- When queue is full, broker responds to new PUBLISH_REQ with `BROKER_BUSY` error
  (do NOT block indefinitely if producer does not honor backpressure)

### `client/`
- `ProducerClient` — on `BROKER_BUSY` response, back off exponentially and retry
  (configurable: `PRODUCER_BACKOFF_BASE_MS`, `PRODUCER_BACKOFF_MAX_MS`)

### Slow-consumer isolation
- Consumer poll requests are served directly from the log; no broker-side buffering
  per consumer. A slow consumer only hurts itself (it does not hold any lock or queue).
- Verify: one stalled consumer does not degrade throughput for a second, active consumer
  on the same topic.

## Acceptance criteria
1. **Memory-bound test (NFR-10):** run a producer at maximum speed with no consumer.
   Broker heap usage stabilizes (does not grow unboundedly); no `OutOfMemoryError`
   observed after 60 seconds.
2. **BROKER_BUSY test:** configure a tiny queue (capacity=10); produce 1000 messages faster
   than Raft can commit them; producer receives `BROKER_BUSY` responses and retries
   successfully with exponential backoff; no messages are lost.
3. **Slow consumer isolation:** start a consumer that sleeps 5 seconds between polls.
   A second consumer on the same topic reads at full speed. Slow consumer's lag does not
   affect the second consumer's throughput.
4. **Recovery after backpressure:** after the burst subsides, producer resumes at full
   speed; no lingering throttle.
5. `./gradlew test` GREEN.

## Out of scope
- Dynamic rate limiting / token buckets (simple queue + back-off is sufficient).
- Producer-side batching (optional future enhancement).
