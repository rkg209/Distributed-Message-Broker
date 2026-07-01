# api-design.md

**Version:** 1.0
**Source:** `planning/02-architecture.md`, `planning/03-system-design.md`, `planning/04-database-design.md`
**Status:** Implementation Reference

---

## Table of Contents

1. [Overview](#1-overview)
2. [Authentication Scheme](#2-authentication-scheme)
3. [Error Handling Conventions](#3-error-handling-conventions)
4. [Pagination Strategy](#4-pagination-strategy)
5. [Versioning Policy](#5-versioning-policy)
6. [OpenAPI Specification](#6-openapi-specification)
7. [Design Notes](#7-design-notes)

---

## 1. Overview

### 1.1 Protocol Summary

The Distributed Message Broker exposes a **custom binary TCP protocol**, not HTTP/REST. Every client-to-broker and broker-to-broker exchange uses the length-prefixed frame format defined in `protocol/`. There is no HTTP layer, no JSON on the wire, and no gRPC — the binary protocol is the portfolio signal for low-level systems competence and is the only supported transport.

The OpenAPI specification in §6 describes the **logical API surface** — the operations, their inputs, their outputs, and their error semantics — in a format that is human-readable and toolable. It is not a description of an HTTP API. The mapping from OpenAPI operation to wire message type is given explicitly in each operation's description. Implementors should treat the OpenAPI document as the authoritative contract specification and the binary codec in `protocol/MessageCodec` as the authoritative serialization reference.

### 1.2 Connection Model

Every client opens one TCP connection per broker it needs to communicate with. A single connection supports **pipelined requests**: the client may send multiple request frames without waiting for responses, matching responses to requests via the 4-byte `correlationId` echoed in every response frame. The broker processes requests from a single connection sequentially (one virtual thread per connection), but the client may have many requests in flight simultaneously across multiple connections.

### 1.3 Endpoint Addressing

Brokers are addressed by `host:port`. The default broker port is `9092`. Clients discover which broker is the leader for a given partition by issuing a `METADATA_REQ` to any broker in the cluster. The metadata response contains the full partition-to-leader map. Clients cache this map and refresh it on `NOT_LEADER` errors.

### 1.4 Scope of This Document

This document covers:

- The **client-facing API**: Publish, Poll, CommitOffset, and Metadata operations.
- The **authentication scheme** applied to client connections.
- The **error handling conventions** shared across all operations.
- The **pagination strategy** for the Poll operation.
- The **versioning policy** for the binary protocol.

The broker-to-broker Raft RPC operations (`APPEND_ENTRIES`, `REQUEST_VOTE`, `HEARTBEAT`) are internal protocol messages. They are documented in the OpenAPI specification for completeness but are not part of the public client API and are not subject to the same compatibility guarantees as client-facing operations.

---

## 2. Authentication Scheme

### 2.1 Design Rationale

Authentication in this system is **connection-scoped and token-based**. The design is deliberately minimal: the correctness and performance invariants are the primary deliverables, and a heavyweight authentication framework (OAuth2, mTLS PKI) would introduce complexity that obscures the core systems work. The scheme chosen — a pre-shared token presented in a handshake frame at connection open — is sufficient to prevent accidental cross-environment connections and to demonstrate the authentication seam without dominating the implementation.

The scheme is designed so that a production hardening pass can replace the token check with mTLS or SASL/SCRAM without changing any other part of the protocol: the handshake frame is the only authentication surface, and it is isolated in `ConnectionAcceptor` before any request frame is processed.

### 2.2 Handshake Protocol

Authentication occurs **once per TCP connection**, immediately after the TCP handshake completes, before any request frame is accepted. The sequence is:

```
Client                                    Broker
  │                                          │
  │── AUTH_REQ ─────────────────────────────►│
  │   (version, token)                       │
  │                                          │
  │                                    [validate token]
  │                                          │
  │◄─ AUTH_RESP ────────────────────────────│
  │   (success=true, brokerId, clusterName)  │
  │                                          │
  │── PUBLISH_REQ / POLL_REQ / ... ─────────►│
  │   (normal operation)                     │
```

If `AUTH_RESP` carries `success=false`, the broker closes the connection immediately after sending the response. No request frames are processed on an unauthenticated connection. If the client sends any request frame before receiving a successful `AUTH_RESP`, the broker closes the connection with an `ERROR_RESP` carrying code `AUTH_REQUIRED`.

### 2.3 Token Format and Validation

**Token format:** a UTF-8 string, maximum 512 bytes, transmitted as a length-prefixed field in `AUTH_REQ`. The token is opaque to the protocol layer; its content is validated by `AuthenticationService` in the broker.

**Token types supported (in priority order):**

| Type | Format | Validation |
|------|--------|------------|
| Pre-shared key (PSK) | Arbitrary UTF-8 string | Constant-time comparison against `broker.auth.tokens` config list |
| Disabled (development mode) | Empty string (`""`) | Always accepted when `broker.auth.enabled=false` |

**Configuration:**

```
broker.auth.enabled=true                          # set false only in dev/test
broker.auth.tokens=token-a,token-b,token-c        # comma-separated list of valid tokens
```

Tokens are loaded at broker startup from the configuration file or environment variable `BROKER_AUTH_TOKENS`. Hot-reload of tokens is not supported in v1; a broker restart is required to add or revoke tokens.

**Constant-time comparison:** token validation uses `MessageDigest.isEqual()` (or equivalent constant-time byte comparison) to prevent timing-oracle attacks. The comparison is performed against every configured token regardless of which one matches, to prevent token enumeration via timing.

### 2.4 Wire Format for Auth Frames

Two new message type bytes are reserved for authentication:

| Type byte | Name | Direction | Purpose |
|-----------|------|-----------|---------|
| 0x20 | `AUTH_REQ` | Client→Broker | Present credentials at connection open |
| 0x21 | `AUTH_RESP` | Broker→Client | Accept or reject credentials |

**`AUTH_REQ` payload:**

```
┌──────────────────┬──────────────────┬──────────────────────────────────┐
│ correlationId 4B │ protocolVersion  │ tokenLength 2B │ token bytes     │
│                  │ 2B               │                │ (max 512 B)     │
└──────────────────┴──────────────────┴────────────────┴─────────────────┘
```

**`AUTH_RESP` payload:**

```
┌──────────────────┬───────────┬──────────────┬──────────────────────────┐
│ correlationId 4B │ success   │ brokerId 4B  │ clusterNameLength 2B     │
│                  │ 1B        │              │ clusterName bytes        │
└──────────────────┴───────────┴──────────────┴──────────────────────────┘
```

When `success=0x00`, `brokerId` is `-1` and `clusterName` is empty. The broker closes the connection after flushing the response frame.

### 2.5 Broker-to-Broker Authentication

Broker-to-broker connections (Raft RPC) use the same handshake protocol with a dedicated internal token configured separately:

```
broker.internal.auth.token=<internal-token>
```

`ConnectionAcceptor` distinguishes client connections from broker connections by the `protocolVersion` field in `AUTH_REQ`: internal connections set the high bit of `protocolVersion` (`0x8000`). This allows `RequestHandler` to enforce that Raft RPC frames (`0x10`–`0x15`) are only accepted on broker-to-broker connections and that client frames (`0x01`–`0x08`) are only accepted on client connections.

### 2.6 Security Boundaries and Limitations

| Property | Status in v1 |
|----------|-------------|
| Confidentiality (encryption) | **Not provided.** TLS is out of scope for v1. Deploy behind a private network or VPN. |
| Integrity (tamper detection) | Provided at the record level by CRC32 in `LogRecord` and `ConsumerGroupOffsetRecord`. Not provided at the transport level. |
| Authentication | Provided by PSK handshake. |
| Authorization | **Not provided.** Any authenticated client may publish to or consume from any topic. Per-topic ACLs are a v2 concern. |
| Token rotation | Requires broker restart in v1. |

---

## 3. Error Handling Conventions

### 3.1 Error Response Structure

Every operation that fails returns an `ERROR_RESP` frame (type byte `0xFF`) in place of the normal response. The `ERROR_RESP` carries:

- The `correlationId` of the request that caused the error (so the client can match it to the pending future).
- A numeric **error code** (2 bytes, unsigned) identifying the failure category.
- A human-readable **error message** (length-prefixed UTF-8 string, max 1024 bytes) for logging and debugging. The message is not intended for programmatic parsing; clients must branch on the error code, not the message text.

**`ERROR_RESP` payload:**

```
┌──────────────────┬────────────────┬──────────────────────────────────────┐
│ correlationId 4B │ errorCode 2B   │ messageLength 2B │ message bytes     │
└──────────────────┴────────────────┴──────────────────┴───────────────────┘
```

### 3.2 Error Code Registry

Error codes are grouped by prefix. The grouping is a documentation convention; the broker does not enforce it programmatically.

| Code | Hex | Name | Retryable | Description |
|------|-----|------|-----------|-------------|
| 1 | `0x0001` | `NOT_LEADER` | Yes | This broker is not the leader for the requested partition. The error message contains the current leader's `host:port`, or `"unknown"` if leadership is in flux. The client must refresh metadata and retry. |
| 2 | `0x0002` | `STALE_LEADER_EPOCH` | Yes | The request carried a leader epoch older than the broker's current term. Refresh metadata and retry. |
| 3 | `0x0003` | `UNKNOWN_TOPIC` | No | The requested topic does not exist in the cluster configuration. |
| 4 | `0x0004` | `UNKNOWN_PARTITION` | No | The requested partition ID is out of range for the topic. |
| 5 | `0x0005` | `OFFSET_OUT_OF_RANGE` | No | The requested `fromOffset` is less than `firstOffset` (data has been deleted by retention) or greater than `nextOffset` (offset does not exist yet). |
| 6 | `0x0006` | `SEQUENCE_GAP` | No | The producer's `sequenceNumber` is more than 1 ahead of the broker's `lastCommittedSeq` for this `(producerId, partitionId)`. The producer skipped sequence numbers, which is a client programming error. Do not retry; the producer must be restarted. |
| 7 | `0x0007` | `DUPLICATE_SEQUENCE` | No (idempotent) | The producer's `sequenceNumber` is ≤ `lastCommittedSeq`. This is a duplicate; the broker returns the original committed offset in the error message. The client should treat this as a success and parse the offset from the message field. |
| 8 | `0x0008` | `BACKPRESSURE` | Yes (with backoff) | The broker's in-flight queue for this partition is full. The client should wait `retryBackoffMs` (default 100 ms) before retrying. |
| 9 | `0x0009` | `RECORD_TOO_LARGE` | No | The record's serialized size exceeds `maxFrameBytes` (default 16 MB). |
| 16 | `0x0010` | `AUTH_REQUIRED` | No | A request frame was received before a successful `AUTH_RESP`. The connection is closed. |
| 17 | `0x0011` | `AUTH_FAILED` | No | The token in `AUTH_REQ` was not recognized. The connection is closed. |
| 32 | `0x0020` | `PROTOCOL_ERROR` | No | A frame was malformed: unknown type byte, length exceeding maximum, or truncated payload. The connection is closed. |
| 33 | `0x0021` | `UNSUPPORTED_VERSION` | No | The `protocolVersion` in `AUTH_REQ` is not supported by this broker. |
| 64 | `0x0040` | `INTERNAL_ERROR` | Yes (with backoff) | An unexpected server-side error occurred. The broker logs the full stack trace. The client may retry after `retryBackoffMs`. |
| 65 | `0x0041` | `NOT_ENOUGH_REPLICAS` | Yes (with backoff) | The Raft group does not have a quorum of live replicas. The write cannot be committed. Retry after the cluster recovers. |
| 66 | `0x0042` | `LEADER_FENCED` | Yes | The leader was fenced mid-write (a new election completed). The write was not committed. Retry; the idempotent producer makes the retry safe. |

### 3.3 Retryability Rules

The client library (`ProducerClient`, `ConsumerClient`) applies the following retry logic automatically. Application code does not need to implement retry loops for retryable errors.

**Retryable without metadata refresh:**
- `BACKPRESSURE` — wait `retryBackoffMs`, retry same broker.
- `INTERNAL_ERROR` — wait `retryBackoffMs`, retry same broker up to `maxRetries` (default 3).
- `NOT_ENOUGH_REPLICAS` — wait `retryBackoffMs` × attempt number (linear backoff), retry same broker.
- `LEADER_FENCED` — refresh metadata, retry new leader.

**Retryable with mandatory metadata refresh:**
- `NOT_LEADER` — parse leader address from error message if present; otherwise refresh via `METADATA_REQ`; retry new leader immediately.
- `STALE_LEADER_EPOCH` — refresh metadata; retry new leader.

**Not retryable (client must surface to application):**
- `UNKNOWN_TOPIC`, `UNKNOWN_PARTITION` — configuration error; no retry.
- `OFFSET_OUT_OF_RANGE` — consumer must adjust its offset; no automatic retry.
- `SEQUENCE_GAP` — producer programming error; producer must restart.
- `RECORD_TOO_LARGE` — payload must be reduced; no retry.
- `AUTH_REQUIRED`, `AUTH_FAILED` — connection closed; no retry on same connection.
- `PROTOCOL_ERROR` — connection closed; reconnect and retry only if the request was idempotent.
- `UNSUPPORTED_VERSION` — broker version mismatch; operator intervention required.

**Special case — `DUPLICATE_SEQUENCE`:**
This code is returned when the broker detects an idempotent duplicate. The client library treats it as a transparent success: it parses the committed offset from the error message string (format: `"duplicate; offset=<N>"`) and resolves the pending `CompletableFuture<PublishResponse>` with a synthetic `PublishResponse(offset=N)`. Application code never sees this error code.

### 3.4 Connection-Closing Errors

The following errors always result in the broker closing the TCP connection after sending the `ERROR_RESP`:

- `AUTH_REQUIRED`
- `AUTH_FAILED`
- `PROTOCOL_ERROR`
- `UNSUPPORTED_VERSION`

The client detects the closed connection via an `IOException` on the next read. `BrokerConnection` catches this, marks the connection as closed, and fails all pending `CompletableFuture`s with a `ConnectionClosedException`. The client library then reconnects and re-authenticates before retrying.

### 3.5 Error Observability

Every `ERROR_RESP` sent by the broker is logged at `WARN` level with the following structured fields:

```
{
  "event": "error_response",
  "correlationId": <int>,
  "errorCode": <int>,
  "errorName": "<string>",
  "partitionId": <int>,          // if applicable
  "producerId": <long>,          // if applicable
  "remoteAddress": "<host:port>",
  "durationMs": <long>           // time from request receipt to error response
}
```

`INTERNAL_ERROR` responses additionally log the full exception stack trace at `ERROR` level.

---

## 4. Pagination Strategy

### 4.1 Offset-Based Pagination

The Poll operation uses **offset-based pagination**. This is the natural model for an append-only log: each record has a stable, monotonically increasing `offset` that never changes after assignment. A consumer reads a batch starting at a given offset and advances its position to `nextOffset` returned in the response. There is no concept of pages, cursors, or tokens — the offset is the cursor.

This model has three properties that make it correct for this system:

1. **Stability:** offsets are immutable. A consumer can re-read any offset range at any time (subject to retention). There is no cursor invalidation.
2. **Resumability:** after a crash, a consumer resumes from its last committed offset. No server-side session state is required.
3. **Parallelism:** multiple consumers in different groups can independently track their own offsets over the same partition log without coordination.

### 4.2 Poll Request Parameters

| Parameter | Type | Default | Constraints | Description |
|-----------|------|---------|-------------|-------------|
| `fromOffset` | long | (required) | ≥ 0 | The first offset to include in the response. Must be ≥ `firstOffset` (else `OFFSET_OUT_OF_RANGE`) and ≤ `commitIndex` (else empty response, not an error). |
| `maxBytes` | int | 65536 | 1 – 16777216 | Maximum total byte size of records to return in one response. The broker returns complete records only; it never splits a record across responses. |
| `maxRecords` | int | 1000 | 1 – 10000 | Maximum number of records to return. The broker stops at whichever limit (`maxBytes` or `maxRecords`) is reached first. |

### 4.3 Poll Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `records` | `LogRecord[]` | Ordered list of records starting at `fromOffset`. May be empty if no committed records exist at or after `fromOffset`. |
| `nextOffset` | long | `fromOffset + len(records)`. The consumer should use this as `fromOffset` in the next `POLL_REQ`. If `records` is empty, `nextOffset == fromOffset` (the consumer does not advance). |
| `highWatermark` | long | The current `commitIndex` for this partition at the time the response was generated. Allows the consumer to determine how far behind the head of the log it is. |

### 4.4 Empty Response Semantics

An empty `records` array is **not an error**. It means one of:

- The consumer has caught up to the `commitIndex` (it is at the head of the log).
- The `fromOffset` equals `nextOffset` (no new messages have been committed since the last poll).

The consumer should implement a **poll loop with backoff**:

```
backoffMs = minPollBackoffMs   // default 10 ms
loop:
    response = poll(fromOffset, maxBytes, maxRecords)
    if response.records is empty:
        sleep(backoffMs)
        backoffMs = min(backoffMs * 2, maxPollBackoffMs)   // exponential backoff, cap 1000 ms
    else:
        backoffMs = minPollBackoffMs   // reset on non-empty response
        process(response.records)
        fromOffset = response.nextOffset
        if shouldCommit():
            commitOffset(fromOffset)
```

The broker does **not** implement long-polling (holding the connection open until records are available). This is consistent with the blocking-IO, virtual-thread model: a long-poll would hold a virtual thread on the broker for the duration of the wait, which is acceptable for virtual threads but complicates the shutdown and timeout logic. The client-side backoff achieves equivalent behavior with simpler broker code.

### 4.5 Offset Boundary Conditions

| Condition | `fromOffset` value | Broker behavior |
|-----------|-------------------|-----------------|
| Consumer starts fresh | 0 | Returns records from the beginning of the retained log (which may be > 0 if retention has deleted old segments). If 0 < `firstOffset`, returns `OFFSET_OUT_OF_RANGE`. |
| Consumer resumes from committed offset | Last committed offset | Normal operation. |
| Consumer requests offset beyond `commitIndex` | > `commitIndex` | Returns empty `records`, `nextOffset = fromOffset`, `highWatermark = commitIndex`. Not an error. |
| Consumer requests deleted offset | < `firstOffset` | Returns `OFFSET_OUT_OF_RANGE`. Consumer must reset to `firstOffset` or to its last committed offset if that is still valid. |
| Consumer requests exact `commitIndex` | == `commitIndex` | Returns the record at `commitIndex` (one record). `nextOffset = commitIndex + 1`. |

### 4.6 Relationship to Consumer Group Offset Commits

Pagination state (the current `fromOffset`) is maintained **client-side** in `ConsumerClient.currentOffset`. It is durably checkpointed by sending a `COMMIT_OFFSET_REQ` to the broker, which persists it in S4 (Consumer Group Offset Log). The two operations are decoupled:

- **Poll** advances the in-memory `currentOffset` on every non-empty response.
- **CommitOffset** durably saves the current position so it survives consumer restarts.

The consumer controls the commit frequency. Committing after every poll gives the strongest resume guarantee (at most one batch re-processed after a crash) at the cost of one extra round-trip per batch. Committing every N batches reduces round-trips at the cost of potentially re-processing up to N batches after a crash. The client library does not commit automatically; the application must call `commitOffset()` explicitly.

---

## 5. Versioning Policy

### 5.1 Version Identifier

The protocol version is a **2-byte unsigned integer** carried in the `AUTH_REQ` frame. The current version is `1` (`0x0001`). The high bit (`0x8000`) is reserved for the broker-to-broker internal connection flag (§2.5) and must not be set in client connections.

Version `0` (`0x0000`) is reserved and rejected by all brokers.

### 5.2 Compatibility Model

The versioning model is **connection-level negotiation at handshake time**. The client declares the highest protocol version it supports in `AUTH_REQ`. The broker responds with the version it will use for this connection (which may be lower than the client's declared version, if the broker supports an older version). All frames on the connection after `AUTH_RESP` use the negotiated version.

```
Client declares:  protocolVersion = 3   (highest client supports)
Broker supports:  versions 1, 2
Broker responds:  negotiatedVersion = 2 (highest common version)
Connection uses:  version 2
```

If the client's minimum acceptable version is higher than the broker's maximum supported version, the broker sends `UNSUPPORTED_VERSION` and closes the connection. The client must surface this as a fatal configuration error.

**`AUTH_RESP` extended for negotiation:**

```
┌──────────────────┬───────────┬──────────────┬────────────────────┬──────────────────────────┐
│ correlationId 4B │ success   │ brokerId 4B  │ negotiatedVersion  │ clusterNameLength 2B     │
│                  │ 1B        │              │ 2B                 │ clusterName bytes        │
└──────────────────┴───────────┴──────────────┴────────────────────┴──────────────────────────┘
```

### 5.3 Backward Compatibility Rules

These rules govern what changes are permitted in a new protocol version without breaking existing clients:

**Backward-compatible changes (minor version increment, same major):**

| Change type | Rule |
|-------------|------|
| Add optional field at end of payload | Permitted. Older decoders ignore trailing bytes. All variable-length payloads are length-prefixed, so the decoder knows exactly how many bytes to consume. |
| Add new `MessageType` byte | Permitted. Older clients that receive an unknown type byte will throw `ProtocolException` and close the connection, but they will never send the new type, so this only affects broker-initiated messages. |
| Add new error code | Permitted. Older clients that receive an unknown error code treat it as `INTERNAL_ERROR` (retryable with backoff). |
| Extend enum values (e.g., new `FsyncPolicy`) | Permitted in config; not applicable to wire format. |

**Breaking changes (major version increment, new version required):**

| Change type | Rule |
|-------------|------|
| Remove or reorder fields in an existing payload | Breaking. Requires a new version. |
| Change the type or size of an existing field | Breaking. Requires a new version. |
| Change the semantics of an existing error code | Breaking. Requires a new version. |
| Change the frame header format (length field, type field) | Breaking. Requires a new version. |
| Change the `AUTH_REQ`/`AUTH_RESP` format | Breaking. Requires a new version. |

### 5.4 Version Lifecycle

| Version | Status | Notes |
|---------|--------|-------|
| `0x0000` | Reserved | Never valid |
| `0x0001` | **Current** | Defined by this document |
| `0x0002`–`0x7FFF` | Future | Reserved for future versions |
| `0x8000`–`0xFFFF` | Internal | High bit set = broker-to-broker connection flag; not a version number |

**Deprecation policy:** a version is deprecated when a successor version is released. Deprecated versions are supported for a minimum of two subsequent releases before removal. Removal of a version is a breaking change and requires a major version increment in the broker's own release versioning (separate from the protocol version number).

### 5.5 Broker Release Versioning vs. Protocol Versioning

These are two independent version numbers:

| Version type | Format | Scope | Example |
|-------------|--------|-------|---------|
| Protocol version | 2-byte unsigned int in `AUTH_REQ` | Wire compatibility | `1` |
| Broker release version | Semantic versioning (MAJOR.MINOR.PATCH) | Software release | `1.3.2` |

A broker release version increment does not imply a protocol version increment. Multiple broker releases may support the same protocol version. A protocol version increment always requires a new broker release, but not vice versa.

### 5.6 Client Library Versioning

The client library (`client/`) declares its minimum and maximum supported protocol versions in `ClientConfig`:

```java
public static final int MIN_PROTOCOL_VERSION = 1;
public static final int MAX_PROTOCOL_VERSION = 1;
```

The client sends `MAX_PROTOCOL_VERSION` in `AUTH_REQ` and accepts any negotiated version in `[MIN_PROTOCOL_VERSION, MAX_PROTOCOL_VERSION]`. If the broker negotiates a version below `MIN_PROTOCOL_VERSION`, the client throws `UnsupportedVersionException` and does not proceed.

---

## 6. OpenAPI Specification

The following OpenAPI 3.1 document describes the logical API. The `x-wire-type` extension field on each operation maps it to the binary message type byte defined in `protocol/MessageType`.

```yaml
openapi: "3.1.0"

info:
  title: Distributed Message Broker API
  version: "1.0.0"
  description: |
    Logical API specification for the Distributed Message Broker.

    **Transport:** Custom binary TCP protocol. This is not an HTTP API.
    All operations are encoded as length-prefixed binary frames as defined
    in `protocol/MessageCodec`. The OpenAPI document describes the logical
    contract — fields, types, constraints, and error semantics — in a
    human-readable and toolable format.

    **Connection lifecycle:**
    1. Open TCP connection to broker `host:port` (default port 9092).
    2. Send `AUTH_REQ` frame immediately.
    3. Await `AUTH_RESP`. On failure, connection is closed by broker.
    4. Send request frames. Responses may arrive out of order; match by
       `correlationId`.
    5. Close connection when done.

    **Versioning:** Protocol version `1` (`0x0001`). Declared in `AUTH_REQ`.

    **Error model:** All errors are returned as `ERROR_RESP` frames
    (type byte `0xFF`) carrying a numeric error code and a human-readable
    message. See the `ErrorResponse` schema and the error code registry
    in §3.2 of api-design.md.
  contact:
    name: Broker Engineering
  license:
    name: MIT

servers:
  - url: "tcp://localhost:9092"
    description: Local development broker (single node)
  - url: "tcp://broker-1:9092"
    description: Cluster node 1
  - url: "tcp://broker-2:9092"
    description: Cluster node 2
  - url: "tcp://broker-3:9092"
    description: Cluster node 3

tags:
  - name: authentication
    description: |
      Connection-level authentication. Must be completed before any
      other operation. One handshake per TCP connection.
  - name: producer
    description: |
      Operations for publishing messages to topic partitions.
      All publish operations must be directed to the partition leader.
      The client discovers the leader via the metadata operation.
  - name: consumer
    description: |
      Operations for reading messages from topic partitions and
      committing read positions. All poll operations must be directed
      to the partition leader.
  - name: metadata
    description: |
      Cluster and partition metadata discovery. May be sent to any
      broker; the responding broker returns the full partition-to-leader
      map as it knows it.
  - name: internal
    description: |
      Broker-to-broker Raft RPC operations. Not part of the public
      client API. Documented for completeness. Only accepted on
      connections that presented an internal authentication token
      with the broker-to-broker flag set in `protocolVersion`.

paths:

  # ---------------------------------------------------------------------------
  # Authentication
  # ---------------------------------------------------------------------------

  /connection/auth:
    post:
      operationId: authenticate
      summary: Authenticate a new TCP connection
      description: |
        Must be the first frame sent on every new TCP connection, by both
        client and broker-to-broker connections. The broker will not process
        any other frame until authentication succeeds.

        On success, the broker responds with `AUTH_RESP` carrying
        `success=true`, the broker's ID, the cluster name, and the
        negotiated protocol version.

        On failure, the broker responds with `AUTH_RESP` carrying
        `success=false` and immediately closes the TCP connection.

        **Wire type:** `AUTH_REQ` (0x20) → `AUTH_RESP` (0x21)
      tags:
        - authentication
      x-wire-type:
        request: "0x20"
        response: "0x21"
      requestBody:
        required: true
        content:
          application/octet-stream:
            schema:
              $ref: "#/components/schemas/AuthRequest"
      responses:
        "200":
          description: Authentication accepted. Connection is now active.
          content: