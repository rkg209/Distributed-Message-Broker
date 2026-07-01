---
id: "01"
title: Wire Protocol & Network Layer
status: todo
phase: 1
depends_on: ["00"]
requirements: [FR-1, FR-2, FR-3, FR-4, FR-5, FR-6]
---

# Spec 01 · Wire Protocol & Network Layer

## What
Implement the custom length-prefixed binary protocol over TCP that all
client↔broker and broker↔broker communication will use. Deliver a virtual-
thread-per-connection server, a matching client, and the full codec for all
message types.

## Why
Every other feature rides this protocol. Getting the framing, type system, and
codec right first means all later specs can focus on logic, not parsing.
The custom binary protocol is also an explicit portfolio signal (CON-3) — it
must NOT be replaced with gRPC, Protobuf, or Netty.

## Wire format
```
┌──────────────┬───────────┬──────────────────────────────────┐
│ length (4B)  │ type (1B) │ payload (length - 1 bytes)       │
└──────────────┴───────────┴──────────────────────────────────┘
```
- `length` = byte count of `type + payload`; big-endian int32.
- Maximum frame size: 16 MB (configurable).
- All multi-byte payload fields: big-endian.

## Message types to implement
| Byte | Name               | Direction      |
|------|--------------------|----------------|
| 0x01 | PUBLISH_REQ        | Client→Broker  |
| 0x02 | PUBLISH_RESP       | Broker→Client  |
| 0x03 | POLL_REQ           | Client→Broker  |
| 0x04 | POLL_RESP          | Broker→Client  |
| 0x05 | COMMIT_OFFSET_REQ  | Client→Broker  |
| 0x06 | COMMIT_OFFSET_RESP | Broker→Client  |
| 0x07 | METADATA_REQ       | Client→Broker  |
| 0x08 | METADATA_RESP      | Broker→Client  |
| 0x10 | APPEND_ENTRIES_REQ | Broker→Broker  |
| 0x11 | APPEND_ENTRIES_RESP| Broker→Broker  |
| 0x12 | REQUEST_VOTE_REQ   | Broker→Broker  |
| 0x13 | REQUEST_VOTE_RESP  | Broker→Broker  |
| 0x14 | HEARTBEAT_REQ      | Broker→Broker  |
| 0x15 | HEARTBEAT_RESP     | Broker→Broker  |
| 0xFF | ERROR_RESP         | Broker→Client  |

## Key classes (in `protocol/`)
- `Frame` — immutable value type wrapping type byte + payload bytes
- `MessageType` — enum of all type bytes above
- `FrameDecoder` — reads from InputStream, blocks until a complete frame, returns typed Request/Response
- `FrameEncoder` — writes a typed object to OutputStream as a length-prefixed frame
- `MessageCodec` — per-type serialization/deserialization (hand-written binary, no JSON/Protobuf)
- `ProtocolException` — thrown on malformed frames; never swallowed

## Server (in `broker/`)
- `ConnectionAcceptor` — accepts TCP connections in a loop; spawns one virtual thread per connection
- `RequestHandler` (stub) — reads frames, logs them, returns a stub response (full logic in later specs)

## Client (in `client/`)
- `BrokerConnection` — single TCP connection; correlation-ID-based request/response matching
- (Full ProducerClient/ConsumerClient are Spec 02+)

## Acceptance criteria
1. A client opens a TCP connection to the server.
2. Client sends a METADATA_REQ frame; server echoes a stub METADATA_RESP.
3. A malformed frame (bad length, unknown type) results in an ERROR_RESP; server does NOT crash.
4. `FrameDecoder` and `FrameEncoder` are unit-tested for round-trip fidelity for every message type.
5. Property test: any random byte sequence fed to FrameDecoder either parses successfully or throws ProtocolException — never hangs, never crashes.
6. The server handles 100 concurrent connections (virtual threads) without error.

## Out of scope
- Actual publish/poll/Raft logic (stubs only).
- TLS / authentication.
- Connection pooling or retry logic in the client.
