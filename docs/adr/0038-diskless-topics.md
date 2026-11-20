# 0038: Diskless topics via `cmdAppendRef` and leaderless produce forwarding

Date: 2026-11-20  
Status: Accepted (C21)

## Context

With unified object storage (ADR 0037) in place, closed segments offload to S3,
but active writes for classic topics still replicate full batch payloads over
Raft across racks. Item 21 adds per-topic diskless mode (`diskless.enabled=true`)
so brokers upload batches straight to object storage and Raft replicates only
compact object references.

## Decision

1. **Per-topic config `diskless.enabled`**: `"true"` or `"false"` (default
   `"false"`), gated on `formatLevel >= 9` and `Config.ObjectStore != nil`.
2. **`cmdAppendRef` (byte 32)**: carries batch metadata (`count`, `pid`,
   `epoch`, `seq`, `minTs`, `maxTs`, `transactional`) plus `{objKey, objOffset,
   objLen, objCRC}` (~90 bytes) instead of the batch payload. Applies with the
   same idempotence and transaction checks as `cmdAppend`.
3. **Leaderless produce on followers**: any follower accepts a `ProduceRequest`
   for diskless topics, uploads the batch directly to `ObjectStore`, and
   forwards only the ~90-byte `cmdAppendRef` to the Raft leader via
   `proposeOrForward`.

## Consequences

- Cross-rack inter-broker replication bytes drop from `O(payload * (N-1))` to
  `O(90 B * (N-1))` per batch.
- Produce latency includes one `ObjectStore.Put` round-trip plus one metadata
  Raft commit, measured and reported plainly in `docs/reports/c21-diskless-topics.md`.
- Snapshot format v13 is reused without changes.
