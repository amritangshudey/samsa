# 0037: Unified object storage for tiered storage and diskless topics

Date: 2026-11-20  
Status: Accepted (C20 / C21)

## Context

Items 20 (tiered storage) and 21 (diskless topics) in `docs/PRODUCTION.md` both
store immutable record batches in S3-compatible object storage while keeping
ordering, offsets, and metadata in Samsa's Raft state machine. We needed a single
architecture that avoids duplicating storage mechanisms, does not break the
single-threaded deterministic core (ADR 0001/0002), and adds zero external
runtime dependencies.

## Decision

1. **Unified `batchRef` representation**: extend `batchRef` with
   `{objKey, objOffset, objLen, objCRC}`. Local batches have `logIndex > 0` and
   `objKey == ""`; tiered and diskless batches have `logIndex == 0` and
   `objKey != ""`.
2. **Async `sys.ObjectStore` boundary**: add `ObjectStore` (`Put`, `Get`,
   `Delete`) and `ObjectHandler.OnObjectDone` to `internal/sys`. Real S3 calls
   use a stdlib-only AWS SigV4 HTTP client on a worker pool that posts
   completions back to the single-threaded driver; the simulator runs a
   deterministic faulty store (`lost`, `slow`, `duplicated`, `torn`).
3. **Format level 8 and snapshot v13**: add `cmdTier` (`byte = 31`) and
   encode remote `batchRef` fields in snapshot v13 so lagging followers and
   replacement nodes only copy the snapshot and un-tiered local tail.
4. **Tiered retention & GC**: `clean()` tiers closed segments to object storage
   (waiting for `Put` confirmation before proposing `cmdTier` and `cmdTrim`)
   instead of copying through Raft; `gcObjects` deletes unreferenced objects
   after `cmdDeleteRecords` expires their `batchRef`s.

## Consequences

- Zero external dependencies in the broker binary (stdlib-only SigV4 S3 client).
- Local disk usage is bounded by the active tail segment; node replacement copies
  only metadata snapshot + un-tiered tail.
- Item 21 (diskless topics) reuses the exact same `batchRef`, snapshot v13,
  remote fetch path, cache, and object GC, adding only per-topic direct-to-object
  produce (`cmdAppendRef`).
