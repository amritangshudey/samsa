# C21 report: diskless topics (`diskless.enabled=true`)

Date: 2026-11-20

## Summary

Implemented per-topic diskless mode (`diskless.enabled=true`) on top of C20's
unified object storage (`internal/broker/diskless.go`, `state.go`, `forward.go`,
`node.go`, `handlers.go`, `metrics.go`):

- **Per-topic config**: `diskless.enabled` (`"true"` / `"false"`), requiring
  `ObjectStore != nil` and `formatLevel = 9`.
- **Metadata-only Raft log (`cmdAppendRef`, byte 32)**: stores only batch
  metadata and `{objKey, objOffset, objLen, objCRC}` (~97 bytes per batch
  regardless of payload size), reusing snapshot v13 without changes.
- **Leaderless writes**: any broker (leader or follower) accepts a
  `ProduceRequest` for diskless topics, uploads raw batch bytes directly to
  `ObjectStore`, and commits `cmdAppendRef` via `proposeOrForward`.
- **Bug fixes from C20 3000-seed verification**:
  - `gcObjects()` no longer deletes an object while `cmdTier` is still in flight
    (`BUGS.md` `C20-3`).
  - `world.go` routes `OnObjectDone` errors through `w.call(inc, ...)` so
    simulated disk crashes during `OnObjectDone` are handled as node crashes.
  - `OnObjectDone` ends with `return n.settle()` so proposals initiated from
    object-store callbacks flush Raft immediately (`BUGS.md` `C21-1`).

## Measurements & Verification

- **Unit test (`TestDisklessProduceIdempotenceAndRaftBytes`)**:
  - 4 KiB batch Raft log footprint: **97 bytes** (diskless) vs **4,211 bytes**
    (classic) — **97.7% reduction** in Raft log and inter-broker replication
    bytes.
  - Idempotent duplicate deduplication and cold-cache `Fetch` from `ObjectStore`: PASS.
- **3-node cluster integration (`TestDisklessTopicsCluster` across 3 racks)**:
  - Non-leader follower accepted direct `ProduceRequest` (`BaseOffset=0`).
  - 60 × 2 KiB sequential synchronous produces: **605.2 ms total**, **10.26 ms
    average latency** per produce (in-process HTTP S3 `PUT` + 3-node Raft commit),
    **127,675 bytes** stored in S3 vs **6,427 bytes** on node 1 local Raft disk.
  - Full 3-node cluster restart (cold cache): all 60 records verified via
    `franz-go` with **60 S3 ranged GETs**.
  - Note: measurements are local against an in-process HTTP server, not real
    cloud S3 latency or pricing.
- **Simulator (`TestSim -seeds=3000`)**:
  - Exercises both tiered storage and diskless `t1` topics under faulty object
    storage (`LostPut=0.15`, `LostGet=0.10`, `TornRead=0.15`, `MaxDelay=20ms`),
    crashes, power losses, and partitions with all R- and I-invariants unchanged.
