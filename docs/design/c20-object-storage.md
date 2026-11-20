# C20 & C21 design: unified object storage (tiered storage and diskless topics)

Status: accepted, 2026-11-20. Production plan items 20 and 21
(`docs/PRODUCTION.md`, section 7). One design covers both tiered storage (item 20)
and diskless topics (item 21).

## 1. Motivation & KIP-1150 / KIP-1163 / KIP-1164 context

Apache Kafka addresses cloud storage and cross-AZ replication costs in two
separate mechanisms:
- **Tiered storage (KIP-405)**: closed local log segments are uploaded to object
  storage (`RemoteStorageManager`) and deleted locally; active writes still go
  to local disk and replicate across brokers.
- **Diskless topics (KIP-1150, KIP-1163, KIP-1164)**: an opt-in per-topic
  configuration (`diskless.enabled=true`). Any broker accepts produce requests
  (leaderless writes), micro-batches payloads into Shared Log Segment Objects in
  object storage, and registers the uploaded object references with a Batch
  Coordinator that assigns official offsets and ordering.

In Samsa, every partition is already a slice of `batchRef`s in the Raft-replicated
state machine — not separate partition files on disk. That means **one unified
reference representation** serves both tiered storage (Item 20) and diskless
topics (Item 21), and Samsa's existing Raft log already *is* the Batch Coordinator.

## 2. Answering the three open questions (`docs/PRODUCTION.md` §7)

### Q1: Copy-forward cleaner (`12a`) vs. stored objects
`clean()` in `internal/broker/retain.go` copies live batches whose `logIndex` lies
in the head Raft segment `[start, end)` forward via `cmdRelocate` so the head
segment can be trimmed. If stored objects still had `logIndex > 0`, the cleaner
would copy them through Raft and re-upload them.

**Answer:**
- Every remote batch (`batchRef.objKey != ""`) has `logIndex == 0`.
- `clean()` only scans batches with `b.logIndex >= start && b.logIndex < end`, so
  already-tiered and diskless batches are **never** copied through Raft or
  re-uploaded.
- When object storage is enabled and `clean()` frees a closed head segment
  `[start, end)`:
  1. Instead of `cmdRelocate`, the leader packs all live local batches in
     `[start, end)` into a single immutable object (`tier/<term>-<start>-<end>.obj`),
     computing CRC32C for each batch slice.
  2. The leader issues an async `ObjectStore.Put` and **waits for confirmation**.
  3. Only after `Put` succeeds does the leader propose `cmdTier` (`byte = 31`),
     which atomically transitions each matching `(topic, part, base, oldLogIndex)`
     `batchRef` to `{logIndex: 0, objKey, objOffset, objLen, objCRC}`.
  4. Once `cmdTier` applies, no `batchRef` points into `[start, end)`, and
     `clean()` trims the local segment with `cmdTrim`.
- **Retention on stored objects**: `expireRecords` (`cmdDeleteRecords`) removes
  expired `batchRef`s as usual. A periodic leader GC pass (`gcObjects`) deletes
  any uploaded `objKey` that is no longer referenced by any `batchRef` in `state`
  — deleting objects directly without copying through Raft.

### Q2: Lagging followers and replacement nodes
A lagging follower or brand-new replacement node receives a Raft snapshot plus
the un-tiered local tail.

**Answer:**
- Format level `8` (`formatLevel = 8`) and `snapshotVersion = 13` persist
  `{objKey, objOffset, objLen, objCRC}` for every `batchRef`.
- A replacement node installing a snapshot gets all object references in the
  snapshot metadata and only copies the un-tiered local Raft tail. It never
  downloads old closed segments over Raft; fetches for older offsets read byte
  ranges directly from the object store.

### Q3: Is tiering worth building if diskless references do the same job?
**Answer:** Yes, because they share 85% of the implementation (`sys.ObjectStore`,
`batchRef` object fields, snapshot v13, async remote fetch + CRC32C verification
+ bounded LRU cache, S3 SigV4 client, and object GC). The only difference is
*when* `Put` happens:
- **Tiered (Item 20)**: hot writes commit to local Raft first for low produce
  latency (~1 ms), then closed segments offload asynchronously.
- **Diskless (Item 21)**: per-topic opt-in (`diskless.enabled=true`) where any
  broker uploads produced batches directly to object storage and commits a tiny
  `cmdAppendRef` (~80 B) through Raft — trading one object-store `Put` on the
  produce path for zero cross-AZ Raft payload replication.

## 3. Core boundary (`internal/sys`)

The deterministic core stays single-threaded and never blocks on network I/O
(ADR 0001 / 0002):

```go
type ObjOpID uint64

type ObjectStore interface {
    Put(id ObjOpID, key string, data []byte)
    Get(id ObjOpID, key string, offset, length int64)
    Delete(id ObjOpID, key string)
}

type ObjectHandler interface {
    OnObjectDone(id ObjOpID, data []byte, err error) error
}
```

- **Real driver (`internal/realsys/s3.go`)**: zero-dependency S3 SigV4 HTTP
  client (`net/http`, `crypto/hmac`, `crypto/sha256`) supporting `PUT`, ranged
  `GET` (`Range: bytes=off-end`), and `DELETE` against any S3-compatible
  endpoint (`-object-store-endpoint`, `-object-store-bucket`,
  `-object-store-region`, `-object-store-access-key`, `-object-store-secret-key`),
  plus a local-directory backend (`file:///path`) for zero-setup testing.
  Executes I/O on a bounded worker pool and delivers `OnObjectDone` onto the
  driver's single event channel.
- **Simulated driver (`internal/simsys/object.go`)**: deterministic in-memory
  object store driven by the simulation clock and RNG, injecting:
  - **lost** writes/reads (returns I/O error or drops unconfirmed write),
  - **slow** operations (delayed completion on the sim event queue),
  - **duplicated** writes (idempotent `Put` of same key),
  - **torn writes** (truncated or bit-flipped object bytes — detected by
    `batchRef.objCRC` on `Get`).

## 4. Read path and caching

- `readBatch` serves local batches (`ref.logIndex > 0`), control batches
  (`ref.control != 0`), and cached remote batches (`n.objCache`) synchronously.
- On a remote batch cache miss during `Fetch` (or `ShareFetch`), `Node` issues
  `n.obj.Get` for the needed byte range(s) and parks the response callback until
  `OnObjectDone` arrives.
- Every remote batch read verifies `crc32.Checksum(data, Castagnoli) == ref.objCRC`.
  On I/O error or CRC mismatch, `Node` retries (bounded) or returns retriable
  `ErrOffsetNotAvailable` / `ErrNotLeaderOrFollower` — never serving corrupted
  or missing committed bytes.

## 5. Safety invariants and mutation

- **Existing simulator invariants (`I1`–`I4`, `R1`–`R9`, `S1`–`S7`)** hold
  unchanged when object storage is enabled under faults (lost, slow, duplicated,
  torn writes, crashes, power losses, node replacements).
- **Mutation (`UnsafeDeleteBeforeUpload`)**: proposes `cmdTier` (allowing local
  segment trim) *before* `ObjectStore.Put` is confirmed (and drops the upload on
  injected loss), causing subsequent reads of tiered offsets to fail or lose
  committed records — caught by the simulator.
