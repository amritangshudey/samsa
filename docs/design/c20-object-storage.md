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

## 6. Addendum (2026-11-21): a `cmdTier` must fit a log entry, and a round must end (BUGS.md S16, S17)

**Problem.** Section 2, Q1 packs a sealed segment's live batches into one
object, named by one `cmdTier` with an entry per batch. The command grew
with the number of batches, and a log entry holds `MaxBatchBytes +
commandOverhead`. Past that, Raft refused it as too large, the refusal
ended the round as an applied command does, and the next round uploaded
the same batches again: for ever, with nothing tiered or trimmed. The
simulator did not notice, because an untiered segment reads like a tiered
one. Reading the code for it showed a second way for tiering to stop: a
leader that stopped serving while its `cmdTier` waited never learned how
the round ended, and its `tiering` flag stayed set (S17).

**Decision.**
- An object takes batches while their entries fit in `MaxBatchBytes`; the
  rest of the segment goes in the next object, one round each. The
  command's header (at most 92 bytes: the type, a key of at most 85
  bytes, two lengths) and Raft's (9) fit in `commandOverhead` (512), so
  one entry (at most 283 bytes, for a topic name of 249) always fits: the
  first entry goes in whatever the limit. `TestTierEntryBytes` ties the
  size of an entry to `encodeTier`.
- `tiering` is state of one time as serving leader, like the counters of
  proposals that are never answered: serving again clears it with them.
- The simulator checks progress, T1: with object storage, the settled
  leader tiers and trims every segment but the one being written, within
  10 s. The node reports the end of its head segment
  (`HeadSegmentEnd`) for it.

**Alternatives rejected.**
- *A larger log entry for `cmdTier`.* The limit bounds every reader of
  the log: followers, snapshots, AppendEntries chunks, recovery. One
  command type above it would need its own bound in each.
- *One object named by several commands.* A crash between them leaves the
  object half referenced; GC and the read path would need to know which
  part. Several objects cost more uploads only for small batches: in
  production at most one per 1 MiB of entries, about 24,000 batches.
- *Backing off after a refused `cmdTier`.* It would slow the loop, not end
  it, and with the bound a refusal cannot happen.

**Second review.**
- At the simulator's 2 KB limit an object holds about 50 batches, so a
  1 MiB segment of 625 batches takes 13 rounds: one upload (at most
  20 ms) and one commit each, well inside T1's 10 s.
- More objects mean more keys in `uploadedObjs`, one an object.
- After S17's reset, an upload of the earlier round can complete and
  propose its command while a new round runs. `applyTier` tiers only
  batches still local and unmoved, so the data is tiered once and the
  other object is garbage for `gcObjects`. Ignoring such uploads, by
  round number, would save one upload a leadership change, and is not
  worth the state.
- T1 could not tell S16 from any other stall, and should not: any stall
  is a failure there.

## 7. Addendum (2026-11-21): compacted topics stay in the log (BUGS.md S18, S19; ADR 0042)

**Problem.** This design did not consider compaction. `docs/PRODUCTION.md`
asked it to say how the cleaner's rewrites meet stored objects, and it
never did. The tierer took a compacted topic like any other. Compaction
then rewrote tiered batches: it matched them by log index 0, gave them a
log index and left them their objects. Once the trim passed such a batch,
the snapshot holding it did not decode, and a node could neither restart
nor install a snapshot (S18). The simulator never drew tiering together
with compaction. Allowing the pair, 9 of 600 seeds failed. Three of them
were a second bug, in the prefetch of tiered reads: a fetch inside a gap
that compaction left before a tiered batch was answered
OFFSET_NOT_AVAILABLE at every retry (S19).

**Decision (ADR 0042).**
- The tierer skips a topic whose `cleanup.policy` includes `compact`, as
  KIP-405 does.
- The cleaner copies what tiering leaves forward, by its rule without
  object storage, with only that counted as live. A head segment that holds
  none of it is tiered and trimmed at once.
- Compaction never rewrites a batch that is in an object.
- The prefetch counts nothing for a first batch that begins before the fetch
  offset, so it also fetches the batch after it.
- The simulator draws tiering with compaction and with rot. T1 passes for a
  head segment that holds a compacted topic's batches (`HeadSegmentEnd`
  returns -1 for it). The mutation `UnsafeTierCompacted` brings S18 back.

**Alternatives rejected.** These are in ADR 0042: compacting objects by
making them local again, and relocating without the rule. For S19, the
prefetch could read the first batch's last offset to see whether the read
skips it. That needs the batch itself, which may not be cached yet, and a
second pass. Fetching one batch too many costs one ranged read, and that
batch is usually the next one the consumer asks for.

**Second review.**
- The change is in what a leader proposes, and no apply changes. Nodes on
  the old and the new code apply the same log alike, so no format level is
  needed. In a rolling upgrade, an old leader can still propose the bad
  `cmdCompact` until every node runs the new code.
- The fix does not repair a log that already holds such a rewrite. `v1.0.0`
  shipped the bug. A cluster that tiered a compacted topic on it can hold a
  batch that is in an object and at a log index. Once trimmed past, that
  batch still breaks snapshots on the new code. Repair would mean decoding
  such a batch as tiered (index 0), which changes the decoder and the state
  hash. Whether to do that, and whether to cut a release, is the
  developer's call (`docs/HANDOFF.md`).
- `keepsLocal` and `liveBytes(true)` scan every compacted partition's
  batches, once a round. The cleaner's loops already scan every batch.
- A fetch that starts in the middle of a batch, after a seek or a commit
  inside one, now prefetches one batch more when the partition limit would
  have stopped it. The read puts it in the cache for the next fetch.
- With compacted topics, the local log holds their live data, up to
  `RetentionSlack` times it plus a segment, as without object storage.
  Tiering waits behind a head segment that holds some of it, until the log
  exceeds that bound. T1 cannot see that wait, and should not fail it. A
  stall like S16 or S17 still fails T1 in every seed whose head has no
  compacted batches.
- Not covered: a diskless compacted topic, and a topic made `compact`
  after it was tiered. Their batches in objects are never compacted, and
  nothing refuses the setting. Both are in ADR 0042's consequences.

## 8. Addendum (2026-11-21): a fetch that waited for objects checks again (BUGS.md S20)

**Problem.** A fetch that needs batches in objects reads them into the
cache first, and is answered when the last read completes. The node's role
can change meanwhile. A follower may begin to install a snapshot, a
follower may lose its leader, or a leader may lose its lease. S13's rule
that an installing node answers no fetch was checked only when the fetch
arrived. In seeds 4174 and 2595 a follower answered during an install,
from a state that named batches its reset log no longer held, and stopped.

**Decision.** When its object reads complete, the fetch checks again, as
`completeFetch` does for a fetch that waited for new records. If the node
may no longer serve it, it answers NOT_LEADER_OR_FOLLOWER, and the client
asks for metadata and fetches elsewhere.

**Second review.**
- Both waits now end in the same check (`canRead() || canServeFetch()`). A
  third wait added to the fetch path must end in it too.
- A refusal costs the client a metadata round trip. It happens only when
  the node's role changed during the read, which is what the client must
  learn anyway.
- `stopServing` already drops a connection with a request in progress, so
  a leader that stops serving never answers. The new check matters for a
  follower, and for a leader whose lease ran out while it stays leader.

