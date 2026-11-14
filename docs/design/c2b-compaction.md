# C2 part (b): key compaction

Production plan item 12b, 2026-11-15. Builds on part (a)
(`docs/design/c2-retention-compaction.md`, ADR 0026 and 0027): the
copy-forward cleaner, topic configs as state, format level 2.

## 1. What compaction is

A compacted topic keeps, for every key, at least the latest value. Older
values of the same key are removed. A consumer that reads the whole topic
from the start sees the final state of every key, and the topic does not
grow with the number of updates, only with the number of keys. It is the
shape of a changelog or a table (Kafka's `__consumer_offsets` is one).

Kafka's guarantees, which Samsa follows (from the Kafka documentation,
"Log Compaction"):

- offsets never change; a removed record leaves a **gap** in the offsets;
  a consumer reading across a gap gets the next record that is left;
- every record that is left is byte-identical in key, value and headers;
- a record with a **null value is a tombstone**: it says "this key is
  deleted". It is kept for `delete.retention.ms` after it became the latest
  record of its key, so that a slow consumer sees the delete, and is then
  removed too;
- records newer than `min.compaction.lag.ms` are never removed (default 0);
- a record without a key cannot be put in a compacted topic.

## 2. What the plan asks for

Plan item 12: "invariant: the latest value of every key survives".
CLAUDE.md: the core stays pure, the simulator must be able to catch a
broken cleaner (a mutation), and the whole feature is behind the format
level (ADR 0024).

## 3. Design

### 3.1 Configs (extends ADR 0027)

- `cleanup.policy`: `delete` (as now), `compact`, or `compact,delete`
  (compact, and also expire by `retention.ms` and `retention.bytes`). Any
  other value is INVALID_CONFIG.
- `delete.retention.ms`: at least 0, default 86,400,000 (Kafka's 24 h).
  Broker flag `-delete-retention-time` for the default.
- `min.compaction.lag.ms`: at least 0, default 0.

All three are in the topic's config map, so they are already state, in
snapshots and in the hash. Nothing in the snapshot changes: the version
stays 8. `compact` and the two new keys are refused below format level 2,
which topic configs already require (they are one more value of a config
level 2 already carries).

### 3.2 What the producer must do

On a topic whose policy includes `compact`, the Produce handler rejects,
with the partition's error code and without proposing anything:

- a batch with a record whose key is null: INVALID_RECORD (Kafka:
  "Compacted topic cannot accept message without key");
- a compressed batch: UNSUPPORTED_COMPRESSION_TYPE (76).

The second is a limit, not Kafka's behaviour. The cleaner must read keys,
and the core has no decompressor (`compress/*` is not in the core's
import allowlist, and adding to it is an architecture decision, not
a convenience; the same limit is in `broker/verdict.go`). Java's default
(`compression.type=none`) and franz-go with `NoCompression()` work; a
client that compresses is refused clearly. `// shortcut:` in the handler;
upgrade by moving a decompressor out of the core (the cleaner runs on the
leader outside apply, so it could use the real system's) when it matters.

A topic whose policy is changed to `compact` later may hold older keyless
or compressed batches. The cleaner keeps those batches whole and says so
once in the log; nothing is lost.

Apply never refuses a committed batch (a command in the log is final). The
check is in the handler only, like the idempotent producer's sequence
check is in apply but size checks are in the handler.

### 3.3 Records removed, and the new command

Only the leader's cleaner decides what to remove, as with retention: it
proposes commands and apply stays pure. A removal is a command that
carries the new bytes of one batch.

`cmdCompact` (18, format level 2): topic, partition, `base` (the batch's
base offset), `from` (the log index the leader read it at), then the
rewritten batch, or nothing at all if every record of the batch is gone.

Apply, when the partition still has a batch at `base` whose log index is
`from` (otherwise it expired, moved or is stale, and the command does
nothing, as for `cmdRelocate`):

- with a batch: the new batch must have base offset `base`, at least one
  record, and its last offset must be below the next batch's base (or the
  partition's end). The reference then points at the new entry, with its
  new size. The span of offsets the old batch covered keeps being covered:
  offsets that are not in the new batch are gaps.
- without a batch: the reference is removed. The offsets it covered are
  gaps until the next batch.

The leader keeps the rewritten batch's **last offset equal** to the old
batch's: the header's LastOffsetDelta stays, only the record count and the
records change. Kafka does the same, which is why every Kafka client
already accepts a batch whose records do not span `lastOffsetDelta + 1`
offsets.

State needs three small changes for gaps:

- `PeekBatch` and `ParseBatch` (produce) still require
  `lastOffsetDelta == count - 1` (a client's batch has no gaps). The new
  `wire.ParseCompacted` is used on the cleaner's output and in apply of
  `cmdCompact`: count >= 1, count <= lastOffsetDelta + 1, and each
  record's offset delta strictly increasing and within the last offset
  (the cleaner builds it, apply only checks the header: it cannot read
  the log, and stays O(1)).
- Everything that reads `p.batches` already finds the batch for an offset
  by binary search on `base`, and uses the next batch's `base` as the end.
  With gaps, that end is too far by the gap: it is only used for the span
  check in `cmdRelocate`, for expiry's `end(j)`, and by
  DeleteRecords and ListOffsets. A relocation of a batch with gaps must
  not check `count == end - base`; it checks `base + lastOffsetDelta <
  end` instead.
- Fetch at an offset inside a gap returns the batch with the next
  records; at an offset past the last record of the last batch, nothing.
  Clients ignore gaps (they are the same as for batches the producer sent
  after other producers' batches).

### 3.4 Finding what is dead

Apply does not know keys, and it must not (a key map in the state machine
would be in every snapshot, hashed, and copied through Raft). The leader
builds the map of what is dead, in memory, when it cleans, as Kafka's
cleaner does:

For a compacted partition, the leader scans its batches from the start,
reads each from its own log (as the cleaner does now), decodes the records
and maps `digest(key)` to the highest offset seen with that key.
`digest` is the first 16 bytes of SHA-256 (the allowlist has
`crypto/sha256`); a collision, 2^-64 for any two keys at 2^32 keys, would
remove a live record and is not defended against beyond that. A
`shortcut:` says so. (Kafka uses MD5 for the same purpose.)

The map is soft state of the node, rebuilt after every change of
leadership or restart on first use, extended as the partition grows and
never saved. It is limited by `-compaction-map-bytes` (default 64 MiB,
about 2.5 million keys at 24 bytes each). When it is full, the scan stops:
only batches below the last scanned offset are rewritten that round, as
Kafka cleans only the part of the "dirty" log its offset map covered. The
rest is only relocated.

A record at offset `o` with key `k` is **dead** if:

1. the map has `k` at an offset above `o`; and
2. its batch's max timestamp is at least `min.compaction.lag.ms` old on the
   leader's clock; and
3. it is not itself the target of the next rule.

A **tombstone** (null value) that is the latest record of its key is
removed once its batch's max timestamp is `delete.retention.ms` old. Until
then it is live. (Kafka starts the clock from when the segment holding it
was cleaned; the batch's timestamp is simpler and removes it no earlier
than Kafka would, which is the safe side.)

For `compact,delete`, expiry (part a) and compaction both run and do not
conflict: expiry removes the oldest batches by DeleteRecords, compaction
rewrites batches above the start.

### 3.5 The cleaner's use of it

Part (a)'s `clean()` already visits every batch referenced in the head
segment and proposes `cmdRelocate` for it. For a compacted partition, per
batch, it now decodes, finds the dead records, and proposes:

- nothing different (`cmdRelocate`) if no record is dead, or the batch
  cannot be decoded (compressed or damaged: kept whole);
- `cmdCompact` with the rewritten batch if some are dead;
- `cmdCompact` with no batch if all are.

So the log grows by the live part of the head segment and the head is
trimmed after it, exactly as in part (a). Compaction adds no new trigger:
dead records are now just space that is not copied. The trigger stays
"disk over slack times live", where live counts what is referenced. With
compaction, live shrinks as dead records are removed from the references,
so the cleaner starts when an overwrite-heavy topic has used its slack.
Compacted data that is never superseded is copied like any other.

**A consequence to say plainly:** until the head segment is reached, dead
records take disk. A topic of a few keys updated millions of times is as
big as the slack allows, not as big as its keys. The bound is the same as
retention's: `slack × max(live, segment) + segment`.

### 3.6 Time, and the leader's clock

Ages use the batch max timestamp against the leader's clock, as retention
does (part a). A producer with a wrong clock can make its data compactable
early or late. The simulator's clients stamp the simulated clock.

### 3.7 Format level and rollback

Level 2 already gates configs and `cmdRelocate`; `cmdCompact` is one more
command at level 2. The simulator's I6 check (restart reproduces the
hash) covers it. An older level-2 build (one without compaction) would stop
on command 18 as on any unknown command (ADR 0024), so the release that
adds it raises nothing but must be flagged in the upgrade notes: **rolling
back to a build without compaction after a compacted topic exists is not
possible.** Item 10's rolling upgrade test covers the forward direction.

## 4. Invariants and how they are checked

New, in the simulator's model (`internal/sim/history.go`):

- **C1, the latest value of every key survives.** The model records, per
  compacted partition and key, the acknowledged record with the highest
  offset. A fetch from the start that returns offsets above the log start
  must contain it, unless it is a tombstone older than
  `delete.retention.ms` plus the lag (then it may be gone).
- **C2, nothing changes except by removal.** Every record a fetch returns
  is byte-identical to what was sent (key, value, timestamp). Already
  checked by `observe` for values, extended to keys.
- **C3, only superseded records disappear.** A record may be missing from
  a read only if (a) it is below the log start, or (b) a record with the
  same key and a higher offset was acknowledged, and its age is at least
  the lag, or (c) it is a tombstone old enough.
- **C4, no record without a key is accepted on a compacted topic** (the
  handler check), and no compressed one.

The model's current contiguity check (`next != base` between batches of
one fetch, and `end = base + len(records)`) must allow gaps on compacted
topics and nowhere else.

The simulator's clients: in a quarter of runs, one topic is created with
`cleanup.policy=compact` (plus `delete.retention.ms` and `min.compaction.lag.ms`
drawn from {0, 1000, 3000} like retention), and they send records with
keys drawn from a small set, with an occasional tombstone. Non-compacted
topics keep sending keyless records (keys on them are also allowed, and
their reads must be unchanged).

Mutations (each must be caught by C1 to C3 over 200 seeds):

1. `UnsafeCompactLatest`: remove the record with the **highest** offset
   instead of the older ones. C1.
2. `UnsafeCompactNoLag`: ignore `min.compaction.lag.ms`. C3.
3. `UnsafeTombstoneEarly`: drop tombstones at once, ignoring
   `delete.retention.ms`. C3.
4. `UnsafeCompactWrongKey`: use only the first 8 bytes of the key's digest
   (a collision on purpose). C1.

## 5. Alternatives

- **A key to offset map in the state machine.** It would let apply check
  every removal itself. Rejected: it is in every snapshot and hash, it
  grows with the number of keys, and the replicated log would carry its
  every update. The simulator checks the leader's decisions instead, as for
  retention.
- **Per-topic logs with Kafka's cleaner thread.** Rejected in part (a).
- **Decompress in the core.** Needs `compress/*` in the allowlist; a
  separate decision. The cleaner could use the real system's decompressor
  outside apply, but then the simulator would not run that code.
- **Keep batches whole, drop only fully-dead ones.** Simpler (no
  rewrite, no new CRC), but a batch with one live record keeps its dead
  records for as long as that record lives, which for a hot key never
  happens. Compaction would not bound the space.

## 6. Second review

1. *Can the cleaner remove the only copy of a key's value?* Only if the map
   says a higher offset exists. The map comes from reading the leader's
   own committed log, and the rewritten batch is built from the same
   read. A relocation or expiry in between moves or removes references,
   not content; the `from` check makes a stale rewrite do nothing. What
   if the higher-offset record itself expires (DeleteRecords or
   `compact,delete` retention) before the older one is read? The older
   one is then the latest left, but the cleaner removed it earlier
   because a newer one existed then. **That is allowed:** retention may
   drop any prefix, and the older record was already superseded. C1 holds
   "latest value of every key *above the log start*".
2. *Can a rewritten batch break the idempotent producer?* The partition's
   producer state (last sequence, last batches) is in `p.producers`,
   not in the log, and is not changed by `cmdCompact`. A retried batch is
   checked against that state, so a batch removed from the log by
   compaction is still recognised as a duplicate. (Kafka keeps empty
   batches for this reason; here the state does it.)
3. *Can the leader change in the middle?* A new leader's map is empty and
   built from its own log, which holds every committed entry the old one
   had. A `cmdCompact` the old leader proposed and that committed is
   applied everywhere; one that did not commit is never applied, and the
   new leader recomputes. Compaction commands are idempotent in effect:
   applying one for a batch already rewritten does nothing (`from` no
   longer matches).
4. *A consumer in the middle of a gap.* A fetch at an offset in a gap
   returns the next batch whose last offset is at or above it. Clients
   accept a first record above the requested offset (compaction in Kafka
   does this). The model's I2 check ("began with a batch covering the
   offset") must treat compacted topics' batches as covering
   `base..base+lastOffsetDelta`, not `base..base+count-1`.
5. *High watermark and `next`.* Unchanged: `p.next` counts offsets
   assigned, not records, so removals leave it alone.
6. *Memory.* The map is bounded by a flag; if the log is bigger than the
   map covers, space is only freed in the covered part each round. The
   unit test must show a log with more keys than the map holds still
   converges (more slowly), not loops forever (the S11 failure for the
   cleaner: a round that frees nothing and is repeated).
7. *A snapshot in the middle.* References are in the snapshot; the new
   command changes only references and sizes, which are already
   serialised. Format level and version are unchanged.
8. *Cost.* A scan reads the whole partition once per leader term, then
   only new batches. The benchmark of part (a) is extended with a
   compacted topic (keys 10^5, 90% of updates to 10% of keys) to measure
   throughput, amplification and space against a delete-policy run.
9. *What if timestamps are -1* (a client without timestamps)? Those
   batches have no age: min lag and delete retention cannot be decided,
   so a record in one is neither compacted for lag nor is a tombstone in
   it ever removed (as for time retention in part (a), same `shortcut:`).
10. *Does this help Kafka's problems* (ADR 0028)? No: it is Kafka's
    feature, completed. It matters because several of the later items
    (consumer group offsets, share groups) are best stored as compacted
    state.

Findings that changed the design while reviewing: removed the key map from
the state machine (first draft); the `base + lastOffsetDelta < end` check
instead of `count == end - base`, because with gaps the count is smaller
than the span; the compressed-batch refusal, because the core has no
decompressor.

## 7. As built (2026-11-15)

What differs from sections 3 to 6:

- **Format level 3, not 2.** `cmdCompact` and the config values are new
  content of the log (ADR 0024). Section 3.1 and 3.7 said no raise; wrong.
- **Sweeps, not the head-segment cleaner.** Section 3.5 had the cleaner
  rewrite batches as it relocates them. As built, a separate sweep
  (`compact.go`) rewrites any batch with dead records, wherever it is.
  Reason: the cleaner starts when disk exceeds slack times referenced
  bytes, and dead records count as referenced, so an overwrite-heavy topic
  would not start it. A sweep shrinks the referenced bytes; the cleaner
  then frees disk as in part (a).
- **A sweep starts** when at least half the counted records have a newer
  record of their key, or the partition has a tombstone; after a sweep
  that removed nothing, the next waits 30 s.
- **The tombstone rule needs a count.** A tombstone that is the latest of
  its key goes only if it is the only record of the key left. The first
  design said old enough; a removed tombstone would then bring an older
  value back when the older record was in a batch not yet rewritten
  (simulator mutation `UnsafeTombstoneOrphan`, C5).
- **Gaps changed three more places than section 3.3 listed:** the snapshot
  reader (a first batch after the start is valid), Trim (a partition's
  start moves only if a batch of it was dropped), and Fetch (a first batch
  that ends before the offset is skipped).
- **Compacted-records and relocated-bytes counters count at apply,** by
  the command's `applied` result, not at proposal; the first version
  counted a stale command twice.
- **Simulator:** one extra topic `tc` in half the runs; records carry one
  of eight keys (six updated all the time, two for tombstones and rare
  puts, so a tombstone often stays latest); one record in 7 is a
  tombstone; 3% of batches to it lack a key and must fail. Acknowledged
  writes to it never use acks=0, which leaves records unanswered and
  blinds the checks for their key. C1 to C5 are in `sim/compaction.go`;
  C1 and C5 run after the final read, C3 (and C1 for a key whose records
  are all answered) while reading. Held back after a power loss that may
  have cut acknowledged data (not strict and epoch above 0).
- **Mutation scores (200 seeds each):** compact away the latest record 67
  (C1); compact inside the lag 61 (C3); remove a tombstone early 8 (C1);
  leave older records behind a removed tombstone 26 (C5); tell keys apart
  by their first byte 82 (C1).
