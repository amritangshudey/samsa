# ADR 0029: Key compaction by batch rewriting, with the keys on the leader only

Date: 2026-11-15. Status: accepted. Design: `docs/design/c2b-compaction.md`.

## Context

Compaction keeps, for every key, at least the latest record. Samsa has one
Raft log for every partition (ADR 0005), and part (a) frees disk by copying
the live batches out of the oldest segment (ADR 0026). Compaction has to
decide which records of a batch are dead, and the state machine, which is
the same on every node and must be pure, must not carry a map of every key.

## Decision

- **Compaction rewrites batches.** A batch with dead records is replaced by
  one that holds the others, with the same base offset and last offset, so
  every record that is left keeps its offset, key, value and timestamp.
  The offsets of the removed records are gaps, as in Kafka.
- **A new command, `cmdCompact` (18).** It carries topic, partition, the
  base offset and log index of the batch it replaces, and the new batch,
  or nothing to remove the batch. Apply replaces the reference only if it
  still points at that log index, as `cmdRelocate` does. Apply checks the
  header (a valid batch, its last offset below the next batch's base) and
  nothing else: it cannot read the log.
- **Keys live on the leader.** The leader reads its own log, counts the
  records of each key (16 bytes of SHA-256 of the key, the highest offset,
  a count, whether the latest is a tombstone) and decides. The map is
  soft state: never saved, rebuilt when a node becomes leader, bounded by
  `-compaction-map-bytes` (default 64 MiB; a partition with more keys is
  not compacted, and a warning says so).
- **A record is dead** if a newer record of its key exists and its batch is
  at least `min.compaction.lag.ms` old. A tombstone that is the latest
  record of its key is dead once its batch is `delete.retention.ms` old
  **and no other record of the key is left in the log** (the count), so a
  removed tombstone never brings back an older value.
- **A sweep** over a partition starts when at least half of its counted
  records have a newer record of their key, or when it has a tombstone
  (then at most every 30 s when the last sweep removed nothing). It is
  separate from the head-segment cleaner of part (a): a sweep shrinks
  batches wherever they are, which shrinks the referenced bytes, and the
  cleaner then frees disk as before.
- **Configs:** `cleanup.policy` accepts `delete`, `compact` and
  `compact,delete`; `delete.retention.ms` and `min.compaction.lag.ms` are
  new. Time retention and size retention apply only if the policy
  includes `delete`.
- **Producer limits:** a compacted topic refuses a record without a key
  (INVALID_RECORD, as Kafka) and a compressed batch
  (UNSUPPORTED_COMPRESSION_TYPE). The second is Samsa's limit: the core has
  no decompressor, and adding `compress/*` to its imports is a separate
  architecture decision.
- **Format level 3.** `cmdCompact` and the new config values are new
  content of the log and of snapshots. The cluster raises the level once
  every voter supports it (ADR 0024). The first draft of the design said
  the level stays 2; that was wrong by the rule in ADR 0024.
- **A first batch that starts after the log start is valid**, in a
  snapshot and in state: compaction can remove the batches around the
  start. Trim moves a partition's start only if it dropped a batch of it.
  Fetch at an offset in a gap starts at the next batch whose last offset is
  at or above it.

## Alternatives

- **A key to offset map in the state machine.** Apply could then check every
  removal. Rejected: it is in every snapshot and hash, it grows with the
  number of keys, and each update would be a replicated write.
- **Compaction only in the head segment, merged with the cleaner.** It
  starts only when disk exceeds the slack over referenced bytes, and dead
  records count as referenced, so an overwrite-heavy topic would grow far
  past its live data before anything started.
- **Decompress in the core.** A separate architecture decision.
- **Remove a batch only if all its records are dead.** A hot key leaves
  one live record in every batch; compaction would not bound the space.

## Consequences

- A compacted partition's disk is bounded by the slack, as for retention,
  not by its keys alone: dead records stay until their batch is rewritten,
  and the rewrite stays in the log until the head reaches it.
- A leader change rebuilds the key maps from the log: a read of every
  compacted partition once per term.
- Clients that compress cannot write to a compacted topic until the
  decompressor decision is made. Java's default and franz-go with
  `NoCompression` work.
- A node of an older build stops on command 18: rolling back after a
  compacted topic exists is not possible (item 10's upgrade test covers
  only the forward direction).
- `shortcut:` markers: a 128-bit digest (a collision removes a live
  record, about 2^-64 per key pair), a fixed dirty ratio of one half, a
  tombstone sweep that reads the whole partition every 30 s, no age for
  batches without timestamps (they are never compacted for lag nor are
  their tombstones removed).
