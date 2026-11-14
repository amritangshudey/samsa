# ADR 0026: Per-topic retention by copying live data forward in the one log

Date: 2026-11-14. Status: accepted. Design: `docs/design/c2-retention-compaction.md`.

## Context

Retention today is one size bound on the whole Raft log. Production plan
item 12 asks for per-topic retention (`retention.ms`, `retention.bytes`)
and key compaction. Raft can drop only a prefix of its log, so a
long-lived record near the head pins everything after it.

## Decision

Keep one log. When the log exceeds a slack factor times the live data, or
the global `-retention-bytes`, the leader copies the batches still
referenced in the head segment to the tail (`cmdRelocate`), then trims the
head. Expiry per topic is a separate command (`cmdExpire`) that only moves
a partition's start. The slack defaults to 2.

Measured with a model of the policy (design, section 2): at slack 2, the
cleaner adds 0 to 26% to the bytes written, across workloads that mix
short and long retention and compacted topics.

## Alternatives

- **Per-topic logs with Kafka-style replication.** No amplification, but
  a second replication protocol beside Raft, outside the state machine
  the simulator checks.
- **Copying whole segments.** Copies dead data too.
- **Cleaning only at the global bound.** Space would be unpredictable.

## Consequences

- Writes cost up to about a quarter more at the default slack, in the
  workloads measured. The benchmark will measure a mixed workload.
- Retention stays inside the replicated state machine. The simulator's
  invariants cover it, and new ones are added (R8 to R10).
- Topic configs and live bytes enter the state snapshot: a new snapshot
  version and format level.
- Compaction (part b) reuses the cleaner: it relocates only the live
  records of a compacted batch.
