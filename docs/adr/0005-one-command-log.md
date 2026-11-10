# 0005 — One replicated command log; partitions are views

Status: accepted, 2026-11-10 (decided during Phase 1, ahead of the Phase 2
design, because `docs/ROADMAP.md` depends on it)

## Context

The broker has several kinds of state: topics, record data, consumer
offsets, group membership. Phase 5 must replicate all of it, and the
simulator must be able to check that replicas agree. Kafka itself keeps a
log file per partition and replicates each separately.

## Decision

Every state change is a command appended to one log per node. Committed
commands are applied in order to a deterministic state machine. A partition
is an in-memory index from partition offset to the log index of the command
that holds the batch. On one node "committed" means synced to disk; in a
cluster it will mean committed by Raft. Details and rules R1 to R7 are in
`docs/ROADMAP.md`, section 1.

## Alternatives

- **A log per partition, as in Kafka, plus a metadata log.** Familiar, good
  fetch locality, per-topic retention. Rejected: on one node, group commit
  needs one fsync per partition touched; under Raft it needs either a Raft
  group per partition (much machinery for three nodes) or writing every
  batch twice (Raft log, then partition log).
- **A Raft group per partition** (Redpanda's design). The right answer for
  scaling out; a second project for one person.

## Consequences

- One fsync stream, one recovery procedure, one replication stream.
- Replica agreement is checkable as a state hash per applied index.
- Fetch locality is worse; retention is global; every node stores
  everything; start-up replays the log until snapshots exist.
- Produce is acknowledged only after commit, whatever `acks` asks for.
- Phase 1's frame `count` field is 1 for every command. It is kept: it
  costs four bytes and leaves the per-partition layout possible later.
