# 0039: Defer multi-Raft sharding (Item 14) after diskless topics

Date: 2026-11-20  
Status: Accepted (C14)

## Context

`docs/PRODUCTION.md` scheduled Item 14 (multi-Raft sharding) to be decided by
ADR *after* Item 21 (diskless topics), because the primary motivation for
sharding a single-Raft broker is lifting the single-leader write-throughput
ceiling.

## Evidence from Items 19, 20, and 21

1. **Read scaling (Item 19, ADR 0031)**: KIP-392 follower fetch lets consumers
   read from any replica in their rack, taking read bandwidth off the Raft
   leader.
2. **Storage scaling (Item 20, ADR 0037)**: closed segments tier to S3-compatible
   object storage (`formatLevel = 8`, `snapshotVersion = 13`), bounding local
   disk to the active segment tail and keeping node restart/replacement fast.
3. **Write scaling (Item 21, ADR 0038)**: `diskless.enabled=true` lets *any*
   broker accept produce requests, uploads batch payloads directly to object
   storage, and replicates only a ~97-byte `cmdAppendRef` per batch through Raft
   (4,211 B -> 97 B for a 4 KiB batch in `TestDisklessProduceIdempotenceAndRaftBytes`,
   a 97.7% reduction in Raft log and inter-broker replication bytes).

## Decision

**Do not build multi-Raft sharding for v1.0.** Keep Samsa's single-Raft
ordering log paired with leaderless diskless writes and follower reads.

## Consequences

- Transactions (Item 15), consumer groups (Item 18), share groups (Item 22),
  and retention/compaction remain atomic within one Raft state machine without
  two-phase commit or cross-shard rebalancing.
- Classic (non-diskless) topics remain bounded by one Raft leader's disk and
  network bandwidth; workloads that exceed that ceiling opt into
  `diskless.enabled=true` per topic.
- Multi-Raft sharding can be revisited only if metadata-only `cmdAppendRef`
  throughput (~97 B/batch) becomes a measured bottleneck in real deployments.
