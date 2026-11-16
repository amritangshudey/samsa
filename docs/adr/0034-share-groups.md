# ADR 0034: Share Groups (KIP-932) in the Raft State Machine

## Status

Accepted

## Context

KIP-932 adds share groups (queue-style consumption with per-record acquisition locks, delivery counts, and explicit `Accept`/`Release`/`Reject`/`Renew` acknowledgements). In Apache Kafka, share-partition state is persisted to an internal `__share_group_state` topic via inter-broker `InitializeShareGroupState`, `ReadShareGroupState`, `WriteShareGroupState`, and `DeleteShareGroupState` RPCs.

## Decision

1. **Store share-group membership and record locks in Samsa's single Raft log** (`formatLevel = 6`, `snapshotVersion = 11`) via `cmdSGBeat`, `cmdSGRemove`, `cmdShareAcquire`, `cmdShareAck`, and `cmdShareExpire`. Because Samsa replicates all state through one Raft group, no inter-broker share-state RPCs or internal state topic are needed.
2. **Deterministic lock expiry**: Acquisition and renewal commands carry `nowMs` from the leader's `sys.Clock`, and the leader proposes `cmdShareExpire(nowMs)` when a lock's `lockUntil <= nowMs`. State transitions in `apply` remain a pure function of the log.
3. **Bounded state**: `spso` (Share Partition Start Offset) advances past contiguous `recAcked`/`recArchived` offsets and deletes their map entries; retention trims below `p.start` also advance `spso`.

## Consequences

- Java 4.4.0 `KafkaShareConsumer` and `ShareGroupDescribe` work against a single-node or multi-node Samsa cluster without internal topics or inter-broker share-state APIs.
- Lock state survives leader failovers and restarts deterministically via the Raft log and snapshots.
