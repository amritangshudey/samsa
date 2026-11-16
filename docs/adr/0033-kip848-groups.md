# ADR 0033: KIP-848 Consumer Group Protocol and Format Level 5

## Status

Accepted

## Context

Kafka 4.0+ introduces the next-generation consumer rebalance protocol (KIP-848), replacing client-driven stop-the-world `JoinGroup`/`SyncGroup` rounds with server-side assignment via `ConsumerGroupHeartbeat` (API 68) and inspection via `ConsumerGroupDescribe` (API 69). Standard Kafka 4.4.0 CLI tools (`kafka-consumer-groups.sh`) also rely on `DescribeTopicPartitions` (API 75) and batched `OffsetFetch` v8–9 / `FindCoordinator` v4.

## Decision

1. **Format level 5 and snapshot version 10**: Store KIP-848 consumer group state (`cgroup` with group epoch, target epoch, assignor, topic metadata snapshot, and sorted `cmember` list with `owned` and `target` partition sets) in the broker state machine via `cmdCGBeat` and `cmdCGRemove`, gated on `formatLevel = 5` and persisted in `snapshotVersion = 10`.
2. **Steady-state zero-write heartbeats**: Only propose `cmdCGBeat` when a heartbeat changes membership, subscription, assignor, owned partitions, topic metadata, or reconciled assignment; steady-state heartbeats are answered directly from applied state.
3. **Strict two-phase revocation**: Never place a partition into another member's `owned` set while `heldByOther` is true, and never grant new partitions to a member while `revoking()` is true. Enforce exclusive ownership via `ConsumerGroupCheck()` in the simulator monitor.
4. **Unified `OffsetFetch` and `FindCoordinator` wire structs**: Represent both pre-batched (`v < 8` / `v < 4`) and batched (`v >= 8` / `v >= 4`) requests/responses as `Groups []OffsetFetchGroup` and `Coordinators []FindCoordinatorResult` in `internal/wire/groups.go` so broker handlers have a single code path across all versions.

## Consequences

- Java 4.4.0 consumers with `group.protocol=consumer` and `kafka-consumer-groups.sh --describe` work out of the box with both `uniform` and `range` server-side assignors.
- Rolling upgrades from v0.6.0 / format level 4 safely refuse `ConsumerGroupHeartbeat` with retriable `ErrCoordinatorNotAvailable` until all voters report format level 5.
- Mixing classic (`JoinGroup`) and consumer (`ConsumerGroupHeartbeat`) members in the same non-empty group is rejected with `ErrInconsistentGroupProtocol`.
