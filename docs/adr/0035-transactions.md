# ADR 0035: Transactions in the Single Raft Log

## Status

Accepted

## Context

Kafka implements transactions using a separate `__transaction_state` internal topic on a transaction coordinator broker that communicates with partition leaders via inter-broker `WriteTxnMarkers` RPCs. In Samsa, all topic partitions, consumer groups, and offsets already live in a single Raft state machine (ADR 0003, ADR 0033, ADR 0034).

## Decision

1. **Single-log atomic commit/abort**: `txnState` (`state.txns`) and per-partition transaction state (`partition.openTxns`, `partition.aborted`) live in the same Raft state machine. `cmdTxnEnd` atomically appends `COMMIT`/`ABORT` control batches to every participating partition and applies staged `pendingOffsets` to `state.groups` in one Raft log entry. No inter-broker `WriteTxnMarkers` RPC is needed.
2. **Client-facing protocol up to KIP-890 Part 2**: Implement `InitProducerId` (22, v0–5), `AddPartitionsToTxn` (24, v0–5), `AddOffsetsToTxn` (25, v0–4), `EndTxn` (26, v0–5), and `TxnOffsetCommit` (28, v0–5), plus `IsolationLevel` filtering in `Fetch` and `ListOffsets`.
3. **Format gating**: Guard the new commands and snapshot v12 behind `formatLevel = 7` so rolling upgrades can still roll back before all nodes support transactions.

## Consequences

- Atomic multi-partition commits and consumer-offset commits require zero two-phase coordinator-to-partition-leader retries, eliminating partial-marker split-brain states.
- Standard Kafka Java 4.4.0 and `franz-go` transactional clients work unmodified, including KIP-890 epoch bumping on `EndTxn` v5.
