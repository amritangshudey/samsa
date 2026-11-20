# C21 design: diskless topics (`diskless.enabled=true`)

Status: accepted, 2026-11-20. Production plan item 21 (`docs/PRODUCTION.md`,
section 7). Builds directly on the unified object storage design in
`docs/design/c20-object-storage.md` (ADR 0037).

## 1. Overview

In Kafka's KIP-1150 / KIP-1163 / KIP-1164, a diskless topic writes incoming
record batches directly to object storage from any broker (leaderless writes)
and registers object references with a Batch Coordinator that assigns official
partition offsets.

In Samsa:
- The existing Raft log **is** the Batch Coordinator.
- `batchRef` already holds `{objKey, objOffset, objLen, objCRC}` (snapshot v13,
  ADR 0037).
- `diskless.enabled=true` is a per-topic configuration (`CreateTopics` or
  `AlterConfigs`), requiring `ObjectStore != nil` and `formatLevel >= 9`.

## 2. Write path (`cmdAppendRef`, format level 9)

1. **Any broker accepts produce**: when a follower receives a `ProduceRequest`
   whose topics are all known diskless topics (`diskless.enabled=true`) and a
   Raft leader is known, the follower accepts the request instead of refusing
   with `NOT_LEADER_OR_FOLLOWER`.
2. **Direct-to-object upload**: the receiving broker validates the batches
   (`wire.ParseBatch`, ACLs, size limits), computes CRC32C, and uploads the raw
   batch bytes directly to `sys.ObjectStore` (`diskless/<nodeID>-<term>-<seq>.obj`).
   Zero batch payload bytes cross Raft or inter-broker links.
3. **Metadata ordering (`cmdAppendRef`, byte 32)**: once `ObjectStore.Put` is
   confirmed, the broker encodes a compact `cmdAppendRef` (~90 bytes: topic,
   partition, record count, producer ID/epoch/seq, timestamps, transactional
   flag, `objKey`, `objOffset`, `objLen`, `objCRC`) and calls `proposeOrForward`:
   - On the leader: proposed directly to Raft.
   - On a follower: forwarded over `msgForward` (`internal/broker/forward.go`)
     to the leader, which proposes `cmdForwarded(cmdAppendRef)`.
4. **Apply**: `state.applyAppendRef` runs the exact same idempotent producer
   admission (`p.admit`) and transaction checks as `cmdAppend`, assigns
   `base := p.next`, advances `p.next += count`, and appends a `batchRef` with
   `logIndex = 0` and `{objKey, objOffset, objLen, objCRC}`.

## 3. Read path, snapshots, retention, and safety

- **Reads**: identical to C20 tiered batches (`missingRemoteBatches` ->
  `fetchRemoteBatches` with CRC32C verification + `objCache` -> `buildFetch`).
- **Snapshots**: snapshot v13 already encodes `{objKey, objOffset, objLen, objCRC}`,
  so no new snapshot version is required.
- **Retention & GC**: `expireRecords` (`cmdDeleteRecords`) removes expired
  `batchRef`s and `gcObjects` deletes unreferenced objects directly from
  `ObjectStore`.
- **Simulator**: R-invariants and I-invariants apply unchanged under faulty
  object storage (`LostPut`, `LostGet`, `TornRead`, `MaxDelay`), crashes, and
  partitions.
