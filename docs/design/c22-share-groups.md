# Design C22: Share Groups (KIP-932)

## 1. Goal

Implement Kafka share groups (KIP-932) so multiple consumers in a share group (`ShareConsumer` in `kafka-clients` 4.4.0) can share partitions as a queue: each record is acquired with a time-bounded lock, delivered with a delivery count, and explicitly acknowledged (`Accept`, `Release`, `Reject`, `Renew`, `Gap`) or redelivered when its lock expires.

## 2. Wire Protocol

Added in `internal/wire/share.go` and `internal/wire/api.go` against the Apache Kafka 4.4.0 JSON message schemas:

- `ShareGroupHeartbeat` (key 76, v1, flexible v0+)
- `ShareGroupDescribe` (key 77, v1, flexible v0+)
- `ShareFetch` (key 78, v1–2, flexible v0+)
- `ShareAcknowledge` (key 79, v1–2, flexible v0+)
- Error codes: `ErrInvalidRecordState` (121), `ErrShareSessionNotFound` (122), `ErrInvalidShareSessionEpoch` (123).

## 3. State Machine & Format Level 6

All share-group membership and per-record lock state lives in the replicated Raft state machine (`formatLevel = 6`, `snapshotVersion = 11` in `internal/broker/state.go`, `internal/broker/snapshot.go`):

- **Commands**:
  - `cmdSGBeat` (21): joins, heartbeats, or leaves a share group; assigns all partitions of subscribed existing topics to every member (`simple` assignor).
  - `cmdSGRemove` (22): removes an expired share-group member.
  - `cmdShareAcquire` (23): acquires available records in `[spso, p.next)` for a member, setting `state = recAcquired`, incrementing `deliveryCount`, and setting `lockUntil = nowMs + lockTimeoutMs`.
  - `cmdShareAck` (24): applies `Accept` (1) / `Gap` (0) -> `recAcked`, `Release` (2) -> `recAvailable` (or `recArchived` if `deliveryCount >= maxDeliveryCount`), `Reject` (3) -> `recArchived`, `Renew` (4) -> extends `lockUntil`; advances `spso` past contiguous terminal records.
  - `cmdShareExpire` (25): returns expired `recAcquired` records (`lockUntil <= nowMs`) to `recAvailable` (or `recArchived` at `maxDeliveryCount`) and advances `spso`.
- **Share sessions & long-polling**:
  - Soft session table per `Node` tracks `(groupID, memberID) -> expectedSessionEpoch` (reset on `0`, deleted on `-1`).
  - `ShareFetch` with no available records and `MaxWaitMs > 0` parks in `Node.parkedShare` and wakes on `cmdAppend`, `cmdShareAck`, `cmdShareExpire`, or timer expiry.
- **Protocol isolation**:
  - `ListGroups` reports `Type: "share"`, `ProtocolType: "share"`.
  - Mixing classic, consumer, and share members in the same non-empty group returns `ErrInconsistentGroupProtocol`.

## 4. Deliberate Bug & Simulator Verification

- `Config.UnsafeDropShareLock`: on `Release` or lock expiry, marks the record `recAcked` instead of `recAvailable`, dropping it without redelivery.
- `internal/sim/sg.go` drives share-group clients (`ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge`, `ShareGroupDescribe`), `Model` tracks every record's share-group delivery/ack state (`S8: every record is acknowledged once or delivered again`), and `TestMutationSuite` verifies `UnsafeDropShareLock` is caught.
