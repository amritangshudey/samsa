# Report: Share Groups — KIP-932 (Item 22)

## Summary

Implemented KIP-932 share groups (`ShareGroupHeartbeat` 76, `ShareGroupDescribe` 77, `ShareFetch` 78, `ShareAcknowledge` 79) with server-side record acquisition locks, lock-expiry redelivery, delivery count tracking (up to 5 deliveries before archiving), format level 6 (`snapshotVersion = 11`), simulator invariant S8 ("every record is acknowledged once or delivered again") and mutation testing (`UnsafeDropShareLock`), and an integration test using Java `KafkaShareConsumer` 4.4.0 and `kafka-share-groups.sh --describe --members`.

## What Was Built

1. **Wire protocol (`internal/wire`)**:
   - `internal/wire/share.go`: `ShareGroupHeartbeat` (76, v1), `ShareGroupDescribe` (77, v1), `ShareFetch` (78, v1–2), `ShareAcknowledge` (79, v1–2).
   - `internal/wire/api.go`: added keys `76`–`79`, `ErrInvalidRecordState` (`121`), `ErrShareSessionNotFound` (`122`), `ErrInvalidShareSessionEpoch` (`123`), and `APIs` entries.
   - `internal/wire/wire_test.go`: round-trip and byte-by-byte truncation tests for all four share-group APIs.

2. **Broker state machine and handlers (`internal/broker`)**:
   - `internal/broker/share.go`: `shareGroup`, `sgMember`, `sharePart`, `shareLock`, commands `cmdSGBeat` (`21`), `cmdSGRemove` (`22`), `cmdShareAcquire` (`23`), `cmdShareAck` (`24`), `cmdShareExpire` (`25`), handlers (`shareGroupHeartbeat`, `shareGroupDescribe`, `shareFetch`, `shareAcknowledge`), long-poll parking (`parkedShare`), lock-expiry timer (`armShareLockTimer`), and `ShareGroupCheck()`.
   - `internal/broker/state.go`, `internal/broker/snapshot.go`: `formatLevel = 6`, `snapshotVersion = 11`, deterministic hashing and snapshot encode/decode of `g.sg`.
   - `internal/broker/node.go`, `internal/broker/groups.go`, `internal/broker/consumer.go`, `internal/broker/admin.go`, `internal/broker/leader.go`: `Config.ShareAcquisitionLockTimeout` (default `30s`), `Config.UnsafeDropShareLock`, `ListGroups` (`Type: "share"`), `DescribeGroups` (`ErrGroupIDNotFound`), protocol mixing checks (`ErrInconsistentGroupProtocol`), and `refusal`.
   - `internal/broker/share_test.go`: `TestShareGroupLifecycleAcquireAckAndExpiry` covering heartbeat, describe, list, fetch, accept/release/reject, lock expiry, session epoch validation, snapshot v11 round-trip, and protocol mixing.

3. **Simulator & Mutation Testing (`internal/sim`)**:
   - `internal/sim/sg.go`: simulated `sgMember` clients exercising `ShareGroupHeartbeat`, `ShareGroupDescribe`, `ShareFetch`, and `ShareAcknowledge`, plus `drainShareGroup` and `shareModel` checking invariant S8 (`ShareAcquired`, `ShareAcked`, `CheckShareFinal`).
   - `internal/sim/monitor.go`: runs `ShareGroupCheck()` after every node event.
   - `internal/sim/sim_test.go`: `drop a share-group lock without redelivery` (`UnsafeDropShareLock`), caught by **94 of 200 seeds** (first at seed 2).

4. **Integration Tests (`integration/`)**:
   - `integration/java/ShareMember.java` & `TestJavaShareGroup` (`integration/groups_test.go`): runs two Java 4.4.0 `KafkaShareConsumer` processes against a single-partition topic, verifies explicit `AcknowledgeType.RELEASE` redelivery and `ACCEPT` commit, inspects the group with `kafka-share-groups.sh --describe --members`, kills one consumer with `SIGKILL` to exercise lock expiry / takeover, and restarts the broker mid-stream.

## Bugs Found & Recorded

- `docs/BUGS.md` **C22-1**: `ShareFetch` long-poll (`parkedShareFetch`) dropped `ackErrs` when waking up or timing out, returning `AckErrorCode = 0` (`ErrNone`) for a failed acknowledgement; fixed by storing `ackErrs` on `parkedShareFetch`.

## Verification

- `go test -count=1 ./internal/wire/ ./internal/broker/ ./internal/raft/ ./internal/scram/ ./internal/storage/ ./cmd/...` and `go vet ./...`: passed.
- `go test -v -count=1 -run "^TestSim$" ./internal/sim/ -seeds=200`: 200/200 seeds passed (`39,190` `SGHeartbeats`, `93,285` `ShareAcquired`, `27,835` `ShareAcked`).
- `go test -v -count=1 -run "TestMutationSuite|TestRegressions|TestShrink|TestTimeline|TestSkipNothing|TestDeterminism" ./internal/sim/`: all passed, including `drop_a_share-group_lock_without_redelivery` (94/200 seeds).
- `go test -v -count=1 -run TestJavaShareGroup .` (`integration/`): passed in 7.20s against Apache Kafka 4.4.0 `KafkaShareConsumer` and `kafka-share-groups.sh`.
