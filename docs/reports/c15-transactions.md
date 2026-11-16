# Report: Transactions (Item 15)

## Summary

Implemented Kafka transactions (`InitProducerId` 22 v0–5, `AddPartitionsToTxn` 24 v0–5, `AddOffsetsToTxn` 25 v0–4, `EndTxn` 26 v0–5 with KIP-890 epoch bump, `TxnOffsetCommit` 28 v0–5, `FindCoordinator` transaction lookup, and `IsolationLevel` filtering in `Fetch` and `ListOffsets`) in the single Raft log, with format level 7 (`snapshotVersion = 12`), simulator invariant T1 and mutation testing (`UnsafeReadUncommittedInReadCommitted`), and integration tests with both Apache Kafka 4.4.0 Java client and `franz-go`.

## What Was Built

1. **Wire protocol (`internal/wire`)**:
   - `internal/wire/txn.go`, `internal/wire/api.go`: `AddPartitionsToTxn` (24, v0–5), `AddOffsetsToTxn` (25, v0–4), `EndTxn` (26, v0–5), `TxnOffsetCommit` (28, v0–5), `InitProducerId` v5, and transaction error codes (`48`–`51`, `90`, `120`).
   - `internal/wire/messages.go`, `internal/wire/batch.go`: `ProduceRequest.TransactionalID`, `FetchRequest.IsolationLevel`, `ListOffsetsRequest.IsolationLevel`, `FetchPartitionResponse.LastStableOffset` & `AbortedTransactions`, `ParseBatch` transactional support, and `ControlBatch` (`COMMIT` / `ABORT` magic-2 markers).
   - `internal/wire/wire_test.go`: round-trip and byte-by-byte truncation tests at every supported version.

2. **Broker state machine and handlers (`internal/broker`)**:
   - `internal/broker/txn.go`, `internal/broker/state.go`, `internal/broker/snapshot.go`: `txnState` (`state.txns`), `cmdTxnInit` (26), `cmdTxnAddParts` (27), `cmdTxnAddOffsets` (28), `cmdTxnOffsetCommit` (29), `cmdTxnEnd` (30), per-partition `openTxns`, `aborted`, `lso()`, transaction timeout timer (`armTxnTimer` / `expireTxns`), `formatLevel = 7`, `snapshotVersion = 12`.
   - `internal/broker/handlers.go`, `internal/broker/producerid.go`, `internal/broker/groups.go`, `internal/broker/leader.go`, `internal/broker/node.go`, `internal/broker/retain.go`, `internal/broker/compact.go`, `internal/broker/share.go`: `READ_COMMITTED` LSO capping and `AbortedTransactions` reporting, `FindCoordinator` (`KeyType = 1`), `Config.UnsafeReadUncommittedInReadCommitted`, and `readBatch` for synthesized control batches.
   - `internal/broker/txn_test.go`: `TestTransactionLifecycleCommitAbortTimeoutAndKIP890`.

3. **Simulator & Mutation Testing (`internal/sim`)**:
   - `internal/sim/txn.go`, `internal/sim/sim.go`, `internal/sim/sim_test.go`: transactional producers (`InitProducerId` v5, `AddPartitionsToTxn`, transactional `Produce`, `EndTxn` v5 epoch bump), `read_committed` consumers checking invariant T1 ("a `read_committed` consumer never reads uncommitted or aborted records"), and mutation `ignore LSO in READ_COMMITTED fetch` (`UnsafeReadUncommittedInReadCommitted`), caught by **108 of 200 seeds** (first at seed 1).

4. **Integration Tests (`integration/`)**:
   - `integration/java/TxnMember.java` & `TestJavaTransactions` (`integration/txn_test.go`): exercises Apache Kafka 4.4.0 `KafkaProducer` (`abortTransaction`, `commitTransaction`, `sendOffsetsToTransaction`, KIP-890 `EndTxn`) and `KafkaConsumer` (`isolation.level=read_committed`), plus `OffsetFetch` verification.
   - `TestFranzGoTransactions` (`integration/txn_test.go`): exercises `franz-go` abort, commit, broker kill/restart, and `ReadCommitted()` fetch.

## Bugs Found & Recorded

- `docs/BUGS.md` **C15-1**: `wire.PeekBatch` did not decode `attrTransactional` / `attrControl`, causing `cmdAppend` to skip `p.openTxns` registration.

## Verification

- `go test -count=1 ./internal/wire/ ./internal/broker/ ./internal/raft/ ./internal/scram/ ./internal/storage/ ./cmd/...` and `go vet ./...`: passed.
- `go test -v -count=1 -run "^TestSim$" ./internal/sim/ -seeds=200`: 200/200 seeds passed (`2,699` `TxnCommits`, `1,821` `TxnAborts`, `5,423` `RCFetches`).
- `go test -v -count=1 -run "TestMutationSuite/ignore_LSO_in_READ_COMMITTED_fetch|TestShrink|TestTimeline|TestDeterminism|TestRegressions" ./internal/sim/`: all passed (`108/200` seeds for `UnsafeReadUncommittedInReadCommitted`).
- `TestJavaTransactions` (2.73s) and `TestFranzGoTransactions` (0.11s) (`integration/`): passed.
