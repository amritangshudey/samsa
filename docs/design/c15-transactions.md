# Design: Transactions (Item 15)

## Goal

Support Kafka transactions across producers, consumers (`read_committed`), and transactional offset commits to the versions current clients negotiate (including KIP-890's epoch bump on `EndTxn` v5), with all transaction state stored in the single Raft log.

## 1. Protocol & Wire (`internal/wire`)

- **New APIs**:
  - `AddPartitionsToTxn` (24, v0–5, flexible v3+, v4+ batched `Transactions` with `VerifyOnly`).
  - `AddOffsetsToTxn` (25, v0–4, flexible v3+).
  - `EndTxn` (26, v0–5, flexible v3+, v5 returns bumped `ProducerId` / `ProducerEpoch` per KIP-890 Part 2).
  - `TxnOffsetCommit` (28, v0–5, flexible v3+, v2+ `CommittedLeaderEpoch`, v3+ group member fields).
- **Updated APIs**:
  - `InitProducerId` (22, v0–5, flexible v2+): captures `TransactionalID`, `TransactionTimeoutMs`, `ProducerID`, `ProducerEpoch` (v3+).
  - `Produce` (0): captures `TransactionalID` (v3+).
  - `Fetch` (1): captures `IsolationLevel` (v4+); `FetchPartitionResponse` encodes `LastStableOffset` (v4+) and `AbortedTransactions` (v4+).
  - `ListOffsets` (2): captures `IsolationLevel` (v2+).
  - `FindCoordinator` (10): handles `KeyType == 1` (`TRANSACTION`) by returning the Raft leader.
- **Record batches** (`internal/wire/batch.go`):
  - `ParseBatch` allows `attrTransactional` (`0x10`) when `ProducerID >= 0` and still rejects `attrControl` (`0x20`) from clients.
  - `ControlBatch` constructs magic-2 `COMMIT` (`0x00000001`) and `ABORT` (`0x00000000`) marker batches (`attrControl | attrTransactional`, 6-byte `EndTransactionMarker` value).

## 2. Broker State & Raft Log (`internal/broker`)

- **Coordinator state (`state.txns map[string]*txnState`)**:
  - Tracks `pid`, `epoch`, `phase` (`Empty`, `Ongoing`, `CompleteCommit`, `CompleteAbort`), `timeoutMs`, `startedMs`, registered `parts`, `groups`, and staged `pendingOffsets`.
  - Raft commands: `cmdTxnInit` (26), `cmdTxnAddParts` (27), `cmdTxnAddOffsets` (28), `cmdTxnOffsetCommit` (29), `cmdTxnEnd` (30).
  - `cmdTxnInit` fences any prior producer epoch, aborts any ongoing transaction (appending abort markers to its partitions), and allocates a new epoch (or new PID on epoch overflow).
  - `cmdTxnEnd` appends a `COMMIT` or `ABORT` control batch to each registered partition; on commit, atomically writes `pendingOffsets` into `s.groups[g].offsets`; on abort, records `(pid, firstOffset, markerOffset)` in `p.aborted` and discards `pendingOffsets`. If `bumpEpoch` (KIP-890 Part 2, `EndTxn` v5), bumps `txn.epoch` (or allocates a new PID on overflow) and returns it in `EndTxnResponse`.
- **Partition state (`partition.openTxns`, `partition.aborted`)**:
  - `p.lso()` returns the minimum `firstOffset` among `p.openTxns`, or `p.next` when none are open.
  - `Fetch` with `IsolationLevel == 1` (`READ_COMMITTED`) caps reads at `p.lso()` and returns `AbortedTransactions` overlapping `[offset, limit)`.
  - `ListOffsets` with `IsolationLevel == 1` and `TimestampLatest` returns `p.lso()`.
- **Transaction timeout**:
  - `Node` arms a timer for ongoing transactions and proposes `cmdTxnEnd{commit: false}` when `startedMs + timeoutMs <= nowMs`.
- **Format & Snapshots**:
  - `formatLevel = 7`, `snapshotVersion = 12` (serializes `s.txns`, `p.openTxns`, `p.aborted`).

## 3. Verification

- **Unit tests** (`internal/wire/wire_test.go`, `internal/broker/txn_test.go`): round-trip/truncation tests, commit/abort visibility under `READ_COMMITTED` vs `READ_UNCOMMITTED`, fencing, KIP-890 epoch bump, transactional offset commit, timeout abort, and snapshot v12 round-trip.
- **Simulator & Mutation** (`internal/sim/txn.go`): transactional producers, `read_committed` consumers, invariant T1 ("a `read_committed` consumer never reads uncommitted or aborted records"), and mutation `UnsafeReadUncommittedInReadCommitted`.
- **Integration** (`integration/`): `franz-go` and Java 4.4.0 transactional producer + `read_committed` consumer + `sendOffsetsToTransaction`, including abort/timeout, broker kill/restart, and KIP-890 epoch bump.
