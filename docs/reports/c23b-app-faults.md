# C23b report: application fault testing for transactions, KIP-848 groups, and share groups

Date: 2026-11-17

## Summary

Extended `-faults` and `samsa verdict` (`internal/broker/verdict.go`,
`internal/broker/faults.go`, `internal/broker/txn.go`, `internal/broker/consumer.go`,
`internal/broker/share.go`) to cover transactions, KIP-848 consumer groups, and
share groups:

- **Verdict**:
  - `records()` skips control marker entries (`cmd[0] != cmdAppend` / `info.Control`),
    records at or above `p.lso()`, and aborted transactional records (`p.aborted`).
  - `Judge()` checks `st.group(p.Group).sg` via `shareDone` (`offset < spso` or
    `shareAcked`/`shareArchived`) when the group is a share group, and `committedPast`
    for classic and KIP-848 groups.
- **Fault injection**:
  - `notleader`: `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`, `TxnOffsetCommit`,
    `ConsumerGroupHeartbeat`, `ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge`.
  - `lostack`: `EndTxn` (after commit/abort) and `ShareAcknowledge` (after ack).
  - `rebalance` / `expire`: `ConsumerGroupHeartbeat` and `ShareGroupHeartbeat`.
  - `emptyfetch`: `ShareFetch`.

## Verification

- `go test -v -count=1 -run "TestJudge|TestFault" ./internal/broker/`:
  - `TestJudgeTransactions` PASS (committed, aborted, open txns + `TxnOffsetCommit`).
  - `TestJudgeShareGroup` PASS (accepted, rejected/archived, pending).
  - `TestFaultsTxnKIP848Share` PASS (`notleader`, `lostack`, `rebalance`, `expire`, `emptyfetch`).
- `SAMSA_BIN=/tmp/samsa-test go test -v -count=1 -run "TestFaultVerdict" ./integration/`:
  - `TestFaultVerdictTransactions` (`seed=7,rate=0.25,kinds=lostack+notleader`):
    - `buggy_duplicates`: caught by `samsa verdict -expect exactly-once` (exit 1, `duplicated`).
    - `fixed_transactional`: `kgo.GroupTransactSession` passes `samsa verdict -expect exactly-once` (exit 0).
  - `TestFaultVerdictKIP848` (`seed=11,rate=0.10,kinds=rebalance+expire+notleader`):
    - commit-before-process caught by `samsa verdict` (exit 1, `lost`).
    - commit-after-process passes `samsa verdict` (exit 0).
  - `TestFaultVerdictShareGroup` (`seed=13,rate=0.10,kinds=rebalance+expire+lostack`):
    - ack-before-produce caught by `samsa verdict` (exit 1, `lost`).
    - produce-before-ack passes `samsa verdict` (exit 0).
