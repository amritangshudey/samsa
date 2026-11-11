# A1 part (b) report: retention and log truncation

Date: 2026-11-11. Design: [`docs/design/a1-snapshots.md`](../design/a1-snapshots.md),
sections 3 and 7.

## What was built

- **Storage:** `TruncatePrefix` deletes whole segments below an offset,
  oldest first, syncing the directory after each (crash test invariant 5:
  a crash leaves a contiguous log); `Reset` empties the log to start at a
  given offset; `CutFor` picks the segment base that leaves about N bytes.
- **Raft:** `Compact` deletes below the newest snapshot's log start, never
  past the commit index; InstallSnapshot in chunks for a follower whose
  next entry was deleted, an `install` marker until its log is durable past
  the snapshot, then promotion to `snapshot`. Mutation
  `MutTrimPastSnapshot`.
- **Broker:** `RetentionBytes` (flag `-retention-bytes`); the leader
  proposes the replicated command `Trim(C)`; applying it drops batch
  references and moves each partition's start; a snapshot follows at once;
  Fetch v5+ reports `LogStartOffset` and answers `OFFSET_OUT_OF_RANGE`
  below it; ListOffsets earliest returns it; a node restores an installed
  snapshot and applies nothing while installing. Mutation
  `UnsafeTrimEarly`. Snapshot format version 2.
- **Simulator:** retention drawn per seed; checker rule R1; clients and the
  final read follow the start; trims and installs counted and required.

## How to run it

```
./bin/samsa -data ./data -retention-bytes 1000000000
make test                      # includes both new mutations
make sim SEEDS=3000
go test ./internal/raft -run TestRandomSchedules
go test ./internal/broker -run TestRetention -v
```

1000 simulator seeds: 34 941 trims, 472 installs, 0 failures (3000 seeds
also clean). Both mutations caught: `UnsafeTrimEarly` at seed 1 of the
simulator, `MutTrimPastSnapshot` by the Raft random schedules.

## What is untested or weak

- **Not run against the real binary with a real client.** The flag exists;
  no integration test fills a log past retention and consumes from the
  start with franz-go, kcat or Java. Also the default segment size (storage
  default) means retention below one segment does nothing.
- **Start-up time was not re-measured.** `BenchmarkRestart` runs without
  retention; with it, the batch index and term runs should shrink with
  the log, but no number in the repo shows that.
- **The checker trusts the reported start** (design section 7): a broker
  that raised it without cause would pass.
- **InstallSnapshot buffers the whole snapshot in memory** (shortcut).
- Fetch below v5 has no log start; the checker then believes any
  out-of-range answer when retention is on (shortcut in `history.go`).
- Retention by size only, one global bound.

## Ten questions

1. Why is `Trim` a replicated command instead of each node cutting its own
   log when it likes?
2. Why may a node delete only below its *own* newest snapshot's log start,
   and what does `UnsafeTrimEarly` break?
3. Why does the follower need both the snapshot and the leader's entries
   from C, unlike in the Raft paper where the snapshot replaces the log?
4. What would go wrong if a follower declined a snapshot by comparing
   indexes only (R2)?
5. Why does the follower keep the install marker until its log is durable
   past the snapshot index, and what does it refuse to do meanwhile?
6. A follower forgets entries below C it may have acknowledged. Why can
   that not cost an election its safety?
7. Why does the leader re-propose a Trim after a change of leader?
8. What does rule R1 check, and what can a buggy broker still get past it?
9. Why does Produce now answer -1 for `LogStartOffset`, and where did that
   come from?
10. Why do the I6 restarts turn retention off, and what does I6 still
    prove?
