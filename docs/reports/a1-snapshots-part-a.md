# A1 part (a) report — local snapshots

Branch `a1-snapshots`, 2026-11-11. Design: `docs/design/a1-snapshots.md`.

## What was built

- **Raft snapshots** (`internal/raft/snapshot.go`): a file holding the last
  included index, the terms up to it as runs, and the caller's state as
  opaque bytes, replaced with temp file, sync, rename, directory sync.
  `SaveSnapshot` syncs the log up to the index first; `Start` loads the
  snapshot, refuses one the log cannot back (past the end, or a different
  term at its index), reads only later entries, and starts with the commit
  index at the snapshot.
- **The broker's state encoding** (`internal/broker/snapshot.go`): topics,
  batch references, groups, members, assignments, committed offsets; sorted,
  versioned, every length checked. `SnapshotEvery` (default 10 000 entries,
  negative to disable).
- **Tests**: encoding round trips and rejections, fuzz targets for both
  decoders, restart from a snapshot at Raft and broker level, the random
  Raft schedules taking snapshots, the simulator drawing `SnapshotEvery`
  from {off, 1, 7, 100}, a new mutation caught at both levels.

## Results

| | |
|---|---|
| Simulator, 5000 seeds, with snapshots in three quarters of them | 0 failures |
| Raft random schedules with snapshots | 2000 seeds, 0 failures |
| Mutation "snapshot before the log is synced" | caught by the Raft schedules (seed 33) and the simulator (seed 3): a node fails to recover, its snapshot ahead of a log that lost its tail |
| Starts from a snapshot | 3962 in 300 simulator seeds |
| Restart, 50 000 entries (`BenchmarkRestart`) | 1.6 ms with snapshots, 55 ms without |

## How to run it

```
go test ./internal/broker -run '^$' -bench Restart
go test ./internal/raft -run Snapshot -v
make sim SEEDS=5000
```

## What is untested or weak

- **Start-up still grows with the log**, more slowly (design doc, section
  6). The acceptance criterion "bounded by the snapshot interval" needs
  part (b).
- **No retention yet**: disk use is unchanged.
- The snapshot is written synchronously; its pause grows with the number of
  batches. Not measured under load.
- The benchmark runs on the simulated disk, so it measures CPU, not I/O.
- The Raft-level seed that catches Figure 8 moved (from 430 to 18) because
  the schedules now draw snapshots too; it is still caught.

## Ten questions

1. Why must the log be synced before a snapshot is written, even under
   `-sync always`? Which node applies entries it has not synced?
2. Why is a snapshot that fails its checksum a reason to stop, not to fall
   back to replaying the log?
3. Why may a node start with its commit index at the snapshot index?
4. Why does the snapshot hold the terms of all its entries, and what would
   go wrong with only the last one?
5. Why does the snapshot not contain record data, and what does that cost?
6. Why is `UnsafeNoFencing` restored from the config and not the snapshot?
7. What makes two equal states encode to equal bytes, and why does it
   matter?
8. Why was the mutation caught as a failed recovery and not as a lost
   record?
9. Where does the remaining linear start-up time come from?
10. What must change in Raft before the log can be cut below the snapshot?
