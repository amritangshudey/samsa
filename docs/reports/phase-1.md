# Phase 1 report — Storage

## What was built

- `internal/storage`: a segmented append-only log.
  - `frame.go`: 20-byte header (CRC-32C, length, base offset, count) plus
    an opaque payload; lengths bounded before allocation.
  - `index.go`: sparse offset index, written once when a segment is sealed,
    checksummed, rebuilt from the segment if missing or damaged.
  - `log.go`: `Open` (create or recover), `Append`, `Sync`, `Read`,
    `StartOffset`/`EndOffset`/`DurableOffset`, `Close`. Rolling syncs the
    old segment, writes its index, creates the new file and syncs the
    directory, in that order. Any write or sync failure poisons the log.
- `internal/simsys`: `Rand` (SplitMix64) and `Disk`, an in-memory disk that
  loses, tears and garbles unsynced data, journals directory operations,
  crashes at a chosen operation, and can fail syncs.
- `internal/realsys`: `Disk` over `os.Root`.
- `docs/BUGS.md`.
- Also written during this phase at the developer's request:
  `docs/ROADMAP.md` (Phase 4 onwards and beyond) and ADR 0005 (one command
  log; partitions are views).

## How to run it

```
make test                    # unit tests on the simulated and the real disk,
                             # 300 crash seeds, mutation checks
go test ./internal/storage -run TestCrashRecoveryProperty -crash-seeds 20000
go test ./internal/storage -run TestCrashSeed -seed 31   # replay one seed
make fuzz FUZZTIME=30s       # FuzzRecovery, FuzzDecodeIndex
```

## Evidence

- Crash property test: 20,000 seeds clean (43 s on the MacBook Air). Each
  seed is about 400 operations with crashes at random disk operations,
  crashes during recovery, and occasional failed syncs, under all three
  fsync policies.
- The property test can fail: with the directory sync removed from roll it
  reports lost durable data at seed 0.
- The sync after truncating a torn tail is shown necessary by a directed
  test: without it, a record that recovery discarded comes back.
- Fuzzing: about 2.6 million executions of `FuzzRecovery` and 4 million of
  `FuzzDecodeIndex` locally, no failures.
- Unit tests pass against real files on macOS; CI adds Linux.

## What is untested or weak

- **The simulator found no bug in the storage engine**, only one in itself
  (BUGS.md, M1). The design review had already removed the bugs I knew to
  look for, and the two mutation checks show the test can see those two.
  Other protocol steps have no mutation check: syncing the old segment
  before roll, syncing the index file, the parent-directory sync on first
  creation.
- **The random property test did not catch the missing recovery sync** in
  2,000 seeds. A directed test covers it, but it shows the random test's
  reach is limited for bugs needing a specific three-step sequence.
- **Real-disk crash behaviour is untested.** The real disk runs only the
  no-crash tests. Nothing here pulls power from a real machine.
- **The simulated disk is kind in known ways:** directory operations
  persist in order; synced data never rots; a sync never lies.
- **No retention, no suffix truncation.** The log only grows.
- **Performance is unmeasured.** Each frame costs two read calls on the
  read path; a sealed segment is opened and closed per read; recovery reads
  frame by frame.
- **All sealed indexes are in memory**, and `Open` leaks the active file
  handle if it fails part-way (the node stops in that case anyway).
- **Review was a second pass by the author.**

## Ten questions

1. Why does every frame carry a CRC, and why CRC-32C in particular?
2. A crash tears the last write. Walk through exactly what recovery does
   and why it stops at the first bad frame instead of skipping it.
3. Why must recovery sync after truncating? Describe the sequence of events
   that goes wrong without it.
4. Rolling a segment does four things in a fixed order. What are they, and
   what is lost if the directory sync is omitted?
5. Why is the index never trusted, and why does that mean it needs no
   atomic rename?
6. `fsync` returned an error. Why does the log refuse all further work
   instead of retrying?
7. What is the difference between `EndOffset` and `DurableOffset`, and
   which one may an acknowledgement depend on under each fsync policy?
8. Records that were never synced can survive a crash and be kept. Why is
   that acceptable here, and when would it stop being acceptable?
9. The simulator's first failing seed was a bug in the simulator. What was
   it, and how do you tell a model bug from a system bug?
10. What does 20,000 clean seeds prove, and what does it not?
