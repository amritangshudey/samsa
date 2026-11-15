# G5 report: 24-hour soak and resource bounds

2026-11-16. Production plan item 6 (`docs/PRODUCTION.md`, gate G5). Design:
[`docs/design/g5-soak.md`](../design/g5-soak.md). Raw outputs:
[`docs/soak/run1/`](../soak/run1/) and [`docs/soak/run2/`](../soak/run2/).

## What was built and run

- **Harness** (`integration/soak_test.go`, `make soak`): starts one real node
  from `bin/samsa` with `-sync always`, `-retention-bytes 268435456` (256 MiB),
  `-producer-expiry 10m`, and `-metrics-listen`.
- **Workload** (24 h):
  - an idempotent producer sending 500 records/s of 1 KiB random uncompressed
    values (`acks=all`), replaced every 5 minutes so producer IDs churn and
    must expire 10 minutes after stopping;
  - two consumers in group `soak`, polling from the start with auto-commit,
    each leaving and rejoining every 7 minutes (rebalances and offset commits);
  - connection churn: a new client every second that connects, pings, and closes.
- **Sampling and bounds**: every 60 s (`samples.csv`, 1,440 samples), the
  harness records RSS (`ps`), data directory bytes, open file descriptors
  (`lsof`), Prometheus gauges (`samsa_connections`, `samsa_raft_log_start`,
  `samsa_raft_last_index`, `samsa_raft_snapshot_index`), and workload counters.
  After 24 h it stops the node with SIGTERM, restarts it on the soaked data
  directory, and measures the time until `/readyz` returns 200.
- **Fix between run 1 and run 2** (`internal/raft/terms.go`, PR #28,
  [`docs/BUGS.md`](../BUGS.md) L1): `Raft.terms` kept one `int64` per log entry
  since index 0 even after `Compact` deleted the segments on disk. `termIndex`
  now stores terms below `logStart - 1` as `(startIndex, term)` runs and keeps
  a per-entry slice only for the live tail, copying the tail on compaction so
  the old backing array can be freed.

## Results: Run 1 vs Run 2

Both runs executed for 24h0m0s on the Apple Silicon MacBook Air (`darwin/arm64`,
Go `go1.27.3`) while other development work, simulator runs, and integration
tests ran on the same machine.

- **Run 1** (started 2026-11-14 00:53 IST, pre-L1 fix): **FAIL** (memory bound).
- **Run 2** (started 2026-11-15 01:38 IST, commit `7d877ab`, with L1 fix): **PASS**.

| Measure | Run 1 (pre-fix) | Run 2 (fixed) | Bound |
|---|---|---|---|
| Verdict | **FAIL** | **PASS** | all bounds met |
| RSS (min / median / max / last) | 14 / 76 / **173** / 115 MiB | 18 / 28 / **32** / 25 MiB | Q4 peak ≤ Q2 peak + 10% + 16 MiB |
| Peak RSS by quarter (Q1 / Q2 / Q3 / Q4) | 65.2 / 109.4 / 155.9 / 173.3 MiB | 30.8 / 31.5 / 32.5 / 29.4 MiB | Run 2 Q4 (29.4 MiB) ≤ 50.7 MiB |
| Data directory (min / median / max / last) | 29 / 224 / 256 / 249 MiB | 29 / 224 / 256 / 198 MiB | ≤ 448 MiB (retention + 3×64 MiB) |
| Open files (min / median / max / last) | 16 / 21 / 27 / 16 | 16 / 21 / 26 / 16 | Q4 peak ≤ Q2 peak + 16 |
| Client connections (min / median / max / last) | 5 / 5 / 12 / 5 | 5 / 5 / 12 / 5 | — |
| Records produced / consumed | 43,060,729 / 43,060,729 | 43,196,136 / 43,196,136 | ≥ 38,880,000 (90% of 500/s) |
| Produce errors / consumer offset gaps | 0 / 0 | 0 / 0 | 0 gaps |
| Final Raft log (`log_start` .. `last_index`, `snapshot`) | 5,911,371 .. 5,946,078 (5,938,252) | 6,003,097 .. 6,030,787 (6,029,963) | `log_start > 0` |
| Start-up time on soaked data (`/readyz` 200) | 182 ms | **63 ms** | < 2 min |

In run 2, ~41.2 GiB of record payload was written across 6,030,787 Raft entries.
Retention trimmed the log to start at index 6,003,097, keeping the data
directory between 190 MiB and 256 MiB in steady state, and 286 generations of
idempotent producers (4 partitions each) were expired cleanly. Restarting on
the soaked directory loaded snapshot 6,029,963, replayed the remaining 824
entries, and served `/readyz` in 63 ms.

## Problems met

- **BUGS.md L1:** `Raft.terms` grew by 8 bytes per entry forever (~48 MiB of
  live slice after 6 million entries, plus Go heap/GC overhead pushing RSS to
  173 MiB in run 1). Fixed in `internal/raft/terms.go` before run 2.
- **Design review 8 (zero-filled values):** the initial short trial compressed
  1 KiB of zeros to almost nothing, so retention never ran. Fixed before run 1
  by disabling producer compression and filling values with pseudo-random bytes,
  and adding the check that `samsa_raft_log_start > 0`.

## How to run it

```sh
make build
caffeinate -i make soak
# Or a shorter run with a smaller retention bound:
SAMSA_SOAK=1 SAMSA_SOAK_DURATION=15m SAMSA_SOAK_RETENTION=67108864 \
  SAMSA_SOAK_OUT=./soak-out go test ./integration -run TestSoak -v -timeout 20m
```

## What is untested or weak

- **Single node only.** The soak runs one node; peer transport queues, follower
  fetch state, and `InstallSnapshot` under long-running churn are covered by
  the simulator and integration tests, not a 24 h soak.
- **Shared machine.** Both 24 h runs shared the laptop with builds, simulator
  runs, and integration tests, which adds jitter to RSS and restart latency
  measurements.
- **Leak sensitivity.** Comparing Q4 peak RSS against Q2 peak RSS with a
  `10% + 16 MiB` allowance over a 12 h gap cannot detect leaks smaller than
  roughly 1.5–2 MiB/hour.
- **Key compaction and per-topic copy-forward cleaner** (items 12a/12b) were
  not exercised in the soak topic configuration; the soak uses `-retention-bytes`
  on a single `delete`-policy topic.

## Ten questions

1. **Why did run 1's memory grow even though log segments on disk were deleted?**
   `Raft.terms` stored an `int64` term per entry from index 0 and never dropped
   entries below `logStart`.
2. **Why does `termIndex` keep per-entry terms from `logStart - 1` rather than
   `logStart`?** When the leader sends `InstallSnapshot` cut at `logStart`, it
   needs the term at `logStart - 1` (`prevTerm`) so the follower can check
   whether its log matches at the cut.
3. **Why must `termIndex.compact` allocate and copy the tail slice instead of
   reslicing `entries[drop:]`?** Reslicing keeps the underlying array referenced,
   so the Go garbage collector could never free the dropped prefix.
4. **Why did the first trial run fail to exercise retention, and how does the
   harness guard against that now?** Zero-filled 1 KiB payloads compressed to a
   few bytes; the workload now sends random uncompressed bytes and `judgeSoak`
   fails if `produced * 1 KiB > 2 * retention` while `samsa_raft_log_start == 0`.
5. **Why does the memory check compare the peak RSS of Q4 against Q2 instead of
   fitting a line from minute 1?** The first quarter is warm-up (heap and page
   cache grow until retention reaches steady state), and comparing quarter peaks
   absorbs the Go GC sawtooth.
6. **What is the smallest leak rate the 24 h memory bound is guaranteed to
   catch?** About 1.6 MiB/h at a 32 MiB Q2 peak (`(3.2 + 16) MiB / 12 h`).
7. **Why can the data directory legally exceed `-retention-bytes` (256 MiB), and
   by how much?** Retention cuts whole 64 MiB segments below the snapshot; the
   bound allows `retention + 3 * 64 MiB` (one segment over the threshold, the
   active segment, plus the snapshot and index files).
8. **Why does consumer churn not cause false offset-gap failures after a
   rebalance?** After a rebalance, consumers resume from the last committed
   offset and may re-read records; the checker tracks the high-water offset
   seen per partition and only counts `r.Offset > next` as a gap.
9. **Why was start-up time on the soaked data 63 ms after 6 million entries?**
   Start-up loads the newest snapshot (`index 6,029,963`) and replays only the
   824 entries appended after it, plus `termIndex` no longer expands historical
   runs below `logStart - 1` into a 6-million-element slice (which took 182 ms
   in run 1).
10. **What does the `-producer-expiry 10m` flag with 5-minute producer
    replacement test?** Every 5 minutes a producer stops and a new one gets a
    fresh producer ID; 10 minutes later the leader proposes `ExpireProducers`,
    verifying that inactive producer state does not accumulate in memory or
    snapshots.
