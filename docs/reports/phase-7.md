# Phase 7 report — Release

Branch `phase-7`, 2026-11-11. Stacked on `phase-6` (PR #7), `phase-5`
(PR #6) and `phase-4` (PR #5), none merged yet.

## What was built

- **A benchmark** (`integration/bench_test.go`, `make bench`, ADR 0014):
  the built binary as real processes, driven by franz-go over TCP; closed
  loop for throughput, open loop at half that rate for latency, measured
  from each record's due time so that stalls are not hidden (coordinated
  omission); 1 and 3 nodes × three fsync policies × three record sizes,
  three runs each. Results and machine details in `docs/benchmarks.md`.
- **`samsa -version`**: the release tag when built by `make release`,
  otherwise the commit Go stamps into the build.
- **Release builds**: `make release VERSION=v…` makes static binaries for
  macOS and Linux, arm64 and amd64, with a checksum file;
  `.github/workflows/release.yml` runs the tests and 1000 simulator seeds
  on a pushed `v*` tag, then publishes a GitHub release with them.
- **README.md**: quick start, a support table where every row links to its
  test, what is not supported, the simulator, the bugs it found.
- **docs/ARCHITECTURE.md**: roadmap sections 1 and 2 rewritten for the code
  as built (Raft, the lease, groups, real I/O, the simulator).
- **`integration/cmd/topic`**: a small tool to create a topic, because the
  broker never auto-creates one and kcat cannot.
- Also: the Java group test flake from Phase 4 explained (BUGS.md M7).
- Fixed: fuzzing on CI found that the CreateTopics request encoder (used
  by the simulator and tests, not the broker) dropped a manual assignment
  (BUGS.md F2).

## Results

| | |
|---|---|
| Benchmark, 18 configurations × 3 runs | 920 s; every open-loop rate sustained |
| 1 node, `always`, 1 KiB | 393 k records/s (403 MB/s); p50 8.9 ms, p99 21 ms, p99.9 26 ms at half that rate |
| 3 nodes, `always`, 1 KiB | 98 k records/s (100 MB/s); p50 22 ms, p99 54 ms, p99.9 66 ms |
| Spread | wide in places (3 nodes, 10 KiB: 8 000–28 000 records/s); some p99s above 100 ms are not explained |
| Fresh clone from GitHub | `make build test` pass; both README snippets run verbatim (one node, three nodes) and read back what they wrote |

Full table: `docs/benchmarks.md`.

## How to run it

```
make bench                         # about 20 minutes; writes bench.md
make release VERSION=v0.1.0        # binaries and SHA256SUMS-v0.1.0 in dist/
./bin/samsa -version
```

## What is untested or weak

- **The release workflow has never run.** Its build step (`make release`)
  was run locally; the job itself runs only on a pushed tag, and pushing
  one publishes a release, which is the developer's decision.
- **Benchmarks are one laptop**, every process on the same cores and SSD,
  one client, three partitions, producers only. No comparison with Kafka.
- **The lease's clock assumption** (rates within 25 %) is now in the
  architecture document, still untested.
- `make bench` is not in CI: CI machines are shared and their numbers
  would vary more than the effects worth measuring.

## Ten questions

1. What is coordinated omission, and how does measuring from the due time
   avoid it? What would a closed-loop latency number have hidden here?
2. Why is latency measured at half the closed-loop throughput, and what
   would the percentiles look like at 100 %?
3. Why can `-sync always` reach hundreds of MB/s on one node, when each
   produce waits for fsync? (Group commit, batching.)
4. Why are three nodes slower than one on the same laptop, and why does
   that say little about three machines?
5. Why are `interval` and `never` faster, and what exactly can each lose?
6. p99.9 from a few hundred thousand samples: how many samples is that
   number based on, and how stable is it?
7. Why does `-version` fall back to the Go build info instead of failing,
   and what does "+dirty" tell a bug reporter?
8. Why does the release job rerun tests and simulator seeds on the tag
   instead of trusting the PR's CI?
9. Every README row links to a test. Which claims in the README are not of
   that kind, and how are they supported?
10. The Java client rejoined after a single-broker restart (M7). Why is
    that the client's decision, and would Kafka see the same?
