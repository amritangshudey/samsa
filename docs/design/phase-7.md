# Phase 7 design — release

Status: draft, 2026-11-11. Roadmap section 6.

## 1. Goal

Make the project something a stranger can clone, build, run, and judge:
numbers with their method, a README whose every claim links to a test,
an architecture document matching the code, and release binaries built by
CI from a tag.

## 2. Benchmarks

**Where.** `integration/bench_test.go`, a test that runs only when
`SAMSA_BENCH=1` (`make bench`). It reuses the integration helpers that
start real processes, so it measures the shipped binary over TCP with a
real client (franz-go), not a function call.

**What.** For each of: 1 and 3 nodes × fsync policy `always`, `interval`,
`never` × record size 100 B, 1 KiB, 10 KiB:

1. *Throughput*, closed loop: produce as fast as the client allows for a
   fixed time (acks from all replicas, no compression, 3 partitions);
   report records/s and MB/s.
2. *Latency*, open loop at a fixed rate: half the measured throughput.
   Record *i* is due at `start + i/rate`; its latency is acknowledgement
   time minus **due** time, not send time. If the broker stalls, the
   generator falls behind and the backlog shows up as latency, instead of
   the generator quietly sending less (coordinated omission). Report
   median, p99, p99.9 and the achieved rate; a run whose achieved rate is
   below 95 % of the target is marked, because then the rate was not
   sustainable.

Each configuration is repeated three times on fresh processes; the table
shows the median run, and the spread (min–max) of the headline number.
Brokers run with `-log-level warn`: the tests' debug logging writes a line
per request.

**Recorded with the results** (`docs/benchmarks.md`): machine, OS, Go
version, the commit, the exact command, the date.

**Not measured:** consumers, many clients, many partitions, compression,
a network between machines, other brokers. No comparison with Kafka is
made: that needs the same machine and durability settings, and is left
out rather than done badly.

Percentiles come from sorting all latencies of a run (at most a few
hundred thousand values). No histogram library: ADR 0004.

## 3. Version and release

- `samsa -version` prints the version. It is set at link time
  (`-ldflags "-X main.version=v0.1.0"`) by the release build; otherwise
  it falls back to what Go stamps into every build from a repository
  (`runtime/debug.ReadBuildInfo`: the commit and whether the tree was
  dirty).
- `.github/workflows/release.yml`: on a tag `v*`, after the CI jobs pass
  for that commit, build `CGO_ENABLED=0` binaries for darwin and linux ×
  arm64 and amd64, write `SHA256SUMS`, and create a GitHub release with
  the `gh` CLI that the runners already have. No new actions or tools.
- Creating the tag is the developer's decision: it publishes. This phase
  builds and tests the workflow's build step locally and leaves the tag.

## 4. README and architecture

- `README.md`: what it is; a 60-second quick start (build, run, produce
  and consume with kcat); a table of supported features, each row linked
  to the test that shows it; what is not supported; how to run and replay
  simulator seeds; the bugs the simulator found, linking `docs/BUGS.md`;
  the benchmark summary linking `docs/benchmarks.md`.
- `docs/ARCHITECTURE.md`: roadmap sections 1 and 2 brought up to date
  with the code as built (Raft, the lease, the node, the simulator),
  linking the ADRs. The roadmap keeps its sections as the plan of record.

## 5. Acceptance

A fresh clone into an empty directory: `make build test`, then the quick
start exactly as written in the README. Recorded in the report.

## 6. Alternatives rejected

- *Go's `testing.B` benchmarks.* Closed loop by construction, and they
  measure a function, not a process behind a socket.
- *An existing load tool (kafka-producer-perf-test, a histogram
  library).* A new dependency, and `kafka-producer-perf-test` is closed
  loop, so it hides stalls (coordinated omission).
- *goreleaser.* A new tool for four `go build` lines.

## 7. Review record

Second pass, looking for numbers that could mislead:

1. Half the closed-loop throughput as the latency rate depends on a
   number measured a moment earlier; if that run was unlucky the rate is
   easy. The table prints the rate used next to the percentiles, so the
   reader sees it.
2. A laptop on battery throttles. Record whether it was on power; repeat
   runs to show the spread.
3. All processes share one machine: the three-node numbers include three
   brokers and the client competing for cores and for one SSD. Say so
   next to the table.
4. `interval` and `never` acknowledge before fsync; their numbers must
   not be read as durable throughput. Label them.
5. Measuring from due time punishes the generator's own pauses (Go GC in
   the client) as if they were the broker's. Accepted: it errs towards
   worse numbers, never better.
