# 0014. Benchmark method: real processes, open-loop latency

## Context

The roadmap asks for throughput and latency numbers that do not mislead.
The usual mistakes: measuring a function instead of the process behind a
socket; a closed-loop generator that sends the next request only after the
last one is answered, so a stalled broker makes the generator slow down and
the stall never shows in the latency (coordinated omission); one run
presented without its spread; and fast numbers from settings that do not
keep acknowledged data.

## Decision

- The benchmark (`integration/bench_test.go`, `make bench`) starts the
  built binary as separate processes and drives them with franz-go over
  TCP, as the integration tests do.
- Throughput is closed loop (as fast as the client's buffer allows).
  Latency is open loop at half that throughput: record *i* is due at
  `start + i/rate`, and its latency is measured from the due time. A rate
  that could not be sustained is marked in the table.
- 1 and 3 nodes × each fsync policy × 100 B, 1 KiB, 10 KiB; three runs on
  fresh processes each; the median run is shown with the throughput spread.
- Percentiles from sorting every latency of a run; no histogram library.
- The results record machine, OS, Go version, commit, power source and the
  command. `interval` and `never` are labelled as not durable.

## Alternatives

- `testing.B`: closed loop, and in-process.
- `kafka-producer-perf-test`: needs a Kafka distribution, and is closed
  loop.
- An HDR histogram module: a dependency (ADR 0004) for what a sort does at
  this scale.

## Consequences

- Numbers are for comparing this project with itself on one machine. All
  processes share the laptop's cores and SSD, so three-node numbers include
  that contention and say nothing about a real network.
- Pauses in the generator itself (its garbage collector) count against the
  broker. That errs towards worse numbers.
- Only producers are measured.
