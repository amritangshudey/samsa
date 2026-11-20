# ADR 0040: Apache Kafka Comparison Benchmark Methodology (G8, G10)

**Status:** Accepted  
**Date:** 2026-11-21  
**Item:** 17 (Kafka comparison benchmark, G8, G10)

## Context

`docs/PRODUCTION.md` requires honest, reproducible numbers comparing Samsa
against Apache Kafka on the same machine using the same client and method (G8),
plus measurements of partition scaling, resource usage, and diskless-topic
storage trade-offs (G10).

A naive comparison between Samsa's default (`-sync always`) and Apache Kafka's
default (OS page-cache writes, async flush) compares different durability
guarantees. Conversely, comparing only fsynced Kafka ignores how Kafka is
actually deployed.

## Decision

1. **Test both durability postures explicitly**: run Apache Kafka 4.4.0 (KRaft)
   both in its default configuration (OS page cache) and with
   `log.flush.interval.messages=1` (per-batch fsync), and run Samsa in both
   `-sync always` (durable) and `-sync never` (page cache, single-node only).
2. **Use the same `franz-go` harness and open-loop latency method** from ADR
   0014 (`integration/bench_test.go`), extended in `integration/bench_kafka_test.go`
   behind `SAMSA_BENCH_KAFKA=1` (`make bench-kafka`).
3. **Measure four dimensions**: produce throughput/open-loop latency, sequential
   consume throughput, partition scaling (10 / 100 / 500 partitions: creation
   time, RSS, disk), and cold startup time / idle RSS.
4. **Publish raw results and limitations** in `docs/reports/g8-kafka-comparison.md`
   and link it from `docs/benchmarks.md`, stating plainly where Samsa loses to
   Apache Kafka.

## Alternatives considered

- **Use `kafka-producer-perf-test.sh` for Kafka and `franz-go` for Samsa**:
  rejected because different client implementations batch and pipeline
  differently; using `franz-go` for both isolates broker behavior.
- **Run 3-node Kafka KRaft on the laptop**: 3 JVM processes + 3 Samsa processes
  would be heavily CPU- and memory-contended on 16 GB RAM and obscure
  broker-level differences. Single-node Kafka vs single-node Samsa isolates
  per-broker cost cleanly, while 3-node Samsa (classic and diskless) shows
  Samsa's own cluster and object-storage overhead.

## Consequences

- Any developer with `make integration` cached can reproduce the full comparison
  via `make bench-kafka`.
- No new dependencies are added.
- The repo documents both Samsa's advantages (startup time, RSS, partition-count
  overhead, per-ack fsync efficiency) and its weaknesses (non-fsynced peak
  throughput vs JVM page-cache Kafka, single-Raft-group write scaling).
