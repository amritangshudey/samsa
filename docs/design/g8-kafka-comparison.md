# Design: Item 17 — Apache Kafka Comparison Benchmark (G8, G10)

## 1. Goal

Satisfy gates G8 and G10 in `docs/PRODUCTION.md`:

- **G8 (Honest numbers)**: benchmark Samsa against Apache Kafka 4.4.0 (KRaft) on
  the same machine, using the same client (`franz-go`) and the same workload,
  recording hardware, exact settings, raw numbers, and where Samsa loses as
  well as where it wins.
- **G10 (Cost and scaling measured)**: measure partition-count scaling
  (10, 100, 500 partitions), startup time, RSS memory, local disk usage, and
  diskless-mode local-disk / object-store overhead on the same harness.

## 2. Method

All runs execute on the same machine with fresh data directories and fresh
broker processes, using `franz-go` (`github.com/twmb/franz-go/pkg/kgo`) with
default idempotence enabled (`acks=all`, no compression, `MaxBufferedRecords(10000)`).

### 2.1 Configurations compared

1. **Samsa 1-node (`-sync always`)**: durable fsync per group-commit batch
   (Samsa's default and required cluster policy).
2. **Samsa 1-node (`-sync never`)**: page-cache write without per-batch fsync,
   matching Apache Kafka's default durability posture on a single broker so
   protocol/engine overhead can be compared separately from `fsync` cost.
3. **Apache Kafka 4.4.0 KRaft 1-node (default flush)**: standard KRaft
   combined broker+controller (`process.roles=broker,controller`), default
   OS page-cache flushing (`log.flush.interval.messages` unset).
4. **Apache Kafka 4.4.0 KRaft 1-node (`log.flush.interval.messages=1`)**:
   explicit per-batch fsync on Kafka to compare apples-to-apples against
   Samsa `-sync always`.
5. **Samsa 3-node (`-sync always`)**: 3-node Raft cluster on localhost.
6. **Samsa 3-node diskless (`-sync always`, `diskless.enabled=true`, `file://` object store)**:
   local disk vs object-store bytes and throughput/latency trade-off (G10).

### 2.2 Workloads

1. **Produce throughput & open-loop latency** (same method as `TestBench`,
   ADR 0014):
   - Topic `bench` with 3 partitions, round-robin.
   - Record sizes: `100 B` and `1024 B`.
   - 1 s warm-up, 5 s closed-loop throughput, then 5 s open-loop latency at
     50% of measured throughput (measured from each record's due time to avoid
     coordinated omission).
   - 3 runs per row; report median throughput with min–max range and
     p50/p99/p99.9/max latencies from the median run.
2. **Consume throughput**:
   - After producing 50,000 × 1 KiB records (50 MiB), measure end-to-end
     sequential consume rate of all 50,000 records from offset 0 with a fresh
     `franz-go` consumer.
3. **Partition scaling (10, 100, 500 partitions)**:
   - Create a topic with `P` partitions, measure topic-creation time, produce
     10 records to every partition (`10 * P` records total), measure RSS memory
     (`ps -o rss=`) and data-directory disk usage (`du -sk`).
4. **Cold startup time & idle RSS**:
   - Time from process `Start()` until `ApiVersions` / `Metadata` succeeds and
     is ready to create a topic; RSS immediately after ready.

## 3. Fairness and limitations

- **Single machine**: all brokers, clients, and local `file://` object store
  share one laptop's CPU cores and SSD. Three-node numbers do not represent
  three physical machines, and `file://` object store latency is orders of
  magnitude lower than real cloud S3.
- **JVM warm-up**: Kafka runs on OpenJDK 27. A 1 s produce warm-up is short for
  C2 JIT compilation; we include a 3 s warm-up for Kafka so JIT has time to
  compile hot paths, and state this explicitly.
- **Durability asymmetry**: Kafka's default single-node config relies on the OS
  page cache and replication across brokers for durability, whereas Samsa's
  `-sync always` calls `fsync` before every acknowledgement. Showing both
  default and fsync-matched modes makes the trade-off transparent instead of
  hiding it.
- **Single-Raft-group write ceiling**: Samsa orders all classic-topic writes
  through one Raft leader; Kafka shards partition leaders across brokers. On a
  multi-broker cluster with many partitions, Kafka's aggregate write throughput
  scales with broker count whereas Samsa's classic topics do not (documented in
  ADR 0039).

## 4. Second review (recorded)

Reviewed against `docs/PRODUCTION.md` (G8, G10) and `docs/adr/0014-benchmark-method.md`:
- *Check 1*: Does `bench_kafka_test.go` add any external dependency? No — uses
  the already-approved `integration/.cache/kafka_2.13-4.4.0` distribution and
  existing `franz-go` test module.
- *Check 2*: Does `make integration` slow down in CI? No — gated behind
  `SAMSA_BENCH_KAFKA=1` (`make bench-kafka`).
- *Check 3*: Are Kafka's strengths measured fairly? Yes — default Kafka (page
  cache) is shown alongside fsynced Kafka and both Samsa sync modes, and the
  3 s JIT warm-up prevents penalizing Kafka for cold bytecode.
- *Check 4*: Is `TestBenchKafka` self-cleaning? Yes — uses `t.TempDir()` for
  Kafka's `log.dirs` and `server.properties` and kills the JVM process in
  `t.Cleanup`.
