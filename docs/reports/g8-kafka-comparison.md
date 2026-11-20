# Report: Item 17 — Apache Kafka Comparison Benchmark (G8, G10)

**Date:** 2026-11-21  
**Commit:** `c283539` (with `integration/bench_kafka_test.go` added in this PR)  
**Design:** [docs/design/g8-kafka-comparison.md](../design/g8-kafka-comparison.md)  
**Decision:** [docs/adr/0040-kafka-comparison-benchmark.md](../adr/0040-kafka-comparison-benchmark.md)  
**Reproduce:** `make bench-kafka`

## 1. Setup

| | |
|---|---|
| Machine | MacBook Air, Apple M5, 10 cores, 16 GB RAM, internal SSD |
| OS | macOS 26A434 (27.0.1) |
| Go | `go1.27.3 darwin/arm64` |
| Java | OpenJDK 27.0.1.0.3 (Homebrew), `KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"` |
| Apache Kafka | 4.4.0 (`kafka_2.13-4.4.0`), KRaft combined `broker,controller` single node |
| Samsa | `bin/samsa` built with `CGO_ENABLED=0 go build -trimpath` |
| Client | `franz-go` v1.22.3, idempotence enabled (default), `acks=all`, no compression, `MaxBufferedRecords(10000)` |
| Produce runs | 3 partitions round-robin; 1 s warm-up (Samsa) / 3 s warm-up (Kafka for C2 JIT), 5 s closed-loop throughput, 5 s open-loop latency at 50% of measured throughput (due-time latency, no coordinated omission); 3 fresh runs per row |

## 2. Raw results

### 2.1 Cold startup and idle RSS

| Broker | Startup ms (median, min–max) | Idle RSS MiB (median) |
|---|---|---|
| Samsa 1-node (`-sync always`) | 29.7 (29.6–38.9) | 12.2 |
| Apache Kafka 4.4.0 KRaft 1-node (default) | 6129.2 (6064.4–6226.9) | 408.3 |

### 2.2 Produce throughput and open-loop latency (3 partitions, acks=all, idempotence on)

**Note:** `Samsa (-sync never)` and `Kafka (default)` acknowledge after writing
to the OS page cache without calling `fsync`; on a single broker, acknowledged
records can be lost on power loss. `Samsa (-sync always)` and
`Kafka (flush=1)` call `fsync` before acknowledging.

| Target | Record | Throughput, rec/s (min–max) | MB/s | Latency rate, rec/s | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|
| Samsa 1-node (`sync=always`) | 100 B | 1,670,229 (1,511,812–1,715,912) | 167.0 | 835,114 | 10.98 | 22.22 | 28.02 | 29.54 |
| Samsa 1-node (`sync=always`) | 1024 B | 308,498 (294,174–340,055) | 315.9 | 154,249 | 10.21 | 19.57 | 24.12 | 26.77 |
| Samsa 1-node (`sync=never`) | 100 B | 3,537,943 (2,917,136–3,541,853) | 353.8 | 1,768,971 | 4.58 | 23.37 | 29.22 | 33.33 |
| Samsa 1-node (`sync=never`) | 1024 B | 769,097 (732,738–1,069,158) | 787.6 | 384,548 | 6.98 | 24.76 | 27.35 | 29.37 |
| Kafka 4.4.0 KRaft 1-node (default) | 100 B | 5,157,603 (4,342,086–5,172,292) | 515.8 | 2,578,801 | 2.86 | 5.05 | 7.12 | 9.72 |
| Kafka 4.4.0 KRaft 1-node (default) | 1024 B | 5,974,632 (4,737,462–5,990,323) | 6,118.0 | 2,987,316 | 1.12 | 20.70 | 38.55 | 41.00 |
| Kafka 4.4.0 KRaft 1-node (`flush=1`) | 100 B | 240,918 (239,044–240,972) | 24.1 | 120,459 (only 69,020 achieved) | 1957.28 | 3721.62 | 3748.34 | 3753.23 |
| Kafka 4.4.0 KRaft 1-node (`flush=1`) | 1024 B | 49,343 (47,133–50,577) | 50.5 | 24,672 | 162.74 | 194.41 | 202.13 | 207.13 |
| Samsa 3-node (`sync=always`) | 100 B | 516,145 (508,952–518,436) | 51.6 | 258,073 (only 147,400 achieved) | 2020.16 | 3729.57 | 3758.01 | 3762.91 |
| Samsa 3-node (`sync=always`) | 1024 B | 71,393 (70,344–73,644) | 73.1 | 35,696 | 73.93 | 104.37 | 115.16 | 120.12 |
| Samsa 3-node diskless (`file://`) | 100 B | 458,724 (458,244–458,995) | 45.9 | 229,362 (only 110,172 achieved) | 2695.53 | 5366.62 | 5417.05 | 5422.25 |
| Samsa 3-node diskless (`file://`) | 1024 B | 103,794 (103,682–104,544) | 106.3 | 51,897 | 90.91 | 117.63 | 124.61 | 126.93 |

### 2.3 Sequential consume throughput (50,000 × 1 KiB = 51.2 MB, 3 partitions)

| Target | Consume rec/s | Consume MB/s | Local Raft / Kafka disk KiB | Object store KiB |
|---|---|---|---|---|
| Samsa 1-node (`sync=always`) | 1,095,030 | 1,121.3 | 50,490 | 0 |
| Kafka 4.4.0 KRaft 1-node (default) | 1,917,968 | 1,964.0 | 84,675 | 0 |
| Samsa 3-node (`sync=always`) | 1,018,489 | 1,042.9 | 50,491 | 0 |
| Samsa 3-node diskless (`file://`) | 924,297 | 946.5 | 7 | 50,488 |

### 2.4 Partition scaling (10, 100, 500 partitions; 10 × 256 B records per partition)

| Broker | Partitions | Create topic ms | Produce all ms | RSS MiB | Data dir apparent size KiB |
|---|---|---|---|---|---|
| Samsa 1-node (`sync=always`) | 10 | 12.3 | 3.0 | 12.5 | 2 |
| Kafka 4.4.0 KRaft 1-node | 10 | 45.8 | 346.0 | 414.1 | 225,285 |
| Samsa 1-node (`sync=always`) | 100 | 12.1 | 6.2 | 13.2 | 27 |
| Kafka 4.4.0 KRaft 1-node | 100 | 46.3 | 592.2 | 425.1 | 1,423,389 |
| Samsa 1-node (`sync=always`) | 500 | 14.4 | 7.4 | 14.4 | 139 |
| Kafka 4.4.0 KRaft 1-node | 500 | 51.6 | 5,011.6 | 513.3 | 10,260,632 |

*Caveat on Kafka's "Data dir apparent size":* `dirSizeBytes` sums `os.FileInfo.Size()`,
which reports the apparent size of Kafka's pre-allocated sparse `.index` and
`.timeindex` files (10 MiB each per partition by default). Actual allocated
filesystem blocks on APFS are much smaller than 9.8 GiB, though each partition
still creates its own directory and three open segment/index files.

## 3. Honest reading: where Samsa loses and where it wins

### Where Samsa loses to Apache Kafka

1. **Page-cache produce throughput (`default` Kafka vs Samsa)**: At 1 KiB
   records, default Kafka 4.4.0 reached **6,118 MB/s** (p50 **1.12 ms**),
   which is **7.8× faster** than Samsa `-sync never` (**787.6 MB/s**, p50
   **6.98 ms**) and **19.4× faster** than Samsa `-sync always` (**315.9 MB/s**).
   Kafka's multi-threaded network/I/O pools append directly to per-partition
   page-cache files in parallel; Samsa routes every produce through a single
   deterministic event loop and Raft log.
2. **Sequential consume throughput**: Kafka consumed at **1,964 MB/s** using
   kernel `sendfile` zero-copy; Samsa consumed at **1,121 MB/s** (1-node) and
   **946.5 MB/s** (3-node diskless) because it reads batches into user space.
3. **Open-loop 100 B saturation in 3-node mode**: At 100 B records, both
   3-node classic (`516k rec/s`) and 3-node diskless (`459k rec/s`) fell behind
   their 50% open-loop target rates during the 5 s window (`147k/258k` and
   `110k/229k` achieved), producing 2–5 s queuing latency. When `franz-go`
   produces small records at open-loop pace instead of full-buffer closed-loop
   batches, each Raft round-trip carries fewer records per fsync.
4. **Multi-broker write scaling**: Not measured here directly, but structural
   (ADR 0039): Kafka shards partition leaders across brokers, so adding brokers
   scales aggregate write throughput. Samsa's classic topics share one Raft
   leader.

### Where Samsa wins

1. **Cold startup and memory footprint**: Samsa starts in **29.7 ms** with
   **12.2 MiB RSS** vs Kafka KRaft's **6,129 ms** (**206× slower**) and
   **408.3 MiB RSS** (**33× more RAM**). This directly serves Samsa's primary
   use case (embedded in test suites and CI without Docker/JVM).
2. **Per-batch fsync durability**: When Kafka is configured to fsync every
   batch (`log.flush.interval.messages=1`), Samsa `-sync always` is **6.3×
   faster** at 1 KiB (**315.9 MB/s** vs **50.5 MB/s**) and **6.9× faster** at
   100 B (**167.0 MB/s** vs **24.1 MB/s**), with **16× lower p50 latency**
   (10.2 ms vs 162.7 ms), because Samsa group-commits all partitions into one
   sequential Raft log.
3. **Partition count scaling**: At 500 partitions, Samsa created the topic in
   **14.4 ms**, produced 5,000 records across all 500 partitions in **7.4 ms**,
   used **14.4 MiB RSS** (+2.2 MiB over idle), and **139 KiB** on disk. Kafka
   took **51.6 ms** to create, **5,011.6 ms** to produce across 500 partitions,
   **513.3 MiB RSS** (+105 MiB over idle), and created 500 partition
   directories with 1,500+ segment/index files.
4. **Diskless 3-node Raft reduction (G10)**: At 1 KiB records, 3-node diskless
   (`106.3 MB/s`) beat 3-node classic (`73.1 MB/s`) by **45%** while cutting
   local Raft disk for 51.2 MB of data from **50,491 KiB** down to **7 KiB**
   (**99.99% reduction**, 50,488 KiB in object storage).

## 4. Untested and weak parts

- Single laptop (Apple M5, 16 GB, internal SSD): no real network latency, no
  cross-AZ bandwidth billing, and `file://` object store latency is microseconds
  rather than S3's 20–100 ms.
- Only 1-node Kafka KRaft was measured (not a 3-node Kafka cluster).
- Compression (`gzip`, `snappy`, `zstd`), TLS/SASL overhead, transactional
  produce throughput, and multi-producer fan-in were not measured here.
- `dirSizeBytes` measures apparent file size, not `st_blocks`, which inflates
  Kafka's sparse `.index` / `.timeindex` numbers (noted explicitly above).

## 5. Ten questions

1. **Why is Kafka default 7.8× faster than Samsa `sync=never` at 1 KiB?**
   Kafka avoids a single-threaded Raft state-machine loop and writes directly to
   per-partition OS page-cache channels across multiple I/O threads.
2. **Why did Samsa 3-node 100 B miss its 50% open-loop target rate?**
   In closed-loop mode `franz-go` fills large batches per Raft round-trip; at
   half that rate in open-loop mode, batches are smaller so the fixed per-Raft-
   round-trip fsync overhead limits records/s.
3. **Why did Samsa 3-node diskless beat 3-node classic at 1 KiB?**
   Each 1 KiB batch is written once to `file://` object storage and replicated
   as a ~97-byte Raft `cmdAppendRef`, avoiding 3× payload replication and
   3× large fsyncs across Raft peers.
4. **Would 3-node diskless still beat 3-node classic against real cloud S3?**
   No for single-client latency (S3 PUT adds 20–100 ms per flush), though
   aggregate multi-client throughput and cross-AZ replication cost savings would
   still favor diskless with batching.
5. **Is Kafka's `log.flush.interval.messages=1` used in production?**
   Rarely — production Kafka relies on replication across 3 brokers (`acks=all`,
   `min.insync.replicas=2`) rather than per-batch fsync. It is included here so
   the cost of fsync is visible on both brokers.
6. **Why did Kafka take 5 seconds to produce 10 records to each of 500 partitions?**
   Creating and opening 500 partition directories and their initial segment +
   index files on first write adds significant per-partition filesystem overhead.
7. **Does Samsa use `sendfile` for Fetch responses?**
   No; Samsa reads from the Raft log or object cache in user space to assemble
   Kafka wire responses (and verify/filter aborted transactions).
8. **How much does `franz-go` idempotence cost compared to `docs/benchmarks.md`?**
   Compare 1-node `always` 1024 B: 315.9 MB/s (idempotence on) vs 402.6 MB/s
   in `docs/benchmarks.md` (idempotence off) — roughly 20% difference, also
   reflecting new state-machine features added since `9d57a53`.
9. **Are the numbers reproducible in CI?**
   `TestBenchKafka` is gated by `SAMSA_BENCH_KAFKA=1` (`make bench-kafka`) so
   shared CI runners (which have noisy neighbors) don't run timing benchmarks.
10. **What does this mean for G8 and G10?**
    G8 and G10 are satisfied: raw numbers, hardware, method, and honest
    strengths/weaknesses against Apache Kafka 4.4.0 are recorded in the repo.
