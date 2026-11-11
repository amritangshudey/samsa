# Benchmarks

Method: [ADR 0014](adr/0014-benchmark-method.md) and
[design/phase-7.md](design/phase-7.md), section 2. Reproduce with
`make bench` (writes `bench.md`).

## Setup

| | |
|---|---|
| Date | 2026-11-11 |
| Commit | 9d57a53, plus the uncommitted benchmark code of Phase 7 (the broker code is unchanged from that commit) |
| Machine | MacBook Air, Apple M5, 10 cores, 16 GB, internal SSD, on mains power |
| OS | macOS 27.0.1 |
| Go | 1.27.3 darwin/arm64 |
| Client | franz-go 1.22.3, one client, acks from all replicas, no compression, idempotence off, 3 partitions round robin, default batching |
| Broker | `bin/samsa`, `-log-level warn`; every node of a cluster on this same machine |
| Runs | 5 s closed loop for throughput after 1 s warm-up; 10 s open loop at half that throughput for latency; 3 runs per row on fresh processes |

## Results

Throughput is the median of three runs, with the range. Latency comes from
the run with the median throughput, measured from each record's due time,
in milliseconds. Every open-loop run reached at least 95 % of its target
rate.

**`interval` and `never` acknowledge before fsync and can lose acknowledged
records on power loss. Their numbers are not durable throughput.**

| Nodes | fsync | Record | Throughput, rec/s (min–max) | MB/s | Latency rate, rec/s | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|
| 1 | always | 100 B | 1560766 (1499615–1647441) | 156.1 | 780383 | 7.76 | 17.79 | 23.85 | 26.65 |
| 1 | always | 1024 B | 393123 (380202–398568) | 402.6 | 196562 | 8.87 | 21.23 | 25.58 | 27.13 |
| 1 | always | 10240 B | 34918 (34622–38824) | 357.6 | 17459 | 10.58 | 112.29 | 147.07 | 153.08 |
| 1 | interval | 100 B | 3320626 (2588625–3328392) | 332.1 | 1660313 | 4.83 | 24.36 | 29.96 | 34.74 |
| 1 | interval | 1024 B | 746246 (678529–818928) | 764.2 | 373123 | 7.83 | 32.99 | 61.72 | 66.83 |
| 1 | interval | 10240 B | 56841 (53961–64598) | 582.0 | 28420 | 9.10 | 28.19 | 32.64 | 37.45 |
| 1 | never | 100 B | 2159919 (2026561–2730124) | 216.0 | 1079960 | 8.48 | 26.50 | 31.01 | 33.65 |
| 1 | never | 1024 B | 731311 (648781–735603) | 748.9 | 365655 | 7.13 | 26.35 | 35.53 | 40.76 |
| 1 | never | 10240 B | 69225 (61141–74449) | 708.9 | 34612 | 10.32 | 211.76 | 266.30 | 274.00 |
| 3 | always | 100 B | 505083 (496934–753313) | 50.5 | 252542 | 19.94 | 49.64 | 61.76 | 67.50 |
| 3 | always | 1024 B | 97840 (83206–98595) | 100.2 | 48920 | 22.13 | 53.96 | 66.07 | 72.16 |
| 3 | always | 10240 B | 9803 (8753–10626) | 100.4 | 4902 | 20.16 | 134.79 | 176.43 | 186.44 |
| 3 | interval | 100 B | 1066146 (991407–1318464) | 106.6 | 533073 | 8.22 | 49.72 | 60.66 | 65.97 |
| 3 | interval | 1024 B | 207981 (202976–222092) | 213.0 | 103991 | 7.67 | 39.79 | 45.35 | 49.05 |
| 3 | interval | 10240 B | 18220 (8661–21142) | 186.6 | 9110 | 7.60 | 50.78 | 59.56 | 63.86 |
| 3 | never | 100 B | 1529234 (1450809–1637359) | 152.9 | 764617 | 7.17 | 39.41 | 47.10 | 49.78 |
| 3 | never | 1024 B | 268746 (267797–284078) | 275.2 | 134373 | 8.31 | 42.76 | 47.54 | 50.62 |
| 3 | never | 10240 B | 24555 (8002–28528) | 251.4 | 12277 | 9.04 | 63.13 | 70.16 | 74.97 |

## Reading it

- **One node, `always`**: 350–400 MB/s with records of 1 KiB and more,
  1.5 M records/s at 100 B. Each produce request waits for fsync, but one
  request carries many records and one fsync covers every request waiting
  for it (group commit).
- **Three nodes cost about 4× at `always`** (100 MB/s). That is three
  brokers, their fsyncs and the client sharing one laptop's cores and one
  SSD, plus a replication round trip before each acknowledgement. It is
  not a measurement of three machines.
- **Medians are 7–22 ms at half load.** Most of that is the client
  collecting a batch while earlier ones are in flight, not the broker.
- **The tails vary from run to run.** Some rows have p99 above 100 ms
  (1 node `always` and `never` at 10 KiB) where neighbours do not; with
  one latency run per row that is not yet explained. The throughput spread
  is wide in places too (3 nodes at 10 KiB: 8 000 to 28 000 records/s).
  Differences under about 30 % between rows are not significant here.
- `never` is not consistently faster than `interval`: at one node and
  100 B it was slower, which is noise of that size.

## Not measured

Consumers; many clients or partitions; compression; a real network; long
runs (minutes, segment rolls, a large log); behaviour under faults. No
comparison with Kafka or any other broker has been made.
