# Samsa

A Kafka-compatible message broker written from scratch in Go, shipped as one
static binary, whose correctness is checked by deterministic simulation:
a whole cluster runs in one process under injected crashes, disk faults and
network partitions, and any failure replays exactly from a seed.

Named after Gregor Samsa, who wakes up in Kafka's *Metamorphosis* as a bug.
The simulator's job is to find them.

This is a learning and portfolio project by one developer. It is not
production software; read [what is not supported](#what-is-not-supported).

## Quick start (60 seconds)

Needs Go 1.27 and, for the last two steps, [kcat](https://github.com/edenhill/kcat).

```sh
git clone https://github.com/amritangshudey/samsa && cd samsa
make build
./bin/samsa -listen localhost:9092 -data ./data &

(cd integration && go run ./cmd/topic -b localhost:9092 -partitions 3 demo)
printf 'hello\nworld\n' | kcat -b localhost:9092 -X enable.idempotence=false -P -t demo -p 0
kcat -b localhost:9092 -C -t demo -p 0 -o beginning -e
```

Topics are never created automatically, hence the `topic` tool. `-sync
always` (the default) acknowledges a write only after fsync; `interval` and
`never` trade that for speed, on a single node only: a cluster needs `always`.
`-retention-bytes N` keeps about N bytes of log, deleting the oldest
segments; consumers below the new start get `OFFSET_OUT_OF_RANGE`.

A three-node cluster on one machine (each node: id, client address, peer
address):

```sh
P=1=127.0.0.1:9092/127.0.0.1:9192,2=127.0.0.1:9093/127.0.0.1:9193,3=127.0.0.1:9094/127.0.0.1:9194
./bin/samsa -node-id 1 -listen 127.0.0.1:9092 -data d1 -peers $P &
./bin/samsa -node-id 2 -listen 127.0.0.1:9093 -data d2 -peers $P &
./bin/samsa -node-id 3 -listen 127.0.0.1:9094 -data d3 -peers $P &
(cd integration && go run ./cmd/topic -b 127.0.0.1:9092 -partitions 3 replicated)
printf 'one\ntwo\n' | kcat -b 127.0.0.1:9093 -X enable.idempotence=false -P -t replicated -p 1
kcat -b 127.0.0.1:9094 -C -t replicated -p 1 -o beginning -e
```

Any node can be named: clients are told which one leads.

## What is supported

Every row links to the test that shows it. "Real clients" means the
unmodified client library or tool, against the built binary, over TCP.

| Feature | Shown by |
|---|---|
| Produce and fetch with franz-go 1.22, across a broker restart | [`TestFranzGoRoundTripAndRestart`](integration/integration_test.go) |
| The official Java client 4.4 (admin, producer, consumer) | [`TestJavaClientRoundTrip`](integration/integration_test.go) |
| kcat / librdkafka | [`TestKcatRoundTrip`](integration/integration_test.go) |
| Compressed batches (gzip, snappy, lz4, zstd), 260 kB records | [`TestFranzGoCompressedAndLarge`](integration/integration_test.go) |
| Consumer groups: join, sync, heartbeat, leave, offset commit and fetch, rebalance when a member dies | [`TestFranzGoConsumerGroup`](integration/groups_test.go), [`TestJavaConsumerGroup`](integration/groups_test.go), [`TestKcatConsumerGroup`](integration/groups_test.go) |
| Zombie members fenced: a commit from an old generation is refused | [`TestSimCatchesMissingFencing`](internal/sim/groups_test.go), [`TestGroupLifecycle`](internal/broker/groups_test.go) |
| Replication with Raft; a three-process cluster survives two leader kills with no record lost | [`TestClusterSurvivesLeaderKill`](integration/cluster_test.go) |
| Acknowledged writes survive power loss (fsync before ack, torn writes, failed fsyncs) | [`TestCrashRecoveryProperty`](internal/storage/crash_test.go), the simulator |
| Every produce and fetch answer checked against an exact model of each partition, under crashes and partitions, 1–5 nodes ([ADR 0007](docs/adr/0007-history-checking.md)) | [`TestSim`](internal/sim/sim_test.go), [`TestRegressions`](internal/sim/sim_test.go) |
| The checks can tell: seven deliberate bugs, each caught | [`TestMutationSuite`](internal/sim/sim_test.go), [`TestRandomSchedulesCatchBrokenRaft`](internal/raft/random_test.go) |
| Malformed network input is rejected, never a panic | fuzz targets in [`internal/wire`](internal/wire), [`internal/raft`](internal/raft); [`TestBadInputClosesConnection`](internal/broker/node_test.go) |
| The core does no I/O and reads no clock (so the simulator can drive it) | [`TestCoreIsPure`](internal/purity_test.go) |

APIs and versions: [ADR 0006](docs/adr/0006-api-versions.md) (produce,
fetch, metadata, offsets, topics) and [ADR 0008](docs/adr/0008-group-api-versions.md)
(groups), each with the versions the three clients were seen to use.

## What is not supported

Transactions, the idempotent producer (clients must set
`enable.idempotence=false`; franz-go: `DisableIdempotentWrite`), log
compaction, retention by time or per topic (only `-retention-bytes`, one
bound for the whole log), deleting records, ACLs, SASL, TLS, tiered storage,
cluster membership changes, the KIP-848 group protocol, static membership,
deleting topics or adding partitions, fetch sessions, more than one Raft
group (all partitions share one log, [ADR 0010](docs/adr/0010-one-raft-group.md)),
authentication between nodes. Limits of each phase are listed in
[`docs/reports/`](docs/reports).

No compatibility beyond the clients and versions above, and no
performance beyond [the measured numbers](docs/benchmarks.md), is claimed.

## The simulator

```sh
make sim SEEDS=5000     # 5000 random seeds; each failure prints its seed
make sim SEED=937       # replay one seed with its full event trace
make test               # unit tests, the mutation suite, the regression seeds
```

Each seed draws a cluster (1, 3 or 5 nodes), a fault mix (power loss with
torn writes, process kills, failed fsyncs, dropped, duplicated and
reordered messages, partitions) and a set of clients, then checks every
answer against a model: no acknowledged record lost, offsets dense and
stable, one leader per term, committed entries never replaced, groups
never split-brained. After the faults it checks that the cluster recovers
(a leader, caught-up replicas, a new write acknowledged). How it works:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

**Bugs it found** in the broker ([docs/BUGS.md](docs/BUGS.md)): a dying
node answering a fetch with data it could not vouch for (S1); a Raft leader
that would never re-send entries below what a follower had acknowledged
(S2); a "superseded" answer mistaken for a refusal (S3); an isolated leader
answering reads (S4) and refusing writes (S5) from stale state, fixed with
a leader lease ([ADR 0013](docs/adr/0013-leader-lease.md)). Plus bugs in the
simulator and tests themselves, recorded there too.

## Performance

One producer, acks from all replicas, on an Apple M5 laptop with every
process on that machine (median of three runs):

| | 100 B records | 1 KiB records | p99 at half load, 1 KiB |
|---|---|---|---|
| 1 node, `-sync always` | 1.56 M rec/s | 393 k rec/s (403 MB/s) | 21 ms |
| 3 nodes, `-sync always` | 505 k rec/s | 98 k rec/s (100 MB/s) | 54 ms |

Method (open-loop latency, measured from each record's due time), the
machine, every policy and size, and the caveats: [docs/benchmarks.md](docs/benchmarks.md). Laptop numbers, for comparing this project with itself; no
comparison with Kafka has been made.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): how it fits together
- [docs/adr/](docs/adr): every significant decision, with alternatives
- [docs/design/](docs/design) and [docs/reports/](docs/reports): each phase's design and end-of-phase report
- [docs/BUGS.md](docs/BUGS.md): every bug found, with seed, cause and fix

## Building and testing

| Command | Does |
|---|---|
| `make build` | the static binary, `bin/samsa` (`-version` prints the commit) |
| `make test` | vet, unit tests, purity check, mutation suite, regression seeds |
| `make race` | tests with the race detector |
| `make fuzz` | every fuzz target for `FUZZTIME` each |
| `make integration` | real clients against the binary (needs kcat and a JDK) |
| `make bench` | throughput and latency, about 15 minutes |
| `make release VERSION=v0.1.0` | binaries for macOS and Linux, arm64 and amd64, with checksums |

Licence: see [LICENSE](LICENSE).
