# Design: Item 16 — Client Compatibility Matrix (B5, G9)

## 1. Goal

Satisfy B5 (`docs/ROADMAP.md`) and G9 (`docs/PRODUCTION.md`):

- Test Samsa against a broad set of real Kafka client libraries across
  languages and protocol generations:
  - **Go**: `twmb/franz-go`, `IBM/sarama`, `segmentio/kafka-go`
  - **Java**: Apache Kafka `kafka-clients` 4.4.0 (default idempotent producer,
    classic consumer group, KIP-848 consumer group, KIP-932 share group,
    transactions)
  - **C (`librdkafka`)**: `kcat`
  - **CLI**: Apache Kafka 4.4.0 tools (`kafka-topics.sh`,
    `kafka-consumer-groups.sh`, `kafka-configs.sh`, `kafka-acls.sh`,
    `kafka-share-groups.sh`)
- **Generate `docs/compatibility.md` directly from test execution** (`make matrix`
  / `SAMSA_MATRIX_OUT`), recording each client's exact version, tested
  capabilities (produce, idempotent produce, consume, consumer groups,
  compression, transactions, admin), pass/fail status, and negotiated Kafka API
  versions.
- Record any protocol bugs uncovered by Sarama or `kafka-go` in `docs/BUGS.md`
  and fix them in `internal/wire` / `internal/broker` without weakening any
  protocol invariant.

## 2. Why these clients

Each client exercises a distinct slice of the Kafka wire protocol:

- **`IBM/sarama`**: defaults to older protocol versions unless configured, uses
  `OffsetFetch`/`OffsetCommit` v3–v8, `JoinGroup`/`SyncGroup` classic groups,
  `Produce` v7–v9, `Fetch` v11, and `Metadata` v5–v9; also tests `ClusterAdmin`
  topic creation and deletion.
- **`segmentio/kafka-go`**: uses a hand-written wire codec with strict header
  and response-length framing, `ListOffsets`, `Fetch`, `Produce`,
  `CreateTopics`, and classic consumer groups (`ReaderConfig{GroupID}`).
- **`twmb/franz-go`**: negotiates the highest ApiVersions Samsa advertises
  (including TopicIDs, KIP-392 rack-aware follower fetch, transactions, and
  flexible tagged headers).
- **Apache Kafka Java 4.4.0**: reference implementation for idempotent
  producers, KIP-848 consumer groups, KIP-932 share groups, and transactions.
- **`kcat` (`librdkafka`)**: C reference client used by Python (`confluent-kafka`),
  .NET, and Rust (`rdkafka`) bindings.

## 3. Matrix generation (`TestClientMatrix`)

`integration/matrix_test.go` runs a structured suite against a live Samsa
broker:

1. **Sarama** (`TestSaramaRoundTripAndGroup`):
   - `ClusterAdmin` create topic (3 partitions).
   - `SyncProducer` with idempotence enabled (`Net.MaxOpenRequests=1`,
     `Producer.Idempotent=true`, `RequiredAcks=WaitForAll`) producing 60
     records across 3 partitions.
   - `ConsumerGroup` consuming all 60 records, committing offsets, and
     verifying no duplicates or gaps.
   - `SyncProducer` with gzip compression.
2. **segmentio/kafka-go** (`TestKafkaGoRoundTripAndGroup`):
   - `Conn.CreateTopics` (3 partitions).
   - `Writer` (`RequiredAcks: RequireAll`) producing 60 records across 3
     partitions.
   - `Reader` with `GroupID` consuming all 60 records and committing offsets.
3. **Matrix generator** (`TestClientMatrix`):
   - Runs a uniform capability checklist across `franz-go`, `IBM/sarama`,
     `segmentio/kafka-go`, Apache Kafka Java 4.4.0, `kcat` (`librdkafka`), and
     Kafka 4.4.0 CLI tools, collecting negotiated API versions from the broker's
     debug log.
   - When `SAMSA_MATRIX_OUT` is set (e.g. `make matrix`), writes
     `docs/compatibility.md` with the exact date, Samsa commit, client
     versions, capability matrix, and negotiated API versions table.

## 4. Second review (recorded)

Reviewed against `CLAUDE.md` and `docs/PRODUCTION.md` (B5, G9):
- *Dependency rule*: root `go.mod` remains standard-library only; `IBM/sarama`
  and `segmentio/kafka-go` are added only to `integration/go.mod` with explicit
  owner approval (2026-11-21).
- *CI impact*: Sarama and `kafka-go` tests are pure Go and run in seconds inside
  the existing `CI/integration` job without extra system packages.
- *Generated table*: `docs/compatibility.md` is generated directly from live
  test results, never hand-typed.
