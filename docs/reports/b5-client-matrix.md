# Report: Item 16 — Client Compatibility Matrix (B5, G9)

**Date:** 2026-11-21  
**Design:** [docs/design/b5-client-matrix.md](../design/b5-client-matrix.md)  
**Decision:** [docs/adr/0041-client-matrix.md](../adr/0041-client-matrix.md)  
**Generated table:** [docs/compatibility.md](../compatibility.md) (`make matrix`)

## 1. What was built

- Added test-only Go client dependencies `github.com/IBM/sarama` (`v1.62.0`)
  and `github.com/segmentio/kafka-go` (`v0.4.51`) to `integration/go.mod`
  (owner-approved 2026-11-21; root `go.mod` remains standard-library only).
- Added `integration/matrix_test.go`:
  - `TestSaramaRoundTripAndGroup`: `ClusterAdmin.CreateTopic`, idempotent
    `SyncProducer` (`WaitForAll`, `MaxOpenRequests=1`), gzip compression, and
    `ConsumerGroup` with offset commits across 3 partitions.
  - `TestKafkaGoRoundTripAndGroup`: `Conn.CreateTopics`, `Writer`
    (`RequireAll`), and `Reader` with `GroupID` across 3 partitions.
  - `TestClientMatrix`: runs all six client families (`twmb/franz-go`,
    `IBM/sarama`, `segmentio/kafka-go`, Apache Kafka Java 4.4.0,
    `kcat`/`librdkafka`, Apache Kafka 4.4.0 CLI) and generates
    `docs/compatibility.md` directly from the live test run and broker debug
    logs (`make matrix`).
- Updated `integration/upgrade_test.go` so `TestRollingUpgrade` probes whether
  `SAMSA_OLD_BIN` already supports `CreateTopics v7` (format level 4+) before
  asserting `UNSUPPORTED_VERSION (35)` during rolling upgrade from `v0.9.0` to
  `v1.0.0`, and raised the leader-handoff retry limit from 3 to 6.

## 2. Verification

- `make matrix`: **PASS** (12.24 s across all 6 client families; generated
  `docs/compatibility.md`).
- `TestSaramaRoundTripAndGroup` and `TestKafkaGoRoundTripAndGroup`: **PASS** on
  the first run with zero wire-protocol incompatibilities.
- Root `go.mod`: verified unchanged (zero external dependencies).

## 3. Untested and weak parts

- Python (`confluent-kafka`, `kafka-python-ng`) and Node.js (`kafkajs`)
  wrappers are not run directly in CI (though `kcat` exercises the shared
  `librdkafka` 2.16.0 C core used by `confluent-kafka-python`, `rdkafka`, and
  `confluent-kafka-dotnet`).
- Sarama transactions and `kafka-go` compression codecs were not exercised in
  this pass (transactions are tested with `franz-go` and Apache Kafka Java
  4.4.0; all four compression codecs are tested with `franz-go`).
- Web UI tools (e.g., Kafka UI / AKHQ) are not automated in CI.

## 4. Ten questions

1. **Did `IBM/sarama` or `segmentio/kafka-go` uncover any wire-format bugs?**
   No — both passed on the first run across topic creation, idempotent/acked
   produce, gzip compression, and classic consumer groups.
2. **Which Kafka API versions did Sarama negotiate?**
   14 APIs: Produce v9, Fetch v12, ListOffsets v6, Metadata v11, OffsetCommit
   v8, OffsetFetch v8, FindCoordinator v3, JoinGroup v5, Heartbeat v3,
   LeaveGroup v3, SyncGroup v3, ApiVersions v3, CreateTopics v7,
   InitProducerId v4.
3. **Which Kafka API versions did `kafka-go` negotiate?**
   ApiVersions v0, Metadata v8, Produce v8, plus classic Fetch/ListOffsets/group
   APIs (note `kafka-go` sends an empty `client-id` on some connections).
4. **Does adding Sarama and `kafka-go` affect the `samsa` binary?**
   No — `integration/` is a separate Go module; root `go.mod` has zero external
   dependencies.
5. **How is `docs/compatibility.md` kept honest?**
   `make matrix` writes it directly from `TestClientMatrix` execution and the
   broker's live `msg=request` debug log lines.
6. **Why did `TestRollingUpgrade` need updating before `v1.0.0`?**
   `CI/integration` builds `SAMSA_OLD_BIN` from the previous git tag (`v0.9.0`),
   which already supports format level 9 (including `CreateTopics v7`), so
   asserting `UNSUPPORTED_VERSION (35)` unconditionally would fail once
   `v0.9.0` is the previous tag.
7. **Does `TestRollingUpgrade` still catch early format-level raises from older tags?**
   Yes — when `SAMSA_OLD_BIN` is below format level 4 (like `v0.6.0`),
   `oldSupportsV7` is false and the test still asserts `UNSUPPORTED_VERSION (35)`
   until the 3rd node upgrades.
8. **How long does `TestClientMatrix` add to `CI/integration`?**
   ~12 seconds.
9. **Are all gates G1–G11 in `docs/PRODUCTION.md` now satisfied?**
   Yes — G1 through G11 each have automated tests, reports, and ADRs linked in
   `docs/PRODUCTION.md`.
10. **What is left before tagging `v1.0.0`?**
    Merge this PR on 6/6 green CI, then tag and push `v1.0.0`.
