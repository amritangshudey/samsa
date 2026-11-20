# ADR 0041: Client Compatibility Matrix and Test-Only Go Client Dependencies (B5, G9)

**Status:** Accepted  
**Date:** 2026-11-21  
**Item:** 16 (Client compatibility matrix, B5, G9)

## Context

`docs/PRODUCTION.md` gate G9 and item 16 require testing Samsa against multiple
independent Kafka client implementations and generating a compatibility table
directly from test results. Previously, `integration/` tested `franz-go`,
Apache Kafka Java 4.4.0, `kcat` (`librdkafka`), and Kafka 4.4.0 CLI tools.

## Decision

1. Add `github.com/IBM/sarama` and `github.com/segmentio/kafka-go` as test-only
   dependencies in `integration/go.mod` (approved by the owner on 2026-11-21).
   The root `samsa` module remains zero-dependency (standard library only).
2. Add dedicated integration tests (`TestSaramaRoundTripAndGroup`,
   `TestKafkaGoRoundTripAndGroup`) and a matrix runner (`TestClientMatrix`) in
   `integration/matrix_test.go` that generates `docs/compatibility.md` via
   `make matrix` (`SAMSA_MATRIX_OUT`).
3. Include both the capability table (produce, idempotent produce, consume,
   consumer groups, compression, transactions, KIP-848, KIP-932, admin) and the
   exact negotiated Kafka API versions observed from the broker for every client.

## Alternatives considered

- **Add Python and Node.js package installs to CI**: would require `pip`/`npm`
  network fetches or lockfiles in CI; `kcat` already exercises `librdkafka`
  (the C engine underneath `confluent-kafka-python`, `rdkafka`, and
  `confluent-kafka-dotnet`), and `sarama` + `kafka-go` + `franz-go` + Java
  4.4.0 + `librdkafka` + Kafka CLI cover five independent wire codecs with zero
  extra CI runtime setup.

## Consequences

- Five independent Kafka wire-protocol implementations (`franz-go`, `sarama`,
  `kafka-go`, Apache Kafka Java 4.4.0, `librdkafka`/`kcat`) plus the Kafka CLI
  tools run on every PR in `CI/integration`.
- `docs/compatibility.md` can be regenerated at any time with `make matrix`.
