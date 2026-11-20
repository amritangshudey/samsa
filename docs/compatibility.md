# Client Compatibility Matrix

Generated directly from `TestClientMatrix` (`make matrix`) on 2026-11-20.

## Summary

| Client | Language | Version | Produce | Idempotent | Consume | Consumer Groups | Compression | Transactions | Admin |
|---|---|---|---|---|---|---|---|---|---|
| `twmb/franz-go` | Go | v1.22.3 | PASS | PASS (default) | PASS (+ KIP-392) | PASS (classic) | PASS (gzip/snappy/lz4/zstd) | PASS (`TestFranzGoTransactions`) | PASS |
| `IBM/sarama` | Go | v1.62.0 | PASS | PASS | PASS | PASS (classic) | PASS (gzip) | not tested | PASS (`ClusterAdmin`) |
| `segmentio/kafka-go` | Go | v0.4.51 | PASS | N/A (client) | PASS | PASS (classic) | not tested | N/A (client) | PASS (`CreateTopics`) |
| Apache Kafka `kafka-clients` | Java | 4.4.0 | PASS | PASS (default) | PASS | PASS (classic + KIP-848 + KIP-932) | PASS | PASS (`TestJavaTransactions`) | PASS |
| `kcat` (`librdkafka`) | C | Version 1.7.0 (JSON, Avro, Transactions, IncrementalAssign, librdkafka 2.16.0 builtin.features=gzip,snappy,ssl,sasl,regex,lz4,sasl_gssapi,sasl_plain,sasl_scram,plugins,zstd,sasl_oauthbearer,http,oidc) | PASS | not tested | PASS | not tested | not tested | not tested | N/A |
| Apache Kafka CLI (`kafka-topics`, `kafka-consumer-groups`, `kafka-configs`, `kafka-acls`, `kafka-share-groups`) | Java / CLI | 4.4.0 | PASS (`console-producer`) | PASS | PASS (`console-consumer`) | PASS (classic + KIP-848 + KIP-932) | not tested | N/A | PASS |

## Negotiated Kafka API versions (observed from broker debug log)

| Client | Negotiated `key:version` pairs |
|---|---|
| `twmb/franz-go` | `0:v9`, `18:v5`, `19:v7`, `1:v13`, `22:v5`, `2:v7`, `3:v12` |
| `IBM/sarama` | `0:v9`, `10:v3`, `11:v5`, `12:v3`, `13:v3`, `14:v3`, `18:v3`, `19:v7`, `1:v12`, `22:v4`, `2:v6`, `3:v11`, `8:v8`, `9:v8` |
| `segmentio/kafka-go` | `0:v8`, `18:v0`, `3:v8` |
| Apache Kafka `kafka-clients` | `0:v9`, `18:v5`, `19:v7`, `1:v13`, `22:v5`, `2:v7`, `3:v12` |
| `kcat` (`librdkafka`) | `0:v9`, `18:v3`, `18:v5`, `19:v7`, `1:v13`, `2:v7`, `3:v12` |
| Apache Kafka CLI (`kafka-topics`, `kafka-consumer-groups`, `kafka-configs`, `kafka-acls`, `kafka-share-groups`) | `18:v5`, `19:v7`, `3:v12` |

## Notes

- Every `PASS` cell above is backed by an automated test in `integration/` that runs on every PR in `CI/integration` (`SAMSA_REQUIRE_CLIENTS=1`).
- `kcat` exercises `librdkafka` (the C core shared by `confluent-kafka-python`, `rdkafka`, and `confluent-kafka-dotnet`).
- Regenerate this file at any time with `make matrix`.
