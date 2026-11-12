# ADR 0016: admin API versions, and topics named only by name

Date: 2026-11-13. Status: accepted. Design: `docs/design/b4-operations.md`.

## Context

The Kafka 4.4.0 CLI tools (`kafka-topics`, `kafka-configs`,
`kafka-consumer-groups`, `kafka-delete-records`) need seven APIs Samsa did
not have. The schemas were read from `common/message/*.json` in
`kafka-clients-4.4.0.jar`, the jar the tools themselves run, and the error
codes from its `Errors` enum (printed by a three-line Java program, not
recalled). The Java admin client takes the highest version both sides
support.

DeleteTopics v6 can name a topic by a topic id (a UUID) instead of a name.
Samsa has no topic ids; Metadata v8, the highest version it speaks, has
none either.

## Decision

| API | Key | Versions | Flexible from |
|---|---|---|---|
| DescribeGroups | 15 | 0–6 | 5 |
| ListGroups | 16 | 0–5 | 3 |
| DeleteTopics | 20 | 1–6 | 4 |
| DeleteRecords | 21 | 0–2 | 2 |
| DescribeConfigs | 32 | 1–4 | 4 |
| CreatePartitions | 37 | 0–3 | 2 |
| DescribeCluster | 60 | 0–2 | 0 |

Every version Kafka 4.4.0 still accepts, so older clients and librdkafka
are served too; the cost is a version check per field, and every version
is round-trip tested and fuzzed.

Topics are named by name only. A request naming a topic by id alone is
answered `UNKNOWN_TOPIC_ID` (100); responses carry the zero UUID. A delete
is one command that removes the topic, its partitions' producer state and
every group's committed offsets for it, so a topic later created under the
same name starts with nothing of the old one.

## Alternatives

- **Only the top version of each API.** The 4.4.0 tools would work;
  librdkafka and older Java clients, which ask for lower versions, would
  get `UNSUPPORTED_VERSION`.
- **Topic ids.** Kafka needs them because a delete takes effect on each
  broker at a different time. In Samsa it is one command applied in log
  order on every replica. Ids would also mean Metadata v10+ and Fetch v13+.

## Consequences

- An admin tool that deletes by id gets an error. The 4.4.0 tools delete
  by name.
- If a client ever needs topic ids (Fetch v13+ uses them instead of
  names), this ADR is replaced.
