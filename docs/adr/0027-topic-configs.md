# ADR 0027: Topic configs as state, set by CreateTopics and IncrementalAlterConfigs

Date: 2026-11-14. Status: accepted. Design: `docs/design/c2-retention-compaction.md`, section 3.

## Context

Per-topic retention (ADR 0026) needs a way for clients to set it. Kafka
has two config-changing APIs: AlterConfigs (key 33), which replaces a
resource's whole config, and IncrementalAlterConfigs (key 44), which sets
or deletes single entries. `kafka-configs.sh --alter` uses the incremental
API, as do the Java and franz-go admin clients. CreateTopics also carries
configs, which Samsa read and dropped until now.

## Decision

- **IncrementalAlterConfigs, versions 0 and 1** (version 1 flexible), as
  in Kafka's `IncrementalAlterConfigsRequest.json` and
  `IncrementalAlterConfigsResponse.json`. AlterConfigs (key 33) is not
  offered.
- **Three topic configs are state:**
  - `retention.ms` and `retention.bytes`: integers of at least -1. A
    value of -1 means no limit; `retention.bytes` is per partition, as in
    Kafka.
  - `cleanup.policy`: only `delete` until compaction (part b) lands.

  They are kept in the state machine, written by `cmdTopicConfigs` and by
  CreateTopics' trailing configs. Snapshot version 8 carries them.
- **What the API accepts:**
  - only SET and DELETE; APPEND and SUBTRACT apply only to list configs,
    and none of the three is one;
  - an unknown name or a bad value is INVALID_CONFIG for the whole
    resource, and nothing of it applies, as in Kafka;
  - broker resources are refused: broker configs are command-line flags.
- **What CreateTopics does with configs:** configs it does not know are
  still accepted and ignored, as before, so existing tools keep working.
  A known one with a bad value is INVALID_CONFIG.
- **Format level 2:** both write paths need it. Below it they answer
  UNSUPPORTED_VERSION, so that an old node never meets a command it
  cannot read (ADR 0024).
- **DescribeConfigs** reports a topic's own value with source
  DYNAMIC_TOPIC_CONFIG (1), and the three as writable.
- **The broker default for `retention.ms` is "forever"** (`-retention-ms
  0`), not Kafka's 7 days. The design said 7 days. Changing it would make
  an upgrade start deleting data that the old build kept. Operators who
  want Kafka's default set the flag.

## Alternatives

- **AlterConfigs (key 33).** Replace-all semantics make a client that
  sets one value silently reset the others. Kafka deprecated it for that
  reason.
- **Rejecting unknown configs in CreateTopics.** Kafka does. Samsa
  accepted them before, and tools pass configs such as `segment.ms` that
  have no meaning here.

## Consequences

- `kafka-configs.sh --alter --entity-type topics` works for the three
  configs. That is untested with the real CLI until the integration test
  covers it.
- Retention configs are replicated and survive restarts and snapshots.
- An upgrade from level 1 keeps `-retention-bytes` working unchanged
  until the cluster reaches level 2.
