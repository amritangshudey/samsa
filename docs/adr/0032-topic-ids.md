# 0032 — Topic IDs in the Metadata Log

## Context

Kafka 2.8+ introduced 16-byte topic UUIDs (`TopicId`, KIP-516) so that
`Metadata` v10–12, `CreateTopics` v7, and `Fetch` v13 can identify topics by
an incarnation-unique UUID rather than by name. When a topic is deleted and
recreated under the same name, a client still holding the old UUID receives
`UNKNOWN_TOPIC_ID` (`100`) instead of reading records from the new topic.

Because Samsa's metadata state machine (`internal/broker/state.go`) is a pure,
deterministic Raft state machine, every replica applying the same log entry or
snapshot must arrive at the exact same `wire.UUID` for every topic.

## Decision

1. **Format level 4 & snapshot version 9**: Topic UUIDs are introduced at
   cluster format level 4 and persisted in snapshot version 9 as a trailing
   array of 16-byte UUIDs parallel to `s.names`.
2. **Proposer-assigned UUIDs on `cmdCreateTopic`**: At format level `>= 4`, the
   proposing node draws 16 bytes from its injected deterministic PRNG
   (`n.rand`), sets RFC 4122 version 4 and variant 1 bits, and appends the UUID
   to `cmdCreateTopic`. Every replica reads that UUID from the log entry.
3. **Deterministic UUIDs on level upgrade (`cmdSetLevel`)**: Topics created
   before the cluster reaches level 4 have no UUID in their `cmdCreateTopic`
   entry. When `cmdSetLevel` raises `s.level` to `>= 4`, every replica assigns
   each existing topic with a zero UUID a deterministic v4 UUID derived from
   `sha256("samsa-topic-id:" + name)`. Recreated topics after the upgrade go
   through `cmdCreateTopic` at level 4 and receive fresh random v4 UUIDs.
4. **UUID-checked `cmdDeleteTopic`**: At format level `>= 4`, `cmdDeleteTopic`
   carries the expected `wire.UUID` of the topic being deleted. If the topic no
   longer exists or has been recreated with a different UUID by the time the
   entry commits, `apply` returns `ErrUnknownTopicID` (`100`) without deleting
   the new topic.

## Consequences

- Deleting and recreating a topic produces a new UUID at level 4, preventing
  stale `Fetch` v13 or `Metadata` v12 requests from crossing topic
  incarnations.
- Rolling upgrades from v0.6.0 (format level 3) to v0.7.0 (format level 4)
  require no extra Raft log traffic during `cmdSetLevel(4)`, and all replicas
  agree on both pre-existing and newly created topic UUIDs (`state.hash()`
  includes `t.id`).
