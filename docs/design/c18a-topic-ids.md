# C18a — Topic IDs and Protocol Versions to Kafka 3.1

Item 18a of `docs/PRODUCTION.md`: assign RFC 4122 v4 UUIDs to topics in the
metadata log and snapshot, and raise `ApiVersions` to v5, `Metadata` to v9–12,
`Fetch` to v12–13, and `CreateTopics` to v5–7.

## 1. Scope

Modern Kafka clients (`franz-go` v1.17+, `librdkafka` v2.3+, Kafka Java 3.1+)
negotiate `Metadata` v12 and `Fetch` v13 when the broker advertises them in
`ApiVersions`. At `Fetch` v13, requests and responses identify topics by a
16-byte UUID (`TopicId`, KIP-516) instead of a topic name string. When a topic
is deleted and recreated under the same name, the recreated topic receives a
fresh UUID so in-flight or cached `Fetch` v13 requests against the deleted
incarnation fail with `UNKNOWN_TOPIC_ID` (`100`) instead of silently reading
from the new topic's log.

This item covers:

| API | Key | Old range | New range | Flexible from | Notes |
|---|---:|---|---|---|---|
| `Fetch` | 1 | 4–11 | 4–13 | v12 | v12 flexible + `LastFetchedEpoch`; v13 replaces topic `Name` with `TopicId` (`UUID`) in `Topics` and `ForgottenTopicsData` |
| `Metadata` | 3 | 1–8 | 1–12 | v9 | v9 flexible; v10 adds `TopicId` in request & response; v11 drops `IncludeClusterAuthorizedOperations` / `ClusterAuthorizedOperations`; v12 makes request & response topic `Name` nullable for UUID-only lookups |
| `ApiVersions` | 18 | 0–4 | 0–5 | v3 | v5 wire format is identical to v4 |
| `CreateTopics` | 19 | 2–4 | 2–7 | v5 | v5 flexible + per-topic `NumPartitions`, `ReplicationFactor`, `Configs` in response; v6 identical to v5; v7 adds `TopicId` (`UUID`) in response |

Error code `100` (`ErrUnknownTopicID`, `UNKNOWN_TOPIC_ID`, retriable) is added
to `internal/wire/errors.go`.

## 2. State Machine and Format Level

### 2.1 Topic UUID assignment and determinism

`state` in `internal/broker/state.go` cannot call `n.rand` directly because
`apply` must be deterministic across all Raft replicas applying the same log
entry. Instead:

1. **Format level 4**: `formatLevel` in `internal/broker/state.go` is raised
   from `3` to `4`.
2. **At level >= 4**: the proposer (`n.createTopics` in `admin.go`) draws 16
   bytes from the node's deterministic PRNG (`n.newUUID()`), sets the RFC 4122
   version 4 (`0x40`) and variant 1 (`0x80`) bits, and encodes the 16-byte UUID
   at the end of `cmdCreateTopic` (`encodeCreateTopic(name, parts, configs, id)`).
   Every replica applying the log entry reads that exact UUID.
3. **Below level 4 (mixed-version rolling upgrade)**: `n.createTopics` encodes
   `cmdCreateTopic` without the trailing 16-byte UUID so pre-v0.7 nodes can
   decode the log entry. `CreateTopics` v7 is gated on `n.levelRefused(4)` so
   clients cannot observe a zero `TopicId` in a v7 response during a rolling
   upgrade.
4. **Upgrade transition (`cmdSetLevel` to >= 4)**: when `cmdSetLevel` raises
   `s.level` to `>= 4`, any pre-existing topic in `s.names` whose `t.id` is the
   zero UUID is assigned a deterministic v4 UUID derived from
   `sha256("samsa-topic-id:" + name)` (`topicUUID(name)`). Because `s.names` is
   sorted and the hash depends only on the topic name, all nodes derive the
   exact same UUIDs at the `cmdSetLevel` barrier without extra Raft entries.
5. **Delete and recreate**: deleting a topic removes `t.id` from `s.byID`.
   Recreating a topic of the same name at level >= 4 draws a fresh UUID from
   `n.newUUID()`, so the new topic has a distinct UUID from the deleted one.
   Furthermore, `cmdDeleteTopic` at level >= 4 optionally carries the target
   `wire.UUID` resolved when the delete was proposed (`encodeDeleteTopic(name, id)`).
   If the topic was already deleted and recreated before a duplicate or delayed
   `cmdDeleteTopic` commits, `apply` sees `t.id != id` and returns
   `ErrUnknownTopicID` (`100`) without deleting the recreated topic.

### 2.2 Snapshot v9 and state hash

- `snapshotVersion` in `internal/broker/snapshot.go` is raised from `8` to `9`.
- `s.encode()` writes snapshot version `9` when `s.level >= 4` and version `8`
  when `s.level < 4` (for mixed-cluster compatibility before `cmdSetLevel(4)`
  commits).
- Version 9 appends `w.UUID(s.topics[name].id)` for each topic in `s.names`
  after the ACL section.
- `decodeState` rejects version 9 snapshots if `s.level < 4`, rejects version
  `<= 8` snapshots if `s.level >= 4`, and rejects zero or duplicate topic UUIDs.
- `state.hash()` folds `t.id[:]` into the CRC32C state hash whenever
  `t.id != (wire.UUID{})`.

## 3. Protocol and Handler Behavior

### 3.1 `Metadata` v9–12

- **v9**: flexible encoding (`compact` strings/arrays + tag buffers).
- **v10–11**: each request topic entry has `TopicId` (`UUID`) and `Name`
  (compact string; nullable in v10–12 per Kafka schema). Each response topic has
  `TopicId` (`UUID`) after `Name`.
- **v11**: drops `IncludeClusterAuthorizedOperations` from the request and
  `ClusterAuthorizedOperations` from the response (KIP-709).
- **v12**: clients may query by `TopicId` with `Name == ""` (encoded as a null
  compact string) or by `Name` with `TopicId == (wire.UUID{})`:
  - If `TopicId != (wire.UUID{})`: look up `s.byID[id]`. If not found, emit a
    `TopicMetadata` entry with `ErrorCode: ErrUnknownTopicID` (`100`),
    `Name: ""` (null string on v12, `""` on v10–11), and `TopicID: id`.
    Wait: what if the client supplies *both* a non-zero `TopicId` and a `Name`?
    Per KIP-516, `TopicId` takes precedence when non-zero.
  - If `TopicId == (wire.UUID{})`: look up `s.topics[name]`. If not found,
    emit `ErrorCode: ErrUnknownTopicOrPart` (`3`), `Name: name`, `TopicID: UUID{}`.

### 3.2 `Fetch` v12–13

- **v12**: flexible encoding; adds `LastFetchedEpoch` (`int32`, default `-1`) in
  each request partition after `FetchOffset`.
- **v13**: replaces `Name` (`string`) with `TopicID` (`UUID`) in both
  `FetchRequest.Topics`, `FetchRequest.ForgottenTopicsData`, and
  `FetchResponse.Topics`.
- **Broker `handleFetch`**:
  - When `reqVersion >= 13`, resolve `ft.TopicID` via `n.st.byID[ft.TopicID]`.
  - If `ft.TopicID` is zero or not in `n.st.byID`, return `ErrUnknownTopicID`
    (`100`) on every partition in `ft.Partitions` and echo `TopicID: ft.TopicID`
    in the response.
  - If resolved to `t`, populate both `Name: t.name` and `TopicID: t.id` on
    `FetchResponseTopic` (wire encoding writes `Name` for `v <= 12` and
    `TopicID` for `v >= 13`).
  - When `reqVersion <= 12` and the topic is not found by `ft.Name`, return
    `ErrUnknownTopicOrPart` (`3`).

### 3.3 `CreateTopics` v5–7

- **v5–6**: flexible encoding; `CreateTopicResult` adds `NumPartitions`
  (`int32`), `ReplicationFactor` (`int16`), and `Configs` (nullable compact
  array of `CreateTopicConfig`). On success, `NumPartitions = int32(len(t.parts))`,
  `ReplicationFactor = int16(len(t.parts[0].replicas))`, and `Configs` lists the
  topic's effective config entries (`retention.ms`, `retention.bytes`,
  `cleanup.policy`) with `Source` set to `1` (`DYNAMIC_TOPIC_CONFIG`) when
  overridden in `t.configs` or `4` (`DEFAULT_CONFIG`) when using broker defaults.
  On error (or `ValidateOnly` when topic creation fails validation),
  `NumPartitions = -1`, `ReplicationFactor = -1`, `Configs = nil` (encoded as
  null compact array `-1`).
- **v7**: adds `TopicID` (`UUID`) after `Name` in `CreateTopicResult`. Gated on
  `n.levelRefused(4)` so mixed-version clusters reject v7 with
  `ErrUnsupportedVersion` (`35`) until all voters reach format level 4.

## 4. Second Review

1. **What happens during a rolling upgrade from v0.6.0 (level 3) to v0.7.0 (level 4)?**
   While any v0.6.0 voter remains in the cluster, `s.level` stays at `3`. New
   topics created during the upgrade use the v0.6.0 `cmdCreateTopic` encoding
   (no trailing UUID) and snapshot v8. `CreateTopics` v7 is refused with
   `UNSUPPORTED_VERSION` (`35`). Once the last node upgrades and announces
   level 4, the leader proposes `cmdSetLevel(4)`, which deterministically
   populates `t.id` for all existing topics via `topicUUID(name)`. Subsequent
   snapshots use v9 and subsequent `cmdCreateTopic` commands carry random v4
   UUIDs from the proposer.
2. **Can a delayed `cmdDeleteTopic` for an old topic incarnation delete a newly
   recreated topic of the same name?**
   No. At level >= 4, `encodeDeleteTopic` includes the target topic's UUID.
   If the topic was deleted and recreated before the second `cmdDeleteTopic`
   applies, `t.id != id` and `apply` returns `ErrUnknownTopicID` without
   touching the new topic.
3. **Does `Fetch` v13 interact with follower fetch (`PreferredReadReplica`)?**
   Yes. `handleFetch` resolves `ft.TopicID` to `t` through `n.st.byID[ft.TopicID]`
   before the per-partition loop, so follower fetch (`fp.ReplicaId == -1` on a
   follower) and leader fetch both use the resolved `t` and `t.name`.
4. **Are there any map-iteration non-determinisms in UUID assignment or snapshot
   encoding?**
   No. `cmdSetLevel` iterates `s.names` (sorted slice), `snapshot.encode` and
   `decodeState` iterate `s.names`, and `CreateTopics` response configs are
   built in fixed key order (`retention.ms`, `retention.bytes`, `cleanup.policy`).
