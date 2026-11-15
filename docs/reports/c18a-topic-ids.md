# C18a report: topic IDs and protocol versions to Kafka 3.1

2026-11-16. Production plan item 18a. Design `docs/design/c18a-topic-ids.md`,
ADR 0032.

## What was built

- **Wire (`internal/wire`):**
  - `ApiVersions` raised to v5 (`ClusterId` and `NodeId` at v5, KIP-1242).
  - `Metadata` raised to v12 (flexible from v9; `TopicId` UUID in requests and
    responses from v10; `IncludeClusterAuthorizedOperations` and
    `ClusterAuthorizedOperations` bounded to v8–10; nullable `Name` in v12
    responses for unknown topic IDs).
  - `CreateTopics` raised to v7 (flexible from v5; `NumPartitions`,
    `ReplicationFactor`, and `Configs` with `Source` in v5+ responses;
    `TopicId` UUID in v7 responses).
  - `Fetch` raised to v13 (flexible from v12; `LastFetchedEpoch` in v12+
    request partitions; `TopicId` UUID replacing `Topic` name in v13
    `FetchRequest.Topics`, `ForgottenTopicsData`, and `FetchResponse.Topics`).
- **State machine and snapshots (`internal/broker`):**
  - `formatLevel` raised from `3` to `4`; `snapshotVersion` raised from `8` to `9`.
  - Every topic in `state` carries `id wire.UUID` and is indexed in
    `byID map[wire.UUID]string`; `state.hash()` hashes each topic's 16-byte UUID.
  - At format level >= 4, `cmdCreateTopic` carries a 16-byte RFC 4122 v4 UUID
    drawn by the proposing leader from `n.rand`; `cmdDeleteTopic` carries an
    optional 16-byte target UUID so a delayed delete for an old incarnation
    cannot delete a newly recreated topic of the same name (`ErrUnknownTopicID`).
  - When a cluster upgrades to format level 4 (`cmdSetLevel(4)`), pre-existing
    topics with a zero UUID receive a deterministic v5-style UUID derived from
    their topic name (`topicUUID(name)`) in sorted `s.names` order.
  - `CreateTopics` v7 is gated on `n.levelRefused(4)` so mixed-version clusters
    refuse v7 with `UNSUPPORTED_VERSION` (`35`) until every voter supports level 4.
- **Simulator (`internal/sim`):**
  - `Model` tracks each topic's `TopicID`, checking that every node and API
    reports the same unique non-zero UUID (`I4`) and that fetches by UUID or
    name after deletion return `UNKNOWN_TOPIC_ID` (`100`) or
    `UNKNOWN_TOPIC_OR_PARTITION` (`3`) respectively (`D1`).
  - Consumers send `Fetch` v13 by `TopicID` once the model has learned the
    topic's UUID from `CreateTopics` v7 or `Metadata` v10–12, and query
    `Metadata` v10–12 by `TopicID`.
  - Admin client tests incarnation isolation (`D2`) on `reborn`: creates at v7,
    deletes by `TopicID` at `DeleteTopics` v6, recreates at v7, verifies the
    new UUID differs from the deleted incarnation's, and verifies `Fetch` v13
    with the old UUID answers `UNKNOWN_TOPIC_ID` (`100`).
  - Mutation `UnsafeReuseTopicID` added to `TestMutationSuite`.

## Results

- **Unit and fuzz tests:** `go test ./internal/wire/... ./internal/broker/...`
  pass; `FuzzDecodeMessage` (1,004,756 executions in 5 s) and `FuzzDecodeState`
  (165,733 executions in 5 s) pass.
- **Simulator:** 200 seeds (`TestSim`), `TestDeterminism`, `TestShrink`, and
  `TestRegressions` pass with 0 failures.
- **Mutation `UnsafeReuseTopicID`:** caught by 66 of 200 seeds in
  `TestMutationSuite` (first at seed 1: `D2: recreated topic reborn got topic id ...`).
- **Real clients (`integration/`):**
  - `franz-go` v1.22.3 negotiates `ApiVersions` v5, `Metadata` v12,
    `CreateTopics` v7, and `Fetch` v13.
  - `apache-kafka-java` 4.4.0 negotiates `ApiVersions` v5, `Metadata` v12,
    `CreateTopics` v7, and `Fetch` v13.
  - `librdkafka` 2.16.0 (`kcat`) negotiates `Metadata` v12 and `Fetch` v13.
  - `TestTopicIDIncarnationAndFetchByID` verifies end-to-end creation at v7,
    `Metadata` v12 lookup by UUID, `DeleteTopics` v6 by UUID, recreation with a
    new UUID, and `Fetch` v13 refusal (`100`) on the old UUID.
  - `TestRollingUpgrade` passes upgrading a 3-node cluster from both `v0.2.0`
    and `v0.6.0` to the new build while a producer writes continuously.

## Problems met

- **BUGS.md C18a-1:** `ApiVersionsRequest` v5 (KIP-1242) adds `ClusterId`
  (compact nullable string) and `NodeId` (`int32`) before the tagged fields.
  On a client's first connection `ClusterId` is null (`0x00`), which `r.Tags`
  read as zero tags; on subsequent connections after `Metadata`, `franz-go` and
  Java 4.4.0 sent `ClusterId = "samsa"`, causing `malformed request body: key 18 version 5`.
  Fixed in `APIVersionsRequest.Decode` and `Encode`.
- **BUGS.md M19:** the simulator's `createTopic` initially required a non-zero
  `TopicID` on `TOPIC_ALREADY_EXISTS` as well as `ErrNone`. Fixed to check
  `TopicID` only on `ErrNone` and query `Metadata` v12 when `admin.go` meets
  `TOPIC_ALREADY_EXISTS` after a dropped connection.
- **BUGS.md M20:** seed 951 in the 3,000-seed CI run exposed the remaining half
  of M14 in `checkCompactedFinal`: an unacknowledged tombstone (`13359`, sent at
  9.958 s, connection closed at 10.012 s) arrived at the leader after a
  concurrently sent put (`13376`, sent at 9.962 s, offset 1730), superseded it,
  and expired during quiesce, while `oldTombID` compared send IDs (`13359 > 13376`).
  Fixed by comparing the unacknowledged tombstone's in-flight end time against
  `r.sent` (`oldTombUntil[k] >= r.sent`), and added seed 951 to
  `internal/sim/regressions.txt`.

## How to run it

```sh
go test -v ./internal/wire -run 'TestRoundTrip|TestFetchNameAndTopicID'
go test -v ./internal/broker -run 'TestTopicIDsAndProtocolVersions|TestSnapshotRoundTrip'
go test -v ./internal/sim -run 'TestSim$|TestMutationSuite/reuse'
make build && (cd integration && go test -v -run 'TestTopicIDIncarnationAndFetchByID|TestFranzGoRoundTrip|TestJavaClientRoundTrip|TestKcatRoundTrip')
```

## What is untested or weak

- **ForgottenTopicsData in Fetch v13:** decoded and encoded with `TopicID` at
  v13, and ignored by the broker because Samsa does not maintain incremental
  fetch sessions (`SessionID` is always `0`).
- **Pre-level-4 topics across a rolling upgrade:** `topicUUID(name)` assigns a
  deterministic UUID when `cmdSetLevel(4)` applies; if a topic is created,
  deleted, and recreated on an old release *before* the cluster upgrades to
  level 4, both incarnations had the same name before level 4 existed, and only
  the surviving incarnation receives `topicUUID(name)` at `cmdSetLevel(4)`.

## Ten questions

1. **Why cannot `state.apply` generate a random UUID when applying `cmdCreateTopic`?**
   `apply` must be a pure function of the log entry so every replica produces
   identical state; randomness is drawn by the proposing leader and carried in
   the command bytes.
2. **What happens to topics that existed before a cluster upgrades from v0.6.0 (level 3) to level 4?**
   When `cmdSetLevel(4)` is applied, `state.apply` iterates `s.names` in sorted
   order and assigns `topicUUID(name)` (a deterministic UUID derived from the
   name) to any topic whose `id` is still zero.
3. **Why is `CreateTopics` v7 refused while the cluster's format level is below 4?**
   A v7 caller expects a non-zero `TopicID` in `CreateTopicsResponse`, which
   requires the level-4 `cmdCreateTopic` format; refusing with
   `UNSUPPORTED_VERSION` (`35`) while an older voter remains prevents writing
   level-4 commands that an old voter cannot decode.
4. **How does `Fetch` v13 identify topics on the wire?**
   Both `FetchRequest.Topics` (and `ForgottenTopicsData`) and
   `FetchResponse.Topics` replace the topic `Name` string with a 16-byte
   `TopicID` UUID.
5. **What error code does `Fetch` v13 return when a topic UUID is not in `byID`?**
   `UNKNOWN_TOPIC_ID` (`100`), whereas `Fetch` v0–12 by name returns
   `UNKNOWN_TOPIC_OR_PARTITION` (`3`).
6. **What if a `Fetch` v13 carries a known `TopicID` but asks for a partition index that does not exist?**
   The topic exists (`byID` resolves it), so the broker returns
   `UNKNOWN_TOPIC_OR_PARTITION` (`3`) for that partition, not `UNKNOWN_TOPIC_ID`.
7. **Why is `MetadataResponse.Topics.Name` nullable only in v12?**
   When a client queries `Metadata` v12 by `TopicID` (`Name` null in the
   request) and the broker does not know that `TopicID`, the broker cannot
   supply a topic name and encodes `Name` as null (`""` via `optString`) with
   `UNKNOWN_TOPIC_ID` (`100`).
8. **Why were `IncludeClusterAuthorizedOperations` and `ClusterAuthorizedOperations` removed in `Metadata` v11?**
   KIP-700 moved cluster-level authorized operations to `DescribeCluster` (key
   60) and dropped the fields from `Metadata` starting at v11.
9. **Why did `ApiVersions` v5 pass on the first connection and fail on the second before the `C18a-1` fix?**
   KIP-1242 added `ClusterId` (compact nullable string) and `NodeId` (`int32`)
   to `ApiVersionsRequest` v5; on bootstrap `ClusterId` is null (`0x00`), which
   `r.Tags` mistook for zero tagged fields, whereas once `Metadata` returned
   `ClusterId = "samsa"` the non-null string failed tag parsing.
10. **How does `cmdDeleteTopic` avoid deleting a newly recreated topic if a duplicate delete was in flight?**
    At format level >= 4, `encodeDeleteTopic` includes the target topic's UUID;
    if `t.id != id` when `cmdDeleteTopic` applies, `apply` returns
    `ErrUnknownTopicID` without deleting the new topic.
