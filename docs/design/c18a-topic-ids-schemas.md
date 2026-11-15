# Item 18a, topic ids: the message schemas (verified 2026-11-15)

Read from the Apache Kafka repository, `clients/src/main/resources/common/message/*.json`, trunk, HTTP 200. These are the facts the design of item 18a
(topic ids, a prerequisite of KIP-848 and KIP-932) must follow. No code yet.

- **Metadata request v9-12** (flexible from 9): `Topics` (nullable from v1) of {`TopicId` uuid v10+, `Name` nullable from v10}; `AllowAutoTopicCreation`;
  `IncludeClusterAuthorizedOperations` only v8-10; `IncludeTopicAuthorizedOperations` v8+.
- **Metadata response v9-12:** `ThrottleTimeMs`, `Brokers` {NodeId, Host, Port, Rack nullable}, `ClusterId` nullable, `ControllerId`, `Topics` {ErrorCode, `Name` (nullable only in v12),
  `TopicId` v10+, IsInternal, Partitions {ErrorCode, PartitionIndex, LeaderId, LeaderEpoch, ReplicaNodes, IsrNodes, OfflineReplicas}, TopicAuthorizedOperations},
  `ClusterAuthorizedOperations` only v8-10. No tagged fields; v13 adds a top-level ErrorCode.
- **Fetch request v12:** flexible; tagged `ClusterId` (tag 0); `Topic` name; partitions add `LastFetchedEpoch` (default -1); `ForgottenTopicsData` by name; `RackId`.
  **v13:** the `Topic` name is replaced by `TopicId` (also in ForgottenTopicsData).
- **Fetch response v12:** flexible; partition tagged fields `DivergingEpoch` (tag 0), `CurrentLeader` (tag 1), `SnapshotId` (tag 2); `Topic` name. **v13:** `TopicId` instead of `Topic`.
- **CreateTopics v5-7:** flexible from 5; the response adds, from v5, `NumPartitions`, `ReplicationFactor`, `Configs` and tagged `TopicConfigErrorCode` (tag 0); **v7** adds `TopicId`.
- **DeleteTopics v6** (the highest): request `Topics` {Name nullable, TopicId} replaces `TopicNames`; response `Responses` {Name nullable, TopicId, ErrorCode, ErrorMessage}.
- **ApiVersions** goes to v5; tagged SupportedFeatures, FinalizedFeaturesEpoch, FinalizedFeatures, ZkMigrationReady (all ignorable).

Samsa offers today: Metadata v1-8, Fetch v4-11, CreateTopics v2-4, DeleteTopics (see `wire.APIs`).
