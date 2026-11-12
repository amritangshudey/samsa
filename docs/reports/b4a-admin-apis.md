# B4a report: admin APIs for the standard Kafka tools

Branch `b4a-admin-apis`, 2026-11-13. Production plan item 3, part (a).
Design: `docs/design/b4-operations.md` (sections 1–4, 6–8). Decision:
[ADR 0016](../adr/0016-admin-api-versions.md).

## What was built

- **Seven APIs** (`internal/wire/admin.go`): DescribeGroups, ListGroups,
  DeleteTopics, DeleteRecords, DescribeConfigs, CreatePartitions,
  DescribeCluster. Each one takes every version Kafka 4.4.0 accepts. The
  schemas come from `kafka-clients-4.4.0.jar`, and the error codes and
  config type ids were printed from the jar, not recalled. All seven are
  round-trip tested and fuzzed.
- **Three commands** (`internal/broker/state.go`):
  - *Delete topic* removes the topic, its producer state and every group's
    committed offsets for it, all in one command. A topic created later
    under the same name starts empty.
  - *Create partitions* only grows a topic. It refuses manual assignments
    (`INVALID_REPLICA_ASSIGNMENT`), because every node holds every
    partition.
  - *Delete records* moves a partition's start to an offset, which may be
    inside a batch. Offset -1 means the high watermark at apply time. The
    start only moves forward; a retention trim now keeps it (`max`).
- **Read-only answers** (`internal/broker/admin.go`):
  - DescribeConfigs reports the configs Samsa really has, all read-only.
    Every node answers it, which differs from the design: a broker's
    configs are asked of that broker.
  - ListGroups is answered by every node; a node that is not serving
    answers an empty list.
  - DescribeGroups is answered by the coordinator.
  - DescribeCluster is answered by every node.
- **Snapshot check**: it now accepts a partition start inside the first
  batch.
- **Simulator**:
  - An admin client (`internal/sim/admin.go`) deletes records, grows
    topics, and creates, fills and deletes a topic named `doomed`.
  - Model rules: R1 (a start never goes back) and D1 (a fetch sent after a
    delete was acknowledged finds no topic).
  - A new mutation, "let retention move a partition's start back".
- **Integration**: `TestKafkaCLI` runs the real Kafka 4.4.0 tools against
  a three-node cluster. `make integration` downloads the Kafka
  distribution, pinned by SHA-512. That download is the test-only
  dependency you approved on 2026-11-13.

## Results

| | |
|---|---|
| Simulator, 5000 seeds | clean; 68413 record deletes, 1897 partitions added, 1836 topics deleted, 5227 handovers |
| Simulator mutation suite, 13 mutations | all caught within 2000 seeds; "retention moves the start back" by R1 at seed 38 |
| Broker tests | delete forgets producer state and offsets (and the `UnsafeKeepOffsetsOnDelete` mutation fails it); a mid-batch start survives a restart from a snapshot; a trim keeps it |
| `TestKafkaCLI` | every tool in design section 1 passes: create, list, describe, alter, configs, delete-records, console consumer with and without a group, consumer-groups list/describe/state, delete, recreate, get-offsets. 150 s, mostly JVM start-up |
| Fuzzing, 60 s per new decoder | clean |
| Checker bugs found | 2 (BUGS.md M9: answers judged against the floor, then the start, at answer time instead of send time) |

## How to run it

    go test ./internal/broker -run 'Delete|CreatePartitions|Describe|ListAndDescribe|Trim'
    go test ./internal/sim
    make sim SEEDS=1000
    make integration     # downloads Kafka 4.4.0 once into integration/.cache

## What is untested or weak

- **The simulator never recreates a deleted topic.** Topics have no ids,
  so once a name is reused, a late answer about the old topic cannot be
  told apart from one about the new topic. Recreation is tested only by
  the broker test and `TestKafkaCLI`. For the same reason, the
  `UnsafeKeepOffsetsOnDelete` mutation is caught by the broker test and
  not by the simulator.
- **Idempotent producer after a recreate** (design review, point 5) has
  not been measured with a real client. The producer's next sequence is
  not 0, and Samsa answers `OUT_OF_ORDER_SEQUENCE_NUMBER`. Whether the
  Java client recovers from that by itself is unknown.
- **DescribeGroups leaves the client host empty.** The host is not applied
  state.
- **Configs cannot be changed.** There is no AlterConfigs, and retention
  is one bound for the whole log, not one per topic.
- **`TestKafkaCLI` is slow** (150 s), because each tool starts a JVM. CI
  time grows by the same amount, plus the download the first time.
- **DescribeConfigs on a follower** may lag on which topics exist, as
  Metadata does.

## Ten questions

1. Why is a topic delete one command, and not one per partition and one
   per group?
2. Kafka needs topic ids to make delete-then-recreate safe. Why does
   Samsa not, and what would make it need them?
3. Why does DeleteRecords with offset -1 resolve to the high watermark at
   apply time and not when the request arrives?
4. Why may a partition's start sit inside a batch, and what did the
   snapshot check have to learn?
5. What breaks if a trim sets `start = batches[0].base` after a
   DeleteRecords? Which rule catches it?
6. Why must the checker judge a DeleteRecords answer against the floor
   and start captured when the request was sent (M9)?
7. Why does every node answer ListGroups, and why is an empty list
   correct from a node that is not serving?
8. Why is DescribeConfigs answered by every node while DeleteTopics
   answers `NOT_CONTROLLER` on followers?
9. Why does the simulator not recreate topics, and which tests cover it
   instead?
10. Why accept every version from Kafka 4.4.0 rather than only the one
    the 4.4.0 tools use?
