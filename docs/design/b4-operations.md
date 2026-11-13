# B4 design: operations

Status: reviewed, 2026-11-13. Production plan item 3 (`docs/PRODUCTION.md`):
"Prometheus metrics, health/readiness, admin APIs (DescribeCluster,
DescribeConfigs, DeleteTopics, DeleteRecords, CreatePartitions), `samsa
check`, config file. Done when the standard Kafka CLI tools run against it
and `check` finds planted corruption."

Two parts, two PRs, like A2:

- **B4a**: the Kafka admin APIs the CLI tools need (sections 2–4).
- **B4b**: metrics, health, `samsa check` and the config file (section 5).

## 1. What the standard tools need today

Measured, not assumed: the Kafka 4.4.0 distribution's CLI tools
(`kafka_2.13-4.4.0.tgz`, SHA-512 pinned in the Makefile, checked against
archive.apache.org) were run against a single `samsa` node built from
branch `b3-raft-hardening`, with `-log-level debug`. The test-only download
was approved by the developer on 2026-11-13.

| Command | Result | Missing API |
|---|---|---|
| `kafka-topics --create --partitions 2` | works | — |
| `kafka-topics --list` | works | — |
| `kafka-cluster cluster-id` | works (`samsa`) | — |
| `kafka-broker-api-versions` | works | — |
| `kafka-topics --describe` | fails | DescribeConfigs (32) |
| `kafka-topics --alter --partitions 3` | fails | CreatePartitions (37) |
| `kafka-topics --delete` | fails | DeleteTopics (20) |
| `kafka-configs --describe` (topic, broker) | fails | DescribeConfigs (32) |
| `kafka-consumer-groups --list` | fails | ListGroups (16) |
| `kafka-delete-records` | not reached (empty input in the probe) | DeleteRecords (21), to confirm |

Requests seen from the tools: ApiVersions v4, Metadata v8 and v5 attempts
(the client negotiates down), CreateTopics v4. The tools did not ask for
DescribeTopicPartitions (75) once ApiVersions showed it unsupported.

ListGroups and DescribeGroups are not in the plan's list, but
`kafka-consumer-groups` is a standard tool and needs them, so they are in
scope. Whether `--describe` also asks for ConsumerGroupDescribe (69, the
new protocol) before falling back is measured during B4a; the fallback is
what Samsa will use.

## 2. Schemas and versions

Read from `common/message/*.json` inside `kafka-clients-4.4.0.jar` of the
same distribution, so they match the tools exactly (rule 5):

| API | Key | Valid versions | Flexible from | Samsa supports |
|---|---|---|---|---|
| DeleteTopics | 20 | 1–6 | 4 | 1–6 |
| DeleteRecords | 21 | 0–2 | 2 | 0–2 |
| DescribeConfigs | 32 | 1–4 | 4 | 1–4 |
| CreatePartitions | 37 | 0–3 | 2 | 0–3 |
| ListGroups | 16 | 0–5 | 3 | 0–5 |
| DescribeGroups | 15 | 0–6 | 5 | 0–6 |
| DescribeCluster | 60 | 0–2 | 0 | 0–2 |

The Java tools take the highest version both sides support, so each top
version is flexible. Older versions cost a version check per field and
serve librdkafka and older Java clients. The wire package already encodes
flexible versions (InitProducerId uses them from v2), and every decoder is
fuzzed, as now. The choice is recorded as ADR 0016.

## 3. The commands: what changes state

Every change is a command in the log, applied by the state machine
(ADR 0005). Each is proposed by the leader, and answered after it commits,
as CreateTopics is now.

### 3.1 DeleteTopics: `cmdDeleteTopic{name}`

Apply removes the topic, its name from the sorted list, and **every
group's committed offsets for it**, in the same command. The topic's
idempotent-producer state goes with its partitions. Its records stay in the
Raft log until retention trims past them; nothing references them, so they
no longer hold the trim back.

**Topic identity.** DeleteTopics v6 names a topic by name or by topic ID
(a UUID). Samsa has no topic IDs, and Metadata v8, the version the tools
negotiate, carries none. Two options:

1. *Topic IDs in the state machine*, and so in Metadata v10+ and Fetch v13+.
2. *Names only, with everything about a topic deleted in one command.*

Option 2. The reason for topic IDs in Kafka is that deletion there is
asynchronous across brokers, so a broker can briefly hold the old topic's
data under a name already reused. In Samsa a delete is one command applied
in order on every replica: after it, no replica has anything of the old
topic, and a topic created later under the name starts empty, with no
committed offsets and no producer state. A request that names a topic only
by ID is answered `UNKNOWN_TOPIC_ID` (100); the response carries the zero
UUID. Topic IDs can come later with Metadata v10+ if a client needs them.

In-flight requests: an append applied after the delete finds no topic and
is answered `UNKNOWN_TOPIC_OR_PARTITION`; a fetch waiting on the topic is
answered the same; a client holding an old offset in a recreated topic
gets `OFFSET_OUT_OF_RANGE` and resets.

### 3.2 CreatePartitions: `cmdCreatePartitions{name, count}`

Only growth: `count` at most the current count → `INVALID_PARTITIONS` (37);
over the bound CreateTopics already uses → the same. `Assignments` must be
null: every node replicates everything, so a placement cannot be honoured →
`INVALID_REPLICA_ASSIGNMENT` (39). `ValidateOnly` checks and proposes
nothing. Apply appends empty partitions. A count at most the current one at
apply time (two racing requests) is a no-op answered `INVALID_PARTITIONS`.

### 3.3 DeleteRecords: `cmdDeleteRecords{topic, partition, offset}`

`offset` -1 means the high watermark; above it → `OFFSET_OUT_OF_RANGE`.
Apply sets `start = max(start, offset)` and drops batches that end at or
below it. A batch that straddles the new start is kept; a fetch from inside
it returns the whole batch, and the client skips records before its
offset, as with Kafka. A fetch below `start` is `OFFSET_OUT_OF_RANGE`, and
ListOffsets earliest answers `start`. The answer's `LowWatermark` is the
start after apply.

**Trim must keep this start.** `cmdTrim` now sets `start =
batches[0].base`; after a DeleteRecords inside a batch that moves the start
back. It becomes `max(start, …)`. The simulator checks it (section 6).

## 4. The answers: what only reads

### 4.1 DescribeConfigs

Answered by every node, from its own configuration and applied state, as
Metadata is (changed while building: the admin client sends a broker's
configs request to that broker itself, so a leader-only answer could never
describe a follower; nothing in the answer is changed by a client, and a
follower is at most a little behind on which topics exist). Resource types: topic (2) and broker (4); others →
`INVALID_REQUEST` for that resource. Keys null means all.

Only what Samsa really has, every entry `ReadOnly` (there is no
AlterConfigs) and `ConfigSource` 4 (static broker config) or 5 (default):

| Resource | Name | Value from |
|---|---|---|
| topic | `cleanup.policy` | `delete` (no compaction until item 5) |
| topic, broker (`log.retention.bytes`) | `retention.bytes` | `-retention-bytes`, -1 if unset |
| topic | `retention.ms` | -1: Samsa retains by size only |
| topic, broker (`log.segment.bytes`) | `segment.bytes` | segment size |
| topic, broker (`message.max.bytes`) | `max.message.bytes` | the batch bound |
| broker | `samsa.sync` | `always`, `interval` or `never` |

Keys Samsa does not have are left out, as Kafka does for unknown keys.
Synonyms are empty, documentation null. An unknown topic is
`UNKNOWN_TOPIC_OR_PARTITION`; a broker name other than this node's id is
`INVALID_REQUEST`.

### 4.2 ListGroups and DescribeGroups

The admin client sends ListGroups to **every** broker in Metadata and
merges the answers. The leader lists every group it coordinates; a node
that is not serving answers an empty list without error, so the merge is
right. `GroupState` is the coordinator's state (`Empty`, `Stable`,
`PreparingRebalance`, `CompletingRebalance`), `GroupType` `classic`.
State and type filters apply.

DescribeGroups goes to the coordinator, which FindCoordinator already says
is the leader; elsewhere `NOT_COORDINATOR`. An unknown group is described
as `Dead` with no members and no error, as Kafka does. Members carry their
metadata and assignment; `ProtocolData` is the chosen strategy.
`AuthorizedOperations` is the "not requested" default.

### 4.3 DescribeCluster

Cluster id, `ControllerId` = the leader's id or -1, the brokers exactly as
Metadata lists them, none fenced. `EndpointType` 2 (controllers) →
`UNSUPPORTED_ENDPOINT_TYPE` (115): Samsa has no separate controllers.

## 5. B4b: running it

Outside the core; `cmd/samsa` and `internal/realsys` only, standard library
only.

- **Metrics**: `-metrics-listen host:port` serves `/metrics` in the
  Prometheus text format, written by hand (the format is lines of
  `name{labels} value`; no client library). The node is single-threaded,
  so the loop publishes a copy of its counters after each event batch
  through an atomic pointer, and the HTTP handler reads only that copy.
  Counters: requests and errors by API, bytes in and out, connections;
  gauges: role, term, commit and applied index, log size, topics,
  partitions, groups. Every name is listed in the README.
- **Health**: `/healthz` answers 200 while the loop runs (the copy is
  younger than a second). `/readyz` answers 200 when the node is the
  serving leader or follows a known leader and has applied what it knows
  committed; else 503 with the reason.
- **`samsa check -data dir`**: offline, read-only. Checks every Raft log
  segment's records and CRCs, the hard state, the snapshot's encoding and
  checksum, the install marker if present, and that the log continues from
  the snapshot. Prints each problem with file and offset; exit status 1 if
  any. It refuses to run on a directory a live node holds (the lock file).
- **Config file**: `-config file` with `key = value` lines, one per flag of
  the same name, `#` comments. Flags given on the command line win. An
  unknown key is an error, not a warning: a typo in `sync` must not
  silently run with the default.

**As built** (B4b), where it differs from the plan above:

- The loop publishes the copy every 250 ms from a ticker on its own
  goroutine (`Loop.Every`), not after each event: a copy per event costs
  a 600-byte copy on every request for no reader, and the ticker also
  proves the loop is responsive when no events arrive, which `/healthz`
  needs.
- Requests are counted by API key (label `api_key`), not "requests and
  errors": most errors are per partition inside a response, and a
  top-level error count would mean a different thing for each API.
  Connections dropped for bad input are counted instead.
- `samsa check` runs the node's own recovery code (`broker.Check`: Raft
  start, snapshot decode, every entry read and replayed) on a disk wrapper
  that records writes instead of making them. A write recovery would make
  (cutting a torn tail, rebuilding an index, promoting an install) is
  printed as "repair needed"; an error is printed as "corrupt". No second
  reader of the formats exists to drift from the first.
- There was no lock file. `realsys.OpenDisk` now takes an exclusive
  `flock` on `LOCK` in the data directory, so a second node on the same
  directory refuses to start, and so does `check` while a node runs.

## 6. What checks it

- **Wire**: table tests per API and version against bytes built from the
  schema; fuzzing for every new decoder.
- **Broker**: each error case above; DeleteTopics then CreateTopics under
  the same name finds no records, no committed offset, no producer state.
- **Simulator**: the workload gains DeleteTopics, CreatePartitions and
  DeleteRecords. The model learns them: after a delete, no read returns
  the old topic's records (new invariant **D1**: a fetch sent after the
  delete was acknowledged finds no topic); after DeleteRecords at `o`, no read returns a record
  below `o` and ListOffsets earliest is at least `o`, across trims and
  snapshots; new partitions start empty. A mutation for each: delete
  without the offsets (`UnsafeKeepOffsetsOnDelete`), trim resetting the
  start (`UnsafeTrimResetsStart`). As built, the simulator does **not**
  recreate a deleted topic: once the name is reused, a late answer about
  the old topic and an answer about the new one cannot be told apart by
  name (ADR 0016), so the model could only judge them by guessing which
  command was applied first. Recreation is covered by the broker test and
  `TestKafkaCLI`. `UnsafeKeepOffsetsOnDelete` is caught by the broker test
  only, for the same reason: without a recreate a kept offset is never
  read.
- **Integration**: every command in the table of section 1 run with the
  real Kafka CLI tools against a three-node cluster, output checked
  (`TestKafkaCLI`, skipped when the distribution is absent, as the Java
  tests skip without a JDK). `samsa check` on a clean directory passes, and
  on one with a byte flipped in a segment, a truncated snapshot and a
  corrupt hard state, each is reported.

## 7. Alternatives rejected

- *Topic IDs now.* Needs Metadata v10+, Fetch v13+ and an ID in every
  command that names a topic; the one-command delete already gives what
  IDs protect against.
- *AlterConfigs.* Would make configuration state of the log (per-topic
  retention). Not asked for; DescribeConfigs says read-only.
- *A Prometheus client library.* A dependency for a text format of a few
  lines.
- *Metrics read straight from the node by the HTTP goroutine.* A data race;
  the race detector would rightly fail it.
- *YAML or TOML config.* No standard-library parser; `key = value` covers
  flags, which are all scalars.

## 8. Review

Second pass, looking for what breaks:

1. **A delete racing a produce**, with the produce proposed first and
   applied second: the append finds no topic. It must not create one, nor
   leave a batch reference to nowhere. Apply returns the error and changes
   nothing.
2. **Delete, then recreate, with a consumer group still committing.** A
   commit proposed before the delete sits earlier in the log, so it is
   applied first and the delete removes it. One proposed after the delete
   and before the recreate is refused at apply (unknown topic), as now.
   One sent after the recreate is the client's: it names the new topic.
   Across a leader change the order still holds, because the log is the
   order. Test: delete and recreate with more partitions while a group
   commits; no offset of the old topic is ever fetched.
3. **DeleteRecords and retention** both move `start`; only up. Trim's
   reset is the bug the mutation plants.
4. **DeleteRecords inside a snapshot**: `start` is part of the partition
   in the snapshot encoding already (retention uses it). Snapshot
   round-trip test covers a mid-batch start.
5. **Idempotent producer after a recreate**: its next sequence for the
   partition is not 0, and Samsa requires 0 for an unknown producer (S6).
   The Java client resets on `UNKNOWN_PRODUCER_ID`-like errors; Samsa
   answers `OUT_OF_ORDER_SEQUENCE_NUMBER`. To be measured with the Java
   client in the integration test; recorded either way.
6. **ListGroups during an election** lists nothing for a moment: no node
   serves. Same as Kafka during coordinator moves.
7. **Metrics copy cost.** Copying counters after every event is cheap only
   if they are few and flat: a fixed array per API key, not maps.
8. **`check` on a live directory** could report a half-written segment as
   corrupt. The lock file stops it; `-force` is not offered.

**Found while building** (after the review):

9. *DescribeConfigs for a broker goes to that broker*, so it is answered by
   every node (section 4.1 changed).
10. *The checker judged DeleteRecords answers against state at answer
    time.* An answer reflects the log when the command was applied, which
    may be before a later trim or delete moved the floor. Fixed by
    capturing the floor and start when the request is sent (BUGS.md M9).
