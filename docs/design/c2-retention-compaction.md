# C2: per-topic retention and key compaction

Production plan item 12. Must show: an ADR with measurements first; the
invariant "the latest value of every key survives".

## 1. The problem

Today, retention is one size bound on the whole Raft log
(`-retention-bytes`, ARCHITECTURE section 1). Every topic loses its oldest
data at the same pace, whatever it holds. Kafka users expect three things
that this cannot give:

- **`retention.ms` per topic:** keep `clicks` one day and `orders` a year.
- **`retention.bytes` per partition.**
- **`cleanup.policy=compact`:** keep the newest record of every key,
  forever, as a table. Kafka's own consumer offsets and Connect configs
  work this way, and so do user tables such as "the latest address of
  each customer".

The shared log makes this hard. Raft can drop only a prefix of its log
(`TruncatePrefix`). A long-lived record near the head pins the head, and
everything after it, however short-lived.

## 2. The choice: copy live data forward, or give topics their own logs

**Copy forward (a log-structured cleaner).** When the log is over its
budget, the leader looks at the head segment. For each batch still
referenced by a partition, it proposes a command carrying the batch's
bytes again. Applying it points the partition at the new copy, with the
same offsets. Once nothing references the head segment, a Trim drops it,
as today. Kafka's offsets never change; only where the bytes live does.

**Per-topic logs.** Topic data leaves the Raft log. Each partition gets its
own segment files, with Kafka-style replication beside Raft. It is the
same shape as Kafka, and closer to sharding (item 14).

### Measurements

The cost of copying forward is write amplification: bytes written by the
cleaner, on top of the clients' own, and replicated like them. It depends
on the workload, and on the *slack*: how many times the live data the log
may grow to. A model in `internal/broker/retention_model_test.go`
measures it. It works in unit records and ticks, and cleans head segments
whenever the log is over slack times the live data.

```sh
go test ./internal/broker -run TestRetentionModel -v -retention-model
```

One retention unit is 100 ticks. Each workload runs for six of its
longest retentions, and the second half is measured.

| Workload | Slack | Write amplification | Space over live |
|---|---|---|---|
| one retention for every topic (today) | 1.5 | 1.00 | 1.49 |
| | 2.0 | 1.00 | 1.99 |
| 90% kept 1 unit, 10% kept 7 | 1.5 | 1.30 | 1.48 |
| | 2.0 | 1.20 | 1.97 |
| | 4.0 | 1.10 | 3.97 |
| 50% kept 1 unit, 50% kept 7 | 1.5 | 1.50 | 1.49 |
| | 2.0 | 1.00 | 1.99 |
| 99% kept 1 unit, 1% kept 30 | 1.5 | 1.18 | 1.47 |
| | 2.0 | 1.13 | 1.97 |
| compacted only, 10k keys | 1.5 | 1.73 | 1.47 |
| | 2.0 | 1.26 | 1.96 |
| | 4.0 | 1.01 | 3.84 |
| 90% kept 1 unit, 10% compacted (100k keys) | 1.5 | 1.21 | 1.47 |
| | 2.0 | 1.12 | 1.97 |

What the numbers say:

- **At slack 2, copying costs 0 to 26% more writes** in every workload
  measured. That is the price of keeping one log. Kafka's own compaction
  rewrites segments too: its cleaner reads and writes every dirty segment.
- **The cost falls fast with slack.** When the budget lasts longer than
  the longest retention, nothing live reaches the head and nothing is
  copied (the 50/50 workload at slack 2).
- **The worst case is a log that is all compacted table, at low slack:**
  1.73 at slack 1.5. Space buys it back: 1.26 at slack 2.

The model leaves out two things. First, a copy's replication cost on the
network: it is the same factor, since copies are replicated like any
write. Second, the leader's reads of the head segment: they are
sequential, and usually still in the page cache when the log is small.

### Decision: copy forward (ADR 0026)

- **It keeps one log, one order and one state machine.** Every rule the
  simulator checks still covers retention, because retention is commands
  in that log. Per-topic logs would need a second replication protocol
  beside Raft, with its own failure modes and its own proof.
- **The cost is bounded and tunable:** at most a quarter more writes at
  the default slack of 2 in the workloads measured.
- **It does not block sharding.** Item 14 gives each Raft group its own
  log, and the cleaner works per log.

## 3. Design, part (a): per-topic retention

### Configuration

- **Topic configs** `retention.ms`, `retention.bytes` (per partition) and
  `cleanup.policy` live in the state machine. They are set by CreateTopics
  configs and by IncrementalAlterConfigs (API 44), and reported by
  DescribeConfigs. Wire versions are checked against Kafka's schemas
  before coding (rule 5) and recorded in the ADR.
- **Broker defaults** apply to topics that set nothing: `-retention-time`
  (default 7 days, as Kafka's `log.retention.hours=168`) and
  `-retention-bytes-per-partition` (default unlimited).
- **`-retention-bytes` keeps its meaning,** a bound on the whole log on
  disk. It becomes a safety limit, and it is no longer the way to set
  retention. When live data alone exceeds it, the oldest live data goes,
  as today, and a metric counts the bytes dropped that way. The soak's
  setting keeps working.

### Expiry: logical, in commands

A command `cmdExpire(topic, partition, start)` moves a partition's start,
as DeleteRecords does. The leader proposes it:

- for `retention.ms`: the first batch whose max timestamp is older than
  now minus the retention, now read by the leader and carried in the
  command (rule R1: apply reads no clock);
- for `retention.bytes`: the first batch that keeps the partition within
  its bound.

Expiry never frees disk by itself. It only stops referencing batches.

### Reclaiming disk: the cleaner

The leader keeps a running sum of the bytes all partitions reference
(state, maintained at apply). When the log on disk exceeds slack times
that sum (`-retention-slack`, default 2), or exceeds `-retention-bytes`,
it cleans:

1. It takes the head segment's index range. For each batch in it still
   referenced, it proposes `cmdRelocate(topic, partition, base, oldIndex,
   bytes)`. The bytes are read from its own log and must be a
   byte-for-byte copy.
2. Applying it: if the partition still references that batch at
   `oldIndex`, it now references the new entry. Otherwise the batch
   expired or was deleted meanwhile, and the command does nothing.
3. Once no reference falls below the segment's end, the leader proposes
   `Trim`, as today.

Copies are committed like client writes. A crash in the middle leaves
either the old reference, or the new one, never neither. A follower
applies the same commands and reaches the same references.

### Invariants (simulator)

- **R8:** a fetch at an offset at or above the partition's start returns
  the bytes the producer wrote at that offset, whatever relocations
  happened (the history model already checks bytes).
- **R9:** a record is gone only after its topic's retention says so, or
  after the global safety limit, which is reported.
- **R10:** the log on disk stays within slack times live, plus one
  segment, once the cleaner has caught up.

The mutation `UnsafeRelocateStale` applies a relocation without checking
that the reference is unchanged. It must be caught: an expired batch
would come back.

### As built (part a)

What changed from the plan above, and why:

- **Expiry reuses DeleteRecords** instead of a new `cmdExpire`. The
  leader computes the offset; the command carries it. Apply still reads
  no clock. One fewer command to version.
- **The live-bytes sum is computed, not kept.** The leader walks every
  batch reference once per look (`retainEvery`, one second), which also
  computes expiry. Keeping a running sum at apply would touch four
  commands and the snapshot for a number only the leader needs.
- **`-retention-time` defaults to forever,** not 7 days (ADR 0027): an
  upgrade must not start deleting what the old build kept.
- **Expiry and cleaning run on a timer** as well as after apply. Before
  that, an idle partition never expired, because nothing was applied.
- **Expiry and the cleaner each wait only for their own proposals.** One
  shared counter starved the cleaner whenever data expired every second
  (BUGS.md C2a).
- **Sizes are bytes on disk:** the log entry with its framing, not the
  batch alone. Otherwise a log of live data only looks over budget
  (BUGS.md S11).
- **The cleaner starts above slack × max(live, segment) plus one
  segment.** Either trigger then means more than a segment of the log is
  dead, so every round frees something and cleaning ends.
- **Rounds follow each other while there is work:** at most 4 MiB of
  copies in flight, and the next round starts as soon as they apply. At
  one round a second the cleaner fell far behind a 150 MB/s log.
- **The safety limit acts at once,** as before C2. It drops live data only
  when live data plus one segment exceeds it, and the dropped bytes are
  counted in the metric `ForcedRetentionBytes`.
- **A relocation also checks its record count** against the offsets the
  batch it replaces spans, so a malformed copy stops the node instead of
  shifting offsets.
- **Snapshots older than version 8 have no batch sizes.** At start, the
  node reads each referenced batch once to fill them in.
- **Below format level 2,** nothing changes: the old global trim runs,
  and topic configs are refused.

Invariants in the simulator:

- **R8** is the history model's existing byte check. It now also runs
  over relocated batches.
- **R9** is new (`Model.Retention`). In runs where only topic retention
  moves a start, each move of a log start is checked against the records
  below it: each must be older than its topic's `retention.ms`. `t0`
  keeps everything (`retention.ms=-1`), so the cleaner has live data to
  copy.
- **R10 is a unit test only** (`TestCleanerCopiesForward`): log at most
  slack times `max(live, segment)` plus one segment. A simulator check
  needs the cleaner to have caught up, which no point in a faulty run
  guarantees.

The mutation is `UnsafeTrimLive`: trim the head without copying. R9
catches it, in 42 of 200 seeds. The planned `UnsafeRelocateStale` does
not work as a mutation; see review question 1.

## 4. Design, part (b): compaction

**Superseded by `docs/design/c2b-compaction.md`**, written after part (a)
merged. The outline below is kept as first written; two things changed:
the key map is not in the state machine, and a compressed batch cannot
be put in a compacted topic.

- A compacted partition's live records are the newest of each key. A
  tombstone (null value) is live for `delete.retention.ms`.
- The cleaner relocates only live records. A batch with dead records is
  rewritten: same offsets, fewer records, a new CRC. Kafka does the same,
  which is why consumers already accept offset gaps in compacted topics.
- The state machine keeps, per compacted partition, a map from key hash
  to offset. It is rebuilt from the log at start, and it is in snapshots.
- **The invariant, from the plan: the latest value of every key
  survives.** The history model keeps the newest acknowledged record of
  each key and checks that a read from the start returns it.

## 5. Alternatives

- **Per-topic logs.** Rejected above: a second replication protocol.
- **Copy at segment granularity** (copy the whole head segment if
  anything in it is live). It is simpler, but it copies dead data too,
  which raises the amplification. Batches are already the unit
  references point at.
- **Cleaning the least-live segment anywhere in the log** (the classic
  log-structured file system policy). Raft can only drop a prefix, so
  only the head can be freed.
- **No slack bound, clean only at `-retention-bytes`.** It makes the
  space unpredictable for a workload whose live data is small.

## 6. Second review

1. *Can a relocation lose data?* Only if it applies after the batch it
   copies was dropped and then resurrects it. The check that the
   partition still references the old index prevents that, and the
   mutation shows the simulator would notice.
   **Corrected while building:** that answer is wrong. A relocation only
   edits a reference that still exists, so an expired batch can never
   come back. Without the index check, a stale copy would point a live
   reference at another copy of the same bytes, which is harmless.
   Only one case does harm: the topic is deleted and created again under
   the same name, and offset 0 then names different data. The check is
   kept for that case. The mutation that tests the cleaner is
   `UnsafeTrimLive`.
2. *Can a relocation race a DeleteRecords that lands inside the batch?*
   The start can sit inside a batch today. Relocation keeps the batch
   whole and leaves the start alone.
3. *Is the leader's read of the head segment trustworthy?* It reads its
   own committed entries, checksummed by storage. A follower that becomes
   leader starts from its own log.
4. *Does write amplification hurt clients?* Copies compete with
   producers for the leader's write bandwidth. At slack 2 the model says
   at most 26% more in the workloads measured; the benchmark will measure
   throughput with a mixed workload before part (a) merges.
5. *Time:* `retention.ms` uses batch max timestamps, as Kafka does, and
   the leader's clock. A producer with a wrong clock can make its data
   expire early or late, also as in Kafka.
6. *Snapshots:* the topic configs and the live-bytes sum are state, so
   they are in the snapshot. That raises the state snapshot version and
   the cluster format level (ADR 0024).
