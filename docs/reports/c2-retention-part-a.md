# C2 report, part (a): per-topic retention

Branch `retention-a`, 2026-11-14. Production plan item 12, part (a).
Design: `docs/design/c2-retention-compaction.md` (section 3 and "As
built"). ADR 0026 (copy forward), ADR 0027 (topic configs).

## What was built

- **Topic configs as state** (format level 2, state snapshot version 8):
  - `retention.ms`, `retention.bytes` (per partition), and
    `cleanup.policy` (only `delete`).
  - Set by CreateTopics configs and by IncrementalAlterConfigs (API 44,
    versions 0–1), which followers forward to the leader.
  - Reported by DescribeConfigs with source DYNAMIC_TOPIC_CONFIG.
- **Broker defaults:**
  - `-retention-time` and `-retention-bytes-per-partition` apply to topics
    that set nothing. Both default to keeping everything.
  - `-retention-slack` defaults to 2.
  - `-retention-bytes` is now a safety limit on the whole log.
- **Expiry** (`broker/retain.go`): every second, and after apply, the
  leader proposes a DeleteRecords for every partition whose oldest
  batches its topic no longer keeps.
- **The cleaner:**
  - It starts once the log from its start exceeds the slack times the
    bytes still referenced.
  - It proposes `cmdRelocate` for each batch still referenced in the
    head segment: a byte-for-byte copy at the tail. Applying it moves
    the reference only if it still points where the leader read it.
  - Once nothing in the head is referenced, it proposes the existing
    `Trim`.
- **Metrics:** `samsa_relocated_bytes_total` and
  `samsa_forced_retention_bytes_total`.
- **Simulator:**
  - In a quarter of the runs each, a default retention of 1 s or 3 s,
    with `t0` kept forever.
  - **R9:** a record leaves the log only once its topic's retention
    allows.
  - The mutation `UnsafeTrimLive`.
  - A coverage counter, "bytes relocated", that must be non-zero over a
    run of seeds.
- **Tests:**
  - Unit tests for the commands, the snapshot round trip, the handler
    (all-or-nothing per resource), forwarding, expiry by time and by
    size, and the cleaner with a restart.
  - `TestKafkaCLI` runs `kafka-configs.sh --alter`, `--delete-config`
    and `kafka-topics.sh --create --config` on a three-node cluster.
  - A benchmark, `TestBenchRetention`.

## Results

**Simulator:** 2,000 seeds pass in 78 s on the 10-core laptop, and the
cleaner relocated 4.9 MB over 1,000 of them ("bytes relocated", now a
required coverage counter). On the way there, the simulator found the
bugs below: S10, S11 and M13.

**Mutations** (200 seeds each, the first catch's invariant):

| Mutation | Caught by | First catch |
|---|---|---|
| trim the head segment without copying its live batches (new) | 39 of 200 | R9, seed 6: `t0` start moved past a record kept forever |
| delete segments before a snapshot covers them | 55 of 200 (50 in A3) | recovery failed, seed 1 |
| let retention move a partition's start back | 8 of 200 (2 in A3) | R1, seed 2 |

The last one rose: the cleaner trims more often, and in more varied
positions, than the global limit alone.

**Cleaner, unit scale** (`TestCleanerCopiesForward`, 4 KiB segments,
90% of records kept 10 s, 10% kept forever): the log stays at 7,988
bytes for 4,120 live. The cleaner copied 67 KB for about 40 KB of client
records. That is an amplification of about 2.6, far above the model's
1.2 at slack 2, because here one segment is as large as all the live
data. The budget is then `2 × max(live, segment)`, so every long-kept
batch is copied about once per segment written.

**Benchmark** (`TestBenchRetention`): a single node, `-sync always`, 1 KiB
records without compression, 64 MiB segments, one minute per run. The
same load runs with both topics kept forever ("baseline"), then with 90%
of records kept 10 s ("mixed"). There are two repetitions.

| Run | Throughput, rec/s | MB/s | Copied by the cleaner | Amplification | Log on disk |
|---|---|---|---|---|---|
| baseline 1 | 160,769 | 164.6 | 0 | 1.00 | 9,984 MB |
| mixed 1 | 144,992 | 148.5 | 768 MB of 8,919 MB | 1.09 | 3,908 MB |
| baseline 2 | 156,855 | 160.6 | 0 | 1.00 | 9,744 MB |
| mixed 2 | 147,618 | 151.2 | 711 MB of 9,081 MB | 1.08 | 4,215 MB |

- **Throughput:** copying forward costs 6 to 10% at this load.
- **Space:** the log stays within its budget. About 2.2 GB is live: the
  10% kept forever, plus 10 s of the rest. The budget is
  2 × live + one segment, about 4.5 GB.
- **Against the model:** it predicted 1.20 at slack 2 for a similar
  90/10 mix. The measured 1.08 is lower, because here the long-kept data
  grows all run long, so the budget grows with it.
- **The method is weak:** two repetitions of one minute each, on one
  laptop.
- **An earlier version kept one cleaning round a second.** It reached
  8.8 GB against a budget of about 4.9 GB, copying only 85 MB. Rounds now
  follow each other while there is work.

## Problems met

- **BUGS.md S10 (an A1 bug, exposed by C2):** the log could be compacted
  below the newest snapshot's log start. A crash before the next
  snapshot then restarted from one whose batches were gone. Found by
  seeds 363, 371 and 710.
- **BUGS.md S11:** the cleaner could copy live data around for ever. Live
  bytes counted batches but not their log framing, so a log of only live
  data still looked over budget. A 1,000-seed run hung.
- **BUGS.md M13:** I6 hashed the state while relocations were in flight.
- **BUGS.md C2a:** steady expiry starved the cleaner, and an idle
  partition never expired. Both were found by the unit test, and both
  are fixed.
- **BUGS.md C2:** `kafka-configs.sh --alter` timed out against a
  cluster. Kafka's admin client sends the request to any broker.
  Followers now forward it.
- **BUGS.md M12:** I6 blamed the broker for the cleaner running after the
  final restart (the M8 pattern again). The check now turns the cleaner
  off.
- **The design's review was wrong about the stale-relocation check.** It
  said an expired batch would come back without it. It cannot. The check
  matters only when a topic is deleted and created again under the same
  name. The design records the correction, and the mutation was replaced
  by `UnsafeTrimLive`.

## How to run it

```sh
go test ./internal/broker -run 'Retention|Cleaner|AlterConfigs|Relocate|TopicConfig' -v
go test ./internal/sim -run 'TestMutationSuite/trim_the_head' -v
make sim SEEDS=1000
cd integration && SAMSA_BENCH_RETENTION=1 SAMSA_BIN=../bin/samsa go test -run TestBenchRetention -v .
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name t --add-config retention.ms=3600000
```

## What is untested or weak

- **R10** (the log stays within its budget) is a unit test, not a
  simulator invariant.
- **R9 covers only time-based retention,** in runs without DeleteRecords
  or the safety limit. `retention.bytes` has unit tests only.
- **Amplification is high when segments are large relative to live
  data,** as in the unit test. The model assumed a segment much smaller
  than the live data.
- **The cleaner reads the head segment on the leader** through the Raft
  log, one batch at a time, up to 4 MiB a round. Throughput with a
  multi-gigabyte head is unmeasured.
- **A batch larger than the current `-max-batch-bytes`** cannot be copied
  (the flag was lowered after it was written). The cleaner then retries
  that head segment every second and never frees it. Only the safety
  limit cuts it.
- **Batches without timestamps** (-1) never expire by time and hold back
  those behind them (a `shortcut:` in the code).
- **`-retention-time` defaults to forever,** not Kafka's 7 days (ADR 0027).
- **Compaction** (`cleanup.policy=compact`) is part (b).

## Ten questions

1. **Why copy data forward instead of giving each topic its own log?**
   One log keeps one order and one state machine, so retention is
   commands the simulator already checks. Per-topic logs need a second
   replication protocol (ADR 0026).
2. **Why does expiry reuse DeleteRecords?** It already moves a
   partition's start and drops whole batches below it. The leader picks
   the offset from its clock; apply reads no clock (R1).
3. **What stops a relocation from resurrecting data?** It can only edit
   a reference that exists, and only if that reference still points at
   the index the leader copied from. After a topic is deleted and
   recreated, the same offset names different data at a different
   index, so a stale copy does nothing.
4. **Why may expiry and cleaning run at the same time?** An expiry only
   removes references; a relocation of a removed reference is a no-op.
   Sharing one in-flight counter starved the cleaner (C2a).
5. **Why does a relocation check the record count?** A copy that spans
   other offsets than the batch it replaces would shift every offset
   after it. The leader never sends one, but apply refuses to trust it.
6. **What does `-retention-bytes` mean now?** It is a safety limit on the
   whole log. Past it, if copying cannot bring the log under, the oldest
   data goes, live or not, and the bytes are counted.
7. **Why is the format level raised to 2?** An old node would stop on
   `cmdTopicConfigs` or `cmdRelocate`, and snapshot version 8 has fields
   it cannot read. Below level 2, configs are refused and the old global
   trim runs.
8. **Why do followers forward IncrementalAlterConfigs?** Kafka's admin
   client sends it to any broker. A NOT_CONTROLLER answer made the
   client retry, against followers, until it timed out.
9. **How does R9 know when a record may expire?** The simulator's
   producers stamp records with the simulated clock, which is every
   node's clock, and the model notes each record's send time. A start
   may pass a record only once that time plus the retention has passed.
10. **What makes write amplification high, and how is it bounded?**
    Long-kept data in the head segment is copied each time the head is
    cleaned. Slack bounds how often: the log must grow to slack times
    the live data before cleaning, so each copy is paid for by that much
    new writing. It is worst when one segment is as large as the live
    data.
