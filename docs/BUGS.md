# Bugs found by simulation

One entry per bug: the seed, the violated invariant, the root cause, the
fix. Bugs in the simulator or the tests themselves are listed separately at
the end, because a model that is wrong in either direction matters too.

## Bugs in the system

Found by the simulator (`make sim SEED=<seed>` replays each on the commit
before its fix).

### S1. A dying node sent a fetch response it could not vouch for (Phase 3)

- **Seeds:** 4, 7 and 8 of the first 8, with the fix removed
  (`make sim SEEDS=8`).
- **Invariant:** I2, "fetch at 35 returned nothing although the high
  watermark is 52".
- **Root cause:** the disk lost power while a fetch was reading its batches
  from the command log. The read failed, the node recorded its fatal error,
  and then still sent the response it had half built: a correct high
  watermark with no records. The node stopped one instruction later, but
  the bytes were already on the wire.
- **Impact:** small on its own (a consumer sees an empty response and
  retries), but the rule it broke is not: after a fatal error nothing the
  node computes can be trusted, so nothing may be sent.
- **Fix:** `Node.respond` sends nothing once the fatal error is set.
- **Lesson:** "stop on fatal error" has to cover output already in
  progress, not only the next request. No unit test had a disk failing in
  the middle of a read.

### S2. A leader would not re-send entries below what a follower had acknowledged (Phase 5)

- **Found by:** `TestSimCatchesAckBeforeSync` on three nodes: runs that
  never finished. The stack showed a leader and a follower exchanging
  AppendEntries and refusals with no end.
- **Root cause:** on a refusal the leader stepped `nextIndex` back, but
  never below `matchIndex + 1` (`onAppendResponse`). A follower that had
  acknowledged entries and then lost them in a power cut refuses
  everything from there on, so the leader asked the same question for
  ever, and the follower could never be repaired.
- **When it matters:** the follower has to forget acknowledged entries.
  With the deliberate bug that is the point; with `-sync=interval` or
  `never` it is what a power cut does (ADR 0012); with `always` it means
  the disk lied.
- **Fix:** the clamp is gone. `matchIndex` still never decreases; only
  what is sent does. `TestFollowerThatLostAcknowledgedEntriesIsRepaired`.
- **Lesson:** "matchIndex never moves backwards" (roadmap bug 5) is about
  what the leader *believes*, not about what it is willing to send again.

### S3. A "you are superseded" answer could be taken for a refusal (Phase 5)

- **Seed:** 193 of `TestRandomSchedules` in `internal/raft`, once S2's
  clamp was removed: "read raft log at -1".
- **Root cause:** a node that receives AppendEntries from an older term
  answers so that the sender learns the new term. The answer had the same
  shape as a refusal. If the sender had meanwhile become leader of that
  very term, and a delayed copy of its old request was answered this way,
  it handled the answer as a refusal of a request it had not sent, and
  stepped `nextIndex` back to -1. Before S2's fix the clamp hid this.
- **Fix:** the superseded answer carries echo -1, which no refusal has,
  and the leader accepts a refusal only if it names an index before the
  one refused. The fuzz corpus of `Step` has both shapes.
- **Lesson:** one message type with two meanings is a bug waiting for the
  right reordering. A guard that makes a value "safe" (the clamp) can
  hide that the value was wrong.

### S4. An isolated leader answered reads from its old state (Phase 6)

- **Seeds:** 2, 7, 40, 57, 62 of the first 200 once partitions existed.
- **Invariants:** I3 ("high watermark 393 is below 395, which was
  acknowledged or seen before the request") and G3 (an offset fetch
  returned a commit that a later acknowledged one had replaced).
- **Root cause:** a leader cut off from the others keeps believing it
  leads until it hears a higher term. Meanwhile the majority elects a new
  leader, which accepts writes. Clients can reach both. The old one went
  on serving fetches and offset fetches from its own applied state. ADR
  0010 had accepted this window as milliseconds; under a partition it
  lasts as long as the partition.
- **Fix:** a leader lease (ADR 0013): followers ignore candidates for an
  election timeout after hearing from a leader, and the leader serves
  only while a majority answered it within ¾ of that.
  `TestLeaderReadsNeedTheLease`; mutation `UnsafeNoLease`.
- **Lesson:** "Raft makes it safe" is true of the log, and only of the
  log. Anything answered from a node's local state needs its own
  argument.

### S5. An isolated leader refused valid writes from its old state (Phase 6)

- **Seed:** 937, found while checking that a mutation's catching seed
  passed without the mutation. It did not.
- **Invariant:** "OffsetCommit for t2/2 failed with code 3": unknown
  topic, for a topic whose creation had been acknowledged.
- **Root cause:** S4's fix covered requests that only read. Produce and
  OffsetCommit are writes, but they check that the partition exists
  against applied state *before* proposing, and that refusal is a read.
- **Fix:** every request that only a leader answers needs the lease.
- **Lesson:** classify by what the answer depends on, not by what the
  request is called. The mutation suite now reruns each catching seed
  without its mutation, so a real bug cannot pass for a caught mutation.

Phase 1's storage engine and the rest of Phase 2's broker passed 5000 seeds
with no other finding. See "what is untested or weak" in the phase reports
for how much that is worth.

### S6. A batch got in ahead of the one before it, and that one's retry was refused (A2)

- **Seeds:** 683 and 2706 of 5000, with the fix removed.
- **Invariant:** I8, "t0/0 refused producer 140737489928192 epoch 0
  sequence 0 (attempt 10) with code 45".
- **Root cause:** a producer pipelined sequence 0 and sequence 3 of one
  partition to node 1. Node 1 answered the first "not leader", became
  leader, and then read the second, which was already on the wire. It
  had no state for the producer on a partition other producers had
  written, and Kafka's rule for that case accepts any sequence, because
  the state may have expired. So sequence 3 was appended without 0, and
  every retry of 0 after it was out of order. The client's records were
  written out of order and the producer was stuck.
- **Fix:** Samsa keeps producer state for ever in part (a), so no state
  means the producer never wrote to the partition, and its first batch
  must be sequence 0 everywhere, not only on a partition never written
  (`partition.admit`). Part (b), which expires state, has to keep this
  for producers it has not expired.
- **Lesson:** the design copied Kafka's rule 4 together with the reason for
  it (expiry), which part (a) does not have. Review point 6 argued that
  pipelined batches stay in order, and it missed the node that refuses one
  request and accepts the next. I8 exists to check that argument.

### S7. Snapshot chunks multiplied until the run stalled (B3)

- **Seed:** 875 of the mutation suite, on branch `b3-raft-hardening`, with
  no mutation. Not an invariant failure: the run made no progress. Peer
  messages grew to hundreds of thousands in seconds of virtual time.
- **Root cause:** every InstallSnapshot answer made the leader send the
  next chunk. The heartbeat also resends the current chunk (to repair a
  lost one), and the network duplicates messages. Each resend and each
  copy therefore started a chain of chunks of its own, and chains never
  merged. A long install under duplication grew without bound. The bug
  dates from snapshot install (Phase 5); B3's new draws only moved seed
  875 onto a schedule that hit it.
- **Fix:** only an answer that shows new progress (an offset beyond the
  one recorded) sends the next chunk. A copy, a stale answer or a restart
  only records the offset, and the next heartbeat resends from there
  (`onInstallResponse`). `TestSnapshotChunksDoNotMultiply` duplicates
  every answer and fails without the fix.
- **Lesson:** no invariant covers liveness or cost, so the suite found this
  only as a hang. AppendEntries has the same shape (each answer may send
  more) but stops when the follower catches up.

### S8. A leader took its lease back while its TimeoutNow was still on the way (C8b)

- **Seed:** 1219, on branch `credentials`, after the new SCRAM and quota
  APIs moved every seed. Three nodes, 30% message drops, 10% duplicates.
- **Reported as:** S6, "node 2 became leader of term 13 while node 1 held
  a lease in term 12".
- **Root cause:** node 1 was told to stop, so it began handing leadership
  to node 2 and sent TimeoutNow (B3). Node 2 did not start an election
  within an election timeout, so node 1 abandoned the transfer and served
  again with a fresh lease, as the design said it should. Then the
  TimeoutNow, or a copy of it, reached node 2. Node 2 started a transfer
  election, which voters grant without the lease rule. Node 2 won while
  node 1 still held its lease, so two nodes could answer reads at once.
  The design's argument for skipping the lease rule ("the only leader it
  could depose asked for it, and gave up its lease first") forgot that
  the leader takes the lease back after abandoning a transfer, and that a
  message can arrive any time later.
- **Fix:** a leader that abandons a transfer after sending TimeoutNow
  steps down instead of serving again. A late TimeoutNow then meets a
  term with no lease. A newer term ignores it, because it is stale. If
  TimeoutNow was never sent (the target never caught up), the leader still
  serves again. `TestTimeoutNowAfterAbandon` delays a TimeoutNow past the
  abandonment. It shows two overlapping leases with the new mutation
  `MutAbandonKeepsLeading`, and none without it. The simulator's mutation
  suite does not catch the switch reliably: one seed in 2000 does, 1219
  itself, so the unit test is the guard (`sim_test.go`, the comment above
  `mutations`).
- **Lesson:** an argument about which messages can arrive must include
  late and duplicate copies of the node's own earlier messages. "The only
  leader it could depose asked for it" was true when the request was sent,
  not when it arrived.

### S9. A node left alone by a change led the moment it restarted (C1)

- **Seed:** 2287, on branch `membership`, before the code was merged.
  Three nodes and two spares, 30% message drops, graceful stops.
- **Reported as:** S6, "node 2 became leader of term 8 while node 3 held
  a lease in term 7".
- **Root cause:** leader 3 removed itself. The configuration {2} reached
  node 2, and node 3 went on leading (thesis 4.2.2), counting node 2's
  acknowledgements for its lease. Then node 2 was stopped and restarted.
  `Start` let a node that is the only voter campaign at once, a shortcut
  for single-node clusters. Node 2 was alone in {2}, so it led term 8
  immediately. That broke its promise to ignore candidates for an election
  timeout after acknowledging node 3. The shortcut assumed nobody else
  could ever have led its configuration, which is true only of the
  bootstrap one.
- **Fix:** only a node alone in its bootstrap configuration campaigns at
  start. One left alone by a change waits for its election timer, like any
  voter. A snapshot keeps the bootstrap configuration implicit, so a
  single-node cluster still leads at once after a restart.
  `TestAloneByChangeWaits` fails without the fix.
- **Lesson:** a shortcut justified by "nobody else can exist" must be
  checked again when something makes others possible. Membership changes
  made a cluster of one a state the cluster can pass through, not only
  start in.

### S10. The log was cut below the newest snapshot's start (A1, exposed by C2)

- **Seeds:** 371, 363 and 710 (`make sim SEED=371`), single nodes with
  `-sync never`, power losses and sync failures.
- **Invariant:** "node 1 stopped with an error no fault explains: read
  committed batch at log index 40: storage: offset out of range: 40 not
  in [47, 278]".
- **Root cause:** after applying entries, the node compacts the Raft log
  to the state's log start, then saves a snapshot if a Trim is pending.
  Compaction was bounded by the newest snapshot's *index*, not by that
  snapshot's *log start*. So:
  1. Trim(47) applies; the state's start is 47.
  2. Compaction deletes the segments below 47. The newest snapshot on
     disk, at index 253, has log start 35 and references a batch at 40.
  3. The snapshot that would cover the Trim fails on a sync fault, and
     the node restarts from snapshot 253.
  4. Batch 40 is gone.
  The order has existed since A1. The cleaner's frequent trims made it
  likely.
- **Fix:** the node remembers the log start of the newest snapshot it
  saved or loaded, and never compacts past it (`compactTo`).
  `UnsafeTrimEarly` keeps the old bound, and its mutation is still
  caught.

### S11. The cleaner copied live data around without end (C2, before merge)

- **Seeds:** a run of 1,000 seeds did not finish in 75 minutes; one of
  3 minutes timed out with every stuck seed inside `clean`, called from
  `retain` on each apply.
- **Invariant:** none fired. Simulated time stopped advancing.
- **Root cause:** the live bytes counted batch bytes only. The log on
  disk also holds a 29-byte frame and entry header and the command header
  per batch, which is a third of a 100-byte batch. Once all dead data was
  gone, the log still looked over budget. Over the safety limit,
  `retain` skips its one-second wait, so on a single node every apply
  started another round in the same instant.
- **Fix:** a batch's size is now the bytes its log entry takes on disk
  (`raft.EntryBytes`). The cleaner starts only above slack ×
  max(live, segment) plus one segment. Either trigger then means more
  than a segment of the log is dead, and every round frees some of it.
  2,000 seeds finish in 73 s.
- **Broken commit in history:** 8eeb220 on the `retention-a` branch
  carries this bug, and `TestSimCatchesMissingFencing` hangs on it. The
  fix is 088852e. The branch was already pushed, and history is never
  rewritten, so the rule that every commit passes its tests is broken
  for that one commit. Bisect past it.

## Bugs found by real clients

### C1. Null record set in a fetch response broke librdkafka (Phase 2)

- **Found by:** the kcat integration test, which hung: kcat never reached
  end of partition.
- **Symptom:** librdkafka logged `Protocol parse failure for Fetch v11 ...
  invalid MessageSetSize -1` on every fetch that had nothing to return, and
  retried every 500 ms forever.
- **Root cause:** `FetchResponse.Records` is declared nullable in Kafka's
  message definition, so an empty result was encoded as null (length -1).
  The Java client and franz-go accept that. librdkafka does not. Kafka's own
  broker sends an empty set (length 0).
- **Fix:** the encoder always writes a length of at least 0 for records.
- **Lesson:** the schema says what is legal, not what every client
  tolerates. This is why the acceptance test uses three independent client
  implementations, and why the plan says to match the reference broker's
  observable behaviour, not only its specification.

### C2. kafka-configs.sh --alter timed out against a cluster (C2, before merge)

- **Test:** `TestKafkaCLI` (Kafka 4.4.0), `kafka-configs.sh --alter
  --entity-type topics`: "Timed out waiting for a node assignment. Call:
  incrementalAlterConfigs", after a request a second for 90 seconds.
- **Root cause:** Kafka's admin client sends IncrementalAlterConfigs for a
  topic to any broker, not to the controller. Samsa's followers answered
  NOT_CONTROLLER. That error is retriable, but the client's retry went to
  another follower again, as with CreateAcls before C8b.
- **Fix:** a follower forwards the command to the leader, as it does ACL
  changes (`forward.go`). It is idempotent, so a retried forward that
  applies twice is harmless. The leader accepts a forwarded topic config
  command only at format level 2. `TestAlterConfigsForwarded` covers it.

### C3. SASL/SCRAM failed against librdkafka 2.3.0 on Ubuntu 24.04 (C18a, before merge)

- **Test:** `TestACLsOtherClients/kcat` and `TestTLSAndSASLOtherClients/kcat`
  on Ubuntu 24.04 (`librdkafka` 2.3.0).
- **Symptom:** first, `librdkafka` aborted immediately after `ApiVersions` v3
  with `Failed to initialize SASL authentication: SASL Handshake not supported
  by broker (required by mechanism SCRAM-SHA-256)`. Second, once `SaslHandshake`
  was sent, `SaslAuthenticate` failed with `wrong SCRAM proof`.
- **Root cause:** two bugs in older `librdkafka` releases:
  1. In `librdkafka` before 2.10 (`rdkafka_feature.c`),
     `RD_KAFKA_FEATURE_SASL_HANDSHAKE` is enabled only if the broker's
     advertised `SaslHandshake` range overlaps `[0, 0]`. Samsa advertised
     `1..1`, so `librdkafka` 2.3.0 cleared `RD_KAFKA_FEATURE_SASL_HANDSHAKE`
     even though it sends `SaslHandshake` version 1 once enabled.
  2. In `librdkafka` <= 2.6.0 (`rdkafka_sasl_scram.c`,
     `confluentinc/librdkafka#4895`), `client-final-message-without-proof`
     concatenated the client nonce a second time (`r=<cnonce><cnonce><snonce>`),
     which Apache Kafka worked around in `ScramSaslServer.java` (`KAFKA-17805`).
- **Fix:** `KeySaslHandshake` advertises `AdvertisedMin: 0` in `ApiVersions`
  while `Min: 1` continues to reject version 0 requests, and `scram.Server.Final`
  accepts both `r=<cnonce><snonce>` and `r=<cnonce><cnonce><snonce>`.

### C4. DescribeTopicPartitions null ELR lists caused NullPointerException in Kafka 4.4.0 CLI (C18, before merge)

- **Test:** `TestJavaKIP848ConsumerGroup` (`kafka-consumer-groups.sh --describe`).
- **Symptom:** `kafka-consumer-groups.sh` crashed with `NullPointerException: Cannot invoke "java.util.List.stream()" because the return value of "DescribeTopicPartitionsResponsePartition.eligibleLeaderReplicas()" is null`.
- **Root cause:** Although `DescribeTopicPartitionsResponse.json` marks `EligibleLeaderReplicas` and `LastKnownElr` as `nullableVersions: "0+"`, Kafka 4.4.0's `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo` calls `.stream()` on `eligibleLeaderReplicas()` without a null check.
- **Fix:** `DescribeTopicPartitionsResponse.Encode` writes empty arrays (`ArrayLen(true, 0)`) instead of null (`-1`) for `EligibleLeaderReplicas` and `LastKnownElr`.

## Bugs found by the soak

### L1. Raft kept one term per entry ever written (G5)

- **Found by:** the 24 h soak (`docs/soak/`), started 2026-11-14 00:53 IST
  on v0.5's code: three nodes under a steady producer and consumer, with
  retention holding the disk at about 269 MB.
- **Symptom:** resident memory grew about 7 MB an hour and never fell:
  27 MB after 1 h, 109 MB after 11 h. Disk stayed flat.
- **Root cause:** `Raft.terms` was a slice with the term of every entry
  since index 0. Compaction cut the log on disk but never the slice, so it
  grew 8 bytes per entry for as long as the node ran. Start and snapshot
  install also expanded a snapshot's runs back into one term per entry.
  It was a recorded shortcut in `snapshot.go` ("replace r.terms by runs
  when the log is cut"), never taken up.
- **Fix:** `termIndex` (`internal/raft/terms.go`). Terms below the log
  start are kept as runs, one per change of term; one term per entry is
  kept only from the entry before the log start (still needed as an
  InstallSnapshot's `prevTerm`). `Compact` trims it and copies the tail so
  the memory is returned; start and install expand only what the log
  still holds. Lookups work for any index, so callers did not change.
  `TestTermIndex` checks it against a plain slice and checks that trimmed
  memory is released. The random Raft schedules found a first draft's
  mistake (a snapshot's runs past the cut kept their terms), before it
  was committed.
- **Lesson:** the simulator runs for minutes of virtual time; a leak of
  8 bytes per entry is invisible there. Bounded memory needs either a run
  long enough to see it or a test that asserts the bound. Every
  `shortcut:` that says "grows with" is a soak finding waiting to happen.

## Bugs found by fuzzing

### F1. Negative batch length crashed the produce handler (Phase 2)

- **Found by:** `FuzzNodeInput` within six seconds; the input is kept in
  `internal/broker/testdata/fuzz/FuzzNodeInput/`.
- **Symptom:** panic, slice bounds out of range, in `Node.produce`.
- **Root cause:** a record batch whose length field is negative was
  correctly rejected by `wire.ParseBatch`, but `ParseBatch` still returned
  the size it had computed from that field, and the handler advanced by it
  before looking at the error code. A negative size became a negative slice
  index. One malformed produce request from any client would have killed
  the broker: exactly what architecture rule 4 forbids.
- **Fix:** the handler stops at the first rejected batch, and `ParseBatch`
  returns a zero `BatchInfo` with any error, so the mistake cannot be
  repeated by another caller.
- **Lesson:** the unit tests covered "corrupt batch" with a bit flip, which
  fails the checksum with a sane length. An error path that returns
  partially filled data is a trap.

### F2. A CreateTopics request lost its manual assignment when re-encoded (Phase 7)

- **Found by:** `FuzzDecodeMessage` on the macOS CI runner (PR #8).
- **Violated:** a decoded message must survive encode and decode unchanged.
- **Cause:** the decoder records that a topic carried a manual replica
  assignment (`HasAssignments`, so that the broker can refuse it), but the
  encoder always wrote an empty assignment list. Only the simulator's
  clients and the tests encode requests, so the broker was never affected;
  but a simulated client could not have sent such a request.
- **Fix:** the encoder writes one assignment when the flag is set. The
  round-trip test's sample request now carries one, and fails without the
  fix.

### R1. Snapshot chunks too large for the decoder: no install ever finished (A1 part b)

- **Found by:** `TestRandomSchedules` in `internal/raft`, seed 8, while
  InstallSnapshot was being written (never merged with the bug).
- **Violated:** liveness of the random schedule: a follower behind the
  leader's log start never caught up.
- **Cause:** the leader cut its snapshot into chunks of `MaxAppendBytes`,
  which may exceed the largest message the receiver's decoder accepts; every
  chunk was refused as malformed and resent.
- **Fix:** chunks are at most `MaxPayloadBytes` less the entry header.

### R2. A diverged follower refused both the snapshot and the entries (A1 part b)

- **Found by:** `TestRandomSchedules`, seed 1053, same stage.
- **Violated:** liveness: a livelock between leader and follower.
- **Cause:** the follower answered "not needed" whenever its log reached
  past the cut, even when its entry at C − 1 was of another term; the
  leader then sent entries from C, which the follower refused for the
  mismatch at C − 1, and the leader went back to the snapshot.
- **Fix:** InstallSnapshot carries the term at C − 1 (`prevTerm`), and the
  follower declines only if it holds that entry with that term.

## Bugs found by unit tests

### C2a. Steady expiry starved the retention cleaner (C2, before merge)

- **Test:** `TestCleanerCopiesForward`, which writes a short-kept topic
  and a long-kept one. The cleaner copied nothing: 62 KB of log for 4 KB
  live.
- **Invariant:** the log stays within slack times the live data (R10).
- **Root cause:** one counter of retention proposals in flight was shared
  by expiry and the cleaner. While data expired every second, an expiry
  was always in flight at the moment the cleaner looked, so it never ran.
- **Fix:** separate counters (`expiring`, `cleaning`). Each waits only
  for its own proposals, which is enough not to propose the same thing
  twice. Neither needs the other to wait: an expiry only removes
  references, and a relocation checks at apply that its reference is
  unchanged.
- **Also found:** an idle partition never expired. Retention ran only
  after apply, and with nothing written, nothing was applied. A
  one-second timer now runs it too.

### C2b-1. A snapshot with a first batch after the log start was refused (C2 part b)

- **Seeds:** 1, 4, 7, 13, 14 and 64 more of the first 300, as soon as a
  compacted topic existed. Reported as "recovery of node 2 failed: snapshot
  at 976: undecodable command in log: snapshot state ... malformed".
- **Root cause:** the snapshot reader required the first batch of a
  partition to begin at or before the partition's start, which was true
  while batches covered every offset. Compaction can remove the batches
  around the start.
- **Fix:** the reader allows a first batch after the start.

### C2b-2. Trim moved a partition's start past records compaction had removed (C2 part b)

- **Seeds:** 25, 65, 93, 166, 201 (`make sim SEED=25`), through R9: "tc/0
  log start moved to 1358 past record 690".
- **Root cause (two):** (1) Trim raised every partition's start to its
  first remaining batch's base, also when it dropped none of that
  partition's batches; with a gap before the first batch that moved the
  start. (2) The expiry loop had no check that the topic's policy includes
  `delete`, so a compact-only topic was also expired by the broker's
  default `retention.ms`. The second is why the first showed: it was a
  change that did not apply (the edit missed its target by a tab) and the
  simulator found it at once.
- **Fix:** Trim leaves a partition alone if it dropped none of its
  batches; expiry skips a topic whose policy has no `delete`.

### C2b-3. Fetch at an offset inside a gap returned a batch of earlier records (C2 part b)

- **Found by:** reading the fetch path while implementing. The unit test
  that fetches from offset 0 after compaction and the simulator's
  consumers cover it; there is no seed.
- **Root cause:** `readBatches` assumed offsets are dense, so the batch
  with the last base at or below the offset contains it. With gaps it may
  end before the offset, and a client that gets only records below its
  position makes no progress.
- **Fix:** the read skips a first batch whose last offset is below the
  offset.

### B1-1. A recovering node waited for entries that were never committed (B1, before merge)

- **Seed:** 417 (`make sim SEED=417`), three voters, one damaged.
- **Reported as:** "with faults stopped for 10s the nodes did not all come
  back, elect a leader and catch up".
- **Root cause:** the first design ended recovery when the node's commit
  reached the end of the log it had held. Part of that log was uncommitted
  entries that the elected leader never had, so the commit index could never
  reach it, and with the other voters unable to elect without the recovering
  one, nothing moved.
- **Fix:** recovery also ends when the leader's AppendEntries carries no
  entries beyond what the node holds.

### B1-2. Damage under a snapshot restarted the node for ever (B1, before merge)

- **Seeds:** 6, 61, 62 and others in the first 500.
- **Reported as:** "node 3 stopped with an error no fault explains: the raft
  log is damaged".
- **Root cause:** the first design repaired only what the start-up scan
  read, which begins after the snapshot. The scrub read the whole log, found
  damage below it, stopped the node, and the restart found nothing to repair.
  And damage under the snapshot is not harmless here: the log is the data
  store, and clients read batches back from it.
- **Fix:** a durable `damaged` marker, and for damage under the snapshot a
  repair that empties the log and the snapshot file.

### B1-3. A snapshot's last entry that was damaged stopped the start (B1)

- **Seed:** 1240.
- **Root cause:** `checkSnapshot` reads the entry at the snapshot index and
  failed with `ErrCorrupt` before any repair ran.
- **Fix:** that failure runs the repair and starts again.

### S13. A follower served from a log that had been reset under its state (C19, before merge)

- **Seed:** 749 (`make sim SEED=749`), the first run with fetch from followers.
- **Reported as:** "node 2 stopped with an error no fault explains: read
  committed batch at log index 1248: storage: offset out of range: 1248
  not in [1553, 1749]".
- **Root cause:** a node receiving a snapshot resets its log to the leader's
  start while its applied state, which refers to batches by log index, is
  still the old one. Nobody read batches from followers before, so nothing
  had read them in that window.
- **Fix:** a node that is installing a snapshot does not answer fetches.

### C19-1. The rack was lost for every node of a real cluster (C19)

- **Found by:** `TestFetchFromTheNearestReplica` on real processes, not by a
  seed: the leader never named a replica.
- **Root cause:** `members()` builds a node's address from the member info in
  the log and fell back to the configured peer only when there was none, so
  the rack, which is only in the configuration, was dropped.
- **Fix:** the rack is read from the configuration either way.

### C18a-1. ApiVersionsRequest v5 failed on the second connection once ClusterId was known (C18a)

- **Found by:** `TestFranzGoRoundTripAndRestart`, `TestJavaClientRoundTrip`,
  and `TestTopicIDIncarnationAndFetchByID` (`closing connection ...
  reason="malformed request body: key 18 version 5"`).
- **Root cause:** `ApiVersionsRequest` v5 (KIP-1242) adds `ClusterId` (compact
  nullable string) and `NodeId` (`int32`, default `-1`) before the tagged
  fields. On a client's bootstrap connection `ClusterId` is null (`0x00`),
  which `r.Tags(true)` read as zero tagged fields and ignored the trailing 5
  bytes. Once `Metadata` answered `ClusterId = "samsa"`, the client's next
  connection sent `"samsa"`, which `r.Tags(true)` tried to parse as 6 tagged
  fields.
- **Fix:** `APIVersionsRequest.Decode` and `Encode` read and write `ClusterId`
  and `NodeId` at version 5+.
- **Lesson:** `r.Tags(true)` stops at the end of the tag list without checking
  for trailing bytes, so a missing nullable field whose bootstrap value is
  `0x00` looks like an empty tag section until a client sends a non-null value.

## Bugs in the simulator and tests

### M8. I6 blamed the broker for a Trim proposed after the final restart (A1 part b)

- **Seeds:** 55, 498 and 975, the first run with retention.
- **Reported as:** I6, "after a restart the state hash is ...; before it
  was ...".
- **Cause:** I6 restarts every node and requires the same state hash. A
  restarted leader writes a no-op, its log crosses a segment, and it
  proposes a Trim: a legitimate change of state.
- **Fix:** the final restarts run with retention off. I6 is about replay
  rebuilding the same state, which this still checks.

### M1. Torn block destroyed bytes that had been synced (Phase 1)

- **Seed:** storage crash property test, seed 1 (before the fix).
- **Reported as:** invariant 1, "end offset 351 is below acknowledged
  durable offset 353".
- **Root cause:** the simulated disk resolves a crash block by block. A
  block straddling the synced end of a file was treated as wholly unsynced
  and could become garbage, including its synced bytes. That breaks rule 1
  of the durability contract, so the model was harsher than any disk the
  contract describes, and it blamed a correct log.
- **Fix:** crash resolution decides per byte whether the byte was written
  since the last sync; only those bytes take the block's fate
  (`simsys.Disk.afterCrash`).
- **Lesson:** a failing seed is a bug in the system, the model or the
  checker, and the first job is finding out which.

### M2. Two wrong expectations in the crash property test (Phase 1)

- Seed 31: a crash armed during recovery fired during the test's own
  verification reads and was reported as a failure. Fix: verification
  restarts on a crash like any other step.
- Seed 50: the test expected exactly one batch from `Read(offset, 0)`, but
  zero-length payloads legitimately fit in a zero-byte budget.

### M4. The checker blamed the broker for a topic created too late (Phase 4)

- **Seed:** 157 of the first 200, on the commit that introduced the group
  workload.
- **Reported as:** "OffsetCommit for t0/0 failed with code 3".
- **Root cause:** the member checked an `UNKNOWN_TOPIC_OR_PARTITION`
  answer against what the model knew when the *response arrived*. The
  topic had been created, and acknowledged to another client, between the
  broker's answer and its delivery. The broker was right.
- **Fix:** like every other check, this one now uses what was known when
  the request was *sent* (`client.commitOffset`).
- **Lesson:** ADR 0007's causality rule is easy to state and easy to
  forget in each new check.

### M5. A node that failed to start was called again, and its death blamed on its successor (Phase 5)

**Found by:** `TestSim` seed 69, the first run of the single-node
simulator on top of Raft ("the final read did not finish").

**What happened.** When power was lost during `OnStart`, the harness threw
the incarnation away without marking it dead. Until Phase 5 that was
harmless, because `OnStart` armed its first timer only after the last disk
operation. Now Raft arms the election timer, and the node its sync timer,
before the first sync. The orphan's timer fired later, its handler failed
on file handles from before the power loss, and the harness answered by
taking down the node that was running at that moment, every 173 ms, for
ever.

**Why it was wrong.** The contract in `sys.Handler` says a driver must not
call a node again after an error. The harness broke it.

**Fix.** `world.start` marks the incarnation dead when `OnStart` fails.
No broker code changed. The same 200 seeds then pass.

### M6. The random Raft test read the logs of powered-off nodes (Phase 5)

The first run of `TestRandomSchedules` reported a log-matching violation
at seed 1. The check compared every pair of nodes, including nodes that
were down, whose in-memory object still described a disk that had since
lost its unsynced tail. Down nodes are now skipped. A second check, "at
least five entries committed per seed", was wrong for crash-heavy seeds
and now only requires that the healed cluster commits something.

### M7. The Java group test blamed the broker for the client's own rejoin (Phase 4, explained in Phase 6)

`TestJavaConsumerGroup` failed about once in 20–30 runs, locally and on
CI: "the restart caused 1 JoinGroup requests". The broker log showed the
surviving member sending JoinGroup straight after the restart, with no
Heartbeat first, so no answer from the broker had asked it to rejoin.
Cause, read in the kafka-clients 4.4.0 source: when no known broker is
reachable, `NetworkClient` re-bootstraps (`metadata.recovery.strategy`
defaults to `rebootstrap`), and `Metadata.bootstrap` bumps the metadata
version and empties the cached topics. The group leader compares its
metadata with the snapshot it made the assignment from, sees the topic
gone, and rejoins ("cached metadata has changed",
`ConsumerCoordinator.rejoinNeededOrPending`). With one broker every
restart leaves no broker reachable; whether the client looks in that
window is timing. Experiment: with the broker held down for 2 s, the
default setting rejoined in 1 of 3 runs, and `none` in 0 of 6. The test
client now sets `metadata.recovery.strategy=none`. A second, smaller test
bug was fixed with it: the wait before the restart reused a "committed"
flag from the first phase, so it did not prove a commit after the
takeover.

What this leaves unproven: a Java client with default settings may
rebalance after a single-broker restart. That is the client's choice and
would happen against Kafka too (not tested against Kafka).

### M3. The checker condemned records that an earlier crash had spared (Phase 3)

- **Seeds:** 1, 3 and 6, under the `interval` and `never` fsync policies.
- **Reported as:** I3, "record 1706 reappeared after a power loss had
  removed it".
- **Root cause:** under those policies a power loss may cut a suffix of a
  partition. When a read contradicted what the model remembered at an
  offset, the model concluded the log had been cut there and marked
  everything it remembered beyond that offset as lost. With two power
  losses that is wrong: records confirmed *after* the first loss sit beyond
  the cut and are alive.
- **Fix:** only what was confirmed no later than the contradicted entry is
  marked lost (`Model.place`). A unit test holds the two-crash history.
- **Lesson:** the same as M1. 81 of the first 200 seeds failed, and 79 of
  those were the checker.

### M9. The checker judged DeleteRecords answers by the state at answer time (B4a)

- **Seeds:** 73, 79, 83, 99, 121 (300-seed run), then 3012, 3359, 3743 and
  4650 (5000-seed run) after a first, partial fix.
- **Reported as:** R1, "the log start went back", and I1, "a DeleteRecords
  to the high watermark answered below an acknowledged record".
- **Root cause:** `RecordsDeleted` compared the answered low watermark with
  the model's floor (first fix: with its start) as they were when the
  answer arrived. A trim or another DeleteRecords applied after this one,
  but answered first, had already moved them. The broker was right: its
  answer is the start at apply time.
- **Fix:** the client captures the floor and the start when it sends the
  request and the check uses those (causality rule: what the request could
  have seen). Unit cases in `history_test.go`.
- **Lesson:** the same as M1 and M3. Every answer is judged against what
  was known when the request was sent, never when it returned.

### M10. S6 condemned a stale leader of an older term (C5)

- **Seeds:** 1946 and 2496 (CI, PR 22), after the new SASL APIs moved
  every seed.
- **Reported as:** S6, "node 1 became leader of term 19 while node 2 held
  a lease in term 20".
- **Root cause:** node 1 became a candidate in term 19 and node 3 voted
  for it. Node 2 then won term 20, with node 3's vote, and took a lease.
  Node 3's term-19 vote answer reached node 1 late, and node 1 became
  leader of term 19. That is legal Raft: a majority is already past term
  19, so node 1 can neither commit nor gain a lease, and steps down at the
  first answer it gets. The monitor treated any new leader under a live
  lease as a violation.
- **Fix:** S6 fails only when the new leader's term is newer than the
  lease holder's. Two leaders of the same term remain S1. The mutation
  suite still catches `UnsafeNoLease`; 3000 seeds pass.
- **Lesson:** the same as M1. Late messages make stale states legal; the
  invariant must say which order of terms it forbids.

### M11. The exhaustive search's one-node reduction skipped schedules (B2)

- **Found by:** comparing the reduced search with the full one, before
  item 11 was merged. In the default configuration, with one election and
  one proposal per node, the full search reached 793 distinct node states
  and the reduced search 63. Both reported no violation.
- **Root cause:** at each state, the reduction followed only one node's
  events, on the argument that events of different nodes commute. They
  do, but another node's event can *enable* new events for the chosen
  node: a message sent to it. The reduction forced the chosen node to act
  before any such message could arrive. So every schedule in which it
  waits for a message, then handles it first, was never searched. In
  partial-order reduction terms, the set was not persistent: a path
  outside it (another node sends, then the chosen node receives) contains
  an event dependent on the set. An earlier symptom pointed the same way.
  The reduced search found `AckBeforeSync` in 19 events; the full search
  found it in 12.
- **Fix:** the reduction is gone. The search follows every enabled event.
  A sound reduction for message passing needs the receive events of every
  node that may still be sent a message (stubborn sets, or dynamic
  partial-order reduction), which is a project of its own. The bounds
  shrank to what the full search finishes (design, section 2).
- **Found with it:** the bound itself was vacuous. With per-node clocks
  starting fresh, a node ignores candidates for an election timeout
  after start, and one election per node never moved a voter's clock
  past it. No leader was ever elected, so "complete, no violation"
  checked almost nothing. Raising proposals from 1 to 2 changed no state
  count, which gave it away. Nodes now start aged, and `TestExhaustive`
  fails unless some state committed a proposal.
- **Lesson:** a reduction is a claim like any other and needs its own
  test. "Events of different nodes commute" was true and not enough.
  So is a bound: a search that completes must show it reached something.
  The test that caught it compares what the two searches reach, not only
  their verdicts: both verdicts were "no violation".

### M12. I6 blamed the broker for the cleaner working after the final restart (C2)

- **Seed:** 2, and others with an admin client, under the mutation suite's
  "seed fails without the mutation" check.
- **Invariant:** I6, "after a restart the state hash is 7fcbafc7; before
  it was 9ac7ff03".
- **Root cause:** the same as M8. The final restart check compares the
  state before and after a restart, but time passes while the cluster
  settles. DeleteRecords had left dead data in the log, so the restarted
  leader's cleaner relocated batches and trimmed. Those are new commands,
  not a replay that went wrong. The check already turned off
  `-retention-bytes` for M8; the cleaner and the topic expiry are new
  sources of the same thing.
- **Fix:** the final restart also turns off `-retention-time` and sets the
  slack to infinity, so the cleaner never runs.

### M13. I6 hashed the state while the cleaner's proposals were in flight (C2)

- **Seed:** 817 (`make sim SEED=817`), five nodes, `retain-ms=3000`.
- **Invariant:** I6, "after a restart the state hash is 4364fda6; before
  it was 940956cb".
- **Root cause:** the check took the leader's state hash with relocations
  proposed but not yet applied. They committed after the restart. Before
  C2 the leader rarely proposed anything once clients were idle.
- **Fix:** the check waits until the leader has applied every entry in
  its log, then hashes and restarts every node in the same instant.

### M14. C1 trusted record ids to order the records of a key (C2 part b)

- **Seed:** 1537, in the first 2,000 seeds with a compacted topic.
- **Reported as:** "C1: record 15344 ... is gone, and nothing later of the
  key is left to replace it".
- **Root cause:** the final check took a tombstone with a higher id as
  later. Ids are in send order, offsets in arrival order; across clients
  and retries they differ. Here a tombstone at offset 981 had a lower id
  than the put at offset 975 that it replaced.
- **Fix:** order by offset where the offset is known, by id only for
  records whose acknowledgement never arrived.

### M15. The final read did not count a tombstone nobody had learned the acknowledgement of (C2 part b)

- **Seed:** 1553.
- **Reported as:** "C5: the tombstone for key key-7 at offset 580 is gone,
  but a record of the key at offset 573, older, is still there".
- **Root cause:** a tombstone has no value to name its id, so a read can
  only match it to a record by the offset the model knows. One whose
  acknowledgement never arrived was skipped, and with it the proof that a
  later record of the key was left, which made the older one look
  orphaned.
- **Fix:** such a tombstone, read in the final pass, is counted as a record
  of its key at its offset.

### M16. The final log comparison read damage that nobody had found yet (B1)

- **Seeds:** 321, 323, 417 and others in the first 500.
- **Reported as:** "S2: nodes 5 and 1 differ at log index 520 (storage:
  corrupt data ...)".
- **Root cause:** S2 reads every entry of every node when a run settles.
  Damage injected shortly before the end had not yet been found by the scrub,
  so the comparison met it.
- **Fix:** when a run settles, the scrub is run once more on the damaged
  node, the node restarts and repairs, and the comparison waits until no
  node is recovering.

### M17. The simulator damaged a cluster that had no minority (B1)

- **Seed:** 417, voters 2 after a membership change.
- **Reported as:** the nodes never elected a leader.
- **Root cause:** with a voter removed, two voters remained, and one of them
  was recovering. A configuration of two has no minority to lose.
- **Fix:** damage and membership changes are not combined in one run.

### M18. The simulator took an answer's sender from the node's state at receipt (C19)

- **Seeds:** 285, 620, 678, 777, 822, 61, 266, 416, 809.
- **Reported as:** "a follower named a preferred read replica", and "R1:
  log start went back from 24 to 11".
- **Root cause:** the model held an answer to the follower rules or the leader
  rules by whether the node was serving when the client processed it. A
  leader that stepped down after answering, or a follower that became the
  leader, was judged by the wrong rules.
- **Fix:** a counter of each node's changes of role. An answer is held to the
  leader's rules only if the node was serving at the request and at the
  receipt with no change between; to the follower's rules only if it was not
  serving throughout; to the lesser ones otherwise.

### M19. The simulator expected CreateTopics v7 to return a TopicID on TOPIC_ALREADY_EXISTS (C18a)

- **Seeds:** 1, 2, 3, 4, 6 and 129 others in the first 200-seed run.
- **Reported as:** `CreateTopics v7 for "t2" returned a zero topic id`.
- **Root cause:** `client.createTopic` required `resp.Topics[0].TopicID != UUID{}`
  whenever `ErrorCode` was `ErrNone` or `ErrTopicAlreadyExists`. On
  `ErrTopicAlreadyExists`, the broker (matching Kafka) returns the zero UUID
  because the request did not create the topic.
- **Fix:** `client.createTopic` requires a non-zero `TopicID` only when
  `ErrorCode == ErrNone`; in `admin.go`, when a retried `CreateTopics` v7
  receives `ErrTopicAlreadyExists` after a dropped connection, it queries
  `Metadata` v12 to read the existing topic's `TopicID`.

### M20. C1 ordered an unacknowledged tombstone by send id instead of its in-flight window (C18a)

- **Seed:** 951, in the 3,000-seed CI run on PR #40.
- **Reported as:** `C1: record 13376, acknowledged at tc/0 offset 1730 with key key-6, is gone, and nothing later of the key is left to replace it`.
- **Root cause:** M14 fixed `checkCompactedFinal` to order acknowledged
  tombstones by offset (`oldTombAt`), but still ordered tombstones whose
  acknowledgement never arrived by record id (`oldTombID[k] > id`). Here one
  producer sent tombstone 13359 for `key-6` at 9.958 s and a second producer
  sent record 13376 for `key-6` at 9.962 s. Record 13376 arrived at leader
  node 2 first (offset 1730) and was acknowledged at 10.012 s; tombstone 13359
  arrived at leader node 2 second (offset 1733), replicated to a quorum, and
  became `recUnknown` when node 2 stepped down at 10.012 s before answering
  that connection. During quiesce the cleaner removed record 13376 (superseded
  by tombstone 13359) and then removed tombstone 13359 once
  `delete.retention.ms` elapsed. Because `13359 < 13376`, `oldTombID` assumed
  the tombstone had preceded record 13376.
- **Fix:** `Model.Unknown` records the clock instant `end` when a record's
  request ends without an answer, and `checkCompactedFinal` tracks
  `oldTombUntil[k]`, the latest time an unacknowledged old tombstone of key `k`
  remained in flight (`oldTombUntil[k] >= r.sent`).

### M21. Un-snapshotted `topicsVersion` caused S4 state divergence after snapshot restore (C18, before merge)

- **Seeds:** 13, 105, 1946.
- **Reported as:** `S4: nodes 1 and 2 applied the same 59 entries and differ: state hash 17e448e7 and 5e1bdbee`.
- **Root cause:** `state.topicsVersion` and `cgroup.seen` were not persisted in snapshots, so a node restoring from a snapshot started with `topicsVersion = 0` and `cg.seen = 0`. If a topic had been created before the snapshot and no `cmdCGBeat` ran before the snapshot, the node replaying from the log saw `cg.seen != s.topicsVersion` on the next `cmdCGBeat` while the snapshot-restored node saw `cg.seen == s.topicsVersion` and skipped `retarget`.
- **Fix:** removed `topicsVersion` and `cg.seen`; `cgNeedsApply` and `applyCGBeat` compare `cg.meta` directly against `metaOf(s.subscribedTopics(cg))`.

### C22-1. `ShareFetch` long-poll dropped `AckErrorCode` on wakeup or timeout (C22, before merge)

- **Seeds:** 24, 81 (reported as `S8: t0/0 offset 1093 was already acknowledged/rejected and was delivered again`).
- **Root cause:** When a `ShareFetch` request carried `AckBatches` whose `applyShareAck` failed (for example `ErrInvalidRecordState` after an acquisition lock expired) and no records were immediately available to acquire, `shareFetch` parked a `parkedShareFetch` without saving `res.shareAckErrs`. On timer expiry or wakeup, `completeShareFetch` passed `nil` for `ackErrs` to `buildShareFetchResp`, returning `AckErrorCode = 0` (`ErrNone`) for a failed acknowledgement.
- **Fix:** `parkedShareFetch` stores `ackErrs` and `completeShareFetch` forwards `pf.ackErrs` to `acquireAndRespondShareFetch` and `buildShareFetchResp`.

### C15-1. `wire.PeekBatch` omitted `Transactional` flag on `cmdAppend` (C15, before merge)

- **Found by:** `TestTransactionLifecycleCommitAbortTimeoutAndKIP890` (`READ_COMMITTED saw uncommitted data: lso=1 recs=72`).
- **Root cause:** `parseBatch` decoded `attrTransactional` into `BatchInfo.Transactional`, but `PeekBatch` (used by `state.apply` on `cmdAppend`) did not read the attributes field, leaving `info.Transactional == false` and skipping `p.openTxns` registration.
- **Fix:** `PeekBatch` decodes `attrs` (`Transactional` and `Control` flags).

### C23b-1. `verdict.records()` read `cmdTxnEnd` control entries as `cmdAppend` batches (C23b, before merge)

- **Found by:** `TestJudgeTransactions` (`out/0: batch at offset 1 is compressed or malformed: wire: malformed input`).
- **Root cause:** `EndTxn` appends a partition batch entry whose `logIndex` points to a `cmdTxnEnd` Raft log entry rather than a `cmdAppend` entry. `verdict.records()` passed the `cmdTxnEnd` payload directly to `appendBatch`/`wire.DecodeBatch`.
- **Fix:** `verdict.records()` skips non-`cmdAppend` log entries, control batches, offsets `>= p.lso()`, and aborted transactional records.

### C20-1. `decodeState` v13 rejected tiered batches with `logIndex == 0` (C20, before merge)

- **Found by:** `TestTieringLifecycleFaultsAndGC` (`snapshot state: 0 bytes left, malformed true`).
- **Root cause:** `decodeState` validated `b.logIndex >= s.logStart` for all batches before decoding v13's `objKey` fields; tiered batches have `b.logIndex == 0` and `b.objKey != ""`.
- **Fix:** For `v >= 13`, allow `b.logIndex == 0` during the initial batch loop and verify in the v13 section that any batch with `b.logIndex == 0` has `b.objKey != ""` or `b.control != 0`.

### C20-2. `cmdTrim` retained control batches with `logIndex < cut` (C20, before merge)

- **Found by:** Simulator seeds 7, 13, 15, 20, 29 (`snapshot state: 0 bytes left, malformed true`).
- **Root cause:** `cmdTrim` skipped `b.control != 0` batches even though control batches carry a non-zero `logIndex` from `cmdTxnEnd`; trimming past them left `b.logIndex < s.logStart`, which `decodeState` rejected.
- **Fix:** `cmdTrim` checks `b.objKey == "" && b.logIndex < cut`, dropping trimmed control batches along with their trimmed transaction records while preserving tiered batches (`b.objKey != ""`).

### C20-3. `gcObjects` deleted tiered object while `cmdTier` was still in flight (C20/C21)

- **Found by:** Simulator seeds 184, 329 (`I2: the leader answered OFFSET_NOT_AVAILABLE`).
- **Root cause:** `tierSegment` recorded `n.uploadedObjs[key] = true` before `cmdTier` applied; a concurrent `retain()` -> `gcObjects()` saw zero `batchRef`s referencing `key` in `state` and deleted `key` from `ObjectStore` before `cmdTier` committed.
- **Fix:** Only add `key` to `n.uploadedObjs` inside the `cmdTier` / `cmdAppendRef` apply callback, and skip `gcObjects` while `n.tiering` is true.

### C21-1. `OnObjectDone` did not call `n.settle()` after callback proposals (C21, before merge)

- **Found by:** `TestDisklessProduceIdempotenceAndRaftBytes` (produce timed out waiting for `cmdAppendRef` to flush).
- **Root cause:** `OnObjectDone` returned `n.fatal` directly instead of `n.settle()`, so Raft proposals enqueued inside an object-store completion callback were not flushed until a later unrelated timer fired.
- **Fix:** `OnObjectDone` returns `n.settle()`, matching `OnTimer`, `OnConnData`, and `OnPeerMessage`.

### C21-2. `cmdForwarded` whitelist rejected forwarded `cmdAppendRef` (C21, before merge)

- **Found by:** `TestDisklessTopicsCluster` (`forwarded command at index 3`).
- **Root cause:** `validForwarded` in `forward.go` allowed `cmdAppendRef`, but `case cmdForwarded:` in `state.go` still restricted inner commands to ACL and topic config commands.
- **Fix:** Allow `cmdAppendRef` inside `case cmdForwarded:` in `state.go`.

### C21-3. Diskless/tiered object keys collided after node restart in the same term (C21, before merge)

- **Found by:** Simulator seeds 12, 56 (`I1/I4: record 537 is at t1/1 offset 0 but was acknowledged or read at offset 6`).
- **Root cause:** Object keys used in-memory sequence counters (`disklessSeq` / `tierSeq`) + `raft.Term()`. Restarting in the same term reset the counter to 0, overwriting earlier committed objects in `ObjectStore`.
- **Fix:** Include a 64-bit random nonce (`n.rand.Uint64()`) in every tiered and diskless object key.

### C21-4. Multi-batch diskless `ProduceRequest` committed batches in separate Raft entries (C21, before merge)

- **Found by:** Simulator seed 32 (`I1/I3: t1/0 offset 68 holds record 11545 but held record 11512`).
- **Root cause:** Proposing each batch of a multi-batch `ProduceRequest` sequentially across async callbacks allowed a concurrent producer's `cmdAppendRef` to interleave between batch 0 and batch 1, breaking contiguous offset assignment.
- **Fix:** Upload all batches of a partition in one `ObjectStore.Put` and commit them atomically in a single `cmdAppendRef` Raft entry.

### C21-5. Duplicate forwarded `cmdAppendRef` appended non-idempotent records twice (C21, before merge)

- **Found by:** Simulator seed 199 (`dup=0.1`, `I1/I4: record 1179 is at t1/1 offset 53 but was acknowledged or read at offset 48`).
- **Root cause:** Network packet duplication delivered the same `msgForward(cmdAppendRef)` twice to the leader; for non-idempotent producers (`ProducerID == -1`), applying it twice appended duplicate records.
- **Fix:** `applyAppendRef` deduplicates by `c.objKey` against recent partition batches, returning the original `baseOffset`.

### C21-6. `completeFetch` and `missingRemoteBatches` missed remote batches under long-poll wakeup and multi-partition budgets (C21)

- **Found by:** Simulator seeds 273, 391, 400, 717, 850 (`I2: fetch returned nothing although HWM > offset` and `I2: OFFSET_NOT_AVAILABLE`).
- **Root cause:** (1) `completeFetch` called `buildFetch` directly on long-poll wakeup without prefetching remote batches; (2) `missingRemoteBatches` subtracted Raft-framed sizes across partitions, skipping a partition's first remote batch that `buildFetch` later tried to read; (3) `gcObjects` could delete an expired object while an in-flight remote fetch was reading it.
- **Fix:** `completeFetch` prefetches `missingRemoteBatches`; `missingRemoteBatches` always prefetches at least the first batch of every partition; `buildFetch` reports `ErrOffsetNotAvailable` per partition only on genuine cache miss; `gcObjects` uses a two-pass grace period.

### C21-7. Concurrent `cmdTier`/`cmdAppendRef` during remote prefetch and unsynced `DeleteRecords` rollback with object GC (C21)

- **Found by:** Simulator seeds 924 and 1473 (`I2: the leader answered OFFSET_NOT_AVAILABLE`).
- **Root cause:** (1) A concurrent `cmdTier` or `cmdAppendRef` applying while `fetchRemoteBatches` was in flight introduced a new uncached remote batch before `finishFetch` ran; (2) under `SyncInterval`/`SyncNever`, `gcObjects` deleted an object after `DeleteRecords` before a crash rolled back the unsynced `DeleteRecords`, resurrecting a `batchRef` to a deleted object.
- **Fix:** `prefetchAndFetch` re-checks `missingRemoteBatches` (up to 4 passes) after a successful prefetch; `gcObjects` runs only under `SyncAlways`.

### M22. C2 and I1/I3 identified a tombstone by a slot that a power loss had cut (nightly, found 2026-11-21)

- **Seeds:** 623297379, 623335888 (nightly 2026-11-16 to 2026-11-20, also 623217199 on an older commit), reported as `C2: tc/0 offset 3: record 1221 was sent with key "key-6" and read with key "key-7"`; 623232890, reported as `I1/I3: tc/0 offset 1069 holds record 9852 but held record 9390`. All on one node with `sync` interval or never, after power losses or failed syncs.
- **Root cause:** a tombstone has no value, so `observeCompacted` names a tombstone it reads by the record the model last placed at that offset (`p.at[off]`). It did so even when that slot was confirmed before a power loss that may have cut the log there, which `place` itself treats as replaceable. Another tombstone legitimately took the offset after the cut: with a different key the checker reported C2; with the same key it re-stamped the stale slot with the current epoch, and the acknowledgement of the real tombstone then looked like a change of an offset within one epoch (I1/I3). The broker was right both times.
- **Fix:** a tombstone read at a slot from an earlier epoch (under a policy that permits loss) is not identified, like a record nobody acknowledged. The five compaction mutations are still caught (scores 58, 64, 10, 23, 79 of 200 seeds).

### M23. The final share-group drain counted acknowledgements the broker had refused (nightly, found 2026-11-21)

- **Seeds:** 623221682 (no faults at all), 623226614, 623265692, reported as `S8: t0/0 offset 1477 was already acknowledged/rejected and was delivered again`.
- **Root cause:** `drainShareGroup` told the model that every acknowledgement it sent was accepted, without reading the response's per-partition `AckErrorCode`. Its own acquisition lock had expired, so the broker correctly refused the ACCEPT (`INVALID_RECORD_STATE`) and delivered the record again; the model, believing it accepted, reported S8. The clients' own path (`sgFetch`) already checked the code.
- **Fix:** the drain counts its acknowledgements only when no partition reports an acknowledgement error. The mutation "drop a share-group lock without redelivery" is still caught (97 of 200 seeds).

### M24. `settled` gave a node that lost its whole log ten seconds to refetch it (nightly, found 2026-11-21)

- **Seeds:** 623301106, 623299220, 623229758, 623258340, 623254219, reported as `with faults stopped for 10s the nodes did not all come back, elect a leader and catch up`, with one node far behind (often `recovering true`). All with `append=1`.
- **Root cause:** with `append=1` the leader sends one entry per AppendEntries and waits for its answer, so a follower catches up at one entry per round trip (about 110 per second). A node whose damage repair emptied its log (damage under the snapshot) needed about 25 s to refetch thousands of entries; `settled` waited `answerBound`, 10 s. The node was making progress the whole time.
- **Fix:** `settled` waits `settleBound`, 60 s; every seed above converges within it. Seeds that agree sooner are unchanged (the wait ends as soon as they agree). With the default `MaxAppendBytes` (1 MiB) the catch-up is a few messages.
