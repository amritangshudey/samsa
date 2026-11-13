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
