# A5b design: application fault testing (`-faults` and `samsa verdict`)

Status: reviewed, 2026-11-14. Production plan item 5b (`docs/PRODUCTION.md`,
section 2). There is no roadmap item; the plan's section 2 is the
specification. This document was written alongside the first version of
the code, not before it. The review in section 6 was done on that code
and changed it.

## 1. The problem

The bugs that lose or duplicate messages are mostly in applications. Two
examples: a consumer commits before it processes, or a producer retry
writes twice. They show only under faults. A healthy broker in a test
never makes those faults, and a network chaos tool cannot make the
specific ones: "this write committed and its answer was lost", or "your
partitions are revoked now, in the middle of your batch".

Samsa has both halves of an answer already:

- **Faults injected by the core.** The core decides every answer itself,
  so it can decide to answer wrongly, on purpose.
- **The ground truth.** The command log holds every record ever
  committed. After a run, the log can say what happened to each input
  record.

## 2. Faults: `samsa -faults seed=N[,rate=R][,kinds=...]`

### 2.1 Where the decision is made

The decision is made inside the core (`internal/broker/faults.go`), at the
moment the node handles an *eligible* request. Each eligible request makes
exactly one draw from the faults' own random stream. The stream is a
splitmix64 seeded with `seed`. A draw below `rate` injects the fault.

- **Its own stream, not the node's `sys.Random`.** Raft draws from the
  node's random for election timeouts. If faults drew from the same
  stream, switching faults on would change elections, and every seed of
  the simulator would change. With a separate stream:
  - a node with faults off draws nothing, and is the node it always was
    (`TestFaulterIsASeededStream`, and the simulator's trace hashes);
  - the faults depend only on the seed and the sequence of eligible
    requests.
- **One draw per eligible request, whatever the outcome.** Fault *k* is
  then decided by request *k* alone. It does not depend on how earlier
  draws came out.

### 2.2 The kinds

| Kind | Eligible request | What the node does instead |
|---|---|---|
| `lostack` | a produce with acks ≠ 0 of which some partition was written | commits the write, then closes the connection without answering |
| `notleader` | produce, fetch, offset commit | answers NOT_LEADER_OR_FOLLOWER (NOT_COORDINATOR for a commit) without acting: the same answer a node gives during a leader change (ADR 0011), so the client takes the same path |
| `rebalance` | a heartbeat of a group with no rebalance in progress | starts a rebalance round and answers REBALANCE_IN_PROGRESS: every member must rejoin and has its partitions revoked |

Each is what a real cluster does sometimes:

- `lostack` is a leader that commits and then dies, or a connection that
  drops at the wrong moment.
- `notleader` is a leader election.
- `rebalance` is another member joining or leaving.

Injected on demand, they turn "sometimes, in production" into "every run,
in CI".

### 2.3 Not done, and why

The plan's section 2 also names:

- session expiry;
- slow or empty fetches;
- offsets cut by retention;
- a leader change in the middle of a request, as distinct from a refusal
  before it.

None of them is needed for the acceptance, which is the
commit-before-process consumer. Each needs its own design:

- **Expiry** needs the group's timers to fire early.
- **Slow fetches** need a timer per held fetch.
- **Retention cuts** change committed state through the log. That makes
  them a replicated command, not a fault in an answer.

They are follow-ups. The guide does not promise them.

## 3. The verdict: `samsa verdict -data D -in T -out T2 [-group G]`

### 3.1 The pipeline

An application reads topic `-in`, as consumer group `-group`, and writes
one record or more per input record to topic `-out`. Each output record
carries the input record's key. Input keys must be unique.

- **Key matching.** This is the one demand the verdict makes of the
  application. It is how the verdict knows which output came from which
  input, without instrumenting the application.
- **Unique keys.** Duplicate input keys would make "processed twice"
  ambiguous. The verdict refuses them, and names the two offsets.
- **Rejected alternative: matching by value.** Many applications
  transform values, so the value cannot identify the record.

### 3.2 Classification

Each input record falls into exactly one class:

- **once**: exactly one output record has its key.
- **duplicated**: more than one has.
- **lost**: none has, and the group committed an offset past the record.
  The application declared it done and never did it. Without `-group`,
  every record with no output counts as lost.
- **not yet processed**: none has, and the group did not commit past it.
  Either the application did not finish, or it would have read it again.

Output records whose key matches no input are counted and reported. They
do not fail the verdict: an application may write other things.

**Exit status:**

- 0: pass. Nothing was lost and nothing is pending. Duplicates also fail
  under `-expect exactly-once`.
- 1: fail.
- 2: the verdict could not be computed (an unknown topic, a duplicate or
  missing key, compressed records, records deleted by retention, corrupt
  data).

`samsa check` now also exits 2 when it cannot run at all. A corrupt
directory is still exit 1.

### 3.3 Offline, on a stopped node

`broker.Judge` loads the data exactly as `broker.Check` does: the same Raft
start, snapshot decode and replay, on the same read-only disk wrapper. It
then reads each input and output batch through the replayed state's batch
references, the way a fetch does.

- **Offline.** The data directory's lock already forbids two readers.
  There is no race with a running node, and no API to design or secure.
  The cost: the test must stop the broker before asking. A SIGKILL is
  enough, since with `-sync always` every acknowledged write is on disk.
- **The seed.** The verdict prints it from `FAULTS`, a file that the
  binary writes in the data directory at start (and removes when started
  without faults). It is written outside the core, beside the `LOCK`
  file, and the core never reads it.

### 3.4 What it cannot read

- **Compressed batches.** The core has no decompressor, and the purity
  rule allows none; adding one is a dependency decision. The verdict
  refuses them and says why. Applications under test must produce
  uncompressed records, for example with `kgo.NoCompression()` or
  `compression.type=none`.
- **Records already deleted** by retention or DeleteRecords. The verdict
  refuses: it cannot judge records it cannot see.

## 4. Determinism: what "the same seed gives the same faults" means

Fault *k* lands on the *k*-th eligible request the node handles. Whether
request *k* is the same request in two runs depends on the client:

- **One client, one connection, no concurrency:** yes.
- **Real clients** (background heartbeats, several connections, retries
  on timers): the order of eligible requests varies between runs. So does
  which request gets each fault.

The seed therefore fixes the fault *stream*, not the schedule. A failing
run is easy to rerun with the same stream, and is likely to fail the same
way: in the acceptance runs, the buggy consumer lost 518, 518 and 520
records of 600. It is not guaranteed to repeat to the byte.

The output says this every time it prints the replay command. Exact
replay needs the simulator, where the clients are part of the
deterministic world.

## 5. Tests

- `TestParseFaults`: the spec grammar, its errors and round trip.
- `TestFaulterIsASeededStream`:
  - the same seed gives the same draws;
  - another seed gives other draws;
  - the rate holds within 10 % over 10 000 draws;
  - a disabled kind does not draw;
  - the zero value injects nothing.
- `TestFaultLostAck`, `TestFaultNotLeader`, `TestFaultRebalance`: each kind
  at rate 1 on the node harness. They check:
  - a lost ack is still written;
  - a refused produce is not written;
  - a rebalance draws once per stable period, and the group re-forms.
- `TestJudge`: a hand-built pipeline with every class and the examples.
  The same data without a group counts every missing record as lost.
- `TestJudgeRefuses`: unknown topic, duplicate key, missing key,
  compressed batch.
- `TestVerdictCatchesCommitBeforeProcess` (integration, real binary,
  franz-go):
  - **Commit before process:** under `kinds=rebalance`, the consumer is
    reported FAIL, with lost records and the replay command.
  - **Commit after process:** the same consumer, under all three kinds
    at rate 0.05, is reported PASS, with duplicates (which at-least-once
    allows). The broker log must show that each kind was injected.

## 6. Second review (on the first version of the code)

1. **A lost ack was counted as a dropped connection.** The first version
   closed the connection through `drop`. That increments
   `samsa_connections_dropped_total`, documented as "bad input or limits",
   and logs a warning. A fault is neither. It now closes the connection
   directly and counts in its own `samsa_faults_injected_total`.
2. **"Lost" was wrong for an application that had not finished.** The
   first classification had three classes. An application killed halfway
   would have been reported as losing everything it had not reached. That
   is the wrong diagnosis, and the wrong one to give a CI log. The
   "not yet processed" class separates the two by the group's committed
   offset. It still fails the verdict, with a message that asks whether
   the application finished.
3. **The fixed consumer stalled on its first run, and the verdict said
   so.** After an injected NOT_LEADER_OR_FOLLOWER on a produce, franz-go
   refreshes metadata at most every `MetadataMinAge` (5 s by default).
   The sample application's "idle for 3 s" timer counted that wait.
   The application stopped early and the verdict reported 575 records as
   not yet processed (not lost: finding 2 at work).
   - The fix is in the test application: idle time counts from the end of
     the work, and `MetadataMinAge` is 250 ms.
   - The guide tells users to shorten their client's metadata back-off in
     tests: Java's `retry.backoff.ms` and `metadata.max.age.ms`, and
     franz-go's `MetadataMinAge`.
4. **The pass was not checked for having been tested.** A fixed
   application passing proves something only if faults happened. The
   integration test now requires each of the three kinds to appear in the
   broker's log.
5. **The verdict replays entries past the commit point,** as `Check`
   does.
   - On one node with `-sync always`, nothing that is acknowledged is
     uncommitted.
   - On a follower of a cluster, an uncommitted tail could count as an
     output that never really happened.
   - The guide says to run the verdict on a single-node broker, which is
     what an application test uses.
6. **The rebalance fault depends on the group being stable.** Drawing on
   every heartbeat, rebalance or not, would have used draws on heartbeats
   that cannot be faulted, and made the rate meaningless during a
   rebalance storm. Drawing only for a stable group means `rate` is "per
   heartbeat of a stable group".
7. **The FAULTS file and a cluster.** Every node of a cluster writes its
   own `FAULTS`, and each node injects from the same seed into its own
   requests. That is consistent, but not a scenario the integration test
   covers. The guide documents single-node use only.
8. **Rejected: an HTTP endpoint for the verdict on a running node.** It
   needs a second reader of the node's state: either the loop publishes
   a copy of every record, or the endpoint reads the log behind the
   node's back. Both break rule 1 or cost more than stopping the broker
   at the end of a test.
