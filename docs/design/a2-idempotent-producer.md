# A2 design — idempotent producer

Status: reviewed, 2026-11-11. Production plan item 1 (`docs/PRODUCTION.md`).
Two parts, each its own PR: **(a) idempotence**, exact, with producer state
kept for ever; **(b) expiry** of that state, which bounds memory and rests
on a timing assumption.

## 1. The problem

A producer that gets no answer (a reset, a leader change) retries. The
first attempt may have been committed, so the batch is appended twice.
Clients are told today to turn idempotence off; the Java client's default
is on. Idempotence makes "at least once" into "exactly once per
partition, per producer session".

## 2. Protocol facts

Read in the source of apache/kafka, trunk at `cf1f88ba88de`, on
2026-11-11. File and line are given where a rule is copied from code.
Nothing here is from memory.

**InitProducerId**, API key 22 (`InitProducerIdRequest.json`,
`InitProducerIdResponse.json`): versions 0–6 (6 is marked unstable),
flexible from 2. Request: `TransactionalId` (nullable string),
`TransactionTimeoutMs` int32, v3+ `ProducerId` int64 and `ProducerEpoch`
int16 (default -1), v6+ two 2PC flags. Response: `ThrottleTimeMs`,
`ErrorCode`, `ProducerId` int64, `ProducerEpoch` int16, v6+ two
ongoing-transaction fields.

- With a null transactional id the broker "always blindly accept[s] the
  request and return[s] a new producerId", epoch 0, ignoring the id and
  epoch in the request (`TransactionCoordinator.scala:126`).
- **The Java client sends it to any node**, not to a coordinator
  (`Sender.java:481`, `leastLoadedNode`; `coordinatorType()` is null
  without a transactional id). On `NOT_COORDINATOR` it would look up a
  transaction coordinator for a null id (`TransactionManager`,
  `InitProducerIdHandler`). So every node must answer.

**Errors** (`Errors.java`): OUT_OF_ORDER_SEQUENCE_NUMBER 45,
DUPLICATE_SEQUENCE_NUMBER 46, INVALID_PRODUCER_EPOCH 47,
INVALID_REQUEST 42, INVALID_RECORD 87.

**A batch** carries producer id, epoch and base sequence in its header.
It "has a producer id" when the id is above -1
(`AbstractRecordBatch.hasProducerId`). Its last sequence is the base plus
the last offset delta, wrapping from 2^31 − 1 to 0
(`DefaultRecordBatch.incrementSequence`). A batch with a producer id and a
negative base sequence is INVALID_RECORD (`LogValidator.java:500`).

**The rules**, per partition and producer id, for a client's batch
(`UnifiedLog.analyzeAndValidateProducerState`, then
`ProducerAppendInfo.checkProducerEpoch` and `checkSequence`), in order:

1. *Duplicate:* state exists with the same epoch, and one of its kept
   batches has exactly this first and last sequence. Nothing is appended;
   the answer is success with the kept batch's offsets
   (`UnifiedLog.java:1262`). Five batches are kept
   (`ProducerStateEntry.NUM_BATCHES_TO_RETAIN`); a new epoch clears them.
2. *Older epoch* than the state's: INVALID_PRODUCER_EPOCH.
3. *No state, and no record was ever appended to the partition*
   (`mapEndOffset() == 0`): the base sequence must be 0, else
   OUT_OF_ORDER_SEQUENCE_NUMBER. "No producer state can ever have been
   lost."
4. *No state otherwise:* any sequence is accepted (state may have expired).
5. *Newer epoch:* the base sequence must be 0, else
   OUT_OF_ORDER_SEQUENCE_NUMBER.
6. *Same epoch:* the base sequence must be the last one plus 1, or 0 after
   2^31 − 1; else OUT_OF_ORDER_SEQUENCE_NUMBER. The current broker never
   answers DUPLICATE_SEQUENCE_NUMBER.

**What the Java client does** (`TransactionManager.java`), which the rules
exist to serve:

- It bumps its epoch **locally**, with no request, when a batch fails or
  the broker reports a gap (`bumpIdempotentProducerEpoch`), and asks for a
  new id only when the epoch reaches 32767.
- The bump is per producer but **takes effect per partition**: a partition
  restarts at sequence 0 under the new epoch once its own in-flight
  batches are done (`maybeUpdateProducerIdAndEpoch`); until then it keeps
  sending the old epoch. So two partitions legitimately see different
  epochs of one producer at the same time.
- After OUT_OF_ORDER_SEQUENCE_NUMBER it re-sends the batch under the new
  epoch: a retry that falls outside the window of five is appended twice.
  The window is safe only because clients keep at most five batches in
  flight.

**Expiry in Kafka** is by time only: a producer idle for
`producer.id.expiration.ms` is dropped (`ProducerStateManager.isProducerExpired`).
Advancing the log start does not drop it (`onLogStartOffsetIncremented`).

Aside, found while reading: Kafka refuses a Produce v3+ partition that
holds more than one batch (`ProduceRequest.java:228`); Samsa has always
accepted several. Left as it is: it accepts more, never less.

## 3. Design

### 3.1 Sequence state

Per partition, a map from producer id to its epoch and its last five
accepted batches (first sequence, last sequence, base offset). It changes
only in `apply` of an Append, so it is replicated and deterministic
(roadmap R1), is in the state hash, and is in the snapshot (version 3).

Apply of an Append whose batch has a producer id follows the six rules of
section 2 exactly. A duplicate answers the kept base offset; a refused
batch answers its error; in both cases the log entry stays in the log and
applies to nothing, as an Append to an unknown topic already does. A batch
without a producer id is appended as before.

The leader checks the batch's shape before proposing (INVALID_RECORD for a
negative sequence). It does **not** check sequences before proposing: what
is in sequence is only known at apply, after the entries ahead of it.

**Mutation:** `UnsafeNoSequenceCheck` appends whatever arrives.

### 3.2 Producer ids

**Every node hands out ids from blocks it reserves on its own disk**
(ADR 0015):

    id = node id (16 bits) | block (28 bits) | index in the block (19 bits)

The node's block number is in a small file, `producer-block`, replaced
atomically (temp file, sync, rename, sync directory) and **always synced,
whatever the sync policy**. A start reserves the next block before
answering anything; so does exhausting a block. Ids are unique by
construction: across nodes by the node id, across restarts by the block.
A node id must be below 65 536, checked at start.

A transactional id is answered INVALID_REQUEST until transactions exist
(plan item 15).

### 3.3 Part (a) keeps producer state for ever

Dropping an entry lets a late retry of that producer's last batch in as
new (rule 4). So part (a) drops nothing: no timing assumption, and no
duplicate is possible from a client that follows the protocol. The cost is
an entry of about a hundred bytes per producer session and partition, for
ever. *shortcut:* unbounded; part (b) bounds it.

### 3.4 Part (b): expiry

As in Kafka, by time, and as with retention, **decided by the proposer and
carried in the command** (R1): the leader remembers, while it serves, the
applied index at intervals; when a remembered index is older than
`ProducerExpiry` (default one day) it proposes `ExpireProducers(L)`, and
every replica drops the producers whose last batch is at a log index below
L. A new leader starts with nothing remembered, so expiry is only ever
late, never early. The assumption, the same as Kafka's: a client stops
retrying a batch long before `ProducerExpiry`. Its own design section and
review come with that PR.

## 4. What checks it

- **Simulator.** A new kind of producer gets an id, keeps up to three
  batches in flight, and after an unknown outcome re-sends the same
  batches, in order, with the same sequences, as real clients do; it gives
  up after two seconds and bumps its epoch. The existing rule of the
  history checker — a record sits at one offset only (reported as I1/I4,
  "record … is at … but was acknowledged or read at …") — now has
  something to catch: a retried batch appended twice.
- **New check I8:** under the strict policy, a producer that follows the
  protocol is never answered a sequence or epoch error.
- **New check I9:** no producer id is handed out twice, across nodes,
  restarts and power losses.
- The mutation must be caught; `TestModel` shows the duplicate rule fires.
- Unit tests in `broker` for each of the six rules, the wrap at 2^31 − 1,
  the snapshot round trip, and the id blocks across restarts.
- Integration: the Java client and franz-go with default settings
  (idempotence on), kcat with `enable.idempotence=true`; the cluster test,
  which today counts duplicates after leader kills, must count none.

## 5. Alternatives rejected

- *Deduplicate in the leader's memory:* lost at a leader change, exactly
  when retries happen.
- *Keep every sequence ever seen:* unbounded per producer; the window of
  five is what clients are built around.
- *Producer ids from a counter in the replicated state* (the roadmap's
  outline): only the leader could answer, and clients ask any node; and
  under `interval` or `never` a power loss can take back the counter, so
  two producers would share an id and one's batches would be dropped as
  duplicates of the other's. That is loss of data acknowledged *after*
  the power loss, which those policies do not permit.
- *Random 63-bit ids:* three lines and no file, but unique only with high
  probability, and "exactly once" would then rest on a number the
  simulator cannot check. The block file costs forty lines and makes I9
  exact.
- *Ids from the Raft term* (unique per leader): followers cannot answer.
- *Drop a producer when retention deletes its last batch* (the first
  draft): retention can delete a batch within its retry window, and the
  retry is then appended again. Kafka does not do it either.

## 6. Review record

Second pass, against the Kafka source instead of the outline:

1. **The draft's producer-id command was wrong twice** (section 5): the
   Java client asks any node, and a replicated counter repeats ids after
   a power loss under the weak policies. Replaced by per-node blocks.
2. **Dropping state on Trim opens a duplicate** that the simulator would
   find, with its tiny segments, and that I would have been tempted to
   excuse in the checker. Replaced by time expiry in a separate part.
3. **"The partition is empty" in rule 3 means never written**, not
   "everything was trimmed": after a Trim a partition with no records
   still accepts any sequence.
4. **Epochs are per partition**, because the client switches partitions to
   a new epoch one at a time. A single epoch per producer id would refuse
   the client's own in-flight batches. The state is already per partition;
   recorded so that nobody "simplifies" it.
5. A duplicate is answered at apply by a later log entry than the
   original. The waiter of the retry gets the original's offset. If the
   original was never committed, the retry is simply the first copy.
6. Pipelined batches after a leader change: the old leader's uncommitted
   entries either survive in order or not at all (log matching), and
   entries of a later term follow them, so the applied order of all copies
   of batches k and k+1 always has a copy of k before any copy of k+1.
   With three in flight and five kept, every retry is found. I8 checks
   this argument instead of trusting it.
7. A snapshot taken between an original and its retry must carry the
   window, or a restart would append the retry: it is in the snapshot,
   and a simulator seed with `SnapshotEvery` 1 restarts there.
8. The id block is durable state that is not a command, so by the working
   method it needs its own crash coverage: the simulator's power losses
   and failed syncs at start, checked by I9.
