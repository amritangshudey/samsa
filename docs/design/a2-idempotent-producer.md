# A2 design — idempotent producer

Status: draft, 2026-11-11. Production plan item 1 (`docs/PRODUCTION.md`).

## 1. The problem

A producer that gets no answer (a reset, a leader change) retries. The
first attempt may have been committed, so the batch is appended twice.
Clients are told today to turn idempotence off; the Java client's default
is on. Idempotence makes "at least once" into "exactly once per
partition, per producer session".

## 2. Protocol facts

Checked against apache/kafka trunk on 2026-11-11 (file names below);
nothing here is from memory.

- **InitProducerId**, API key 22 (`InitProducerIdRequest.json`,
  `InitProducerIdResponse.json`): versions 0–6, flexible from 2. Request:
  `TransactionalId` (nullable string), `TransactionTimeoutMs` int32,
  v3+ `ProducerId` int64 and `ProducerEpoch` int16 (default -1), v6+ two
  2PC flags. Response: `ThrottleTimeMs`, `ErrorCode`, `ProducerId` int64,
  `ProducerEpoch` int16, v6+ two ongoing-transaction fields.
- **Errors** (`Errors.java`): OUT_OF_ORDER_SEQUENCE_NUMBER 45,
  DUPLICATE_SEQUENCE_NUMBER 46, INVALID_PRODUCER_EPOCH 47,
  INVALID_TXN_STATE 48, INVALID_PRODUCER_ID_MAPPING 49,
  UNKNOWN_PRODUCER_ID 59, PRODUCER_FENCED 90.
- **Sequence rules** (`ProducerAppendInfo.java`), per partition and
  producer id, for a client append:
  1. epoch older than the stored one: INVALID_PRODUCER_EPOCH, before any
     sequence check;
  2. no state for this producer and the partition is empty: the first
     sequence must be 0, else OUT_OF_ORDER_SEQUENCE_NUMBER;
  3. no state, partition not empty: any first sequence is accepted;
  4. newer epoch than stored: the first sequence must be 0;
  5. same epoch: first sequence must be last + 1, where 0 follows
     2^31 − 1; else OUT_OF_ORDER_SEQUENCE_NUMBER.
- **Duplicates** (`ProducerStateEntry.java`): the last 5 batches are kept
  per producer (`NUM_BATCHES_TO_RETAIN`); a batch of the same epoch whose
  (first, last) sequence equals a kept one exactly is a duplicate. The
  batch list is cleared when the epoch changes.
- **Not verified verbatim:** that a duplicate is answered with error 0 and
  the original `BaseOffset` (`UnifiedLog.java` was only partly readable).
  To settle with the Java client in the integration test before the ADR.

## 3. Design

Everything lives in `apply`, so it is replicated and deterministic
(roadmap R1).

- **Producer ids are allocated by a command.** `InitProducerId` with a
  null transactional id proposes `cmdInitProducer`; applying it returns
  `state.nextProducerID++` with epoch 0. A transactional id is refused
  until transactions exist (item 15).
- **State:** per partition, a map producer id → {epoch, last 5 batches as
  (first sequence, last sequence, base offset)}.
- **Apply of an Append** whose batch carries a producer id ≥ 0: find a
  duplicate first (answer success with the kept base offset, append
  nothing); otherwise the rules of section 2; otherwise append and record.
  A batch without a producer id is appended as today.
- **The log entry of a refused or duplicate append stays in the log** and
  applies to nothing, as a rejected append already does.
- **Bounding the table:** Kafka expires producers by wall-clock time,
  which apply may not read. Here a producer's entry is dropped when a
  `Trim` removes its last batch: deterministic, and then rule 3 accepts
  the producer's next batch as Kafka does after expiry. *shortcut:* no
  bound without retention; add a proposer-decided expiry command if the
  table is measured to matter.
- **Snapshot:** version 3 adds `nextProducerID` and the tables.
- **Mutation:** `UnsafeNoSequenceCheck` appends whatever arrives.

## 4. What checks it

- Simulator: producers get an id, and after an unknown outcome resend the
  *same* batch with the same sequence, as real clients do. New invariant
  **I7: no record appears at two offsets of a partition**, with a
  `TestModel` case showing it fires; the mutation must be caught.
- Unit tests in `broker` for each rule of section 2.
- Integration: Java and franz-go with default settings (idempotence on),
  kcat without `enable.idempotence=false`; README quick start loses the
  flag.

## 5. Alternatives rejected

- *Deduplicate in the leader's memory:* lost at a leader change, exactly
  when retries happen.
- *Keep every sequence ever seen:* unbounded; Kafka's window of 5 is what
  clients are built around (`max.in.flight.requests.per.connection` ≤ 5).

## 6. Review record

To be filled on the second pass, before coding.
