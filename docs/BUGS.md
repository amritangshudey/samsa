# Bugs found by simulation

One entry per bug: the seed, the violated invariant, the root cause, the
fix. Bugs in the simulator or the tests themselves are listed separately at
the end, because a model that is wrong in either direction matters too.

## Bugs in the system

None yet. Phase 1's storage engine passed the crash property test once the
model bug below was fixed. That is weaker evidence than it sounds: see
"what is untested or weak" in `docs/reports/phase-1.md`.

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

## Bugs in the simulator and tests

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
