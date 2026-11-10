# Bugs found by simulation

One entry per bug: the seed, the violated invariant, the root cause, the
fix. Bugs in the simulator or the tests themselves are listed separately at
the end, because a model that is wrong in either direction matters too.

## Bugs in the system

None yet. Phase 1's storage engine passed the crash property test once the
model bug below was fixed. That is weaker evidence than it sounds: see
"what is untested or weak" in `docs/reports/phase-1.md`.

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
