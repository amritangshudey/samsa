# 0007 — The simulator judges client-visible history, with causality

Status: accepted, 2026-11-10

## Context

A simulator needs an oracle: something that decides whether what the broker
did under faults was correct. The oracle is the part most likely to be
subtly wrong, in either direction, and it has to survive the replacement of
the single-node commit path by Raft in Phase 5.

## Decision

1. **Clients speak wire bytes and record a history**: what they sent, what
   they were told, and when. A model (`internal/sim/history.go`) checks the
   history. It never reads the broker's internal state to decide
   correctness. The one exception is I6, which compares the node's state
   hash before and after a restart, a statement about the node and not
   about clients.
2. **The model is exact, not a general linearizability checker.** A
   partition is a totally ordered log, so "what must offset N hold" has one
   answer, and checking is a table lookup with a readable error.
3. **Causality by snapshot.** A response is held only to what was known
   when its request was sent: each request carries the log end the model
   expected at that moment (the floor) and the number of power losses so
   far (the epoch).
4. **The promise depends on the fsync policy.** Under `always`, nothing
   acknowledged may ever be lost. Under `interval` and `never`, a power
   loss may cut a suffix of a partition and nothing else: lost records must
   stay lost, and whatever survives is unchanged.
5. **Swarm configuration.** The seed chooses which faults exist in a run
   and how often, the fsync policy, the segment size and the client mix.
6. **The checker is tested from both sides**: hand-made bad histories it
   must refuse (`TestModel`), and a deliberately broken broker it must
   catch from seeds (`TestSimCatchesAckBeforeSync`).
7. **The suite asserts it did work**: minimum totals of acknowledged
   records, reads, crashes and interrupted recoveries.

## Alternatives

- **Compare against internal state.** Validates the implementation against
  itself, and breaks when the implementation changes.
- **Record histories and run a generic checker** (Knossos, Porcupine). A
  dependency, exponential in the worst case, and it reports "not
  linearizable" where this model says which record at which offset.
- **Ignore responses that cross a fault.** Simple, and blind exactly where
  the acknowledge-before-sync bug shows: an acknowledgement in flight when
  the power fails.
- **Every fault enabled in every run.** Constant crashes keep the log
  short and starve the long-running cases.

## Consequences

- The checker moves to Phase 6 unchanged, and can judge a history recorded
  from real clients against the real binary (roadmap item A4).
- The model must be told what the client could know. Retries of writes
  with unknown outcome would be reported as duplicates; clients therefore
  never retry, and the model has to learn about retries before the
  idempotent producer (roadmap A2) can be tested.
- Error responses are treated as "definitely not written". That is true on
  one node. With replication, a timeout means "unknown" and the clients
  must classify error codes accordingly (Phase 5).
