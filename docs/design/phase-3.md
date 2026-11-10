# Phase 3 design — Simulator v1

Status: approved 2026-11-10 under the standing delegation. Review findings
are in section 8.

## 1. Concepts

**Simulation as a test oracle problem.** Running the broker under random
faults is easy. Knowing whether what it did was *correct* is the hard part.
The simulator therefore has two halves: a world that drives the node, and a
checker that holds a model of what clients were promised.

**Virtual time.** Nothing sleeps. Events sit in a priority queue ordered by
virtual time; the loop pops the earliest and runs it. A minute of timeouts
costs microseconds, and the order of events is a function of the seed.

**History-based checking.** Clients record what they sent and what they
were told. The checker never looks inside the broker to decide correctness;
it judges only what a real client could observe. That keeps the invariants
valid when the implementation changes (Raft in Phase 5).

**Unknown outcomes.** If a connection dies before the response arrives, the
client does not know whether its write happened. The checker must allow
both, but still require all-or-nothing and at-most-once.

**Causality.** "After the write was acknowledged, a read must see it" is
only true for reads that *started* after the acknowledgement arrived. A
response already in flight may be older. Every check that compares a read
with earlier acknowledgements uses what the client knew when it sent the
request.

**Proving the checker.** A simulator that never fails proves nothing. Each
invariant has a unit test on a hand-made bad history, and a deliberately
broken broker must be caught from a seed.

## 2. What is simulated

One node (Phase 6 adds more), the Phase 1 simulated disk, and in
`internal/sim`:

- **Scheduler and clock.** A heap of `(time, sequence)` events. `SetTimer`
  with zero delay runs after everything already queued for that instant,
  matching the contract and the real loop.
- **Network.** Each connection is two ordered byte streams. Bytes are cut
  into random fragments (often one byte) with random delays, so framing is
  tested constantly. Order within a stream is preserved, as TCP does.
- **Clients** that speak real wire bytes through `internal/wire`, each
  request at a random supported version. This exercises the lower versions
  that real clients never choose (see ADR 0006).
  - producers: create topics, send batches of unique values to random
    partitions, sometimes several partitions and batches per request,
    sometimes a corrupted batch that must be rejected;
  - consumers: fetch with random offsets, limits and long-poll settings,
    and ask for the latest offset;
  - any client may drop its connection at any moment.
- **Faults.**
  - power loss at an arbitrary instant (unsynced data lost or torn);
  - power loss at the N-th disk operation from now, which lands inside a
    handler, including inside recovery;
  - process kill (the node is discarded but the disk keeps everything
    written, as the OS cache would);
  - failed fsync (the node must stop);
  - client disconnects.
  After a crash the node restarts after a random delay on the same disk.
- **Swarm configuration.** The seed chooses the fsync policy (mostly
  `always`), which faults are enabled and how often, the number of clients
  and the storage segment size (small, to force rolls). Seeds therefore
  differ in kind, not only in detail.

## 3. Invariants

"Strict" means the fsync policy is `always`.

| | Invariant | Checked |
|---|---|---|
| I1 | An acknowledged record is never lost: every later read (by causality) sees it at the acknowledged offset. Strict only; under other policies a power loss may remove a suffix, and nothing else. | on every fetch, and in a full read at the end |
| I2 | Offsets are dense: a fetch returns consecutive batches starting with the one containing the requested offset; acknowledged offsets never overlap. | on every fetch and acknowledgement |
| I3 | A record, once observed at an offset, never changes (strict), and the high watermark never moves backwards (strict, by causality). | on every fetch |
| I4 | Every record read was sent by some client, to that partition, exactly once in the log (no phantoms, no duplicates). | on every fetch |
| I5 | A write the broker rejected with an error is never readable. | on every fetch |
| I6 | Recovery is idempotent: restarting on an unchanged disk yields the same state hash. | at the end |
| I7 | Every request is answered or its connection closed within a bound of virtual time while the node is up. | continuously |
| D | Determinism: the same seed yields the same trace hash and the same counts. | separate test, a sample of seeds run twice |

At the end of a run faults stop, the node is brought up, and a verifier
reads every partition from offset 0 to the end, which makes I1 a statement
about the whole history and not only about what consumers happened to read.

## 4. Structure

```
internal/sim/
  world.go     scheduler, clock and network adapters, node lifecycle, faults
  client.go    connection handling and the producer/consumer/vandal behaviours
  history.go   the model and the invariant checks (no dependency on the world)
  sim.go       Config, Run, Result, swarm configuration, final checks
```

`history.go` takes plain events (sent, acknowledged, rejected, unknown,
observed, crashed) and returns an error on the first violation. It knows
nothing about the simulator, so Phase 6 and the black-box test of roadmap
item A4 can reuse it on histories from elsewhere.

The broker gains `StateHash` (for I6, and for replica agreement later) and
one switch, `UnsafeAckBeforeSync`, that breaks the commit rule on purpose.

## 5. Running it

```
make sim SEEDS=5000        # many seeds; a failure prints the seed
make sim SEED=1234         # one seed, with the event trace
```

CI runs a fixed budget of seeds on every push, the determinism check, and
the mutation check. A failure message contains the seed and the command.
Every bug found goes to `docs/BUGS.md` with its seed.

## 6. Alternatives rejected

- **Clients that call the node's Go methods directly.** Faster and simpler,
  but the decoders, framing and response ordering would sit outside the
  simulated system.
- **Checking by comparing with internal broker state.** Simple, and wrong:
  it validates the implementation against itself.
- **A general linearizability checker** (as in Jepsen's Knossos). The log's
  semantics are simpler than a register's: offsets give a total order, so a
  direct model is exact, far cheaper, and gives readable failure messages.
- **Real time with a fake clock.** Not deterministic (Phase 0 design).

## 7. Limits

- One node: no network partitions or message loss between nodes yet.
- The client model has no retries of unknown-outcome writes, so duplicates
  from retries (expected without the idempotent producer) are not modelled.
- The disk model's stated kindnesses (Phase 1) still apply.
- The simulator covers the core. `realsys` is outside it by construction.

## 8. Review record

Second pass, looking for ways the simulator could pass while the broker is
wrong, or fail while it is right:

1. **Stale responses.** Comparing a fetch with acknowledgements that arrived
   after the fetch was sent would raise false alarms; ignoring causality
   entirely would hide real loss. Each fetch carries a snapshot of what its
   client's world knew at send time.
2. **Weak policies after a crash.** Under `interval` and `never`, loss of an
   acknowledged suffix is permitted, so the model forgets its observations
   at a power loss in those modes, but still checks density, phantoms,
   duplicates and rejected writes.
3. **Vacuous success.** A seed in which nothing was acknowledged or nothing
   crashed passes trivially. The suite asserts totals across seeds (records
   acknowledged, fetches checked, crashes, recoveries interrupted).
4. **Map iteration in the world** would make runs irreproducible while each
   run still looked fine. Connections and partitions are visited in sorted
   order, and the determinism test runs every seed twice.
5. **A kill is not a power loss.** Treating every node failure as power
   loss would never test recovery over a log with an unsynced but intact
   tail. Both are modelled.
6. **I7's bound must exceed the longest legal wait** (the largest fetch
   `MaxWaitMs` a client uses), or long polls are reported as hangs.
