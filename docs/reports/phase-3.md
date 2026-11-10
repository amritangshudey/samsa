# Phase 3 report — Simulator v1

Branch `phase-3`, 2026-11-10. This completes the MVP (Phases 0 to 3).

## What was built

- `internal/sim`, about 1500 lines plus tests, itself subject to the core
  purity check (no goroutines, no real time, no map-order dependence):
  - `world.go`: a virtual-time event queue; the node's clock and network;
    connections as two ordered byte streams cut into random fragments,
    often single bytes; node lifecycle; fault injection.
  - `client.go`: clients that encode real Kafka requests at a random
    supported version each time. Producers (several partitions and batches
    per request, pipelining, `acks` of -1, 1, 0 and an invalid value,
    damaged and oversized batches, unknown topics and partitions),
    consumers (random offsets, byte limits and long-poll settings),
    ListOffsets, Metadata, ApiVersions including unsupported versions, and
    a vandal that sends noise.
  - `history.go`: the model and invariants I1 to I5 (ADR 0007).
  - `sim.go`: swarm configuration from the seed, the run, the final full
    read of every partition, and the restart check (I6).
- Faults: power loss at an arbitrary instant; power loss at the n-th disk
  operation (inside handlers and inside recovery); process kill; failed
  sync; client disconnects.
- Broker changes: `Node.StateHash`; `Config.UnsafeAckBeforeSync`, a switch
  that breaks the commit rule so the simulator can prove it notices; and
  the fix for bug S1.
- `make sim`, a CI job running 3000 seeds, ADR 0007, design doc.

## Results

| | |
|---|---|
| 5000 seeds, about 65 s on 10 cores | 0 failures after the fixes below |
| Work done in those seeds | 11.1 M requests, 22.5 M records acknowledged, 154 M records read back and checked, 88 154 power losses, 25 413 kills, 21 013 failed syncs, 17 842 recoveries interrupted by a further power loss |
| Broker that acknowledges before syncing | caught by 134 of 200 seeds |
| Determinism | 20 seeds run twice: identical trace hashes and counts |
| Bugs found | S1 in the broker; M3 in the checker (`docs/BUGS.md`) |

## How to run it

```
make sim                 # 1000 seeds
make sim SEEDS=5000
make sim SEED=4          # one seed, with every event and the node's logs
go test ./internal/sim   # 200 seeds, determinism, mutation, checker tests
```

A failure prints the seed, the configuration the seed chose, the violated
invariant and the command that replays it.

## What is untested or weak

- **One real bug in 5000 seeds is a modest haul.** Either Phases 1 and 2
  are sound, or the simulator is weaker than it looks. Evidence for the
  first: one deliberate bug is caught by two seeds in three. Evidence
  still missing: only one mutation exists. Phase 6 plans at least eight;
  until then, bugs in rarely reached code could pass.
- **The model's weak-policy rules are intricate** (M3 was a mistake in
  them), and the mutation test only covers the strict policy. Nothing
  proves the simulator would catch a broker that loses a non-suffix under
  `interval`.
- **Single node.** No partitions, no message loss or reordering between
  nodes, no clock skew. The network never loses bytes inside a live
  connection (TCP does not either), and never applies backpressure: a slow
  client and the output-buffer limit of the real loop are not modelled.
- **`realsys` is outside the simulator.** Its evidence is the race
  detector and the real-client tests.
- **The disk model's kindnesses remain**: synced data never rots, directory
  operations persist in order.
- **Clients never retry**, so duplicates from retries are not modelled, and
  every error response is treated as "not written". Both must change with
  replication (ADR 0007).
- **The vandal could, with negligible probability, emit a valid request**
  and cause a false alarm. Not observed.
- **Determinism is checked on 20 seeds per test run**, not on all, and by
  a 32-bit hash.
- **Virtual time per seed is 10 s.** Bugs that need a long uptime (many
  segment rolls with the 1 MiB segment setting, counters wrapping) are out
  of reach.
- **The clock never jumps** across a restart, although the contract allows
  it.
- The design review was again a second pass by the same author.

## Ten questions

1. What is the test oracle problem, and why does the simulator check a
   history of client observations instead of the broker's state?
2. A fetch response shows a high watermark lower than an offset already
   acknowledged to another client. Is that a bug? What do you need to know?
3. Why are there two kinds of node failure (kill and power loss), and
   which bugs does each one find that the other cannot?
4. Under `-sync interval`, what exactly may the broker lose in a power
   loss, and how does the model check "that and nothing else"?
5. How is the acknowledge-before-sync bug caught? Trace the events from
   the acknowledgement to the failing check.
6. What is swarm testing, and why would enabling every fault in every run
   find fewer bugs?
7. What makes the simulation deterministic, and what would silently break
   that? How would you notice?
8. Why is the network delivering one byte at a time a useful fault, and
   what real behaviour does it stand for?
9. Bug S1: why is "the node stops on a fatal error" not enough?
10. 5000 green seeds: what does that prove, what does it not prove, and
    what would raise your confidence?
