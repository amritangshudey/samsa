# 0001 — Deterministic core: the driver calls handlers, the core calls injected interfaces

Status: accepted, 2026-11-10

## Context

The project's central claim is that any failure found under simulated faults
reproduces from a seed. That requires an execution to be a pure function of
the seed. A server's nondeterminism comes from time, network, disk,
randomness and scheduling.

## Decision

A node is a single-threaded object implementing `sys.Handler`. A driver calls
one handler at a time, to completion. While handling an event the node makes
plain synchronous calls on `sys.Clock`, `sys.Network`, `sys.Disk` and
`sys.Random`, which are passed in. A handler that returns an error declares
the node dead.

The production driver is an event loop fed by I/O goroutines. The simulated
driver is a scheduler that chooses the next event from a seeded generator and
owns virtual time.

## Alternatives

- **Pure `step(event) -> []effect`.** Would allow simulating slow disk
  operations interleaved with other events. Rejected: every disk read becomes
  a suspension point and the storage engine turns into hand-written
  continuations.
- **Goroutines under a controlled scheduler.** Rejected: needs a patched
  runtime or third-party simulator and depends on runtime internals.
- **`testing/synctest`.** Virtualises time but not scheduling order, so runs
  are not a function of a seed.

## Consequences

- Disk calls block the node. Throughput depends on group commit, triggered by
  a zero-delay timer. To be measured in Phase 7.
- Faults are injected at call boundaries. A crash can fall before, inside or
  after any disk call, but two disk operations on one node never overlap.
- Bugs in the real driver and real I/O implementations are outside the
  simulator's reach and need integration tests and the race detector.
