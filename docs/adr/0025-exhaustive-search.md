# ADR 0025: Exhaustive breadth-first search of three Raft nodes

Date: 2026-11-14. Status: accepted. Design: `docs/design/b2-exhaustive.md`.

## Context

The simulator and the random Raft schedules sample schedules. Production
plan item 11 asks for small configurations to be checked exhaustively
within a bound, and for a mutation to be found with a shortest
counterexample.

## Decision

- Search `raft.Raft` itself: three nodes, breadth-first, every enabled
  event from every state. Prune by a fingerprint that includes the ghost
  state the properties read.
- Bound by per-node budgets (elections, ticks, heartbeats, proposals,
  power losses, losses, duplicates) and by the number of events per path.
  `go test` runs depth 10; a CI job runs depth 12, with one power loss
  or one loss per node, and the shortest-counterexample search.
- Give each node its own clock, and start every node aged past the
  start-up lease.
- Deliver dead messages at once: responses from an older term, which
  `Step` ignores and always will.
- Copy states by reflection instead of replaying them.
- Require evidence that the search is not vacuous: some state must have
  committed a proposal.
- No partial-order reduction.

## Alternatives

- **A one-node partial-order reduction:** built, then found unsound
  (BUGS.md M11).
- **A sound reduction** (stubborn sets, DPOR): its own project.
- **TLA+ on a specification:** it checks a model, not this code.
- **Replaying every child:** simple, but quadratic in depth.

## Consequences

- Within the bounds, no schedule violates S1, S2, S3 or S5. The bounds
  are short: one election and one commit, with one fault at most.
- `AckBeforeSync` is found with its shortest counterexample, 11 events.
  The other Raft mutations remain mainly the random tests' job (report).
- Snapshots, compaction, membership changes and transfers are outside
  the search.
