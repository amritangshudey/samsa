# B2 report: exhaustive checking of small configurations

Branch `exhaustive`, 2026-11-14. Production plan item 11.
Design: `docs/design/b2-exhaustive.md`. ADR 0025.

## What was built

- **An exhaustive search of three Raft nodes** (`internal/raft/explore_test.go`):
  - breadth-first, every enabled event from every state;
  - events: deliveries, losses, duplicates, election timers, ticks,
    heartbeats, proposals, power losses and restarts, each with a
    per-node budget, and a limit on events per path;
  - S1, S2, S3, S5 and "no node stops itself" checked after every event;
  - states pruned by a fingerprint that includes the ghost state;
  - responses Raft will always ignore are delivered at once;
  - each state is a deep copy of its parent (`deepcopy_test.go`), not a
    replay;
  - a non-vacuity check: some state must commit a proposal.
- **Per-node clocks** in the Raft test cluster (`cluster.ownClocks`), and
  nodes that start aged past the start-up lease.
- **`TestExhaustiveShortest`:** `AckBeforeSync` found with its shortest
  counterexample, 11 events.
- **A CI job, `exhaustive`,** for the larger bounds and the shortest
  counterexample.

## Results

| Bound (one election, one proposal per node) | Default | Classic |
|---|---|---|
| depth 10 (`go test`) | 35,266 states, 1.6 s | 63,973 states, 2.2 s |
| depth 12 | 196,107, 8 s | 256,104, 10 s |
| depth 12, one power loss each | | 2,880,972, 1 min 49 s |
| depth 12, one loss each | | 1,422,410, 1 min 10 s |

No violation in any of them. Times are on the 10-core laptop.

**`AckBeforeSync`,** classic Raft, one election and one power loss per
node: found after 409,296 states in 26 s. The shortest path is 11 events.

1. Node 1 wins term 1 and replicates its no-op to node 2.
2. Node 2 answers before syncing.
3. Node 2 loses power and restarts without the entry.
4. Node 2 wins term 2 with node 3's vote.
5. Node 2's AppendEntries asks node 1 to replace an entry it counted
   committed.

**Other mutations:** the search did not finish, so they were not found.

- **Settings:** a vote not persisted (`vote-unpersisted`), or commit by
  counting replicas of any term (`commit-any-term`); classic Raft; two
  elections and one power loss per node; depth 14.
- **Result:** neither search finished in 15 minutes.
- **What a violation needs:** at least two terms and a well-placed crash,
  which puts it deeper than the search reaches.
- **Who catches them:** the random schedules
  (`TestRandomSchedulesCatchBrokenRaft`) and the simulator's mutation
  suite.

## Problems met

Two of them are in BUGS.md as M11.

- **The first search did not terminate in the default configuration.**
  A heartbeat could fire whenever nothing from its node was in flight,
  so answers piled up one copy per round. Heartbeats now have a budget.
  Responses that Raft will always ignore are delivered at once.
- **The one-node partial-order reduction was unsound.** It cut the
  default bound from 2.8 million states to 986. Both searches reported
  no violation, so the verdicts agreed. Comparing what each reached did
  not: in one configuration, the full search reached 793 distinct node
  states and the reduced one 63. The reduction never let the chosen node
  wait for a message another node had yet to send. It is gone.
- **The bound was vacuous.** With fresh per-node clocks, no vote was
  ever granted within one election per node, because a node ignores
  candidates for an election timeout after start. Raising proposals from
  1 to 2 left the state count unchanged, which gave it away. Nodes now
  start aged, and the test demands a committed proposal.
- **The design's first second review was wrong.** It accepted the
  reduction's argument and named a different step as the weakest. The
  design now records that (review, question 7).

## How to run it

```sh
go test ./internal/raft -run '^TestExhaustive$' -v                    # depth 10, both configurations
go test ./internal/raft -run '^TestExhaustive$' -v -explore-depth 12 -explore-crashes 1 -explore-mode classic
go test ./internal/raft -run '^TestExhaustiveShortest$' -v -explore-shortest
go test ./internal/raft -run '^TestExploreMutation$' -v -explore-mutation vote-unpersisted -explore-crashes 1 -explore-mode classic
```

The flags `-explore-elections`, `-ticks`, `-heartbeats`, `-proposals`,
`-crashes`, `-drops`, `-dups`, `-depth` and `-mode` set the bound.

## What is untested or weak

- **The bounds are short.** Depth 12 holds one election and one commit,
  with one fault at most. Most of Raft's hard cases (several terms,
  crashes on more than one node, a returning partition) are beyond it.
  They are the random schedules' and the simulator's job.
- **No reduction.** Without a sound partial-order reduction, the state
  count grows by 1.25 to 2 times per event, and deeper bounds are out of
  reach. A sound one (stubborn sets or DPOR) is the way to go deeper.
- **Raft only.** Snapshots, compaction, membership changes, leadership
  transfers and the broker are outside the search.
- **One crash outcome per path.** The simulated disk decides what a power
  loss keeps; torn writes are not enumerated.
- **The default configuration (PreVote and CheckQuorum) is searched only
  at depths 10 and 12.**

## Ten questions

1. **What does "the search completes within its bound" claim, exactly?**
   No schedule of at most 10 (or 12) events, within the per-node budgets
   and from nodes started aged, violates S1, S2, S3 or S5 for three
   nodes.
2. **Why breadth-first?** The first violation found is then a shortest
   one. Depth-first finds one sooner but not the shortest.
3. **Why is the ghost state in the fingerprint?** Two states that look
   the same now may differ in history, such as who led term 2. Merging
   them could hide an S1 or S3 violation.
4. **Why was the one-node reduction unsound when events of different
   nodes do commute?** Another node's event can enable a new event for
   the chosen node: a message to it. The reduction made the chosen node
   act before that message could exist.
5. **How was it caught?** By comparing the distinct node states each
   search reached. The verdicts agreed: both found nothing.
6. **Why must nodes start aged?** A node ignores candidates for an
   election timeout after it starts. Only its own events move its clock,
   so with one election per node no vote was ever granted.
7. **Why deliver dead messages at once?** Raft ignores a response from an
   older term now and forever, because terms never go down. Leaving them
   in flight multiplies states without changing behaviour.
8. **Why is the counterexample 11 events and not fewer?** Breadth-first
   order shows that no path within the budgets is shorter. Losing a
   committed entry needs a commit (5 events), a crash and restart (2),
   and a second leader who lacks the entry (4).
9. **Why copy states instead of replaying paths?** A replay costs the
   whole path for every state. The copy is checked to behave exactly as
   the original does.
10. **What would let the search go deeper?** A sound partial-order
    reduction that includes the receives of every node that may still be
    sent a message, symmetry between node ids, or a better encoding of
    the messages in flight.
