# Phase 6 report — Cluster simulation

Branch `phase-6`, 2026-11-11. Stacked on `phase-5` (PR #6), itself on
`phase-4` (PR #5); neither is merged yet.

## What was built

- **A hostile network between simulated nodes** (`internal/sim/
  network.go`): per message drop (0, 1, 10 or 30 %), duplication (0, 1 or
  10 %) and random delay, so messages reorder; partition scenarios
  (isolate a node, cut one link one way, a bridge where one node sees
  both sides of a split), each healed after 50 ms to 2 s.
- **Swarm draws per seed**, added to Phase 5's: the network settings
  above, partitions on or off, clusters of 1, 3 or 5 nodes, node faults
  every 20 ms to 2 s, one Raft entry per message in a third of the seeds.
  Faults and partitions hit the leader half the time.
- **Quiet period checks**: L1 (a leader is elected, all nodes catch up,
  and a new write is acknowledged within 5 s) and L2 (all nodes converge
  to the same log and state hash). N1 was already covered by I3 and the
  high-watermark floor. `TestLivenessChecksFire` shows L1 fires.
- **A leader lease** (ADR 0013), because the new faults found stale
  reads: followers ignore candidates for an election timeout after
  hearing from a leader, and a leader answers clients only while a
  majority answered it within ¾ of that.
- **The mutation suite** (`TestMutationSuite`): seven deliberate bugs,
  each caught by the simulator within 2000 seeds and confirmed by
  rerunning the catching seed without it; the eighth is caught at the
  Raft level (below).
- **A regression seed list** (`internal/sim/regressions.txt`, run on
  every push) and **a nightly CI job** of 30 000 seeds from a different
  first seed each night.

## Results

| | |
|---|---|
| Simulator, 5000 seeds, full fault model | 0 failures |
| Work in those seeds | 11.5 M records acknowledged, 78 M read back and checked, 126 098 power losses, 126 722 kills, 15 228 partitions, 64 589 leaders elected, 4.1 M requests refused by a non-leader and retried, 629 145 replica state comparisons |
| Simulator seeds run in this phase, in total | about 21 000 |
| Raft random schedules, with the lease | 15 000 seeds, 0 failures (and 20 000 in Phase 5) |
| Speed | 35 seeds/s on this 10-core laptop, 10.4 seeds/s on one core; 5000 seeds in 310 s |
| Profile | about half the time is the Go runtime (garbage collection, idle workers); reading the log to replicate it is the largest share of our own code (13.5 %). A lighter GC setting did not help. Nothing was optimised |
| Real clients, all earlier tests, and three processes with two leader kills | pass; new leader after 0.63 s; no record lost or duplicated |
| Bugs found in the system | 2 (S4, S5), both stale answers from an isolated leader |

**The mutation suite**, and what caught each:

| Mutation | Caught by | First seed |
|---|---|---|
| grant a vote before it is on disk | S1, two leaders in one term | 1120 |
| grant a vote comparing log length only | S3, a committed entry replaced | 35 |
| truncate on a message that merely carries fewer entries | I1/I3, a log end moved back | 4 |
| acknowledge entries before they are synced | S3 | 1 |
| serve as soon as elected | a valid write rejected (unknown topic) | 1 |
| answer reads without the lease | I1/I3 | 1 |
| accept offset commits from any generation | G4 | 1 |
| commit an earlier term's entry by counting (Figure 8) | **not caught by the simulator**; caught by the Raft random schedules, S3 | 430 (Raft level) |

Phase 5's switches remain in the Raft-level suite, which catches all five
of its mutations.

## How to run it

```
make test                               # includes the mutation suite and the regression seeds
make sim SEEDS=5000
make sim SEED=937                       # replay one seed with its full trace
go test ./internal/sim -run TestMutationSuite -v
go test ./internal/raft -run TestRandomSchedules -raft-seeds 20000
```

## What is untested or weak

- **Figure 8 is caught only at the Raft level.** The mutation's code
  path runs about eight times per simulator seed, and never does harm in
  2000 seeds: harm needs five events in a row (two leaders each crashing
  with an entry nobody else has, the first returning to push its entry to
  a majority, the message with its own no-op lost, and its death before
  the next heartbeat). Five measures to provoke it are in and did not
  suffice. A targeted scenario, or a model checker for the Raft core,
  would close this.
- **The vote-before-persist mutation needs about 1100 seeds**, so a
  change to the swarm can push it past the 2000 budget.
- **The lease assumes clock rates differ by less than 25 %** over an
  election timeout. The simulator gives every node the same clock and
  cannot show what happens with drift.
- **Elections are slower**: followers ignore candidates for an election
  timeout after the leader's last message.
- **Clients are never partitioned from nodes**, only nodes from each
  other. A client that cannot reach the leader is not simulated.
- **Regression seeds rarely replay their original schedules**, because
  each phase changes what a seed draws. The mutation suite, not the
  list, is what keeps each fix checked.
- **The per-push CI budget is unmeasured on CI.** 3000 seeds took 5 min
  there before this phase's heavier draws; the first push will tell.
- **One mutation from the design was dropped** as unreachable (a
  follower committing past what a message confirmed); the argument is in
  the design doc, section 8, and is reasoning, not a test.
- **No check-quorum**: an isolated leader keeps its role and refuses
  requests until the partition heals; clients go back to Metadata, which
  that node answers without naming itself.
- **A peer that lies about leading is not survived.** Fuzzing found that
  an AppendEntries contradicting committed entries, claiming the current
  or a later term, makes a node stop with an error. That is deliberate
  (it proves a broken peer), and `FuzzStep` now allows exactly that. It
  means one faulty node can stop the others: Byzantine faults are out of
  scope, and there is no authentication between nodes (ADR 0010).
- The design review was again a second pass by the same author.

## Ten questions

1. Why is the network model here weaker than TCP, and what in real
   peer connections reorders or duplicates anyway?
2. Swarm testing: why do runs with every fault enabled at once find less
   than runs with a random subset?
3. Raft makes the log safe. Why was it not enough to make a fetch
   answered by the leader safe? Give the interleaving of S4.
4. Why did S5 need the lease for writes, when writes go through the log?
5. Derive the lease: why ¾ of an election timeout, what each side must
   promise, and why the clocks need equal rates but not equal readings.
6. Why must a node that has just started ignore candidates too?
7. Compare the lease with ReadIndex and with reads through the log: what
   does each cost and what does each assume?
8. L1 is checked only after faults stop and all nodes are up. Why not
   during the faults, and what liveness problem could still hide?
9. Why does the mutation suite rerun the catching seed without the
   mutation? What would have happened in this phase without it?
10. Figure 8 is caught by one test layer and not the other. What do the
    two layers do differently, and what would you add to the simulator
    to catch it there?
