# C1 report, part a: membership changes in Raft, the broker and the simulator

Branch `membership`, 2026-11-14. Production plan item 9, part a.
Design: `docs/design/c1-membership.md`. ADR 0022.

## What was built

- **Raft** (`internal/raft/membership.go`):
  - configuration entries in force when appended and falling back when
    truncated, and the configuration in the snapshot (version 2, version 1
    still read);
  - learners that catch up before voting;
  - `AddLearner` and `ChangeMembers` with the leader's rules, including
    Ongaro's 2015 erratum;
  - a removed leader that leads until the removal commits, then steps
    down;
  - mutations `MutTwoChanges` and `MutChangeBeforeCommit`.
- **Broker** (`internal/broker/membership.go`):
  - AddRaftVoter and RemoveRaftVoter, answered when committed;
  - Metadata, DescribeCluster and the leader lookup from the voters in
    force;
  - `Config.Join` for a node that joins.
- **Simulator** (`internal/sim/voters.go`): half the multi-node seeds add
  two spare nodes and a client that adds and removes voters through the
  Kafka APIs, with timeouts from 20 ms to 3 s, under every fault the run
  already had.

## Results

- **3000 simulator seeds pass.** Multi-node seeds change membership with probability one half,
  with 1812 voter changes acknowledged.
- **5000 random Raft schedules pass.** Half of them add and remove voters
  among five nodes.
- **Mutations:**
  - `MutTwoChanges` is caught by the random schedules (seed 135: a
    committed entry replaced).
  - `MutChangeBeforeCommit` is caught by `TestErratumCounterexample`,
    which plays out the erratum's counterexample 1. Without the mutation
    the change is refused.
  - Neither is reachable through the broker, which serialises changes and
    accepts them only while serving. Section "weak" below explains why.
  - The existing simulator mutation suite still catches every mutation it
    caught before.
- **Bugs found before merge:**
  - Seed 31: non-voters must stand for election (design, finding 10).
  - Seed 174: but not once their removal committed (finding 10).
  - Seed 181: a lone leader had no heartbeat for later peers (finding 11).
  - Seed 2287: a node alone by a change led at start, under a live lease
    (BUGS.md S9).

## How to run it

```sh
make test
make sim SEEDS=3000
go test ./internal/raft -run TestRandomSchedules -raft-seeds=5000
```

The binary cannot join a node yet (part 9b).

## What is untested or weak

- **The two Raft rules are defence in depth.** The broker never lets them
  decide, so only Raft-level tests show that they work.
- **Classic-mode seeds (no PreVote) suffer from disruptive removed
  nodes.** Terms without a leader rose from about 17,800 to 45,700 in 3000
  seeds. They are safe, but slower.
- **Part 9b is not built:**
  - no real-process test;
  - no `-join` flag;
  - the transport's address book is fixed;
  - DescribeQuorum is missing;
  - no directory ids.
- **Ids are never reused with a new disk.** A replacement uses a new id.
  The simulator follows that rule, and nothing in the binary enforces it
  yet.
- **Catching up is judged against the commit index,** not the
  dissertation's rounds. Under constant load a learner is "caught up" if
  it holds everything committed when asked, so the new voter may still
  lag by the uncommitted tail.

## Ten questions

1. Why does a single-server change need no joint consensus? Draw the
   majorities for 3 to 4 and 4 to 3.
2. Why is a configuration in force when appended rather than when
   committed, and what must a node do when that entry is truncated?
3. State Ongaro's 2015 erratum and walk through counterexample 1. Why
   does committing an entry of the current term first prevent it?
4. Why must a node outside its configuration still stand for election,
   and why must it stop once its removal commits (seeds 31 and 174)?
5. How does a leader that removes itself keep leading, and what does it
   count for commitment and for its lease meanwhile?
6. Explain BUGS.md S9. Why was the single-node shortcut safe before
   membership changes and not after?
7. Why does a joining node start with no configuration instead of the
   bootstrap list, and why must ids not be reused with a new disk?
8. Why does the simulator not catch MutTwoChanges, and where is it
   caught instead?
9. Why does the configuration in the snapshot leave the bootstrap one
   implicit?
10. What is a learner, when does it become a voter here, and how does
    that differ from the dissertation's round-based test?
