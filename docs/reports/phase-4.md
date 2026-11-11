# Phase 4 report — Consumer groups

Branch `phase-4`, 2026-11-11.

## What was built

- **Wire** (`internal/wire/groups.go`): codecs for FindCoordinator,
  JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit and
  OffsetFetch at their non-flexible versions (ADR 0008), in the round-trip
  and fuzz tests.
- **State machine** (`internal/broker/state.go`): three new commands,
  `OffsetCommit`, `GroupGeneration` and `GroupAssignment`. A commit's
  member and generation are checked in apply, at its place in the log
  (ADR 0009). Group state is part of the state hash.
- **Coordinator** (`internal/broker/groups.go`): the rebalance protocol as
  soft state on top: collecting JoinGroups until every member has rejoined
  or the rebalance timeout passes, choosing protocol and leader, holding
  SyncGroups for the leader's assignment, session timers, a deadline for a
  leader that never syncs, member ids from the injected random source, and
  the `MEMBER_ID_REQUIRED` round trip.
- **Node**: a fourth dependency, `sys.Random` (real: `realsys.Random`);
  a table of one-shot timers; one "withdraw the held request" hook per
  connection.
- **Simulator** (`internal/sim/groups.go`): a group-member client that
  joins, syncs, heartbeats, commits and reads offsets at random versions,
  and at random leaves, falls silent, commits as a zombie, or commits
  without membership. The model checks G1 to G4 and G6; G5 is the existing
  answer-time watchdog. A final pass reads every committed offset.
- **Mutation switch** `Config.UnsafeNoFencing`: commits are accepted from
  any generation.
- **Real clients** (`integration/groups_test.go`): the roadmap's
  acceptance scenario with franz-go and with two Java processes, a kcat
  group consumer, and a commit without membership.

## Results

| | |
|---|---|
| Unit tests, lint, race detector, fuzzing of node input and decoders | pass |
| 5000 simulator seeds | 0 failures |
| Group work in those seeds | 93 229 joins, 80 909 assignments received, 201 830 commits acknowledged, 311 004 offset fetches checked, 41 142 zombie commits refused |
| Broker without fencing | caught by 145 of 200 seeds, always as G4 |
| Broker that acknowledges before syncing (Phase 3's mutation) | still caught, 127 of 200 |
| Two further bugs put in by hand and removed again (every member given the first member's assignment; one accepted commit in seven not stored) | each caught within the first 10 seeds, as G1 and G3 |
| Checker against hand-made histories | 22 cases: each of G1 to G4 and G6 fires on a wrong history and accepts the legal look-alike |
| franz-go, two members, one cut off without leaving | survivor takes over and reads exactly the unread records: 500 records, none twice |
| Java client, two processes, one killed with SIGKILL | the same |
| Broker killed with SIGKILL under a stable group | both clients continue; no JoinGroup is sent after the restart |
| Bugs found by the simulator | none in the broker; M4 in the checker (`docs/BUGS.md`) |

Two bugs were found by reading the coordinator before any test ran: after
a generation was applied, session timers were cancelled for the *new*
members instead of the old (the code held a pointer that apply had already
overwritten); and a LeaveGroup naming two members could start a second
round while the first one's generation was still being proposed.

## How to run it

```
make test                # includes the coordinator tests
make sim SEEDS=5000
make integration         # needs kcat and a JDK
./bin/samsa -listen localhost:9092 -data ./data
kcat -b localhost:9092 -G mygroup mytopic
```

## What is untested or weak

- **The simulator found no broker bug in this phase.** The hand-made
  mutations show it can; the two bugs found by review show the code was
  not born correct. Both review bugs were on paths the unit tests reached
  at once, so the simulator never got the chance. Only one group mutation
  is permanent; Phase 6 should add the two that were tried by hand, and
  ones for the soft state (a round that never completes, a session timer
  that is never re-armed).
- **Liveness is checked only as "answered within 10 s".** Nothing checks
  that a group converges: that after faults stop, members end up stable
  with every slot owned. A coordinator that kept every group rebalancing
  for ever would pass.
- **Simulated members all offer the same single protocol.** Protocol
  choice with differing lists is covered by a unit test only.
- **One Java test run in 34 failed and could not be reproduced.** See the note at the end of this report.
- **Real-client coverage is narrow**: one version of each client, default
  settings, at most two members, no static membership, no
  `group.protocol=consumer`, no client-side assignor other than the
  defaults.
- **No DescribeGroups or ListGroups**, so `kafka-consumer-groups.sh` and
  similar tools cannot see groups.
- **Nothing is ever deleted**: groups, offsets, and the soft record of a
  group id that was only ever probed. A client can grow the broker's
  memory by inventing group ids.
- **A member that left or expired can still commit** until the next
  generation is applied (ADR 0009). Kafka refuses at once.
- **Session timers restart in full on a broker restart**, so a member that
  died just before is noticed up to one session timeout late.
- **A v0–v3 JoinGroup whose response is lost** leaves a member nobody
  knows about until its session expires. This is the flaw KIP-394 fixed;
  current clients do not use those versions.
- **Under `interval` and `never`, a power loss may roll a group back** to
  an earlier generation and earlier offsets. The model accepts this; no
  mutation proves it would reject anything worse.
- The design review was again a second pass by the same author.

## Ten questions

1. Why does a consumer group need a coordinator at all? What breaks if
   consumers just agree among themselves?
2. JoinGroup and SyncGroup: what does each step decide, and why is the
   assignment computed by a client and not by the broker?
3. What is a generation, and trace exactly how it stops a consumer that
   was paused for a minute from overwriting a newer commit.
4. Why is the generation check done in `apply` and not in the request
   handler? Give the interleaving that the handler-only version gets
   wrong.
5. Which parts of group state are commands and which are soft? For each
   soft part: what happens to it on restart, and why is that safe?
6. The broker is killed and restarted under a stable group. Why does no
   client rejoin? What would have forced a rebalance?
7. Session timeout, rebalance timeout, `max.poll.interval.ms`: what does
   each bound, and which one fires when a consumer's processing hangs but
   its heartbeat thread lives?
8. A member holds a JoinGroup open for the whole rebalance. Why can it not
   heartbeat meanwhile, and how does the broker avoid expiring it?
9. Invariant G3 says a fetch returns an acknowledged commit "unless a
   later one could have replaced it". Two commits race on different
   connections: which answers are legal, and how does the checker decide
   without knowing the log order?
10. What does `MEMBER_ID_REQUIRED` fix? Describe the failure in the older
    protocol versions.

## Note: one unexplained test failure

`TestJavaConsumerGroup` failed once, in the second of three consecutive
runs of `make integration`. The output of that run was filtered down to
pass/fail lines before anyone looked, so the message is lost. It ran for
the same 8 s as a passing run, which means every phase of the test
completed and one of the last checks failed: either a record was delivered
twice, or the Java client sent a JoinGroup after the broker restart.
It did not happen again in 33 further runs of that test (and 28 of the
franz-go one). A failing run prints the broker's full log, so the next
occurrence, locally or in CI, can be diagnosed. Until it is reproduced and
explained, treat "no rebalance after a broker restart" for the Java client
as usually true, not proven.
