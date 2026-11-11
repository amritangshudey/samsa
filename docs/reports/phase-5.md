# Phase 5 report — Replication with Raft

Branch `phase-5`, 2026-11-11. Stacked on `phase-4` (PR #5), which is green
and not yet merged.

## What was built

- **`storage.TruncateSuffix`**: removes the end of the log durably, with
  crash tests in the Phase 1 style and a mutation switch.
- **`internal/raft`**: Raft as a pure state machine. The hard state file
  (term and vote, replaced atomically); message codecs; elections,
  replication, the current-term commit rule, a no-op on election;
  followers answer only after their sync. It is driven by three calls:
  `Step` (a peer message), `OnTimer`, `Flush`.
- **The broker on Raft** (`internal/broker/node.go`): the node proposes
  through Raft and applies what Raft reports committed. It serves clients
  only while it leads and has applied an entry of its own term. The
  Phase 2 local log is gone: one node is a cluster of one.
- **Not-leader answers** (`leader.go`, ADR 0011): each API's retriable
  error from any node that is not serving; Metadata and FindCoordinator
  name the leader from any node.
- **Peer transport** (`internal/realsys/peer.go`): TCP between nodes, a
  hello frame, bounded queues, reconnection. `sys.Network.SendPeer` and
  `sys.Handler.OnPeerMessage` are the two new lines of the contract.
- **`-peers`**: `samsa -node-id 1 -peers 1=host:9092/host:9192,2=...`.
- **Simulator**: one or three nodes per seed, each with its own disk and
  faults; clients connect to any node and follow not-leader answers; a
  monitor checks Raft's safety from inside the nodes (S1 to S5).
- **ADRs** 0010 (one Raft group), 0011 (not-leader answers), 0012
  (persist before send; fsync policies in a cluster).

## Results

| | |
|---|---|
| Unit tests, lint, race detector | pass |
| The roadmap's twelve expected bugs | a test for each; 1 and 2 also as mutation switches |
| Raft random schedules (3 or 5 nodes; loss, duplication, reordering, power loss) | 20 000 seeds, 0 failures; checks one leader per term, log matching, committed entries never change, terms never decrease, and that the healed cluster converges |
| The three deliberate Raft bugs under those schedules | each caught: any-term commit at seed 166, vote before persist at seed 82, acknowledge before sync at seed 0 |
| Fuzzing the message decoder and `Step` | about 3.5 and 1.5 million inputs in 15–20 s each, no finding |
| Crash tests | `TruncateSuffix` 5000 seeds, hard state file 2000 seeds |
| Simulator, 5000 seeds, one- and three-node clusters | 0 failures |
| Work in those seeds | 23.0 M records acknowledged, 165 M read back and checked, 85 618 power losses, 60 248 kills, 60 930 leaders elected, 2.03 M requests refused by a non-leader and retried elsewhere, 319 807 replica state comparisons |
| Broker that acknowledges before syncing | caught by 180 of 200 seeds |
| Broker that votes before persisting | caught by 5 of 1000 seeds (the test runs the first 200, which contain one) |
| Broker without fencing (Phase 4) | still caught, 140 of 200 |
| **Broker that commits earlier-term entries by counting (Figure 8)** | **not caught by the simulator: 0 of 1000 seeds** |
| Three processes, franz-go, leader killed twice with a restart between | every acknowledged record read back, in order; 0 duplicates in the runs made; with one node of three left, nothing is acknowledged |
| Time to a new leader after SIGKILL (two runs) | 0.76 s and 1.32 s until another node is named leader, with the default 300–600 ms election timeout |
| All earlier real-client tests (franz-go, Java, kcat) against one node on Raft | pass |

Bugs found (details in `docs/BUGS.md`):

- **S2**, by the simulator: a leader would never re-send entries below
  what a follower had acknowledged, so a follower that lost acknowledged
  entries could not be repaired and the two exchanged messages for ever.
- **S3**, by the random schedules: the answer "you are superseded" could
  be taken for a refusal by a node that had since become leader.
- **M5**, simulator harness: a node whose start failed was called again.
- **M6**, test: the random test read logs of powered-off nodes.

"Every earlier test unchanged" (roadmap task 7) held with two edits, both
because an election adds a no-op entry: the durable index expected by the
fsync-policy test, and the simulator's restart invariant, which counted
applied entries.

## How to run it

```
make test
make sim SEEDS=5000
go test ./internal/raft -run TestRandomSchedules -raft-seeds 20000
make integration                     # includes the three-node test

./bin/samsa -node-id 1 -listen localhost:9092 -data d1 \
  -peers 1=localhost:9092/localhost:9192,2=localhost:9093/localhost:9193,3=localhost:9094/localhost:9194
# the same with -node-id 2 -listen localhost:9093 -data d2, and 3
kcat -b localhost:9092,localhost:9093,localhost:9094 -L
```

Data directories written by earlier phases are not read: the log moved
from `log/` to `raft/` and its entries have a new header.

## What is untested or weak

- **The simulator does not catch the Figure 8 bug.** The rule is tested
  step by step and by the random schedules in `internal/raft`, but the
  whole-system simulator passed 1000 seeds with the rule removed. Its
  network between nodes delays and never loses, so a leader's entry
  almost never reaches one follower and not the other. Phase 6 adds loss
  and partitions; this mutation must be caught there, and the test for it
  is the first thing to write.
- **The vote-before-persist bug is caught rarely** (5 in 1000). The test
  passes on one seed. A change to the simulator can move that seed.
- **Weak fsync policies in a cluster are not tested under power loss.**
  ADR 0012 says what should happen (acknowledged writes can be lost,
  replicas can diverge, a node that notices stops). The simulator only
  kills processes in that configuration.
- **No partitions, no message loss between nodes in the simulator.** So:
  no evidence about a leader that is cut off. There is no check-quorum:
  such a leader keeps accepting writes it can never commit, and their
  clients wait until it hears of a new term. It may also answer fetches
  with a stale end of log in that window.
- **Liveness is thin.** The simulator requires answers within 10 s and
  that, once faults stop, all nodes return, elect a leader and agree. It
  does not measure how long the cluster is without a leader during
  faults.
- **Only franz-go was run against three nodes.** The Java client and kcat
  were run against one node only, so the not-leader codes are verified
  against the official error table but not against those clients'
  behaviour.
- **Failover as a client sees it is slow**: in both runs measured, the
  franz-go producer needed about 5 s to continue after the leader died,
  though a new leader existed after 0.76 s and 1.32 s. The cause was not
  investigated; the client's metadata refresh is the suspect.
- **Held requests are dropped by closing connections** on a leadership
  change. Correct, and rough on clients.
- **The peer transport trusts the id a connection announces.** No
  authentication, no TLS. It was tested for delivery, reconnection and
  rejecting strangers, not under load or slow peers.
- **The log only grows**; a restarted node applies it from the start; a
  replaced node receives all of it, one bounded message at a time.
  Membership cannot change.
- **S3 and S4 are partial while the cluster runs** (terms only; sampled).
  They are complete when it settles, which every seed does at least
  three times at its end.
- **Metadata from a follower may be behind.**
- **The Phase 4 flake** (`TestJavaConsumerGroup`, once in 34 runs) is
  still unexplained. It did not occur in this phase's runs of the suite.
- The design review was again a second pass by the same author.

## Open decisions for you

1. **Should a cluster refuse to start with `-sync=interval` or `never`?**
   It currently starts, and a power cut can then break replica agreement
   (ADR 0012). Refusing is one line.
2. **PR #5 (Phase 4) is green and unmerged**: merging was denied to me by
   the permission system. Phase 5 is stacked on it.

## Ten questions

1. What does "committed" mean in Raft, and why is "stored on a majority"
   not enough? Walk through Figure 8.
2. Why must a node write its vote to disk before answering? Construct the
   two-leaders-in-one-term failure, crash included.
3. A follower receives an AppendEntries that carries fewer entries than
   it already holds. When must it truncate, and when must it not?
4. Why does a new leader append a no-op, and why does this broker refuse
   clients until that no-op is applied?
5. The leader counts its own log towards the majority only up to its
   synced index. What goes wrong if it counts everything it has appended?
6. `matchIndex` never decreases, yet bug S2 was fixed by letting the
   leader send entries below it. Why are both right?
7. What does a client see when the leader dies in the middle of its
   produce request? Why can the record end up written twice, and what
   would prevent that?
8. Which state does a node drop when it loses leadership, and why is each
   piece safe to drop? What does a follower do with group commands?
9. Under `-sync=interval`, which Raft assumption is no longer true after
   a power cut, and what can follow from that?
10. The simulator catches a broker that acknowledges before syncing in
    180 of 200 seeds, and the Figure 8 bug in none of 1000. What is
    different about the two bugs, and what does the simulator lack?
