# Phase 6 design — cluster simulation

Status: draft for review, 2026-11-11. Roadmap section 5.

## 1. Goal

Phase 5's simulator delays messages between nodes and never loses them.
That was enough for one leader per term and replica agreement under
crashes, and not enough to catch the Figure 8 bug: 0 of 1000 seeds. This
phase makes the network between nodes hostile, checks that the cluster
recovers when the hostility stops, and proves with a suite of deliberate
bugs that the checks can tell.

## 2. Fault model

**Between nodes, per message** (`incarnation.SendPeer`): dropped with
probability *drop*; otherwise delivered after a delay, and with
probability *dup* delivered a second time after an independent delay.
Delays are those of Phase 5 (mostly under 3 ms, one in ten up to half an
election timeout), so messages are reordered too.

**Per link, directed:** a cut drops everything from A to B. Cuts are
installed by **scenarios**, one at a time, each healed after a random
time between 50 ms and 2 s:

| Scenario | Cuts |
|---|---|
| isolate | one node from all others, both directions (often the leader) |
| split | a minority from the majority, both directions |
| one-way | one directed link |
| bridge | two nodes cannot see each other; a third sees both |

**Clients:** connection resets and fragmented, delayed bytes, as before.
Clients reach any node, whatever the cuts: the network between clients
and nodes is not partitioned. Shortcut, listed as a limit.

**Real TCP** neither reorders nor duplicates within one connection, but
does across reconnects, and the transport drops messages when a queue is
full. The model here is weaker than TCP, which is the point: the protocol
must not depend on more.

**Not modelled:** corruption of peer messages (TCP checksums and the
length-checked decoder; the decoder is fuzzed), clock drift (no node reads
another's clock), and Byzantine nodes.

## 3. Schedules

**Swarm configuration.** Each seed draws, independently: whether each
fault kind is on (power, disk crash, kill, failed sync, partition
scenarios), and for the network a *drop* in {0, 1 %, 10 %, 30 %} and a
*dup* in {0, 1 %, 10 %}. Phase 5's draws (cluster size, fsync policy,
election timeout, clients) stay.

**Safety under anything.** Faults may take down or cut off a majority at
once. No invariant of safety (S1–S5, I1–I5, G1–G4, G6) may break whatever
happens.

**Quiet period.** At the end, faults stop, cuts heal, drop and dup go to
zero, every node is brought up. Then:

- **L1.** Within 10 s of virtual time a node serves as leader, all nodes
  have caught up, and a new produce is acknowledged within 5 s more.
- **L2.** All nodes reach the same applied index and the same state hash
  (Phase 5's settle check, which also compares whole logs).
- **N1** (a consumer never sees a record that later disappears or
  changes) is Phase 3's I3 together with the high-watermark floor, which
  already apply to every fetch from any node. No new code.

The final restarts of Phase 5 (I6) follow.

## 4. The mutation suite

Each switch removes one step. A test turns each on and requires a
violation within a fixed number of seeds. The switches are a set of flags
in `raft.Config.Unsafe` (replacing Phase 5's two booleans) plus the
broker's own.

| # | Mutation | Expected to show as |
|---|---|---|
| 1 | commit an earlier term's entry by counting replicas (Figure 8) | S3 |
| 2 | grant a vote before it is on disk | S1 |
| 3 | grant a vote comparing log length only | S3 |
| 4 | follower commits up to its last index, not to what the message confirmed | S3 / S4 |
| 5 | follower truncates whenever a message carries fewer entries than it holds | S3 / I1 |
| 6 | acknowledge entries before they are synced (strict policy) | I1 / S3 |
| 7 | serve clients as soon as elected, before an entry of the term is applied | I1 / I3 |
| 8 | accept offset commits from any generation | G4 |

A mutation that no seed catches is a finding: a missing fault or a missing
invariant, fixed before the phase ends.

## 5. Throughput and budgets

Measure seeds per second on this machine and on CI, profile one run, and
set: the per-push CI budget (the existing `sim` job), a nightly budget,
and a regression list of seeds that CI always runs (`internal/sim/
regressions.txt`, one seed per line with a comment naming the bug).

## 6. Alternatives rejected

- *Partitioning client connections too.* More realistic, and it makes
  "the client cannot reach the leader" a normal state that the liveness
  checks would have to excuse. Later, with a client that tracks reachable
  nodes.
- *Exhaustive interleavings (model checking) for Raft.* The random
  schedules already explore at a few thousand seeds per second; a model
  checker is a separate project.

## 7. Review record

Second pass, hunting for checks that could pass vacuously:

1. A partition that never lands on the leader catches little. Scenarios
   pick the leader as the isolated node half the time.
2. Drops at 30 % with an isolated node can keep a cluster leaderless for a
   whole run. Fine for safety; the quiet period is where liveness is
   judged, and it heals everything first.
3. Liveness must not be judged while a majority is down: the quiet period
   restarts every node before L1's clock starts.
4. A mutation suite where every mutation is caught by the same invariant
   would hint at one strong check and several weak ones. Report which
   invariant caught each.
