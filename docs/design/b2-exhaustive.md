# B2: exhaustive checking of small configurations

Production plan item 11. Must show: the search completes within its
bound; a mutation is found with a shortest counterexample.

## 1. What is searched, and why Raft

Random simulation samples schedules. Exhaustive search visits every
schedule of a small model up to a bound, so within that bound it shows the
absence of a violation, not merely that none was met.

**The model is Raft:** three `raft.Raft` instances on simulated disks,
driven by the same step-by-step test cluster as the random-schedule test
(`internal/raft/raft_test.go`). The safety properties live in Raft. The
broker above it is a deterministic function of the committed log (S4
follows from S3 for a deterministic apply, and the simulator checks S4).
The broker's own state (groups, producers, connections) would multiply
the state space without adding consensus behaviour.

## 2. Design

### Events

At each state, the enabled events are:

| Event | Enabled when | Budget, per node (flag, default) |
|---|---|---|
| deliver a message in flight | always | |
| lose a message in flight | | `-explore-drops` (0) |
| deliver one twice | | `-explore-dups` (0) |
| fire the election timer | armed, node up | `-explore-elections` (1) |
| time passes, no timer firing | node up | `-explore-ticks` (0) |
| fire the heartbeat timer | armed, node up | `-explore-heartbeats` (0) |
| propose on a leader | node leads | `-explore-proposals` (1) |
| power loss | node up | `-explore-crashes` (0) |
| restart | node down | |

`-explore-depth` (10) bounds the events per path. Identical messages in
flight are one event: delivering either gives the same state. A message
is named by its sender, receiver and bytes, not its position, so that a
path replays exactly.

**Each node has its own clock** (`cluster.ownClocks`). Its election
timer or a tick moves only that node's clock, by two election timeouts,
past every timeout it has. Times in the fingerprint stay few and exact.

**Nodes start aged:** each node's clock is moved past one election
timeout before the search begins, as in a cluster that has been up that
long. A node ignores candidates for an election timeout after it starts
(the lease rule). With fresh clocks, no vote was ever granted within one
election per node, and the search was vacuous (BUGS.md M11).

**Heartbeats have a budget.** An earlier rule let a heartbeat fire
whenever nothing was in flight from its node. Answers still in flight
then piled up, one copy per round, without end.

### Search

- **Breadth-first, every enabled event from every state,** so the first
  violation found is a shortest one within the bound. Each frontier path
  is replayed from the start once. Its children are deep copies of the
  replayed state (`deepCopy`, reflection over every field), each stepped
  by one event. A path is stored as the index of each event in the list
  `enabled` offers, one byte per step. Paths are expanded in parallel and
  merged in frontier order, so the result does not depend on scheduling.
- **States are pruned by fingerprint:**
  - every node's Raft state: hard state, role, leader, commit index,
    votes, every peer's progress, transfer and acknowledgement fields,
    times, and every log entry;
  - each node's clock, armed timers, budget used, and whether it is down;
  - the messages in flight, as a sorted multiset;
  - the **ghost state** that the history properties need: each term's
    leader, the committed entries seen so far, and each node's highest
    term.

  Including the ghost state makes pruning sound for the properties
  checked. Two paths merge only if they agree on the history those
  properties look at. The fingerprint is 16 bytes of a SHA-256 of a
  canonical encoding.
- **Dead messages are delivered at once** (`dropDead`). These are a
  response from an older term than the receiver's, or a PreVote response
  from no newer term. `Step` ignores them, and a node's term never goes
  down (S5, checked), so a later delivery would change nothing either.
  Delivering them at once is a legal schedule, not a cut.
- **No partial-order reduction.** A one-node reduction was built and
  found unsound (BUGS.md M11): it never let the chosen node wait for a
  message another node had yet to send.
- **Non-vacuity.** `TestExhaustive` fails unless some state committed a
  client proposal. A bound too small to elect a leader would otherwise
  "complete" while testing nothing, as it once did.

### Properties checked in every state

The random-schedule test's properties:

- **S1:** at most one leader per term, ever.
- **S2:** log matching among running nodes. If two logs hold an entry
  with the same index and term, all earlier entries are equal.
- **S3:** an entry seen committed is never a different entry on any node.
- **S5:** a node's term never goes down, across restarts too.
- **No node stops itself.** Raft stops on a request to replace a
  committed entry, the check that catches a lost acknowledged write.

### Bounds

Three nodes, one election and one proposal each, no heartbeats unless
stated. Two configurations: the default (PreVote and CheckQuorum) and
classic Raft. Measured on the 10-core laptop:

| Bound | Default | Classic |
|---|---|---|
| depth 10 (`go test`) | 35,266 states, 1.6 s | 63,973 states, 2.2 s |
| depth 12 | 196,107, 8 s | 256,104, 10 s |
| depth 12, one power loss each | | 2,880,972, 1 min 49 s |
| depth 12, one loss each | | 1,422,410, 1 min 10 s |
| no depth limit | not run | does not finish in 5 min; 2.5 million states by depth 17 |

`go test` runs depth 10 for both (36 s under the race detector). The
`exhaustive` CI job runs the depth-12 rows and the shortest
counterexample below.

### Mutations and the shortest counterexample

With `AckBeforeSync` (classic Raft, one election and one power loss per
node, no depth limit), the search finds a violation after 409,296 states
in 26 s. Breadth-first order makes it a shortest one: 11 events.

1. Node 1 wins term 1 with node 2's vote and replicates its no-op.
2. Node 2 answers before the entry is on its disk, and node 1 counts it
   committed.
3. Node 2 loses power, restarts without the entry, and wins term 2 with
   node 3's vote.
4. Node 2's AppendEntries asks node 1 to replace the committed entry.

`TestExhaustiveShortest` asserts exactly 11 events. The mutations that
need two terms and a crash in more than one node are recorded in the
report, with the bounds at which they were or were not found. The random
schedules catch all of them (`TestRandomSchedulesCatchBrokenRaft`).

## 3. Alternatives

- **The whole broker in the search.** Groups, producers and connections
  multiply the states without exercising consensus further, and S4 is
  already checked by the simulator on every seed.
- **Depth-first with a depth limit.** It needs less memory, but the first
  counterexample is not the shortest. Iterative deepening repeats all the
  work at every depth.
- **TLA+ or another model checker on a specification.** It checks a model
  of Raft, not this code. The point here is the code.
- **Replaying every child from the start.** It needs no copy, and was the
  first version. It made each state cost its whole path, so deep bounds
  were quadratic. The deep copy is checked by a test
  (`TestDeepCopyCluster`): a copy fingerprints as the original does,
  stepping it leaves the original alone, and both reach the same state on
  the same events.
- **A one-node partial-order reduction.** It cut the default bound from
  2.8 million states to 986, and it was unsound (BUGS.md M11).
- **A sound partial-order reduction** (stubborn sets, or dynamic POR as
  in Flanagan and Godefroid's 2005 paper). For message passing it must
  include the receives of every node that may still be sent a message.
  That is a project of its own. Without it, the bounds stay small.
- **Symmetry reduction** (treating node ids as interchangeable). Raft
  breaks ties by id in places, so its soundness needs an argument. It
  gains at most six times for three nodes.

## 4. Second review

1. *Is the fingerprint complete?* It holds every field of `Raft` that
   decisions read in these runs: no snapshots, compaction, membership
   changes or transfers occur. The disk's unsynced bytes are not in it.
   Every event flushes, so outside mutations that skip a sync, the disk
   holds what memory holds. A mutation that skips a sync can merge a
   synced state with an unsynced one, which may hide some paths to it. It
   cannot produce a false violation, since every violation found replays.
2. *Can the search fail to terminate?* Every budget is finite, and so is
   the depth. Without a depth limit, the log grows only through
   proposals and one no-op per term, terms are bounded by elections, and
   clocks move only on budgeted events.
3. *Is dropping dead messages sound?* Only a mutation that lets a term go
   down could revive one. That is an S5 violation at the restart where it
   happens, which ends the path.
4. *Does starting aged hide anything?* It removes schedules in which a
   vote arrives within an election timeout of start-up. Raft ignores
   those votes anyway, and the random schedules start fresh.
5. *A crash keeps whatever the simulated disk decides,* which is one
   outcome per path, not every possible torn write. Torn writes are the
   simulator's and the storage tests' job.
6. *What does "complete" claim?* That no schedule of at most the stated
   number of events, within the per-node budgets, violates S1, S2, S3 or
   S5, for three nodes started aged. Nothing beyond the bound. Depth 10
   or 12 is short: a leader election and one commit, with one fault at
   most.
7. *Was this review wrong before?* Yes. Its first version accepted the
   reduction's argument and named its weakest step. That step was not the
   flaw. Testing the reduction against the full search, on what each
   reached, found the flaw.
