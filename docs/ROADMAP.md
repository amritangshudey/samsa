# Roadmap: architecture and plan after the MVP

Written 2026-11-10, during Phase 1, at the developer's request: a plan
detailed enough that the work after the MVP can be carried out later by
someone (or some model) who was not present for the design.

`docs/PLAN.md` is the summary of all phases. This document is the detailed
specification for Phase 4 onwards, plus the work beyond Phase 7 that would
make the project exceptional. Where the two disagree, this document wins for
Phase 4 and later.

## 0. How to use this document

- Sections 1 and 2 are the architecture every later phase assumes. Read them
  first, every time.
- Sections 3 to 6 are Phases 4 to 7. Section 7 is the work beyond.
- Section 8 is the working method. It is not optional.
- This was written before Phases 2 and 3 existed. Names of types and files
  may differ in the code. **The decisions, invariants and acceptance criteria
  are binding; the names are not.** Before starting any phase, read the
  phase reports in `docs/reports/` and the ADRs, and reconcile.
- **Nothing here is a wire format.** Kafka field layouts, API versions and
  error code numbers are deliberately absent. Architecture rule 5 applies:
  take them from the official protocol documentation and the message
  definition files in the Apache Kafka repository
  (`clients/src/main/resources/common/message/*.json`), and from what the
  test clients are observed to send. KIP numbers below are pointers for
  reading and must themselves be checked.

## 1. The central idea: one replicated command log

Everything that changes the broker's state is a **command**. Commands are
appended to one log per node. A command takes effect only once it is
**committed**, and committed commands are **applied**, in log order, to a
deterministic **state machine**. All client-visible state is derived from
the applied prefix of the log.

```
 client request
      |
      v
 +----------+  propose(cmd)  +-------------+  committed(index)  +---------------+
 | handlers | -------------> | command log | -----------------> | state machine |
 +----------+                +-------------+                    +---------------+
      ^                        MVP:  local log, committed = fsynced      |
      |                        Ph.5: Raft log, committed = on a majority |
      +---------------- result of apply, completes the waiter -----------+
```

- **Single node (MVP):** "committed" means "synced to the local disk".
- **Cluster (Phase 5):** "committed" means "durable on a majority" (Raft).
  Nothing above the log changes.

What is a command: create topic; append a record batch to a partition;
commit a consumer offset; set a consumer group's generation or assignment.

**Partitions are views.** There is no file per partition. A partition is an
in-memory index: for each of its batches, the partition offset of the
batch's first record and the log index of the command holding it. A fetch
looks up the index and reads those commands from the log. The batch's base
offset is assigned when the command is applied (it is a pure function of the
log prefix) and stamped into the batch bytes when they are sent to a
consumer.

### Why this shape

- **One definition of durable.** One log, one fsync stream, one recovery
  procedure, one thing to replicate. Group commit covers all partitions with
  a single fsync.
- **Replication becomes a substitution.** Phase 5 replaces "local log" with
  "Raft log". Consumer groups, topics and data all become replicated at once
  because they were all commands already.
- **It is checkable.** Two replicas that have applied the same log prefix
  must have identical state. A hash of the state machine at each applied
  index is a complete replica-agreement invariant for the simulator.
- **It is explainable.** "State machine replication over a single log" is
  one sentence, and every design question reduces to "is it a command, or is
  it local soft state?"

### What it costs (say these out loud in an interview)

- Every node stores every partition; one leader serves all writes. Storage
  and write throughput do not scale by adding nodes. (Section 7, C3.)
- A partition's batches are interleaved with other partitions' in the log,
  so a fetch has worse locality than Kafka's file-per-partition.
- Retention is global to the log, not per topic. (Section 7, C2.)
- On start the node replays the whole log to rebuild the state machine,
  until snapshots exist. (Section 7, A1.)
- Offsets are assigned at apply, so a produce cannot be acknowledged before
  commit. `acks=0` and `acks=1` are treated as `acks=all`. That is stronger
  than asked for and is allowed.

### The rules that keep it correct

R1. **Apply is deterministic.** `apply(state, command)` reads no clock, no
    random source, no disk other than the log, and iterates no map in
    unordered fashion. Anything time- or chance-dependent (timestamps, a
    generated member id) is computed by the proposer and carried in the
    command.

R2. **Only committed commands are applied, in index order, exactly once per
    start.** Until snapshots exist, a restart applies from index 0.

R3. **Client-visible reads see only applied state.** A fetch never returns
    a batch whose command is not committed.

R4. **A response that reports success is sent only after the command it
    depends on is applied.** The waiter is completed by apply.

R5. **Soft state is rebuilt, never trusted.** Connections, pending requests,
    timers, delayed fetches and in-progress rebalances are local to a node
    and die with it or with its leadership. Each must have a defined
    behaviour on restart and on loss of leadership (usually: fail the waiter
    with a retriable Kafka error so the client retries).

R6. **The command encoding is versioned from day one**: one leading byte for
    the command type, and decoders that reject unknown types with an error.
    A log written by an older build must stay readable.

R7. Commands are decoded with the same care as network input: lengths
    bounded, no panics. A corrupt command is `ErrCorrupt`, not a crash loop.

## 2. Component map (target after Phase 3)

```
cmd/samsa/                 flags, wiring
internal/sys/              interfaces (Phase 0)
internal/storage/          the log: frames, segments, recovery (Phase 1)
internal/wire/             Kafka protocol: primitives, headers, per-API codecs
internal/broker/           the node
    node.go                sys.Handler: connections, framing, dispatch
    command.go             command types, encode/decode
    state.go               the state machine: topics, partition indexes,
                           groups, offsets; Apply; StateHash
    proposer.go            propose -> commit -> apply -> complete waiters
    produce.go fetch.go    request handlers
    purgatory.go           delayed operations and their timers
internal/realsys/          real disk, TCP event loop, clock
internal/simsys/           simulated disk, network, clock
internal/sim/              harness, workload, history, invariant checkers
internal/raft/             Phase 5
integration/               separate Go module: real-client tests, benchmarks
```

The single seam between "one node" and "a cluster" is the proposer's
dependency. Keep it this small:

```go
// CommandLog is what the proposer needs from whatever makes commands durable.
type CommandLog interface {
    // Propose appends cmd and returns the index it will have if it commits.
    // It fails with ErrNotLeader on a node that may not accept writes.
    Propose(cmd []byte) (index int64, err error)
    // Committed returns the index below which every command is committed.
    Committed() int64
    // Read returns the committed command at index.
    Read(index int64) ([]byte, error)
}
```

The node is told that `Committed()` may have advanced by an event (a flush
timer on one node; Raft progress in a cluster) and then applies. In the MVP
`CommandLog` is a thin wrapper over `storage.Log` whose `Committed` is
`DurableOffset`.

## 3. Phase 4: consumer groups

### 3.1 Concepts to explain in the design doc

Why consumers need a coordinator; group, member, generation; the two-step
rebalance (JoinGroup collects members and picks a protocol; SyncGroup
distributes the assignment computed by the group leader, which is a client);
why assignment is done client-side; session timeout versus rebalance
timeout versus `max.poll.interval`; committed offsets and at-least-once
delivery; what a zombie consumer is and how the generation fences it.

### 3.2 Scope

The classic group protocol: FindCoordinator, JoinGroup, SyncGroup,
Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch. Verify which versions the
test clients use, and whether a current client defaults to the newer
consumer protocol (KIP-848) and how it falls back when the broker does not
advertise the newer API. Not in scope: static membership (KIP-345), the
KIP-848 protocol, offset expiry, transactions on offsets.

### 3.3 Design

**What is a command, and what is soft state.**

| Thing | Kind | Why |
|---|---|---|
| Committed offset | command `OffsetCommit` | must survive |
| A completed generation: id, protocol, leader, member list with each member's subscription metadata and timeouts | command `GroupGeneration` | survives restart and failover without forcing a rebalance |
| The assignment for a generation | command `GroupAssignment` | members that sync late or again must get the same answer |
| Group becoming empty | `GroupGeneration` with no members | |
| Members waiting in a JoinGroup or SyncGroup | soft | a pending request cannot outlive its connection |
| A rebalance in progress | soft | on restart or failover the group is at its last committed generation; members are told to rejoin |
| Session timers | soft | restarted with a full timeout on start and on gaining leadership |

**State machine per group** (in `state.go`): `Empty`, `Stable(generation)`,
and for the leader-local soft part `PreparingRebalance`,
`CompletingRebalance`. Only the first two exist in applied state.

**Flow.**

1. JoinGroup arrives. Unknown member with no id: generate an id at the
   proposer (client id plus a value from `sys.Random`) and follow what the
   negotiated version requires (newer versions make the client rejoin with
   the id first: see KIP-394).
2. The group enters `PreparingRebalance`. The response is held. A timer is
   armed for the rebalance timeout (the largest among members). The first
   join of an empty group may wait a short configurable delay so that
   consumers started together land in one generation.
3. When every known member has rejoined, or the timer fires (members that
   did not rejoin are dropped): choose the protocol that all members
   support, pick the leader (keep the previous one if still present),
   propose `GroupGeneration`. On apply, answer every held JoinGroup; the
   leader's answer contains the member list.
4. `CompletingRebalance`: SyncGroup requests are held until the leader's
   arrives carrying the assignment. Propose `GroupAssignment`. On apply,
   answer all of them, and any that come later for the same generation.
5. `Stable`: heartbeats reset the member's session timer. A fired session
   timer, a LeaveGroup, or a JoinGroup from a new or changed member starts
   step 2. While a rebalance is in progress, heartbeats answer with the
   rebalance-in-progress error so members rejoin.
6. OffsetCommit checks group, member id and generation (unless it is the
   "simple consumer" form with no group membership), then proposes
   `OffsetCommit`. The timestamp is taken at the proposer.

Every held request has a timer and a defined answer when the connection
closes, when the node stops being able to serve (Phase 5), and when the
timer fires. There must be no path on which a held request is never
answered and never dropped.

**Delayed operations.** Phase 2 will have a purgatory for delayed fetches.
Reuse it: a delayed operation is (timer id, completion function, cancel
function), indexed so that it can be found by what completes it.

### 3.4 Tasks, in order

1. Design doc; ADR for negotiated versions; ADR for "what is replicated in a
   group".
2. Codecs for the seven APIs, with fuzz targets and round-trip tests.
3. `OffsetCommit` / `OffsetFetch` on the state machine (no membership yet).
   Integration: a client using manual assignment commits and reads back.
4. Group state machine as pure functions with table-driven tests: given a
   state and an event, the next state and outputs. No network involved.
5. JoinGroup / SyncGroup / Heartbeat / LeaveGroup handlers on top.
6. Restart behaviour: groups rebuilt from the log; timers re-armed.
7. Simulator: group workload and invariants.
8. Integration tests with two real clients.

### 3.5 Invariants for the simulator

G1. In an applied generation with an applied assignment, no partition is
    assigned to two members. (The assignment bytes are opaque to the broker;
    the simulated clients know what they assigned, so the checker lives in
    the workload.)
G2. Generation ids of a group strictly increase.
G3. An acknowledged offset commit is returned by every later OffsetFetch
    until a later commit for the same partition is acknowledged.
G4. A commit from a member of an older generation is rejected.
G5. Every held request is answered or its connection is closed within a
    bounded virtual time.
G6. After a crash and restart, G3 still holds.

### 3.6 Acceptance

- Two franz-go consumers in one group share a topic's partitions; one is
  killed; the other takes over and resumes from the committed offsets. The
  same with the Java client.
- The broker is restarted while a group is stable: consumers continue
  without losing committed offsets.
- G1 to G6 hold over the CI seed budget, and each is shown to fire on a
  deliberately wrong history.

### 3.7 Pitfalls

- A member that rejoins with unchanged metadata while the group is stable
  must not always trigger a rebalance; a leader that rejoins must. Check
  the reference behaviour before deciding.
- The rebalance timer and a join that completes the group can occur in the
  same instant. Whichever event runs second must find the work done and do
  nothing.
- Clients send Heartbeat on a separate schedule from poll. Tests that pass
  with one client's timing can fail with another's.
- Member ids must be unique for the life of the cluster, including across
  restarts, and may not come from a counter that restarts at zero.

## 4. Phase 5: replication with Raft

### 4.1 Concepts to explain in the design doc

Consensus as agreement on a log; terms as logical time; why a majority;
election safety; log matching; leader completeness; why a leader may only
commit an entry of its own term by counting replicas (the paper's Figure 8);
what must be on disk before which message is sent; how Kafka's ISR
replication differs and what each gives up.

### 4.2 Design

**Placement.** `internal/raft` is a core package. A Raft instance is driven
by the node: it receives peer messages, timer events and proposals, and it
calls `sys.Clock`, `sys.Random`, the peer network and its log. It implements
`CommandLog`.

**The log.** A `storage.Log` in its own directory. One Raft entry is one
frame with `count` 1, so the storage offset is the Raft index minus a fixed
base. The payload begins with the entry's term and an entry kind (command
or no-op), then the command bytes. Storage gains one operation:
`TruncateSuffix(offset)`, durable before it returns, refused below the
commit index.

**Hard state.** `currentTerm` and `votedFor` in one small checksummed file,
replaced atomically: write a temporary file, sync it, rename it over the
real one, sync the directory. Read at start; a missing file means term 0.

**Peers.** A static list given at start: node id, peer address, client
address. No membership change.

**Peer transport.** Raft is designed for a network that loses, duplicates
and reorders messages. Model exactly that:

```go
// Added to sys.Network in this phase.
SendPeer(to NodeID, msg []byte)          // best effort; may be lost
// Added to sys.Handler.
OnPeerMessage(from NodeID, msg []byte) error
```

The real implementation keeps one TCP connection per peer with
length-prefixed messages, reconnects with backoff, and drops messages when
its bounded queue is full. Connection management stays out of the core.
Peer messages are untrusted input: bounded and fuzzed like client requests.

**Persist-before-send table.** This is the heart of correctness.

| Before sending | must be durable |
|---|---|
| a granted vote | `currentTerm`, `votedFor` |
| any message carrying a new term | `currentTerm` |
| an AppendEntries success for entries up to index i | entries up to i (acknowledge `DurableOffset`, not `EndOffset`) |
| (leader) counting itself toward commit of index i | its own log up to i |

The leader may send entries to followers before its own sync completes, as
long as it counts itself only up to its own durable index.

**Commit and apply.** `commitIndex` is the highest index of the leader's
current term stored durably on a majority. On winning an election the
leader appends a no-op so that earlier entries become committable. When
`commitIndex` advances the node applies (rule R2) and completes waiters.

**Clients and leadership.** Metadata names the current leader as the leader
of every partition and as coordinator. A node that is not leader answers
writes, fetches and group requests with the retriable errors Kafka clients
understand for "wrong broker" (verify which error each API expects). When a
node loses leadership it fails every waiter that way.

**Reads.** A fetch is served by the leader from applied state. A deposed
leader that has not noticed yet can still serve a fetch, and that is safe:
it returns only committed batches, which never change. It may report a
stale end of log. No read-index machinery is needed for fetch. Group reads
(OffsetFetch) can be stale in the same window, which can cause a consumer
to re-read: acceptable under at-least-once, and improved in section 7, B3.

**Duplicates.** A client that retries a produce after a leader change can
write the batch twice. Without the idempotent producer this is expected and
documented; section 7, A2 removes it.

**Timing.** Election timeout randomised from `sys.Random` within a
configured range; heartbeat interval well below it. One AppendEntries in
flight per follower, resent on a timer. Pipelining is section 7, B3.

### 4.3 Tasks, in order

1. Design doc; ADRs: topology, persist-before-send, acks semantics.
2. `storage.TruncateSuffix` with crash tests in the Phase 1 style.
3. Hard-state file with crash tests.
4. Message codecs for RequestVote and AppendEntries and their responses,
   fuzzed.
5. Raft as a state machine tested without a network: feed messages and
   timer events, assert outputs. Start with election, then replication,
   then commit. Include the paper's Figure 7 follower logs and Figure 8.
6. Peer transport in `realsys`; `SendPeer` in `simsys` (reliable for now).
7. Swap the node's `CommandLog` for Raft. A one-node cluster must pass
   every earlier test unchanged: this is the proof that the seam is right.
8. Not-leader handling for every API; Metadata from cluster config.
9. Three-node integration test on localhost with a leader kill.
10. Simulator: three nodes, crashes and disk faults (network faults are
    Phase 6).

### 4.4 The bugs to expect

Each of these is a classic. Write a test for each before the code.

1. Replying to a vote request before the vote is durable: two leaders in
   one term after a crash.
2. Committing an entry of an earlier term by counting replicas (Figure 8).
3. Truncating the log on an AppendEntries that is merely stale or
   duplicated. Truncate only at a real conflict (same index, different
   term), never because a message carried fewer entries than are present.
4. Acknowledging entries that are appended but not synced.
5. Trusting a response without checking its term and which index it
   acknowledges; `matchIndex` must never move backwards.
6. Resetting the election timer on any message. Reset only on a valid
   AppendEntries from the current leader or on granting a vote.
7. Forgetting to clear `votedFor` when the term rises, or clearing it
   without persisting.
8. Comparing logs for the vote by length alone. Compare last term first,
   then last index.
9. Applying an entry twice, or applying past `commitIndex`.
10. A leader that keeps answering writes after seeing a higher term.
11. A new leader serving a fetch before it has applied everything it has
    committed.
12. Losing held client requests on a leadership change (rule R5).

### 4.5 Invariants (checked in the simulator from this phase on)

S1. At most one leader per term.
S2. Log matching: if two logs hold an entry with the same index and term,
    the logs are identical up to that index.
S3. Leader completeness: an entry committed in term t is in the log of
    every leader of a later term.
S4. State machine safety: the state hash at applied index i is the same on
    every node that has applied i.
S5. `commitIndex` and `currentTerm` on a node never decrease, across
    restarts as well.
S6. All client-level invariants from Phases 3 and 4, with "acknowledged"
    meaning acknowledged by any node.

### 4.6 Acceptance

- The one-node cluster passes all earlier tests.
- Three nodes on localhost: a client produces continuously, the leader is
  killed, and afterwards every acknowledged message is consumed. Duplicates
  are permitted and counted; loss is a failure.
- S1 to S6 hold over the CI seed budget with crashes and disk faults.

## 5. Phase 6: cluster simulation

### 5.1 Network fault model

Between nodes, per message: delivered after a random delay, dropped,
duplicated, or reordered (which random delays already give). Per link:
cut in one direction or both. Per scenario: a partition (majority and
minority, or a leader isolated, or a bridge where one node sees both
sides), healed after a random time. Client connections can be reset and
client requests delayed.

Real peer links are TCP and do not reorder or duplicate inside one
connection, but they do across reconnects, and the protocol must be
correct under the weaker model anyway. Say so in the report.

### 5.2 Fault schedules

- **Swarm testing:** each seed first draws which fault kinds are enabled
  and how aggressive each is. Runs with everything enabled at once rarely
  get far enough to hit deep states; runs with a random subset do.
- Every schedule ends with a quiet period: faults stop, the network heals,
  all nodes are restarted if down. Liveness is checked there.
- Faults stay within what the system claims to tolerate: at most a minority
  of nodes down or cut off at once when checking liveness. Safety must hold
  under any schedule at all, including a majority failing.

### 5.3 Invariants

All of S1 to S6 and G1 to G6, plus:

L1. After the quiet period begins, a leader is elected and a new produce is
    acknowledged within a bounded virtual time.
L2. After the quiet period, all nodes converge to the same applied index
    and state hash.
N1. A consumer never observes a record that is not committed (it never
    sees something that later disappears or changes).

### 5.4 Proving the checkers work: the mutation suite

Following the pattern set in Phase 1 (`skipRollDirSync`): each classic bug
from 4.4 that can be expressed as "skip this step" gets an unexported
switch. A test turns each on and requires the simulator to report a
violation within a fixed number of seeds. At least eight mutations. A
mutation that is never caught means a missing invariant or a missing fault,
and is a finding to fix, not a test to delete.

### 5.5 Tasks, in order

1. Design doc: fault model, schedule generator, what is and is not modelled.
2. Simulated peer network with delay, drop, duplicate.
3. Link cuts and partition scenarios.
4. Swarm configuration from the seed.
5. Quiet period and liveness checks.
6. Mutation suite.
7. Throughput of the simulator itself: measure seeds per second, profile,
   set the CI budget (per push) and the nightly budget.
8. Every failure found goes to `docs/BUGS.md` with its seed, and the seed
   joins a regression list that CI always runs.

### 5.6 Acceptance

- At least eight mutations, each caught.
- The regression seed list and the CI budget run clean.
- The report states seeds per second, total seeds run, and bugs found.

## 6. Phase 7: release

- **Benchmarks.** A load generator in the `integration` module. Measure
  throughput and latency (median, p99, p99.9) for one node and three nodes,
  per fsync policy, at several message sizes. Use an open-loop generator at
  fixed rates for latency, so that a stalled broker shows up as latency and
  is not hidden by the generator slowing down (coordinated omission).
  Record machine, OS, Go version, commands, and run-to-run variance. Laptop
  results are for comparing this project with itself; any comparison with
  another broker needs the same machine, the same durability settings, and
  that caveat stated next to the number.
- **README.** What it is; a 60-second quick start; a table of what is
  supported, each row linked to the test that shows it; what is not
  supported; how to run a simulator seed; the bugs the simulator found.
- **Architecture document.** Sections 1 and 2 of this file brought up to
  date with the code, linking the ADRs.
- **Release.** Version stamped into the binary; builds for macOS and Linux,
  arm64 and amd64, produced by CI on a tag; checksums.
- **Acceptance.** A fresh clone builds, tests, and runs the quick start as
  written. No claim without a link to a test or a benchmark.

## 7. Beyond Phase 7: what would make it exceptional

Grouped by value for effort. Each item lists why it matters, the design in
outline, the acceptance test, and what it depends on. Sizes are relative:
S is days, M is a week or two, L is several weeks, for one person.

### Tier A: do these first

**A1. Snapshots and log truncation (M).**
Why: without them, start-up time and disk use grow forever, and that is the
first hole an interviewer will find.
Design: the state machine can serialise itself at an applied index
(topics, partition indexes, groups, offsets) to a checksummed snapshot
file, written with the temp-file, sync, rename, sync-directory sequence. On
start: load the newest valid snapshot, apply the log from there. The
snapshot does not contain record data, which stays in the log, so the log
can only be cut below the oldest batch still wanted: this is where
retention (time or size, global) is defined. A follower that has fallen
behind the cut needs the snapshot plus the retained log from the leader
(Raft's InstallSnapshot, sent in chunks).
Acceptance: start-up time is bounded by snapshot interval, shown by a
benchmark; simulator runs with snapshots and truncation enabled keep every
invariant; a mutation ("truncate past the snapshot index") is caught.
Depends on: Phase 5 for InstallSnapshot; the local part can be done after
Phase 3.

**A2. Idempotent producer (M).**
Why: it turns "duplicates are possible after a retry" into "exactly once
per partition", the natural strengthening of the central invariant.
Design: implement the producer-id request and the sequence checks of
KIP-98 (verify the current protocol). Producer ids are allocated by a
command. Per partition and producer the state machine keeps the last few
(sequence, offset) pairs; an append whose sequence was already applied
returns the original offset without appending; a gap is an error. All of
this lives in apply, so it is replicated and deterministic for free.
Acceptance: new invariant "no record appears twice in a partition" holds
under retries, leader changes and connection resets; real clients run with
idempotence enabled (the Java client's default) without falling back.
Depends on: Phase 5. Do after A1, because the producer table must be in
the snapshot.

**A3. Simulator power tools (S each).**
Why: the simulator is the project's distinguishing feature. These make it
visibly a tool and not a test.
- *Seed shrinking:* given a failing seed, repeatedly remove or shorten
  faults and workload steps and re-run, keeping the smallest schedule that
  still fails. Needs the schedule to be an explicit list derived from the
  seed, not random draws spread through the run.
- *Timeline output:* `samsa sim --seed N --trace` writes the event history
  as a single HTML file: one lane per node and client, messages as arrows,
  faults as shaded spans, the violated invariant marked.
- *Coverage of the fault space:* count, per run and in total, how often
  each interesting state was reached (leader crash during commit, torn
  write in recovery, vote split, ...), and print the ones never reached.
- *Mutation score in CI:* the suite from 5.4 as a required check.
Acceptance: a failing seed from `docs/BUGS.md` shrinks to a schedule a
person can read; the timeline of that seed makes the bug evident.

**A4. Black-box chaos test of the real binary (M).**
Why: the simulator tests the core against a model. This tests the real
I/O layer, which the simulator cannot reach, and answers "but does the
real thing survive?"
Design: start three real processes on localhost. Put a small user-space
TCP proxy (in the test code) on every peer and client link so that links
can be cut, delayed and reset without root. Kill processes with SIGKILL.
Drive real clients, record a history in the same format the simulator
uses, and run the same checkers on it.
Acceptance: the shared checkers pass on real histories across a scripted
set of scenarios; an intentionally broken build (a mutation switch exposed
through a hidden flag in a test build) is caught.
Limit to state: SIGKILL does not lose the OS page cache, so this does not
test power loss. Only the simulator does.

### Tier B: depth

**B1. Storage faults beyond crashes (L).**
Why: real disks corrupt data that was synced. Most systems ignore it. A
replicated system can repair it, and showing that is rare.
Design: extend the simulated disk with latent corruption of synced blocks
and misdirected writes. A background scrub re-reads and checksums sealed
segments. A node that finds a corrupt committed entry does not truncate
(that could lose committed data, see "Protocol-Aware Recovery for
Consensus-Based Storage", FAST 2018): it marks the entry faulty and
fetches it from a peer. Requires distinguishing "torn tail of the last
write" from "corruption in the middle", which the Phase 1 recovery rules
already do.
Acceptance: with corruption on a minority of nodes no committed data is
lost and the cluster repairs itself; the naive mutation ("truncate at the
first bad frame anywhere") is caught losing data.

**B2. Exhaustive checking of small configurations (M).**
Why: random simulation finds bugs; exhaustive search of a small model
shows their absence within a bound. The deterministic core makes it cheap.
Design: a second driver for the same nodes. At each step it enumerates
every enabled event (deliver this message, fire this timer, crash this
node), and explores them depth-first, replaying from the start for each
path (deterministic, so replay is exact), pruning states already seen by
state hash. Bound: three nodes, two client operations, one crash.
Acceptance: the search completes within the bound and checks S1 to S4 on
every state; a mutation is found with a shortest counterexample.

**B3. Raft hardening and performance (M).**
PreVote and CheckQuorum (a partitioned node must not disrupt a healthy
leader when it returns); leadership transfer for clean shutdown; pipelined
AppendEntries with flow control; a read-index or lease for group reads that
must not be stale. Each with a simulator scenario that fails without it.

**B4. Operations (S to M).**
A metrics endpoint in Prometheus text format, written by hand; graceful
shutdown; the admin APIs the standard tools call (describe cluster,
describe configs, delete topics, delete records: verify the list against
what `kafka-topics` and a common web UI send); a `samsa check` command
that verifies a data directory offline.

**B5. Compatibility matrix (S).**
Run the round-trip tests against more clients (librdkafka through kcat and
a Python binding, Sarama, KafkaJS) and several versions of each, and
generate the README table from the test results.

### Tier C: large, and optional

**C1. Cluster membership changes (L).** Single-server changes as in the
Raft dissertation. Hard to get right; the simulator is what makes it
credible. Only after A1 (a new node needs a snapshot).

**C2. Per-topic retention and key compaction (L).** The shared log makes
this harder than in Kafka. Either rewrite live data forward (a
log-structured garbage collector) or give large topics their own logs.
Decide with measurements, and write the ADR first.

**C3. Sharding across Raft groups (L).** Partitions are assigned to
several groups so that leadership and storage spread over nodes. This is
the answer to "it does not scale out", and it is a second project.

**C4. Newer consumer group protocol, KIP-848 (L).** Moves assignment to the
broker. Worth it only if a target client requires it.

**C5. TLS and SASL (M).** Needed for any real deployment, of little
interest for the correctness story.

**C6. Transactions (L).** Needs A2. Out of scope unless it becomes the
goal.

### Suggested order

```
Phase 4 -> 5 -> 6 -> 7
              \
               A1 -> A2
               A3 (any time after Phase 3; shrinking and timeline first)
               A4 (after Phase 5)
               B2 (after Phase 5), B1 (after A1), B3, B4, B5 as wanted
               C* only by explicit decision
```

If time allows only three things after Phase 7: A3, A1, A2, in that order.

## 8. Working method for whoever does this

1. Read `CLAUDE.md`. Its rules apply to everything here.
2. One phase or roadmap item at a time. For each: design doc in
   `docs/design/`, critical second-pass review recorded in it, tests first,
   small commits, PR, green CI, report in `docs/reports/`.
3. **Never weaken an invariant, raise a timeout, or skip a seed to make a
   failure go away.** A failing seed is a bug in the system, the model or
   the checker. Find out which, fix that, and record it in `docs/BUGS.md`.
4. **Never state a protocol detail from memory.** Check the documentation,
   then check a real client, then write the ADR.
5. After any change to the core, run the determinism check (the same seed
   twice gives the same trace hash) before anything else.
6. Every new invariant gets a test showing it fires on a bad history. Every
   new protocol step that exists for safety gets a mutation switch and a
   test showing the simulator notices its absence.
7. New soft state needs an answer to rule R5 written in the code next to
   it. New durable state must be a command (rule R1) or have its own crash
   tests.
8. Stop and ask the developer when: a dependency would be added to the
   broker module; a decision here turns out to be wrong or impossible; a
   Tier C item seems necessary; a simulator failure cannot be explained
   after a real attempt. Write the question in `docs/QUESTIONS.md` with
   what was tried.
9. Do not claim compatibility or performance that a test or benchmark in
   the repository does not show.

## 9. The story each part tells

For interviews, the one-line point of each piece:

- Phase 1: I know what fsync does and does not promise, and I tested my
  recovery against a disk that lies in every way the contract allows.
- Phases 2 and 4: I implemented a real protocol against real clients from
  the specification, and I never trust bytes from the network.
- Phase 3 and 6: I can make a distributed system's bugs reproducible, and I
  can show that my tests are able to fail.
- Phase 5: I implemented consensus from the paper and can name the twelve
  ways it usually goes wrong and the test for each.
- A1, A2: I closed the two honest gaps (unbounded log, duplicates).
- A3, A4, B1, B2: I pushed the testing idea further than most production
  systems do.
