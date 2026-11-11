# Architecture

How Samsa is built, as of Phase 7. The plan it came from is
[ROADMAP.md](ROADMAP.md), sections 1 and 2; this document describes the
code. Every significant decision has an ADR in [adr/](adr).

## 1. One replicated command log

Everything that changes the broker's state is a **command** in one log
([ADR 0005](adr/0005-one-command-log.md)). A command takes effect only once
it is **committed**, and committed commands are **applied**, in log order,
to a deterministic **state machine**. Everything a client can see is
derived from the applied prefix.

```
 client request
      |
      v
 +----------+  Propose(cmd)  +-----------+  Committed() moves  +---------------+
 | handlers | -------------> | Raft log  | -----------------> | state machine |
 +----------+                +-----------+                    +---------------+
      ^                      committed = synced on a majority        |
      |                      (one node: synced locally)              |
      +--------------- result of apply completes the waiter ---------+
```

The commands (`internal/broker/state.go`): create topic, append a record
batch to a partition, commit an offset, set a group's generation, set a
group's assignment. Each starts with a type byte; an unknown type stops the
node rather than being skipped. Raft adds a no-op entry at the start of
each leader's term.

**Partitions are views.** There is no file per partition. A partition is an
in-memory index from its offsets to the log entries holding its batches; a
fetch reads those entries from the log. Offsets are assigned at apply, so a
batch's base offset is a pure function of the log prefix.

**One node is a cluster of one.** There is no separate single-node path:
a lone node elects itself, and an entry commits when it is synced
([ADR 0010](adr/0010-one-raft-group.md), [ADR 0012](adr/0012-durability-in-a-cluster.md)).

What this shape costs: every node stores every partition and one leader
takes all writes, so adding nodes adds no capacity; partitions interleave in
the log, so fetches have poor locality; retention is one size bound for the whole log,
not per topic; `acks=0` and `acks=1` behave as `acks=all`.

**Snapshots** ([design](design/a1-snapshots.md)). Every `SnapshotEvery`
applied entries (default 10 000), the node syncs its log and writes a
snapshot: the terms of the entries so far, as runs, and the encoded state
machine (`broker/snapshot.go`; record data stays in the log). A start loads
it, reads only the entries after it, and applies from there. A snapshot
that the log cannot back stops the node.

**Retention** ([design](design/a1-snapshots.md), section 3). When its log
exceeds `RetentionBytes`, the leader proposes `Trim(C)`, C a segment base
at or below the commit index. Applying it, on every replica, drops batch
references below C and moves each partition's start; fetches below it
answer `OFFSET_OUT_OF_RANGE`. The node then snapshots, and deletes
segments only below its newest snapshot's log start
(`raft.Compact`). A follower whose next entry was deleted gets the
leader's snapshot by InstallSnapshot (`raft/snapshot.go`), in chunks,
keeps it as an `install` marker until its log is durable past it, and
meanwhile applies nothing.

The rules that keep it correct (roadmap R1–R7): apply reads no clock, no
randomness and no unordered map, and anything time- or chance-dependent is
decided by the proposer and carried in the command; only committed commands
are applied, in order; a success response is sent only after the command it
depends on is applied; soft state (connections, held requests, timers,
rebalances in progress) dies with the node or its leadership and fails its
waiters with a retriable error; commands are decoded as carefully as
network input.

## 2. Determinism and the purity rule

The core is single-threaded per node and does no I/O. It is a set of event
handlers (`sys.Handler`: `OnStart`, `OnTimer`, `OnConnOpen`, `OnConnData`,
`OnConnClose`, `OnPeerMessage`) that act only through interfaces passed in:
`Clock` (now, timers), `Network` (send to a client connection or a peer),
`Disk` and `File`, `Random`. `internal/sys/sys.go` defines them.

There are two implementations of each: real ones in `internal/realsys`
(TCP event loop, files, wall clock) and simulated ones in `internal/simsys`
and `internal/sim` (virtual time, an in-memory disk that tears unsynced
writes on power loss, a hostile network).
[`internal/purity_test.go`](../internal/purity_test.go) enforces the rule:
every package under `internal/` except `realsys` may import only an
allowlist of standard packages, may use nothing from `time` except
`Duration`, and may contain no `go` or `select` statement
([ADR 0001](adr/0001-deterministic-core.md), [ADR 0003](adr/0003-layout-and-purity-check.md)).

## 3. The parts

```
cmd/samsa/            flags; wires realsys into the broker
internal/sys/         the interfaces
internal/storage/     the log: checksummed frames, segments, an offset index,
                      recovery that truncates a torn tail (ADR 0002)
internal/wire/        Kafka encoding: primitives, headers, each API's messages,
                      record batches; every decoder bounded and fuzzed
internal/raft/        Raft as a pure state machine; its log is storage.Log
internal/broker/      the node
    node.go           connections, framing, dispatch, propose/apply, leadership
    state.go          the applied state machine and its hash
    handlers.go       produce, fetch, metadata, offsets, topics
    groups.go         the group coordinator's soft state (rounds, timers)
    leader.go         what a node answers when it may not serve
internal/realsys/     TCP loop (loop.go), peer transport (peer.go), files
internal/simsys/      the simulated disk
internal/sim/         the simulator (section 5)
integration/          separate module: real clients, the benchmark, the
                      quick-start topic tool
```

**Raft** (`internal/raft/raft.go`, [ADR 0010](adr/0010-one-raft-group.md)).
Raft index equals the storage offset. Term and vote are in a hard-state file
replaced atomically and synced before any message that depends on them is
sent. A follower acknowledges entries only after syncing them; a leader
counts itself only for synced entries. A leader commits only entries of its
own term (Raft's Figure 8 rule). Messages are length-checked and fuzzed; a
peer that contradicts committed entries stops the node, because that proves
a broken peer.

**When a node serves** (`internal/broker/leader.go`,
[ADR 0011](adr/0011-not-leader-answers.md), [ADR 0013](adr/0013-leader-lease.md)).
A node answers leader-only requests only while all three hold: it is the
Raft leader; it has applied an entry of its own term, so its state includes
everything earlier leaders committed; and it holds a **lease**, meaning a
majority answered its heartbeats within ¾ of an election timeout.
Followers ignore candidates for an election timeout after hearing from a
leader, which is what makes the lease safe: no other leader can exist while
it holds. Otherwise the node answers with the not-leader error each API's
clients retry on. In Metadata a node names itself leader only while it
serves; otherwise it names the leader it knows of, or none. The lease
assumes node clocks run at rates within 25 % of each other.

**Durability.** Under `-sync always` a produce is acknowledged after the
batch is synced on a majority. `interval` and `never` acknowledge before
the local sync and may lose an acknowledged suffix on power loss, a promise
the simulator checks too ([ADR 0002](adr/0002-disk-durability-contract.md),
[ADR 0012](adr/0012-durability-in-a-cluster.md)).

**Consumer groups** ([ADR 0009](adr/0009-group-state-in-the-log.md)).
Generations, assignments and committed offsets are commands, so they are
replicated and survive restarts. Fencing (refusing a member of an old
generation) is decided at apply, from log state. Rounds in progress, held
requests and session timers are soft state; after a restart or a change of
leader, every member gets a fresh session timer and members whose requests
were held reconnect.

## 4. Real I/O

`realsys.Loop` runs the single-threaded core: goroutines read sockets and
fire timers, and a channel serialises their events into handler calls.
Peers connect over TCP with a hello naming the node id, length-prefixed
frames, at most 16 MiB a message, and a bounded queue that drops when full
(Raft tolerates loss). A connection that stops reading is closed.

## 5. The simulator

`internal/sim` runs a whole cluster and its clients in one goroutine on
virtual time, all randomness from one seed ([ADR 0007](adr/0007-history-checking.md)).

- **World** (`world.go`, `network.go`): nodes on simulated disks; power
  loss (unsynced writes torn or lost), kills, failed fsyncs, crashes during
  recovery; between nodes, messages dropped, duplicated, delayed and
  reordered, and partitions (isolate a node, cut one link, a bridge).
  Each seed draws its own mix (swarm testing).
- **Clients** (`client.go`): producers, consumers and group members that
  pick a random supported version for each request, with resets and
  fragmented bytes.
- **Checkers.** `history.go` holds an exact model of each partition and
  checks every answer: nothing acknowledged lost (under `always`), offsets
  dense and never moving, no record changing. `groups.go` checks the group
  invariants (generations only move forward and all members of one hear
  the same thing; each member gets what the leader assigned and no slot
  goes to two members; no commit accepted from an older generation;
  acknowledged commits are what an offset fetch returns, across crashes). `monitor.go` checks Raft: one leader per term, a term that never goes
  back and a vote that is never forgotten across restarts, committed entries
  never replaced, replicas that applied the same prefix having equal state
  hashes. A failure prints the seed and the command that replays it.
- **Quiet period.** Faults stop and everything heals; then a leader must
  appear, every node catch up, and a new write be acknowledged in time; then
  every node is restarted and must rebuild the same state.
- **Proof the checks work.** `TestMutationSuite` turns on deliberate bugs
  (vote before persisting, vote by log length, truncate on a stale message,
  acknowledge before sync, serve before applying the term's entry, read
  without the lease, no fencing) and requires each to be caught, then
  replays the catching seed without the bug to confirm the system is clean.
  The Figure 8 bug is caught by the Raft-level random schedules instead
  (`internal/raft/random_test.go`).

Every bug found, in the broker or in the tests, is in [BUGS.md](BUGS.md).
