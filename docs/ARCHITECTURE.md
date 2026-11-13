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
group's assignment, and the admin commands (B4): delete a topic (with its
producer state and every group's offsets for it, in one command; topics
are named by name only, [ADR 0016](adr/0016-admin-api-versions.md)), grow
a topic's partitions, and delete a partition's records below an offset. Each starts with a type byte; an unknown type stops the
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
answer `OFFSET_OUT_OF_RANGE`. A start moved by DeleteRecords may sit inside
a batch, and a trim never moves a start back (`max`). The node then snapshots, and deletes
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
cmd/samsa/            flags, config file, metrics HTTP, `check`; wires realsys into the broker
internal/sys/         the interfaces
internal/storage/     the log: checksummed frames, segments, an offset index,
                      recovery that truncates a torn tail (ADR 0002)
internal/wire/        Kafka encoding: primitives, headers, each API's messages,
                      record batches; every decoder bounded and fuzzed
internal/raft/        Raft as a pure state machine; its log is storage.Log
internal/broker/      the node
    node.go           connections, framing, dispatch, propose/apply, leadership
    state.go          the applied state machine and its hash
    producerid.go     producer ids from per-node blocks (ADR 0015)
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
a broken peer. Three additions from [B3](design/b3-raft-hardening.md):

- **PreVote.** A node asks whether others would vote for it before it
  starts a new term, so a node that comes back from a partition no longer
  arrives with a higher term that deposes the leader.
- **CheckQuorum.** A leader that has not heard from a majority within an
  election timeout steps down.
- **Leadership transfer.** The leader stops serving, brings the chosen
  follower up to date, and tells it to stand at once. Its vote request is
  exempt from the lease rule, because the only lease involved has just
  been given up.

On SIGTERM the real loop calls `OnStop` and runs until the node no longer
leads, for at most 10 s. The simulator's `stop` fault does the same, and
half the time cuts off everything sent to the stopping leader just after
it starts handing over.

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

**Operations** ([ADR 0017](adr/0017-operations-interfaces.md)). Every 250 ms
the loop calls a hook on its own goroutine (`Loop.Every`); `cmd/samsa`
uses it to store `node.Metrics()` behind an atomic pointer, which the HTTP
handlers for `/metrics`, `/healthz` and `/readyz` read. Nothing else
touches the node from another goroutine. `samsa check` runs the node's own
recovery (`broker.Check`) on a disk wrapper that records writes instead
of making them. `OpenDisk` holds a `flock` on the data directory's `LOCK`.

**TLS and SASL** ([design](design/c5-tls-sasl.md),
[ADR 0019](adr/0019-tls-and-sasl.md)). TLS is the I/O layer's: `cmd/samsa`
wraps the client and peer listeners with `tls.NewListener`, and
`Loop.PeerTLS` dials peers with mutual TLS; the core sees plain byte
streams. SASL is the core's: each `conn` carries an `authState`; until it
holds a user, dispatch accepts only ApiVersions, SaslHandshake and
SaslAuthenticate and closes the connection on anything else. SCRAM is the
pure package `internal/scram`; the node supplies nonces from `sys.Random`.
Credentials come from a users file loaded at start and from replicated
state: `cmdAlterCredentials` holds stored and server keys, which the
leader derives before proposing ([design](design/c8b-credentials-forwarding.md),
[ADR 0021](adr/0021-credentials-and-forwarding.md)).

**ACLs** ([design](design/c8-acls.md), [ADR 0020](adr/0020-acls.md)).
ACLs are applied state: `cmdCreateACL` and `cmdDeleteACLs` in the log, a
slice in creation order in `state`, a section of the snapshot (version 5)
and part of the state hash. `acl.go` holds the authorizer, a pure function
of that slice, the configuration, the connection's principal and host,
the operation and the resource, with `StandardAuthorizerData`'s rules.
Each handler asks `n.may(c, op, resource, name)` where KafkaApis checks.
The host comes from `OnConnOpen`, which carries the client's IP address.
A follower forwards CreateAcls and DeleteAcls (`forward.go`): it sends the
encoded command to the leader on the peer channel (broker messages start
with 0xB0, Raft's with version 1), the leader proposes `cmdForwarded`, and
the follower completes its waiting request from its own apply of it.

**Application faults** ([design](design/a5b-app-faults.md),
[ADR 0018](adr/0018-application-faults.md)). `Config.Faults` gives the node
a splitmix64 stream of its own (never `sys.Random`, so faults off changes
nothing). Each eligible request draws once: a produce that wrote
(`lostack`: commit, then close the connection unanswered), a produce,
fetch or offset commit (`notleader`: the non-leader refusal of ADR 0011),
a heartbeat of a stable group (`rebalance`: `startRound`). `broker.Judge`
loads data as `Check` does and reads the input and output topics through
the replayed batch references; `samsa verdict` prints it, with the spec
the binary kept in the data directory's `FAULTS`.

**Idempotent producer** ([design](design/a2-idempotent-producer.md),
[ADR 0015](adr/0015-producer-ids.md)). Any node answers InitProducerId
with an id made of its node id, a block number it synced to the file
`producer-block` before using it, and an index in the block. Each
partition keeps, per producer id, the epoch and the last five batches'
sequences and offsets. This is applied state, so it is replicated, hashed
and in the snapshot. A batch is checked at apply, not before proposing,
because what is in sequence depends on the entries ahead of it. A retry is
answered with the first copy's offset and appends nothing. A gap or an old
epoch is refused. A producer unknown to the partition must start at
sequence 0 (BUGS.md S6). The leader samples `(time, applied index)` while
it serves. Once a sample is `-producer-expiry` old (default 24 h), it
proposes `ExpireProducers` with that index. Every replica then drops the
producers whose last batch is older, so the replicas agree without
comparing clocks.

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
  fragmented bytes. Idempotent producers (`idempotent.go`, `sync always`
  runs only) re-send unanswered batches unchanged and give up after two
  seconds with a new epoch. They check I8 (a producer that follows the
  protocol is never refused a sequence) and I9 (no producer id is handed
  out twice). Each seed sets producer expiry to never or 15 s. A retry written twice puts one record at two offsets, which
  the history model reports.
- **Checkers.** `history.go` holds an exact model of each partition and
  checks every answer: nothing acknowledged lost (under `always`), offsets
  dense and never moving, no record changing. `groups.go` checks the group
  invariants (generations only move forward and all members of one hear
  the same thing; each member gets what the leader assigned and no slot
  goes to two members; no commit accepted from an older generation;
  acknowledged commits are what an offset fetch returns, across crashes).
The model also follows the admin client (`admin.go`): a partition's start
only goes forward (R1), DeleteRecords answers the start it set, and a
fetch sent after a topic's delete was acknowledged finds no topic (D1). `monitor.go` checks Raft: one leader per term, a term that never goes
  back and a vote that is never forgotten across restarts, committed entries
  never replaced, replicas that applied the same prefix having equal state
  hashes, and no node elected while another holds a lease. A failure prints the seed and the command that replays it.
- **Quiet period.** Faults stop and everything heals; then a leader must
  appear, every node catch up, and a new write be acknowledged in time; then
  every node is restarted and must rebuild the same state.
- **Proof the checks work.** `TestMutationSuite` turns on deliberate bugs
  (vote before persisting, vote by log length, truncate on a stale message,
  acknowledge before sync, serve before applying the term's entry, read
  without the lease, no fencing, snapshot before sync, delete segments
  early, no sequence check, expire producers at once, keep the lease
  during a leadership transfer, let retention move a start back) and requires each to be caught, then
  replays the catching seed without the bug to confirm the system is clean.
  The Figure 8 bug is caught by the Raft-level random schedules instead
  (`internal/raft/random_test.go`).

**Tools** ([design](design/a3-sim-tools.md)). Faults are numbered as they
happen (`Result.Faults`). `Config.Skip` skips some of them while still
making the draws that chose them, so a run that skips nothing is the
original run. `Shrink` uses this for delta debugging over the faults. A
timeline (`Config.Timeline`) records node spans, leaders, faults, client
acks and reads and peer messages, with no effect on the run, and renders
them as HTML. The mutation suite reports a score: how many of 200 seeds
catch each bug.

**The real binary** ([design](design/a4-chaos.md)). `integration/chaos_test.go`
runs three processes with a user-space TCP proxy on every client and peer
link, applies faults from a seed (SIGKILL, SIGTERM, isolation, one-way
cuts, delays, resets) and judges every franz-go answer with the same
`sim.Model` (in `Rebatched` mode: a real client forms its own batches).
A build with `-tags unsafe` exposes the broker's mutation switches through
a hidden `-unsafe` flag, so the test can show it catches a broken build.

Every bug found, in the broker or in the tests, is in [BUGS.md](BUGS.md).
