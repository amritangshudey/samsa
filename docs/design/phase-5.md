# Phase 5 design — Replication with Raft

Status: approved 2026-11-11 under the standing delegation. Review findings
are in section 11. `docs/ROADMAP.md` section 4 is the brief this document
answers; where they differ, the difference is listed in section 10.

Sources: Ongaro and Ousterhout, "In Search of an Understandable Consensus
Algorithm" (extended version), in particular Figure 2 (the rules), Figure 7
(follower logs) and Figure 8 (the commit rule); Ongaro's dissertation,
chapter 3 and section 10.2 on what must be on disk. Kafka error codes for
"wrong broker" will be taken from `Errors.java` and checked against what
each test client does with them, and recorded in an ADR; none is stated
here.

## 1. Concepts

**Consensus as agreement on a log.** If every node applies the same
commands in the same order to the same deterministic state machine, every
node has the same state. So the whole problem is: agree on one sequence of
log entries although nodes crash and messages are lost. Phases 2 to 4
already built everything above the log as commands; this phase replaces
"the log on my disk" with "the log a majority agrees on".

**Terms are logical time.** Time is cut into terms, numbered upwards. Each
term has at most one leader. Every message carries its sender's term; a
node that sees a higher term adopts it and stops being leader or
candidate; a message from a lower term is refused. A term is how the
cluster tells a current leader from one that was cut off and does not know
it yet.

**Why a majority.** Any two majorities of the same cluster share at least
one node. A leader needs a majority of votes, and an entry needs to be on
a majority to commit, so whoever wins the next election must have asked at
least one node that holds every committed entry.

**Election safety.** A node votes at most once per term, and remembers the
vote on disk. Hence two candidates cannot both collect a majority in one
term.

**The vote restriction and leader completeness.** A node refuses its vote
to a candidate whose log is less up to date than its own (compare the term
of the last entry, then the length). Together with the majority overlap,
the winner holds every committed entry. Entries therefore only ever flow
from leader to followers, and a leader never overwrites its own log.

**Log matching.** If two logs hold an entry with the same index and term,
the logs are identical up to there. It holds because a leader creates one
entry per index in its term, and a follower accepts new entries only if
its log matches the leader's at the entry just before them.

**Figure 8: the commit rule.** An entry from an *earlier* term that sits
on a majority can still be overwritten: a node that lacks it may hold a
later-term entry at the same index and win an election. So a leader counts
replicas only for entries of its own term. Earlier entries commit
indirectly, when a current-term entry after them commits. For this reason
a new leader appends an empty entry (a no-op) at once.

**What must be on disk before which message.** A node may forget nothing
it has told anyone. A vote must be durable before it is granted, or the
node could restart and vote again in the same term. Entries must be
durable before they are acknowledged, or a "majority" could evaporate in a
power cut.

**How Kafka's ISR replication differs.** Kafka keeps, per partition, a set
of in-sync replicas; a write commits when all of them have it, and a
separate controller (itself now a Raft quorum, KRaft) decides who is in
the set and who leads. That tolerates f failures with f+1 replicas where
Raft needs 2f+1, and lets a slow replica be dropped from the set rather
than outvoted. It costs a second system to manage membership, and safety
then depends on that system and on configuration (`min.insync.replicas`,
unclean leader election). Samsa uses one Raft log for everything: more
replicas for the same fault tolerance, one leader for all partitions, and
one algorithm to get right.

## 2. Placement and the seam

`internal/raft` is a new core package under the purity check. One `Raft`
value per node, driven by the node. It never touches the network or the
clock itself:

```go
// What the node gives Raft.
type Env interface {
    Send(to sys.NodeID, msg []byte)          // best effort
    After(d time.Duration, fn func()) Timer  // the node's one-shot timers
    Rand() uint64
}

func (r *Raft) Start() error                  // recovery
func (r *Raft) Step(from sys.NodeID, msg []byte)
func (r *Raft) Propose(cmd []byte) (index int64, err error) // ErrNotLeader
func (r *Raft) Committed() int64
func (r *Raft) Read(index int64) (cmd []byte, noop bool, err error)
func (r *Raft) Leader() (id sys.NodeID, known bool)
func (r *Raft) Flush() error                  // sync the log, then act on it
```

The broker today calls `storage.Log` directly in five places
(`Append`, `Sync`, `DurableOffset`/`EndOffset`, `Read`, replay). Those go
through the three methods the roadmap's `CommandLog` names, plus `Flush`
and a callback `OnRoleChange`. There is one implementation, Raft: a
cluster of one node elects itself, and its majority is its own disk, so
"committed" is again "synced locally". The single-node path of Phases 2 to
4 is not kept as separate code. That every earlier test passes on a
one-node Raft is the proof that the seam is right (roadmap task 7).

## 3. The log and the hard state

**Entries.** A `storage.Log` in directory `raft`. One entry is one frame
of count 1 whose payload is: term (8 bytes), kind (1 byte: command or
no-op), then the command. Raft index i is storage offset i; the first
entry has index 0 and "nothing before it" is index -1, term 0.

The broker's partition index stores log indexes, and a fetch reads the
command back by index, so the node strips the 9-byte prefix on read.
No-ops take an index and are skipped by apply. The applied state and its
hash depend only on (index, command) pairs, which are the same on every
node: the state hash stays a replica-agreement check (S4).

A data directory written by Phase 4 (directory `log`, no entry header) is
not read. Nothing has been released, so there is no migration; the
directory name differs so that an old one is ignored, not misparsed.

**`storage.TruncateSuffix(offset)`.** Removes every record at or after
offset, and is durable before it returns: truncate the file (or remove
whole later segments, newest first), sync, sync the directory if segments
were removed. Raft refuses to call it below the commit index. After a
crash at any point inside it, recovery must yield a prefix of the old log
that is at least as long as `offset`; crash tests in the Phase 1 style
check exactly that.

**Hard state.** `currentTerm` and `votedFor` in one file of fixed size
with a checksum, replaced atomically: write `hardstate.tmp`, sync it,
rename over `hardstate`, sync the directory. Missing file: term 0, no
vote. A file that fails its checksum stops the node; it cannot happen
under the disk contract (rename is atomic), so it means corruption, and
guessing a term would break election safety.

The commit index is not stored. After a restart it starts at -1 and is
learned again: a leader recomputes it once its no-op commits, a follower
is told. Applied state is rebuilt by applying as the commit index
advances, not by replaying the whole log at start, because a restarted
node cannot know which of its entries are committed.

## 4. Persist before send

| Before | must be durable |
|---|---|
| granting a vote | `currentTerm`, `votedFor` |
| any message carrying a new term | `currentTerm` |
| acknowledging entries up to i | the log up to i |
| a leader counting itself for i | its own log up to i |

Hard state is written and synced inside the handler that changes it,
before the handler sends anything. Elections are rare, so the cost is
irrelevant.

The log keeps group commit. A follower appends on receipt and answers
after the node's flush timer has synced, with the durable index. A leader
sends entries to followers at once, without waiting for its own sync, and
counts itself only up to its durable index.

**The fsync policies.** "Durable" above means `DurableOffset` under
`-sync always`. Under `interval` and `never` it means `EndOffset`: the
node treats appended as durable, as it did on one node. A cluster run that
way survives process crashes of a minority, and a power cut on a majority
at once can lose acknowledged writes and can break Raft's safety argument
outright (a node may forget entries it acknowledged). This is stated in
the ADR and the simulator checks the cluster invariants only under
`always`.

## 5. The algorithm, as implemented

State per node: role, `currentTerm`, `votedFor`, the log, `commitIndex`,
the leader it believes in; as candidate, the votes received; as leader,
per follower `nextIndex`, `matchIndex` and whether a request is in flight.

**Messages.** RequestVote, AppendEntries and their responses, in a private
binary encoding (version byte, type byte, fixed fields, entries). They are
untrusted input: every length is bounded before allocation and the decoder
is fuzzed. A message from a node id that is not a configured peer is
dropped.

**Every message** first: if its term is higher, persist the new term with
no vote and become follower; if lower, answer with our term (requests) or
ignore (responses).

**Election.** A follower or candidate whose election timer fires
increments its term, votes for itself, persists both, and asks everyone.
The timeout is drawn anew from `sys.Random` in [T, 2T) each time it is
armed. The timer is re-armed only on a valid AppendEntries from the
current term's leader and on granting a vote, not on any message. A vote
is granted if we have not voted for someone else this term and the
candidate's last (term, index) is at least ours. A majority of votes makes
a leader, which appends a no-op and sends AppendEntries to all.

**Replication.** One AppendEntries in flight per follower, carrying
entries from `nextIndex` up to a byte budget, with the index and term of
the entry before them and the leader's commit index. It is sent again when
the heartbeat timer fires. The follower:

1. refuses if it has no entry at the previous index with that term, and
   says how long its log is so the leader can jump back;
2. walks the new entries: one that is already present with the same term
   is skipped; at the first index where its entry has a *different* term
   it truncates from there, and appends the rest. A message that merely
   carries fewer entries than the follower holds truncates nothing;
3. advances its commit index to min(leader's commit, index of the last
   entry this message confirmed), never backwards;
4. answers, after the sync, with the highest index it now knows matches.

The leader, on a response of its current term: on success raises
`matchIndex` (never lowers it) and `nextIndex`; on refusal lowers
`nextIndex` and sends again.

**Commit.** After any change to a `matchIndex` or to its own durable
index, the leader takes the highest index held durably by a majority; if
that entry's term is the current term, it becomes the commit index.

**Losing leadership.** A leader that sees a higher term becomes follower
before doing anything else, and tells the node, which fails what it was
holding (section 6).

There is no check-quorum and no pre-vote: a leader cut off from the others
keeps believing it leads until it hears a higher term. It can commit
nothing in that time. Both are listed as limits.

## 6. The node on top

**Apply.** Unchanged in shape: when `Committed()` has advanced, the node
reads each entry, applies commands in index order, and completes the
waiter for that index if it has one. A follower has no waiters and applies
all the same. Waiters now remember the term they were proposed in: if the
entry that commits at their index has another term, their command was
overwritten and they fail.

**Not leader.** Only the leader proposes, and only the leader serves
fetches, offset fetches and group requests. Any other node answers with
the retriable error each API defines for a wrong or unknown leader or
coordinator; the choice per API is verified against the three clients and
recorded in the ADR. Metadata is served by every node: all brokers from
the static configuration, and the believed leader as leader of every
partition and as controller; with no known leader, the partitions carry
the "no leader yet" error.

**A new leader serves nothing until it has applied its own no-op**
(bug 11): before that it does not know how much of its log is committed,
and its applied state may be behind what clients were already told.

**Soft state and leadership (rule R5).** On losing leadership the node:
fails every waiter with the not-leader error; answers parked fetches,
held JoinGroups and held SyncGroups the same way; cancels session,
rebalance and sync timers; forgets issued member ids and rounds. On
gaining leadership, once the no-op is applied, it does what `OnStart` did
in Phase 4: a fresh session timer per applied member, and a sync deadline
for groups that are completing. Connections stay open throughout.

**Fetch on a deposed leader.** It can still answer before it notices. It
returns only committed batches, which never change, so this is safe; the
end of log it reports may be stale. OffsetFetch can be stale in the same
window: a consumer may re-read, which at-least-once permits.

**Duplicates.** A producer that retries after a leader change may write a
batch twice: the first attempt may have committed although its
acknowledgement was lost. Expected without the idempotent producer
(roadmap A2), and counted, not hidden, in the tests.

## 7. Peers and transport

Static configuration: for each node its id, peer address and client
address (`-peers 1=host:port/host:port,...`). No membership change.

```go
// sys, added in this phase.
type NodeID int32
// Network
SendPeer(to NodeID, msg []byte) // best effort: may be lost, never corrupted
// Handler
OnPeerMessage(from NodeID, msg []byte) error
```

`realsys`: one outgoing TCP connection per peer, length-prefixed messages,
a hello frame naming the sender, reconnect with backoff, a bounded queue
that drops the oldest message when full. All of that is outside the core.
`simsys`/`sim`: delivery after a random delay; in this phase no loss,
duplication or reordering beyond what delays cause (Phase 6 adds them).

## 8. Tests

Raft is tested as a state machine with no network: a harness holds N
`Raft` values on simulated disks, collects what each sends, and delivers,
drops, duplicates or reorders at will, with a manual clock.

The roadmap's twelve expected bugs, each with a test written first:

| # | Bug | Test |
|---|---|---|
| 1 | vote answered before it is durable | grant, crash before sync is possible to observe, restart: the same term's second request is refused; and a disk that fails the hard-state sync sends no vote |
| 2 | committing an earlier term's entry by counting | the Figure 8 schedule, step by step |
| 3 | truncating on a stale or duplicate AppendEntries | deliver an old, shorter message after a newer one: log unchanged |
| 4 | acknowledging unsynced entries | follower's response carries the durable index; no response before flush |
| 5 | trusting a response blindly | stale-term and reordered responses: `matchIndex` never decreases |
| 6 | election timer reset by any message | a stream of refused votes and stale AppendEntries does not stop an election |
| 7 | `votedFor` not cleared, or cleared without persisting | higher term seen, crash, restart: may vote once in the new term |
| 8 | comparing logs by length | longer log with older last term loses the vote |
| 9 | applying twice or past the commit index | node-level: applied sequence equals committed prefix across restarts |
| 10 | deposed leader keeps answering writes | higher term arrives: pending proposals fail, new ones are refused |
| 11 | new leader serves before applying | fetch during the window is answered not-leader |
| 12 | held requests lost on a leadership change | every held request is answered (the I7 watchdog, and unit tests) |

Plus: Figure 7's six follower logs each converge to the leader's; three
and five nodes elect exactly one leader per term under random delivery;
crash tests for `TruncateSuffix` and the hard-state file; fuzzing of the
message decoder and of `Step` with arbitrary bytes.

**Simulator.** Three nodes, each with its own disk; clients connect to any
node and follow not-leader errors. Faults as in Phase 3, per node. New
invariants, checked by a monitor that may read node internals (unlike the
client-history checks):

- S1 at most one leader per term;
- S2 log matching, compared pairwise on durable prefixes when quiet and on
  every append in the unit harness;
- S3 every entry seen committed is in the log of every later leader;
- S4 equal state hash at equal applied index;
- S5 a node's term and commit index never decrease while it runs, and its
  term never decreases across restarts;
- S6 the client invariants of Phases 3 and 4, with "acknowledged" meaning
  acknowledged by any node.

Mutation switches, each with a test that the simulator notices: count
replicas for any term (Figure 8); answer AppendEntries before the sync;
grant votes without persisting.

**Integration.** Three processes on localhost; a franz-go producer writes
continuously; the leader is killed with SIGKILL; afterwards every
acknowledged record is consumed. Duplicates are counted and reported; a
missing record fails the test.

## 9. Alternatives rejected

- **Keep the Phase 4 local log for one node and add Raft beside it.** Two
  commit paths, and the single-node one would stop being tested by the
  cluster work. One path, exercised by every test, is worth the cost of a
  self-election at start.
- **Replicate per partition, as Kafka does.** Scales writes, and needs a
  metadata quorum as well as per-partition logs. Out of scope (roadmap C3).
- **Store the commit index.** Saves re-learning it after a restart. It is
  never needed for safety, and a stored value that is wrong is worse than
  none.
- **A sync inside the AppendEntries handler** instead of the flush timer.
  Simpler to reason about, and one fsync per message instead of one per
  burst.
- **Pre-vote and check-quorum now.** They fix disruption by a rejoining
  node and stale leaders that keep serving reads. Neither is needed for
  safety. Listed for later.
- **Use an existing Raft library.** The point of the phase is to implement
  it; and none of them runs on `sys.Disk` and a virtual clock.

## 10. Differences from the roadmap

- The roadmap offsets Raft indexes from storage offsets "by a fixed base".
  Here they are equal, with -1 for "before the first entry".
- The roadmap's `CommandLog.Read` returns a command; here it also says
  whether the entry is a no-op, since no-ops occupy indexes.
- The local single-node log is removed rather than kept behind the seam.
- Simulator invariants S1 to S5 read node internals. ADR 0007's rule that
  correctness is judged from client-visible history still governs S6.

## 11. Review record

Second pass, hunting for anything told to another node or a client before
it is durable, and for soft state that outlives leadership:

1. **Leader counts itself too early.** The first draft advanced the commit
   index on `EndOffset`. Under `always` the leader's own disk is one of
   the majority only once synced: commit uses the durable index.
2. **Follower commit index past what it verified.** min(leader commit,
   last index of the log) is wrong when the follower holds a stale suffix
   beyond the entries this message checked. It is min(leader commit, last
   index this message confirmed).
3. **Restart applies uncommitted entries.** Replaying the whole log at
   start, as Phases 2 to 4 did, would apply entries that may yet be
   truncated. Apply follows the commit index only.
4. **Waiter completed by a different entry.** After a leadership change an
   index may commit holding another leader's command. Waiters carry their
   term; and losing leadership fails all waiters at once, so the check is
   a second line of defence.
5. **Truncation under a parked fetch or the partition index.** Only
   uncommitted entries are truncated, and only applied (committed) entries
   are indexed, so nothing applied ever points at a removed entry.
6. **Timers that fire after a role change.** Every Raft timer callback
   checks the role and term it was armed for.
7. **Group timers on a follower.** A follower applies group commands but
   must not expire members: all group timers exist only on the leader.
8. **Member ids across leaders.** Still 128 random bits from each node's
   own source, carried in the command: unique without coordination.
9. **Hard-state write fails.** The node stops (durability contract rule
   5); nothing was sent yet, because the write precedes every send.
10. **One-node cluster.** Majority of one: the node elects itself on the
    first election timeout. Start-up therefore waits one timeout before
    serving. Special-cased: a node whose configuration lists only itself
    starts an election at once.
