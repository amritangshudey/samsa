# B3 design: PreVote, CheckQuorum, leadership transfer, graceful shutdown

Status: reviewed, 2026-11-12. Production plan item 2 (`docs/PRODUCTION.md`).
Sources: the Raft dissertation (Ongaro 2014), sections 3.10 (leadership
transfer), 4.2.3 (disruptive servers), 6.2 (CheckQuorum), 9.6 (PreVote).

## 1. The problem, as the code stands

Followers already ignore a RequestVote for an election timeout after
hearing from a leader (the lease rule, ADR 0013). That stops a returning
node from **winning** an election, but not from **disrupting** one:

1. Node C is cut off. Its election timer fires again and again; each time
   it starts a new term. After ten timeouts its term is ten ahead.
2. The partition heals. The leader's next AppendEntries reaches C, whose
   term is higher, so C answers "you are superseded" with its term.
3. The leader adopts C's term and steps down. Nobody leads until an
   election finishes: an outage of an election timeout or more, caused by
   a node that was only away.

Two more gaps:

- **A leader that has lost its majority stays leader.** It stops serving
  (no lease), but it keeps sending AppendEntries to the nodes it can still
  reach. Under an asymmetric partition those nodes keep hearing from a
  leader, ignore every candidate, and the majority side cannot elect.
- **Stopping the leader costs an election.** A kill or SIGTERM leaves the
  cluster without a leader for at least an election timeout; clients see
  refused requests and time-outs for that long.

## 2. Design

### 2.1 PreVote

When its election timer fires, a node first asks, without changing its
term: "would you vote for me in term T+1?" (`PreVote`, carrying T+1 and its
last index and term). A receiver answers yes if it would vote, by the same
log rule as RequestVote, **and** it has not heard from a leader for an
election timeout (the same check as now, so it can never contradict the
lease). It changes nothing: no term, no vote, nothing on disk. Only with a
majority of yes answers (its own included) does the node start a real
election (`campaign`).

A refused PreVote's answer carries the receiver's term. If that is higher
than the asker's, the asker adopts it: a higher term really exists.

The returning node C above now keeps its old term while away (its
PreVotes reach nobody), and on return the leader's AppendEntries carries
the same or a higher term: C simply follows.

A single-node cluster skips PreVote, as it skips voting.

### 2.2 CheckQuorum

At every heartbeat the leader checks that a majority, itself included, has
answered within the last election timeout (any answer counts, as for the
lease). The clock for a follower starts when the node became leader, so a
new leader gets a full timeout. If the check fails, the leader becomes a
follower in the same term. Nothing is lost: its uncommitted entries stay in
its log as they would after any step-down.

### 2.3 Leadership transfer

`TransferLeadership(to)` on a leader, `to` = 0 meaning "the most up to date
follower" (highest match index, then lowest id):

1. The leader **stops serving at once**: its lease is void, proposals are
   refused. Waiting clients are answered as at any loss of leadership.
2. When `to`'s match index reaches the leader's last index, the leader
   sends `TimeoutNow`.
3. `to` starts an election immediately, with a RequestVote marked as a
   transfer. Receivers do not apply the lease rule to it: the only leader
   whose lease that rule protects is the one that asked for the transfer
   and gave its lease up in step 1. The log rule and one vote per term
   apply as always.
4. If the leader is still leader one election timeout later, the transfer
   is abandoned and it serves again.

Why the bypass is safe in general: a transfer vote for term T+1 is sent
only after a TimeoutNow from the leader of term T. A node that heard from a
different leader recently has a term of at least that leader's; if it is
T+1 the node has already voted in T+1, and anything older is stale. So a
transfer vote can only help the transferring leader's chosen successor.

### 2.4 Graceful shutdown

On SIGTERM or SIGINT the real event loop calls the node's `OnStop`, then
keeps delivering events until the node reports `Stopped`, or for at most
ten seconds. A node that leads a cluster of more than one transfers
leadership and is stopped once it is no longer leader or the transfer has
failed; any other node is stopped at once. The interface is optional
(`sys.Stopper`), so the simulator and tests opt in.

## 3. What checks it

- **Raft scenario tests** (`internal/raft`): a node cut off for ten
  election timeouts returns, and the leader and term are unchanged. With
  the mutation `MutNoPreVote` the same test must see the term change: the
  scenario fails without PreVote. An asymmetric partition where the
  leader reaches one follower but not the majority: a new leader must be
  elected; with `MutNoCheckQuorum` none is. A transfer hands leadership to
  the chosen follower in one election, without a second candidate; one to
  a dead follower is abandoned and the leader serves again.
- **The random schedule test** and **the simulator** run with all of it on,
  so every existing safety check (S1–S5, I1–I9, groups) covers the new
  paths. The simulator gains a fault: a graceful stop of the leader (stop,
  wait until stopped, then a kill).
- **Mutation in the simulator**: `MutTransferKeepsLease` keeps serving
  during a transfer. Reads from the old leader may then miss writes the new
  one acknowledged; the history checker should catch it. (As built, it does
  not: see review point 8. A monitor check, S6, catches it instead.)
- **Integration**: SIGTERM to the leader of three real processes while a
  franz-go producer writes: no produce fails, nothing is lost or written
  twice, a new leader is named.

## 4. Alternatives rejected

- *Only CheckQuorum, no PreVote.* Does not stop a returning node's higher
  term from deposing the leader (step 2 of section 1).
- *Ignore higher terms in AppendResponse from a node we think is lagging.*
  Breaks the rule that a higher term always wins, which Raft's proofs rest
  on.
- *PreVote that also increments the receiver's term on a yes.* That is a
  vote by another name and brings the disruption back.
- *Transfer by simply stepping down.* Then everybody's lease rule holds
  them back for an election timeout: the outage the transfer is meant to
  avoid.
- *Shutdown that waits for in-flight requests to commit.* The client
  retries them anyway (idempotent producer), and waiting delays the
  handover.

## 5. Review

Second pass, looking for what breaks:

1. **PreVote and the lease must use the same check.** If a PreVote were
   granted while the lease rule would refuse the real vote, a node could
   pass PreVote and then fail every election, for ever. Both use
   `contact + ElectionTimeout`; written once, used twice.
2. **A leader ignores PreVotes and RequestVotes** (it has a leader:
   itself), except a transfer vote, which the leader that asked for the
   transfer must accept, or a three-node cluster with one node down could
   never transfer.
3. **CheckQuorum right after election**: every follower's last answer is
   from before the election. Counting from `max(answered, leaderSince)`
   gives the new leader a full timeout.
4. **A transfer racing a new proposal.** The broker stops serving before
   the transfer starts, so it proposes nothing more; Raft refuses
   proposals while transferring as a second guard. Proposing during a
   transfer would make the target's log never catch up.
5. **The target dies after TimeoutNow.** The leader abandons the transfer
   after an election timeout and serves again; the other followers' lease
   rule has run out by then, so a normal election also works.
6. **Mixed versions.** Old nodes drop the new message types as malformed:
   a cluster mixing builds from before and after this change may fail to
   elect (an old majority never answers PreVotes). Rolling upgrades are
   item 10; until then all nodes run the same build. Recorded in the
   report.
7. **Determinism.** Nothing new reads a clock except through `Env.Now`;
   the transfer target is chosen by match index and node id, never map
   order.
8. **Found while testing: the transfer mutation was unreachable.** As
   first written it also made `Transferring` false, so `Stopped` was true
   at once and the simulator killed the leader before any window opened;
   2000 seeds, never caught. With `Transferring` truthful, still not
   caught in 2000 seeds: a stale read needs the old leader to keep serving
   after its successor has acknowledged a write, and the old leader
   usually hears the transfer vote first. Two changes. The `stop` fault
   now, half the time, cuts every link *into* the stopping leader shortly
   after the handover starts (its own TimeoutNow still goes out). And the
   Raft monitor checks the guarantee itself, S6: no node becomes leader
   while another holds a lease. It is the property the lease rests on
   (ADR 0013), and the checker no longer has to wait for a client to hit
   the window. Caught at seed 11.
9. **Found while testing: snapshot chunks multiplied** (BUGS.md S7), a bug
   from Phase 5 that B3's new draws exposed as a stalled seed.
