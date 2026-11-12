# B3 report: PreVote, CheckQuorum, leadership transfer, graceful stop

Branch `b3-raft-hardening`, 2026-11-13. Production plan item 2.
Design: `docs/design/b3-raft-hardening.md`.

## What was built

- **PreVote** (`internal/raft`): when its election timer fires, a node first
  asks whether the others would vote for it in the next term. Nothing
  changes at the receivers. It starts a real election only with a majority
  of yes answers. A yes requires the asker's log to be up to date and the
  receiver to have heard from no leader for an election timeout, the same
  check as the lease rule, written once.
- **CheckQuorum**: at each heartbeat a leader checks that a majority
  answered within an election timeout, counting from when it became
  leader. If not, it becomes a follower in the same term.
- **Leadership transfer**: `TransferLeadership(to)` (0 picks the most
  up-to-date follower). The leader gives up its lease and refuses
  proposals, then sends `TimeoutNow` once the target holds the whole log.
  The target stands at once with a transfer vote, which is exempt from the
  lease rule. A transfer still pending after an election timeout is
  abandoned.
- **Graceful stop**: `sys.Stopper` (`OnStop`, `Stopped`). On SIGTERM or
  SIGINT the real loop calls `OnStop` and keeps running until `Stopped`,
  for at most 10 s. A second signal kills at once. The broker's leader
  hands over; any other node stops immediately.
- **Mutations**: `MutNoPreVote`, `MutNoCheckQuorum`, `MutTransferKeepsLease`.
- **Simulator**: a new fault, a graceful stop (`stop`); half the time it
  also cuts every link into the stopping leader just after the handover
  starts. Half the seeds run the paper's Raft, without PreVote and
  CheckQuorum (`classic=true` in the setup line). A new monitor check,
  **S6**: no node becomes leader while another holds a lease.
- **Bug fixed** (BUGS.md S7, from Phase 5): each heartbeat resend and each
  duplicated answer started its own chain of snapshot chunks. Seed 875
  stalled on it. Now only an answer showing progress sends the next chunk
  (`TestSnapshotChunksDoNotMultiply`).

## Results

| | |
|---|---|
| Returning node, 10 election timeouts away | leader and term unchanged; with `MutNoPreVote` the leader is deposed |
| Leader that can only send, to one follower | the majority elects a new leader; with `MutNoCheckQuorum` it never does |
| Transfer | one election, to the chosen follower; to a dead follower it is abandoned and the leader serves again |
| Real cluster, SIGTERM to the leader during writes | exited in 43 ms; 66 batches of 10 acknowledged, none failed, each read back once |
| Raft random schedules, 300 seeds; 7 Raft mutations | clean; all caught. "Vote before it is on disk" is caught only on a classic seed (86) |
| Simulator, 5000 seeds | clean, 5111 handovers, 68213 leaders checked against S6 |
| Simulator mutation suite, 12 mutations | all caught within 2000 seeds; "keep serving during a transfer" by S6 at seed 11 |
| Seed 875 (S7) | stalled before the fix; 0.4 s after. In `regressions.txt` |

## How to run it

    go test ./internal/raft -run 'Returning|CheckQuorum|Transfer'
    go test ./internal/broker -run GracefulStop
    make integration     # TestClusterGracefulStop
    make sim SEEDS=1000

## What is untested or weak

- **PreVote hides the vote bugs.** With it, contested elections are rare,
  and a double vote or a vote by length rarely does harm. Both test suites
  therefore run half their seeds as classic Raft. A bug that only appears
  with PreVote on *and* contested elections would be found only by the
  other half's luck.
- **Mixed versions** cannot elect reliably: an old node drops PreVote as
  malformed. Every node must run the same build until rolling upgrades
  (item 10).
- **The transfer target is chosen by match index alone.** A follower that
  is up to date but slow or overloaded can still be picked.
- **A graceful stop does not wait for requests in flight.** They are
  answered "not leader" or their connection closes, and clients retry. The
  integration test shows franz-go doing so with no error. The Java client
  is not tested here.
- **The transfer mutation is caught by S6, not by a client.** No seed in
  2000 showed a stale read from it: the old leader nearly always hears the
  transfer vote before its successor commits anything. S6 checks the
  lease guarantee directly, so it does not depend on that luck, but the
  history checker alone would not have caught this bug.
- **No check covers liveness or cost.** S7 surfaced only as a hang. A
  message storm that slowed a run without stopping it would pass.
- **CheckQuorum uses the leader's clock**, as the lease does. Clocks are
  assumed to run at rates within 25 % of each other (ADR 0013).

## Ten questions

1. Why does the lease rule (ignore candidates for an election timeout) not
   stop a returning node from disrupting the leader?
2. What exactly does a PreVote change at the receiver, and why must that
   be nothing?
3. Why must PreVote and the lease rule use the same "heard from a leader"
   check?
4. Why does a refused PreVote's answer carry the receiver's term, and why
   may the asker adopt it?
5. What failure does CheckQuorum fix that PreVote does not?
6. Why does CheckQuorum count from `max(answered, leaderSince)`?
7. Why is the transfer vote allowed to bypass the lease rule? What would
   break if any vote could?
8. Why does the leader stop serving before it sends TimeoutNow, and not
   after?
9. Why do half the seeds run without PreVote, and what does that cost?
   Why is S6 a stronger check of the transfer than a stale read?
10. What does a client see during a graceful stop, and why is no write
    lost or doubled?
