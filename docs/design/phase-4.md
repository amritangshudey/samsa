# Phase 4 design — Consumer groups

Status: approved 2026-11-10 under the standing delegation. Review findings
are in section 9.

Sources, read on 2026-11-10 from the Apache Kafka repository (`trunk`):
the message definitions of the seven APIs
(`clients/src/main/resources/common/message/*.json`); `Errors.java`; and,
for behaviour, the classic-group paths of
`group-coordinator/.../GroupMetadataManager.java` and
`classic/ClassicGroup.java`. `docs/ROADMAP.md` section 3 is the brief this
document answers.

## 1. Concepts

**Why a coordinator.** Several consumers want to share a topic so that
each partition is read by exactly one of them, and to continue where
another stopped. Somebody must know who is alive and store how far each
partition has been processed. That is the group coordinator: a broker.

**Group, member, generation.** A group is named by the client. A member is
one consumer in it, identified by a member id the broker issues. Each time
membership changes, the group moves to a new *generation*, numbered
upwards. The generation is a fencing token: requests carry it, and a
request from an older generation is refused.

**The two-step rebalance.**

1. *JoinGroup*: every member announces itself and the assignment
   strategies ("protocols") it supports. The broker holds the requests
   until all known members have rejoined or a timeout passes, then picks a
   protocol they all support, picks a leader, and answers. Only the leader
   is told the member list.
2. *SyncGroup*: the leader, a client, computes who gets which partition
   and sends that. The broker hands each member its share.

**Why assignment happens in a client.** The broker stores and relays
assignments as opaque bytes. New strategies need no broker change, and the
broker need not know what a "partition" means to the group (Kafka Connect
uses the same protocol to assign tasks).

**Three timeouts.** *Session timeout*: no heartbeat for this long and the
member is declared dead. *Rebalance timeout*: how long a rebalance waits
for members to rejoin. `max.poll.interval.ms` is a client-side limit on
time between polls; the client sends it as its rebalance timeout.

**Committed offsets and at-least-once.** A member periodically stores the
next offset it will read. After a crash or a rebalance, whoever owns the
partition resumes there. Work done after the last commit is redone:
at-least-once.

**Zombies.** A member that stalled (a long GC pause), was declared dead,
and then wakes up believing it still owns its partitions. Its next commit
carries the old generation and is refused, so it cannot move the offset
backwards under the new owner.

## 2. Versions

By the rule of ADR 0006 (Kafka 4's minimum up to the last non-flexible
version):

| API | key | Supported | Notes |
|---|---|---|---|
| OffsetCommit | 8 | 2–7 | 2 is Kafka 4's baseline; 7 adds the group instance id |
| OffsetFetch | 9 | 1–5 | 2+ may ask for all topics; 8+ batches groups |
| FindCoordinator | 10 | 0–2 | 4+ batches keys |
| JoinGroup | 11 | 0–5 | from 4 the member id must be obtained first |
| Heartbeat | 12 | 0–3 | |
| LeaveGroup | 13 | 0–3 | 3 carries a list of members |
| SyncGroup | 14 | 0–3 | |

What the three clients negotiate, and whether the Java client's default
group protocol is the classic one, is observed by the integration tests and
recorded in ADR 0008. The newer consumer protocol (KIP-848) is announced by
an API this broker does not advertise.

**Static membership** (a `group.instance.id`, KIP-345) is out of scope. The
field is read and ignored: such a member is treated as an ordinary dynamic
member. This is stated in the report as a limit.

## 3. What is a command and what is soft state

The roadmap's table, made concrete. Three new commands:

```
3 OffsetCommit:     group, generation, member id, commit time,
                    then (topic, partition, offset, leader epoch, metadata)*
4 GroupGeneration:  group, generation, protocol type, protocol, leader,
                    then (member id, client id, session timeout,
                          rebalance timeout, (protocol name, metadata)*)*
5 GroupAssignment:  group, generation, then (member id, assignment)*
```

Applied state per group: generation, protocol type and name, leader, the
members of that generation with their metadata, whether an assignment has
been applied and each member's share, and the committed offsets.

**Derived states.** In applied state a group is `Empty` (no members),
`CompletingRebalance` (members, no assignment yet) or `Stable` (members and
assignment). Only `PreparingRebalance` is soft: it is a round of JoinGroup
requests being collected on one node.

**Validation happens in apply.** A commit is checked against the
generation *in the log order*, not against what the handler saw when the
request arrived:

- `OffsetCommit` with generation -1 and no member id (a consumer that
  assigns partitions itself) succeeds only if the group has no members;
  otherwise the member must exist and the generation must match, and a
  group still completing its rebalance answers `REBALANCE_IN_PROGRESS`.
- `GroupGeneration` applies only if its generation is exactly one more
  than the group's. One node has one proposer, so this never fails here;
  under Raft a deposed leader's proposal can arrive late, and this is the
  check that discards it.
- `GroupAssignment` applies only to the current generation, and only once.

So fencing (invariant G4) is a property of the state machine, replicated
with it, not of timing on one node.

**Soft state per group, per node:** the round in progress (who has
rejoined, their held requests, the rebalance timer), member ids issued but
not yet used, held SyncGroup requests, members that left or expired since
the last generation, and one session timer per member.

**On restart** soft state is empty. Each applied member gets a fresh
session timer. Members continue heartbeating into the generation they
know; a member whose join was in flight gets `UNKNOWN_MEMBER_ID` (its
issued id was soft) and starts over.

## 4. The coordinator's rules

Let *expected* be the applied members minus those that left or expired.

**JoinGroup.**

| Situation | Action |
|---|---|
| Session timeout outside the configured range | `INVALID_SESSION_TIMEOUT` |
| Empty protocol list or type, or a protocol type different from the group's, or no protocol in common with the other members | `INCONSISTENT_GROUP_PROTOCOL` |
| No member id, version 4+ | issue an id, remember it for one session timeout, answer `MEMBER_ID_REQUIRED` |
| No member id, version 0–3 | issue an id and treat as joined |
| Unknown member id | `UNKNOWN_MEMBER_ID` |
| Issued id, first use | joined: start or extend a round |
| Known member, round in progress | joined |
| Known member, no round, and it is the leader or its protocols changed | joined: start a round |
| Known member, no round, nothing changed | answer at once with the current generation (member list only for the leader) |
| Group already has the maximum number of members | `GROUP_MAX_SIZE_REACHED` |

A round starts by arming the rebalance timer with the largest rebalance
timeout among expected members and joiners, and by answering held
SyncGroups with `REBALANCE_IN_PROGRESS`. The round completes when every
expected member has joined, or when the timer fires. On completion:

- protocol: among those every joiner supports, each joiner votes for its
  first; most votes wins, ties to the smaller name;
- leader: the previous leader if it rejoined, else the first joiner;
- propose `GroupGeneration` with the joiners (possibly none: the group
  becomes empty). The held JoinGroups are answered when it is applied.

A JoinGroup that arrives after the proposal and before its apply waits for
the next round, which starts as soon as the generation is applied.

**SyncGroup.** Wrong member or generation: `UNKNOWN_MEMBER_ID`,
`ILLEGAL_GENERATION`. Round in progress: `REBALANCE_IN_PROGRESS`. Stable:
the member's assignment, at once. Completing: hold; when the leader's
arrives, propose `GroupAssignment` (members the leader omitted get an empty
assignment) and answer everyone on apply. If no assignment is applied
within the rebalance timeout, a new round starts, so a leader that never
syncs cannot stall the group.

**Heartbeat.** Checks member and generation; resets the session timer;
answers `REBALANCE_IN_PROGRESS` during a round, otherwise success
(including while completing, as Kafka does).

**LeaveGroup** and **session expiry** remove the member from *expected*
and start a round, or complete one that was only waiting for that member.

**OffsetCommit.** Metadata over 4096 bytes: `OFFSET_METADATA_TOO_LARGE`.
Otherwise proposed; the result of apply is the answer. An accepted commit
from a member also counts as a heartbeat.

**OffsetFetch.** Reads applied state. No membership check. A partition
with no commit is offset -1 with no error. A null topic list (version 2+)
returns every committed partition of the group.

**FindCoordinator.** This node, for key type 0 (group). Other key types:
`COORDINATOR_NOT_AVAILABLE`.

**Member ids** are the client id, a dash, and 128 random bits in hex from
the injected `sys.Random`, which the node now takes as a fourth
dependency. They never repeat across restarts, and they are chosen by the
proposer and carried in the command, so apply stays deterministic (R1).

**Timers.** The node gains a table of timer id to function. Every held
request is covered: a JoinGroup by the rebalance timer, a SyncGroup by the
sync deadline, and both by their connection closing.

## 5. Simulator

New client behaviour, *member*: finds the coordinator, joins (through
`MEMBER_ID_REQUIRED`), computes a round-robin assignment if leader, syncs,
then heartbeats, commits and fetches offsets; reacts to errors as a real
client does; and at random leaves, goes silent, or carries on as a zombie
with a stale generation. Session and rebalance timeouts are short and
random.

| | Invariant | How |
|---|---|---|
| G1 | In one generation no partition is given to two members, and each member receives exactly what the leader sent it | the model records every assignment sent and received |
| G2 | A group's generation never goes backwards for an observer, and all members of a generation agree on leader and protocol | on every JoinGroup response, by causality |
| G3 | OffsetFetch returns an acknowledged commit unless a later one could have replaced it | see below |
| G4 | A commit carrying a generation older than one already seen is refused | on every OffsetCommit response |
| G5 | Every held request is answered or its connection closed in bounded time | the Phase 3 watchdog (I7) |
| G6 | G3 across crashes, under the strict policy | same check; the model forgets only under weak policies |

**G3 precisely.** Each commit carries a unique offset, so a fetched value
names its commit. Let S be the latest send time among commits acknowledged
before the fetch was sent. A fetched commit is wrong if it was itself
acknowledged before S: the commit sent at S came strictly after it in the
log. "No commit" is wrong if S exists. This needs no assumption about the
order of acknowledgements from different connections.

Under weak fsync policies a power loss may roll groups back; the model
then forgets generations, assignments and S, as it does for log ends.

## 6. Alternatives rejected

- **Keep group membership only in memory**, as a first version of Kafka
  did, and store only offsets. Every restart, and later every leader
  change, would force all groups to rebalance. As commands, a failover
  leaves stable groups stable.
- **Make every heartbeat a command.** Liveness would then be replicated,
  at the cost of a log write per heartbeat per member. Session timers are
  soft and restart with a full timeout; a dead member is noticed one
  session timeout after a failover instead of on time.
- **Validate commits in the handler only.** Simpler, and wrong under
  replication, where "the state when the request arrived" is not a
  position in the log.
- **Broker-side assignment** (the KIP-848 model). A better protocol, but a
  second one; the classic protocol is what all three test clients speak.
- **An initial rebalance delay** (Kafka waits 3 s on the first join so
  that consumers started together share one generation). Not built: the
  only cost is one extra rebalance at start-up.

## 7. Limits

- No static membership, no KIP-848, no offset expiry, no group deletion,
  no DescribeGroups or ListGroups (so admin tools cannot inspect groups).
- Groups and offsets are never removed: an abandoned group stays in the
  state machine until snapshots and expiry exist.
- A group generation is one command and must fit in one log payload.

## 8. Acceptance (roadmap 3.6)

Two franz-go consumers share a topic; one is killed; the other takes over
and resumes from the committed offsets. The same with the Java client. The
broker restarts under a stable group and consumers continue with their
offsets. G1 to G6 hold over the CI seeds, and each fires on a deliberately
wrong history.

## 9a. What changed during implementation

- **No client id in `GroupGeneration`.** Section 3 listed one per member.
  Nothing reads it without DescribeGroups, so it is not stored.
- **Requests that arrive while a generation is being proposed** are not
  kept in a round's table (review item 1). They are set aside and run
  again, from the start, when the generation has been applied. Same
  behaviour, and one place decides what a request means.
- **The first member of an empty group forms a generation at once**, as
  section 6 says; a second consumer started with it causes a second
  generation a moment later. Seen in the integration tests, harmless.
- **G5 is the existing watchdog** (I7): no request may stay unanswered for
  10 s on a live connection. The simulated members' rebalance timeouts are
  at most 3 s, so a held request that outlives its timers is caught.

## 9. Review record

Second pass, hunting for requests that are never answered and for state
that differs between replay and live execution:

1. **A round whose proposal is in flight.** Joins arriving between propose
   and apply were lost in the first draft. They now stay in the round's
   table and trigger the next round on apply.
2. **The timer and the last join in the same instant** (roadmap pitfall).
   Completion clears the round before proposing, and the timer callback
   checks that the round it was armed for is still the current one.
3. **A member that leaves during a round it had already joined.** Its held
   JoinGroup is gone with its connection or was answered; it is removed
   from the joiners as well as from *expected*.
4. **Replay.** Applying `GroupGeneration` must not touch timers or
   connections: apply changes only applied state, and the proposer's
   completion callback (which does not run on replay) does the rest.
   Session timers are armed from applied state in `OnStart`.
5. **A commit racing a rebalance.** Decided by log order in apply, which
   is the same on every replica.
6. **Leader never syncs.** Covered by the sync deadline.
7. **Unbounded growth from hostile clients.** Issued-but-unused member ids
   expire after one session timeout and count towards the group size
   limit; group ids and protocol metadata are bounded by the request size
   limit, and groups by nothing yet (stated as a limit).
8. **Closed connection with a held request.** `OnConnClose` already
   removed parked fetches; held joins and syncs are removed the same way.
