# C1 design: cluster membership changes

Status: reviewed, 2026-11-14. Production plan item 9 (`docs/PRODUCTION.md`):
"Cluster membership changes (single-server), node replacement. Must show:
simulator adds and removes nodes under faults; mutation 'two changes at
once' caught."

The item is built in two parts, each with its own PR and report:

- **9a, this design's core:** membership as Raft log entries, the leader's
  rules for changing it, the broker's view of it, and the simulator adding
  and removing nodes under faults. This is where the evidence lives.
- **9b:** the real binary. That means peer addresses that change while
  running, the Kafka quorum APIs, a `samsa` command to add and remove
  nodes, node replacement, and a test on real processes. Section 7 sketches
  it; it gets its own design section before it is built.

## 1. Sources

Read on 2026-11-14.

- **Ongaro's dissertation, chapter 4** (`membership/safety.tex` and
  `availability.tex` in github.com/ongardie/dissertation):
  - **Only one server is added or removed at a time.** Then any majority of
    the old configuration overlaps any majority of the new one.
  - **A configuration is a log entry.** It "takes effect on each server as
    soon as it is added to that server's log". Commitment uses a majority
    of the new configuration.
  - **When the change is complete.** The change is complete when its entry
    commits. Only then may the leader answer, may a removed server shut
    down, and may the next change start.
  - **Truncation.** "A log entry for a configuration change can be
    removed (if leadership changes); in this case, a server must be
    prepared to fall back to the previous configuration in its log."
  - **Requests are processed without consulting the configuration.** A
    server accepts AppendEntries from a leader outside its configuration,
    and grants votes to such a candidate.
- **The erratum** (the dissertation README links Ongaro's raft-dev post of
  2015-07-10, "bug in single-server membership changes"):
  - **The bug:** a leader whose uncommitted configuration came from an
    earlier term can overwrite a later committed one. This needs a cluster
    of even size, competing changes and a change of leader.
  - **The fix,** verbatim: "a leader may not append a new configuration
    entry until it has committed an entry from its current term."
- **Kafka KIP-853** schemas (trunk):
  - AddRaftVoter (80, versions 0–1);
  - RemoveRaftVoter (81, version 0);
  - DescribeQuorum (55, versions 0–2).

  `kafka-metadata-quorum.sh add-controller` needs a Kafka controller
  properties file and a `meta.properties` directory id
  (`MetadataQuorumCommand.handleAddController`), so it cannot drive
  Samsa. `describe` and `remove-controller` send only the API. That is for
  9b.

## 2. Raft: the configuration in the log

- **A third entry kind, `kindConfig`.** Its payload is the configuration,
  and Raft parses only part of it:
  - the voters, as node ids, sorted and unique, at least one;
  - an opaque blob per voter that Raft never interprets.

  The broker puts each voter's client and peer addresses in the blob, so
  a new node learns every address from the log. Raft bounds the count and
  the sizes before allocating (rule 4).
- **The configuration in force** is the last `kindConfig` entry in the
  log. Without one, it is the configuration the snapshot holds. Without
  that, it is the bootstrap configuration from `Config.Peers`.
  - Raft keeps the list of configuration entries above the snapshot,
    with their indexes.
  - Truncating the log pops the ones it removes, so the node falls back
    as the dissertation requires.
  - Opening a node rebuilds the list from the log, which it already reads
    entry by entry to get the terms.
- **The snapshot holds the configuration as of its index.** This is
  snapshot file version 2, and InstallSnapshot carries it. A node that
  installs a snapshot takes the snapshot's configuration, unless its log
  keeps a later configuration entry above the snapshot.
- **Every majority uses the configuration in force:**
  - elections, including PreVote;
  - commitment;
  - the read lease;
  - CheckQuorum;
  - the choice of transfer target.

  The leader counts itself only while it is a voter.
- **Messages from any node are processed,** not only from voters, as the
  dissertation says. Whom to accept is a question for the transport,
  which authenticates peers (mutual TLS, C5). Raft still drops a message
  whose sender id is its own.
- **A node that is not a voter in its own configuration never starts an
  election or PreVote.** It is a new node that has not yet received the
  entry adding it, or a removed node that has received its removal. It
  still answers AppendEntries and votes.

## 3. Raft: the leader's rules for a change

`ChangeVoters(add or remove one id, blob) (index, error)` on the leader
refuses with a distinct error when:

1. **It is not the leader,** or it is transferring.
2. **No entry of its current term has committed yet** (the erratum). Its
   no-op is enough. The caller retries.
3. **A configuration entry is still uncommitted.** This is "two changes
   at once". The caller retries.
4. **The change is not exactly one voter:**
   - an add of a present voter;
   - a remove of an absent one;
   - a remove of the last voter.
5. **An added node has not caught up.** See below.

Otherwise it appends the entry, which is in force at once, and returns
its index. The caller learns the outcome as for any proposal: the entry
commits, or leadership is lost.

**Catching up before voting** (dissertation 4.2.1). A node added with an
empty log would make the cluster wait for it, or lose availability if
another node failed meanwhile. So an add has two steps:

1. **The leader takes the new node as a learner.** It replicates to the
   node, which gets AppendEntries and snapshots like a follower, without
   counting it in any majority.
2. **The leader appends the configuration entry** once the learner's
   match index reaches the leader's last index as of the previous round.
   This is the dissertation's round test, with rounds bounded by a heartbeat.

If the learner has not caught up after 10 election timeouts, the add
fails with a timeout and the learner is dropped. Learners are leader soft
state, lost with leadership, like everything else in progress.

**Removing the leader itself** (dissertation 4.2.2). The leader appends a
configuration without itself and goes on leading, not counting itself in
majorities, until the entry commits. Then it steps down. Its CheckQuorum
and the others' election timers elect a successor from the new
configuration. A graceful handover would be faster. That is left to 9b,
which can transfer leadership first.

**Disruptive removed nodes** (dissertation 4.2.3). PreVote and the lease
rule (B3) already protect against them. A removed node that never
received its removal times out and asks for PreVotes. Every voter that
hears from the leader ignores it. A removed node that did receive its
removal never campaigns (section 2).

## 4. Broker

- **The configuration's blob holds each voter's id, client address and
  peer address.** This is the same data as `-peers`. Metadata, DescribeCluster
  and the non-leader refusals list the voters in force, not `cfg.Peers`.
  `cfg.Peers` becomes the bootstrap configuration only.
- **Replication factor:** CreateTopics checks against the current voter
  count.
- **The node tells the I/O layer when the voters change,** so that it can
  reach new peers. The simulated network reads the configuration from the
  world; the real transport is 9b.
- **Snapshots of the broker state are unchanged.** The configuration lives
  in Raft's snapshot, not the broker's.

## 5. Simulator

- **The world has spare node slots.** A run starts with N voters and up
  to two spares that are created with empty disks and no configuration.
  A spare knows only the bootstrap list, so it waits.
- **An admin client asks the leader for a change, every few seconds.** It
  adds a spare, or removes a voter, never going below one voter. Removing
  the leader is allowed. The admin retries refusals 2 and 3.
- **A removed node is stopped once its removal commits,** and its slot
  may later hold a spare with a new id and an empty disk. That is node
  replacement. Ids are never reused (section 6, finding 3).
- **Faults continue throughout:** crashes, partitions, drops, duplicates,
  disk faults.
- **The monitor** checks S1–S6 against the configuration in force on
  each node. New rules:
  - **M1:** no two configuration entries are uncommitted in one leader's
    log at once;
  - **M2:** a committed configuration differs from the committed one
    before it by exactly one voter.
- **Mutations:**
  - `MutTwoChanges` skips rule 3, "two changes at once". The plan names
    this one.
  - `MutChangeBeforeCommit` skips rule 2, the erratum.

  Each must be caught by the suite or, if the simulator cannot reach it
  reliably, by a Raft unit test, as with S8.

The Raft random-schedule test (`random_test.go`) also gets membership
changes, because it faults harder than the world does.

## 6. Second review

1. **The read lease across a change.** The lease counts acks from a
   majority of the configuration in force, which is the new one from the
   moment it is appended. A rival candidate that still has the old
   configuration needs a majority of it, and that majority overlaps any
   majority of the new one (one server changed). So at least one of its
   voters has heard from the leader and ignores it. A leader that is
   removing itself counts only the others. The lease stays sound. It is
   checked by S6, which the monitor keeps.
2. **A wiped node must not start from the bootstrap list.** If a voter
   loses its disk and restarts with `-peers`, it believes it is a voter
   with an empty log and no memory of its votes. It can vote twice in one
   term. This is not new: it is the disk-loss case of item 13 (B1). But
   membership makes it routine, because a replacement node starts empty.
   **Decision:** the bootstrap configuration applies only to the nodes
   that form a new cluster. A node that joins an existing cluster starts
   with no configuration (`-join` in 9b; a spare in the simulator) and
   learns its configuration from the leader.
3. **Reusing a node id is unsafe without more machinery.** Take a node
   removed in term T, wiped, and added back under the same id while the
   term is still T. It has forgotten its vote in T. A losing candidate of
   term T whose log still holds the old configuration counts that node as
   a voter. If the node now votes for it, there are two leaders in term
   T (S1). Kafka solves this with directory ids (KIP-853): a voter is an
   id and a disk.
   **Decision for 9a:** ids are never reused, so replacement uses a new
   id. In the simulator a spare always has an id never seen before. 9b
   decides between directory ids and a refusal.
4. **Does the removed node learn of its removal?** If the leader stopped
   sending to it as soon as the removal was appended, the node would
   usually never learn of it. It would then time out and send PreVotes
   forever (harmless, because voters ignore them while they hear from a
   leader, but noisy). **Decision:** the leader keeps replicating to a
   removed node, without counting it, until the removal commits. After
   that it stops. A removed node that has the entry never campaigns.
5. **A removed node that missed its removal cannot win.** It needs votes
   from a majority of the old configuration. That majority overlaps the
   majority of the new configuration that committed the removal, and a
   node in the overlap has a longer log and refuses. This is the normal
   Raft argument. The new configuration is ordinary log content.
6. **The erratum at a change of leader.** A new leader may find an
   uncommitted configuration from an earlier term in its log. Rule 2
   blocks new changes until its own no-op commits, and that commits the
   old configuration too. Rules 2 and 3 are both needed: rule 3 alone
   allows exactly the erratum's counter-example.
7. **Determinism.** Learners and voters are slices sorted by id, never
   map iteration. A configuration is encoded sorted, and decoding rejects
   unsorted or duplicate ids, so every node sees the same bytes and the
   same order.
8. **Mixed versions.** An older node fails on the new entry kind: it
   stops with an error and does not misread the entry. Rolling upgrades
   are item 10, which must order this. Recorded.
9. **Scope check.** 9a changes Raft's majority arithmetic everywhere. The
   existing tests (random schedules, the simulator, S1–S6) run unchanged
   on a static configuration first, and only then is membership switched
   on. A refactor that breaks them shows up before the new feature
   hides it.

## 7. Part 9b, in outline

- **The real transport:** the peer address book changes while the node
  runs. It dials new voters and drops removed ones.
- **The Kafka quorum APIs:**
  - DescribeQuorum (55) for `kafka-metadata-quorum.sh describe --status`;
  - RemoveRaftVoter (81) for `remove-controller`;
  - AddRaftVoter (80) for a `samsa node add` command, since Kafka's tool
    cannot describe a Samsa node.

  Directory ids decide whether a wiped node may rejoin under its old id.
- **Node replacement:** remove the dead node, then add the new one under a
  new or the same id.
- **A test on three, then four, then three processes,** with clients
  producing throughout.
