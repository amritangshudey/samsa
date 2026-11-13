# ADR 0022: Single-server membership changes in the log

Date: 2026-11-14. Status: accepted. Design: `docs/design/c1-membership.md`.

## Context

Production plan item 9: nodes are added, removed and replaced while the
cluster runs. Until now the voters were the `-peers` list, fixed for life.
The algorithm was read from Ongaro's dissertation, chapter 4, and the 2015
erratum. The Kafka APIs were read from the KIP-853 schemas (design,
section 1).

## Decision

- **Single-server changes** (thesis 4.1), not joint consensus. One voter is
  added or removed at a time. Then every majority of the old configuration
  overlaps every majority of the new one, and a configuration can take
  effect as soon as it is in a node's log.
- **A configuration is a Raft log entry** (`kindConfig`). It is in force
  when appended, falls back when truncated, and is kept in the Raft
  snapshot (version 2). Every majority uses the configuration in force:
  - elections, including PreVote;
  - commitment;
  - the lease;
  - CheckQuorum.
- **The leader's rules:**
  - one change at a time (no uncommitted configuration);
  - no change before an entry of its own term has committed (the
    erratum);
  - an added node first catches up as a learner.
- **Who stands for election:** a node outside its configuration still
  stands until its removal commits (thesis 4.2.2), without counting its
  own vote. A node that knows its removal committed does not stand.
- **Only the bootstrap configuration may lead at start** when it is a
  single node (BUGS.md S9).
- **The Kafka APIs AddRaftVoter (80, versions 0–1) and RemoveRaftVoter
  (81, version 0)** request changes. Errors and authorization are
  Kafka's:
  - REQUEST_TIMED_OUT while another change is in progress;
  - DUPLICATE_VOTER;
  - VOTER_NOT_FOUND;
  - ALTER on the cluster.

  Each voter's listeners live in the configuration, so a joining node
  learns every address from the log.
- **A joining node starts with no configuration** (`Config.Join`), never
  from the bootstrap list. Ids are not reused with a new disk.

## Alternatives

- **Joint consensus** (thesis 4.3) allows arbitrary changes in one step,
  at the cost of a second kind of configuration and two majorities for
  every decision. Kafka's KIP-853 also changes one voter at a time.
- **A separate controller quorum, as in Kafka.** Samsa has one Raft group,
  and the voters are the brokers (ADR 0010).
- **Taking a configuration into force only when committed** (etcd's way).
  A leader then cannot easily know when a majority of the old
  configuration has adopted the new one. The dissertation explains why it
  chose the other way.
- **Directory ids now** (KIP-853's guard against a wiped disk rejoining
  under its old id). Deferred to part 9b. Until then ids are not reused.

## Consequences

- **Two Raft rules sit behind the broker's own guards,** which run one
  change at a time and only while serving. The simulator cannot reach
  them. The random Raft schedules catch MutTwoChanges, and a step-by-step
  replay of the erratum's counterexample catches MutChangeBeforeCommit.
- **Without PreVote** ("classic" seeds), a removed node that missed its
  removal raises its term and disrupts the leader when it is contacted
  again. That is the dissertation's "disruptive servers" case, which
  PreVote handles. The simulator's count of terms without a leader rose
  accordingly.
- **The real binary has no way to join yet.** The peer transport has a
  fixed address book, and there is no `-join` flag. That is part 9b.
