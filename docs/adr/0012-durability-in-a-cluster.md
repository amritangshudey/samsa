# 0012 — Persist before send, and what the fsync policies mean in a cluster

Status: accepted, 2026-11-11

## Context

Raft's proofs assume that a node never forgets what it told another node:
its term, its vote, and the entries it acknowledged. ADR 0002 says bytes
are durable only after `Sync` returns. Between those two facts sits every
classic Raft bug that only shows after a power cut. Separately, the broker
has three fsync policies (`-sync=always|interval|never`) whose meaning was
defined for one node.

## Decision

**Persist before send, always, for term and vote.** The hard state is
written to a temporary file, synced, renamed and the directory synced,
before any message that depends on it leaves the node. This holds under
every fsync policy: it is rare and it is what keeps "one leader per term"
true.

**Entries, under `-sync=always`:** a follower answers AppendEntries only
after the entries are synced, and the leader counts its own log towards
the majority only up to its synced index. "Committed" means synced on a
majority. An acknowledged write survives the simultaneous power loss of
every node.

**Entries, under `-sync=interval` and `-sync=never`:** nodes acknowledge
and count entries as soon as they are appended (`raft.Config.
AckBeforeSync`). "Committed" then means *written, not synced,* on a
majority. A process crash loses nothing. A power cut on enough nodes can
lose acknowledged writes, and because a node may forget entries it
acknowledged, Raft's guarantees no longer hold in that case: replicas can
diverge, and a node that notices stops with an error rather than continue.
A leader re-sends entries to a follower that has lost them.

**`acks` in Produce** is unchanged from ADR 0005: 0 gets no response; 1
and all are both answered after commit.

## Alternatives

- *Sync entries always in a cluster, whatever the flag says.* Safe, and
  makes the flag a lie: users of the weaker policies are asking for
  speed at the price of a window of loss.
- *Under weak policies, sync on followers but not on the leader* (or the
  reverse): a half measure with no clear statement of what survives.
- *Refuse to start a multi-node cluster with a weak policy.* Considered
  seriously, because the failure mode is ugly. Rejected at first; see the
  question in the Phase 5 report.

## Amendment (2026-11-11)

The developer chose to refuse: `samsa` exits with an error when `-peers`
names more than one node and `-sync` is not `always`. The rules above for
the weak policies stay in the code, and the simulator still runs them in
clusters (it configures nodes directly), but no released binary can start
in that state.

## Consequences

- With `-sync=always` every commit costs a sync on a majority; group
  commit still shares one sync among requests that arrive together.
- The simulator runs power loss, mid-operation disk crashes and failed
  syncs against clusters only under `always`. Under the weak policies it
  injects process kills only. **What a cluster with a weak policy does
  after a power cut is specified above and not tested.**
- Mutation switches prove the tests notice each rule being broken:
  `UnsafeVoteWithoutPersist` (vote sent before it is durable) and
  `UnsafeAckBeforeSync` (entries acknowledged before sync under the
  strict policy).
