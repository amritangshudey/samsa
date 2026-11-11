# 0010 — One Raft group, fixed membership, the leader serves everything

Status: accepted, 2026-11-11

## Context

ADR 0005 put every state change in one command log. Phase 5 has to make
that log survive the loss of a machine. The questions are how many
replicated logs there are, who may answer clients, and how nodes find each
other.

## Decision

- **One Raft group for the whole cluster.** The command log *is* the Raft
  log. Every node stores every entry and applies it to the same state
  machine, so topics, records, consumer groups and offsets are all
  replicated by the same mechanism.
- **Only the leader serves.** Produce, fetch, offset and group requests
  are answered by the leader alone, and only once it has applied an entry
  of its own term (`Node.serving`). Metadata and FindCoordinator are
  answered by any node and name the leader (ADR 0011).
- **Fixed membership.** The node list is a start-up flag
  (`-peers id=clientAddr/peerAddr,...`), the same on every node. There is
  no joining, leaving or replacing of nodes.
- **Raft lives in `internal/raft`** as a pure state machine driven by
  `Step`, `OnTimer` and `Flush`. A Raft index is a storage offset. A
  single node is a cluster of one that elects itself at start; there is no
  second, non-replicated code path.
- **No snapshots, no log compaction.** A node that restarts applies its
  log from the beginning as the commit index reaches it.
- **No pre-vote and no check-quorum.** A leader that is cut off keeps
  believing it leads until it hears a higher term. It can commit nothing
  in that time.

## Alternatives

- *A Raft group per partition*, or *a leader and replica set per
  partition with a separate controller quorum*: both scale
  writes across nodes and both multiply the protocol state by the number
  of partitions. Rejected for this project's goal: one mechanism that can
  be explained and model-checked end to end.
- *Followers serve fetches*: needs a read index or leases to be correct.
  Left out; listed in the roadmap.
- *Keeping the Phase 2 local log for single-node use*: two paths to test
  for no gain.

## Consequences

- Write throughput and storage do not grow with the number of nodes; a
  three-node cluster is one node's capacity with two spare copies.
- Replacing a dead machine means starting a node with the same id and an
  empty disk, which then receives the whole log. There is no other way to
  change the cluster.
- Start-up time grows with the log, on every node, until snapshots exist.
- A fetch answered by a leader that has been replaced but has not heard
  of it can miss the newest records (it never returns uncommitted ones).
  With this phase's reliable network the window is a few milliseconds;
  Phase 6 adds partitions and has to deal with it. *(2026-11-11: it
  was not a few milliseconds under partitions, and is closed by ADR
  0013.)*
