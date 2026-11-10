# 0009 — What part of a consumer group lives in the command log

Status: accepted, 2026-11-11

## Context

A group coordinator holds three kinds of state: committed offsets, who is
in the group and what each member was assigned, and the machinery of a
rebalance in progress (requests being held, timers). ADR 0005 says every
state change is a command in one log. Phase 5 replaces that log with a
replicated one, so whatever is not a command is lost on failover.

## Decision

Three commands, applied by the deterministic state machine
(`internal/broker/state.go`):

| Command | Carries |
|---|---|
| `OffsetCommit` | group, generation, member id, commit time, offsets |
| `GroupGeneration` | a completed generation: number, protocol, leader, members with their metadata and timeouts |
| `GroupAssignment` | the leader's assignment for one generation |

Everything else is soft state on the node (`softGroup` in
`internal/broker/groups.go`): the round of JoinGroups being collected,
held SyncGroups, member ids issued but not yet used, members that left or
expired since the last generation, and all timers.

Two rules follow.

1. **Validation happens in apply.** Whether a commit's generation is
   current is decided at the commit's position in the log, not when the
   request arrived. Fencing a zombie is therefore a property of the state
   machine, identical on every replay and, later, on every replica.
   `GroupGeneration` applies only if it is exactly one generation ahead,
   and `GroupAssignment` only once per generation, which is what will
   discard a deposed leader's late proposals.
2. **Soft state must be safe to lose.** After a restart the group is at
   its last applied generation, every member gets a fresh session timer,
   and clients whose requests were held see their connection close and
   retry. Measured with real clients: a broker killed with SIGKILL under a
   stable group comes back and neither franz-go nor the Java client sends
   a single JoinGroup.

## Alternatives

- **Offsets in the log, membership in memory.** Simpler. Every restart, and every failover
  in Phase 5, would force every group to rebalance.
- **Heartbeats as commands.** Liveness would be replicated, at one log
  write per heartbeat per member. Instead, a dead member is noticed one
  session timeout after a restart rather than on time.
- **Validate in the handler only.** Wrong under replication, where "the
  state when the request arrived" is not a position in the log, and it
  would leave a window between check and apply even on one node.
- **An `__consumer_offsets` topic as in Kafka.** The same idea with more
  machinery: one log is already there.

## Consequences

- A member that left or expired remains a member in applied state until
  the next generation is applied. Its commits are still accepted in that
  window; nobody else owns its partitions yet, so nothing is overwritten.
- A generation is one command and must fit in one log record (1 MiB of
  member metadata by default). Groups are limited to 1000 members.
- Groups and offsets are never deleted. This needs snapshots (roadmap A1)
  and offset expiry; the commit time is stored for that.
- The simulator's group invariants (G1 to G4, G6) are written against
  client-visible history, so they carry over to Phase 5 unchanged.
