# 0011 — What a node that is not the leader tells a client

Status: accepted, 2026-11-11

## Context

With several nodes, a client will often talk to one that cannot serve it:
a follower, a node with no leader yet, or a leader that has just been
replaced. Kafka clients recover by themselves if they are given the error
code their protocol expects. A wrong code turns a failover into a fatal
error in the application.

## Decision

A node that is not serving answers at once, per API:

| API | Answer | Code |
|---|---|---|
| Produce, Fetch, ListOffsets | each partition | `NOT_LEADER_OR_FOLLOWER` (6) |
| CreateTopics | each topic | `NOT_CONTROLLER` (41) |
| JoinGroup, SyncGroup, Heartbeat, LeaveGroup | top level | `NOT_COORDINATOR` (16) |
| OffsetCommit | each partition | `NOT_COORDINATOR` (16) |
| OffsetFetch | top level (v2+) and each partition | `NOT_COORDINATOR` (16) |
| FindCoordinator | the leader's id and address, from any node; with no known leader `COORDINATOR_NOT_AVAILABLE` (15) | |
| Metadata | answered by any node from what it has applied: every configured node as a broker; the leader as controller and as leader of every partition; with no known leader, controller -1 and `LEADER_NOT_AVAILABLE` (5) on each partition | |
| ApiVersions | answered by any node | |

A node names itself leader only once it is serving.

The codes, their names and that each is retriable were read from
`org.apache.kafka.common.protocol.Errors` in kafka-clients 4.4.0, not
from memory. `internal/broker/leader.go` holds the table;
`TestOnlyTheLeaderServes` checks every row.

**Requests in progress when leadership is lost** (a produce awaiting
commit, a parked fetch, a held JoinGroup or SyncGroup) are not answered:
their connections are closed. Whether a pending write will commit under
the new leader is unknown, and a closed connection says exactly that.

## Alternatives

- *Forward the request to the leader.* Hides failover from clients and
  adds a proxy path with its own failure modes. Kafka clients already
  know how to find a leader.
- *Answer held requests with their not-leader error instead of closing.*
  Kinder to the client (no reconnect), more code per API. Not done; the
  simulator's answer-time watchdog shows that nothing is left hanging
  either way.
- *`COORDINATOR_LOAD_IN_PROGRESS` (14) while a new leader has not yet
  applied its own entry.* More precise; the generic not-coordinator
  answer has the same effect on the client.

## Consequences

- Shown by test: franz-go continues producing across two leader kills
  (`TestClusterSurvivesLeaderKill`). Not shown: the Java client and kcat
  against a multi-node cluster; they are only tested against one node.
- A producer whose connection is closed mid-request retries, and may
  write a batch twice if the first attempt did commit. There is no
  idempotent producer (roadmap A2).
- Metadata from a follower can be a few entries behind: a topic that was
  just created may be missing there for a moment. The simulator's
  freshness check applies only to answers from the leader.
