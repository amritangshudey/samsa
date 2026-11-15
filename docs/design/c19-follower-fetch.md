# C19: fetch from the nearest replica (KIP-392)

Production plan item 19, 2026-11-15. ADR 0031. Report
`docs/reports/c19-follower-fetch.md`.

## 1. What and why

In Kafka a consumer reads from the leader of each partition. In a cluster
spread over availability zones, two thirds of that traffic crosses a zone,
and cloud providers bill it. KIP-392 (accepted; in Kafka since 2.4) lets a
consumer say which rack it is in; the broker names a replica in that rack,
and the consumer reads from it.

Samsa already has the follower's data: every node applies the same log, so
every node holds every partition up to what it has applied, which is only
what is committed. Until now a follower refused fetches. This item makes it
answer them.

## 2. The protocol (checked against the KIP page, 2026-11-15)

- **Fetch v11** carries the consumer's `RackId` (an existing field that
  Samsa read and dropped). The response's `PreferredReadReplica` (int32 per
  partition, -1 for none) names the node to read from next.
- The leader answers with the replica named **and no records**: the consumer
  moves, then fetches from the replica.
- A follower returns only committed data. A fetch for an offset it has not
  applied is **OFFSET_NOT_AVAILABLE (78)**, which is retriable and does not
  make a consumer reset; OFFSET_OUT_OF_RANGE would.
- Metadata and DescribeCluster carry each broker's `rack`.

## 3. Design

- **Racks:** `-peers id=client/peer@rack,...`. The rack is part of the
  configuration every node is started with, so every node knows every rack;
  it is not in the log. A node added later with `samsa node add` has no rack
  (limit, reported).
- **Choosing a replica (leader only):** for a fetch of v11 or later with a
  rack, if the leader is itself in that rack it answers as before. Otherwise
  it names the lowest-numbered member in the rack that is not the leader and
  whose match index (what the leader knows is durable there) is at least the
  commit index. If no member qualifies, it answers as before. The partition
  must have answered without error.
- **Serving as a follower:** a node that is not the leader answers a fetch of
  v11 or later if it has heard from its leader within an election timeout, is
  not recovering from damage, and is not receiving a snapshot. It reads from
  applied state, so nothing uncommitted. Below v11 it refuses as before, so
  old clients are sent to the leader.
- **Behind:** an offset past what it has applied is OFFSET_NOT_AVAILABLE;
  below its start is OFFSET_OUT_OF_RANGE as before. A fetch long-polls
  as on the leader and is woken when the node applies more.
- **What a follower may answer that the leader would not:** stale data. A
  deleted topic, a moved log start or a revoked ACL reaches a follower when it
  applies the command, so for a short while it answers as the cluster was.
  Kafka's followers do the same.
- **Metric:** `samsa_follower_fetch_bytes_total`.

## 4. The simulator

- Racks `r0..r2` by node id. Consumers send a random rack in Fetch v11 and
  follow a named replica (pinned to it, then free again after a while). Any
  fetch that lands on a follower is served.
- **The model:** an answer from a follower is held to what holds of
  committed data (every record is one that was sent, at its offset, in its
  place), not to what a leader promises (the floor, a log start that never
  goes back, a topic that exists after creation or not after deletion).
  OFFSET_NOT_AVAILABLE is accepted from a follower and is an error from a
  leader. A named replica must be a node in the consumer's rack, and its
  answer carries no records.
- **Which node answered** is judged by whether it was serving when the
  request was sent, when it was answered and in between (a counter of its
  changes of role): an answer that may have come from either is held to the
  lesser rules.
- **Mutation:** `UnsafeFollowerOutOfRange`: a follower tells a consumer that
  is ahead of it that its offset is out of range. The existing "reported out
  of range, but the log end was already" check (I1) catches it.
- **Coverage:** follower reads and named replicas.

## 5. Alternatives

- **Rack in the log** (so racks of added nodes are known everywhere): needs
  the member info to carry it. Left out: the common case is a fixed cluster.
- **Leader serves some data with the redirect** (as a hint): Kafka returns
  none; clients ignore it.
- **A follower fetches from the leader what it lacks:** a blocking read. The
  consumer waits and retries instead.

## 6. Second review

1. *Can a follower serve data the leader later loses?* It serves applied
   state, applied only after commit, and a committed entry is never lost
   (the rest of the project's claim). The simulator's records-are-the-ones-sent
   check applies to every answer.
2. *A snapshot being installed:* the log is reset to the leader's start and
   the state is still the old one, which refers to entries that are gone.
   The first version served during it and the node stopped on
   "read committed batch ... not in [1553, 1749]" (seed 749). A node
   receiving a snapshot does not serve.
3. *A consumer that bounces:* the leader names a replica, the consumer goes
   there, it is behind and answers NOT_AVAILABLE, the consumer retries; for
   as long as the replica lags, the leader keeps naming it only if its match
   index has reached the commit index, so a lagging one is not named. In
   between, the consumer waits one retry. Kafka's client also falls back to
   the leader after a lease.
4. *Racks of members added later:* none, so they are never named.
5. *Authorization:* a follower checks ACLs from its applied state, which can
   be behind: a revocation takes effect there when applied.
6. *Does a faulted leader name a replica?* The not-leader fault fires only
   when the node cannot read.

## 7. As built

As designed, with: the rack is also read from the first configuration when
the member's address comes from the log (`members()`): the first version
lost it for every node in a real cluster and named none (found by the
real-process test); and the fetch faults of item 23a draw after the
redirect.
