# C19 report: fetch from the nearest replica

2026-11-15. Production plan item 19. Design `docs/design/c19-follower-fetch.md`,
ADR 0031.

## What was built

- **Wire:** Fetch v11 `RackId` is read (it was dropped) and
  `PreferredReadReplica` written; Metadata and DescribeCluster carry each
  broker's rack; error 78 OFFSET_NOT_AVAILABLE.
- **Racks:** `-peers id=client/peer@rack`.
- **Leader:** names the lowest-numbered member in the consumer's rack that is
  not itself and has caught up (its match index has reached the commit index),
  with no records.
- **Follower:** answers fetches of v11 or later from applied state while it
  hears its leader, is not recovering and is not receiving a snapshot.
  OFFSET_NOT_AVAILABLE for an offset it has not applied. Counter
  `samsa_follower_fetch_bytes_total`.
- **Simulator:** racks, consumers that send a rack and follow a named replica,
  the model's rules for follower answers, one mutation, two coverage counters.
- **Tests:** wire round trips (and fuzz), broker (a trio with three racks: the
  named replica, the leader's own rack, no rack, a lagging replica; a follower
  asked for an offset it lacks), and `TestFetchFromTheNearestReplica` on three
  real processes with franz-go.

## Results

**Simulator:** 3,000 seeds pass; in them 14,682 fetches were answered by a
follower with records, and 7,347 answers named a replica.

**Mutation** `UnsafeFollowerOutOfRange` (a follower that has not applied an
offset says out of range, which sends a real consumer to its reset policy):
caught by 21 of 200 seeds, first by I1 ("reported out of range, but the log
end was already ...").

**Real processes:** three nodes in racks a, b and c. A franz-go consumer
(`kgo.Rack`) in the leader's rack read 3,000 records and no follower served
a byte. One in a follower's rack read all 3,000 and that follower reported 40,881
bytes served; the leader's consumer-visible answers carried no records for it.

**Bytes between racks** (the plan's G10 asks for this): in that run the
consumer in the follower's rack read 40,881 bytes from its own rack's node,
and the leader answered its fetches with a name and no records. The cross-rack
bytes of that consumer were the names only. This is one small run, not a
measure of a workload; a bill cannot be measured on one machine.

## Problems met

- **BUGS.md S13:** a follower served while a snapshot was being installed, when
  its log had been reset under its state (seed 749); it stopped on "read
  committed batch". A node installing a snapshot now refuses.
- **BUGS.md C19-1:** the rack was lost for every node of a real cluster
  because the member's address came from the log; found by the real-process
  test.
- **BUGS.md M18:** the simulator judged an answer by the node's role at
  receipt, not at the time; nine seeds failed on it.

## How to run it

```sh
go test ./internal/broker -run 'Rack|OnlyTheLeader' -v
make sim SEEDS=1000
go test ./internal/sim -run 'TestMutationSuite/a_follower_tells' -v
make build && cd integration && SAMSA_BIN=../bin/samsa go test -run TestFetchFromTheNearestReplica -v .
./bin/samsa -peers 1=h1:9092/h1:9093@a,2=h2:9092/h2:9093@b,3=h3:9092/h3:9093@c -node-id 1 -data d
# consumers: client.rack=b
```

## What is untested or weak

- **Java and librdkafka** were not run with `client.rack`; only franz-go.
- **Racks of nodes added with `samsa node add`** are not known, so they are
  never named. The rack is not in the log.
- **Stale reads:** a follower answers as the cluster was until it applies
  the next command. A deleted topic, a moved log start or a revoked ACL reach
  it late. Kafka's followers have the same property.
- **The lagging-replica retry** costs a consumer one round trip; the simulator
  checks that the answer is right, not that the consumer returns to the leader
  after a time (the client does that, not the broker).
- **The simulator holds answers that may come from either role to the
  lesser rules**, which weakens its checks for them.

## Ten questions

1. **Why can a follower serve at all?** Every node applies every committed
   entry, in order, and serves only what it has applied.
2. **Why OFFSET_NOT_AVAILABLE and not OUT_OF_RANGE?** The consumer is not wrong,
   the follower is behind; out of range sends it to its reset policy (the
   mutation).
3. **Why does the leader send no records with the name?** Kafka does; the
   consumer moves, and records from the leader would cost the traffic being
   avoided.
4. **Why only from v11?** Older fetches carry no rack, so no redirect can
   happen; a client below v11 would not know to expect a follower's answers.
5. **How does the leader know a replica is caught up?** Its match index (what
   the replica has acknowledged as durable) has reached the commit index.
6. **Why must a follower not serve during a snapshot install?** Its log has
   been reset under the state it serves from (seed 749).
7. **Why is the rack not in the log?** Cost against the common case of a
   fixed cluster; the limit is stated.
8. **Can a follower return uncommitted data?** No: it reads applied state.
9. **What does a deleted topic look like from a follower?** Present until it
   applies the delete.
10. **What would the plan's cross-rack measurement need?** A cluster with
    real rack boundaries and a workload; here one run on one machine.
