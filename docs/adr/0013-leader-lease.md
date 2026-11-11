# 0013 — A leader answers clients only while it holds a lease

Status: accepted, 2026-11-11. Corrects a consequence accepted in ADR 0010.

## Context

ADR 0010 accepted that "a fetch answered by a leader that has been
replaced but has not heard of it can miss the newest records", calling
the window a few milliseconds. Phase 6's partitions made the window as
long as the partition: an isolated leader kept answering fetches, offset
fetches and refusals from its old state while the majority elected a new
leader and accepted writes. A client that had read from the new leader
then saw the end of a partition move backwards (I3), an old committed
offset (G3), or "unknown topic" for a topic that existed (BUGS.md S4,
S5). Raft makes writes safe on its own; reads from local state are not.

## Decision

A **lease**, as in the Raft thesis (sections 4.2.3 and 6.4.1):

1. **Followers are sticky.** A node ignores RequestVote entirely,
   including its term, while it has heard from the leader of its term
   within one election timeout, and for one election timeout after it
   starts (it may have answered a leader just before it stopped). A
   leader ignores RequestVote too.
2. **Stamps.** Each AppendEntries carries the leader's clock reading when
   sent; each response, success or refusal, echoes it.
3. **The lease** holds while the majority-th most recent of {now, each
   follower's latest echoed stamp} is less than ¾ of an election timeout
   ago. No other leader can be elected before a majority has gone a full
   election timeout without hearing from this one.
4. **Every leader-only request needs the lease**: Produce, Fetch,
   ListOffsets, CreateTopics, all group APIs, offset commit and fetch.
   Writes are decided by the log, but their refusals ("unknown topic")
   are read from applied state before anything is proposed. A parked
   fetch checks again when it completes. A node names itself in Metadata
   and FindCoordinator only while it holds the lease.

The quarter is the margin for clocks that **run at different rates**;
the clocks need not agree on the time, because each stamp is compared
only with the clock that made it.

## Alternatives

- *ReadIndex*: confirm leadership by a round of heartbeats before each
  read. No clock assumption; one round trip added to every read. Leases
  were chosen for read latency and because the simulator's clocks are
  exact; ReadIndex is the upgrade if clock-rate drift ever matters.
- *Read through the log* (propose a no-op per read): simplest and
  slowest.
- *Check-quorum* (the leader steps down when it loses the majority):
  helps clients find the new leader sooner. It does not by itself make
  reads safe. Not done.

## Consequences

- Elections take up to one election timeout longer after a leader dies,
  since followers keep ignoring candidates for that long.
- A node returning from a partition with a high term no longer deposes a
  working leader: a side effect similar to pre-vote.
- Under heavy message loss the lease lapses briefly and the leader
  answers "not leader" to requests it could have served; clients retry.
- Safety now depends on clock rates differing by less than 25 % over an
  election timeout. The simulator does not model drift.
- Mutation switch `UnsafeNoLease`; the simulator catches it.
