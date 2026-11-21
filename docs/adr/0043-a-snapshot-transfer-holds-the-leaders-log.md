# ADR 0043: A snapshot transfer holds the leader's log

**Status:** Accepted  
**Date:** 2026-11-21  
**Item:** A1 (snapshots), after `docs/BUGS.md` S21; amends `docs/design/a1-snapshots.md` section 3.3

## Context

A follower whose next entry the leader no longer holds needs the leader's
snapshot, then the log from the leader's log start (the cut). The snapshot
goes in chunks of at most one command, one chunk in flight per follower.
The leader always sent its newest snapshot, and started again from the
first byte whenever that was not the one under way. A transfer that lasts
longer than the time between two snapshots therefore never ended. In seed
1668 the leader took a snapshot per entry, a transfer needed about a
thousand round trips, and it started 375 times.

Keeping the snapshot under way is not enough by itself. The leader may
compact its log past that snapshot during the transfer. The follower could
then not continue from the log after it, so the transfer would have to
start again with a newer snapshot. A leader that writes a segment or more
during the transfer does compact that far.

## Decision

1. A follower's progress keeps the snapshot being sent: its index, its
   bytes, and the next offset. The leader sends that snapshot's chunks until
   the follower holds the log past it. It takes the newest snapshot only
   when there is none under way, or when its log start has passed the one
   under way.
2. While a follower that answered within an election timeout has a
   snapshot under way, the leader compacts nothing. That lasts from the
   first chunk until the follower holds the log past the snapshot. So the
   cut it was sent stays in the leader's log. A follower that stops
   answering does not hold the log: it may be gone for good.

## Alternatives

- **Start again with the newest snapshot (before).** It never ends a
  transfer longer than the interval between snapshots (S21).
- **Keep the snapshot under way, and compact as before.** A leader that
  writes a segment or more during the transfer compacts past it.
  `TestSnapshotTransferOutlivesNewerSnapshots` fails that way.
- **Bigger chunks, or several in flight.** A transfer would take fewer
  round trips, but the race would stay, only rarer.
- **Defer snapshots while a transfer runs.** The broker decides when to
  take a snapshot, and a trim frees disk only once a snapshot follows it.
  Deferring would tie the broker to Raft's transfers. The hold has the same
  effect on disk and stays inside Raft.
- **Hold for a fixed time.** Any bound is arbitrary. A big state over a slow
  link exceeds it, and the race is back.
- **Send the snapshot as one message, as etcd does.** The peer transport
  bounds every message to one command (rule 4). A bigger message needs a
  streaming transport of its own.

## Consequences

- During a transfer the leader's log keeps what it writes meanwhile. With
  object storage, local disk then exceeds ADR 0037's bound by that much,
  until the follower holds the log past the snapshot.
- A follower that keeps answering but never ends a transfer would hold the
  log without end. Only a broken peer does that. A follower whose transfer
  fails sends no answer, so the hold lapses after an election timeout.
- The leader keeps one more snapshot in memory for each follower that
  receives an older one than its newest.
- After the hold, a leader that compacts past the follower's next index
  sends it another snapshot. Each one moves the follower forward. A follower
  slower than the write rate still falls behind, as it does with entries.
