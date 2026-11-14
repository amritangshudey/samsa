# ADR 0030: Repair damage to the Raft log from the other nodes

Date: 2026-11-15. Status: accepted. Design: `docs/design/b1-storage-faults.md`.

## Context

A disk can change data that was synced and acknowledged. Samsa's log is
also its data store: batches are read back from it at their log index, long
after the entries have been applied. Before this ADR a node refused to start
on damage in a sealed segment, and cut a damaged active segment at its first
bad frame, which can silently forget acknowledged entries on a node that then
votes ("Protocol-Aware Recovery for Consensus-Based Storage", FAST 2018).

## Decision

- **Damage is a bad frame in a sealed segment.** A bad frame in the active
  segment stays a torn tail. The two cannot be told apart there (a crash
  leaves garbage blocks in unsynced data, and valid ones can follow), and
  treating them as damage would send a cluster that restarts together into
  recovery at once. The limit is stated.
- **A scrub finds damage early.** Once a second the node reads up to
  `-scrub-bytes-per-second` (default 8 MiB) of sealed segments and checks
  each frame; after a full pass it rests for `-scrub-interval` (24 h). A read
  of damaged data does the same job when a client or a follower needs it.
- **A run that finds damage records it and stops.** The next start repairs.
  Repair happens only at start, where nothing has been applied.
- **Repair cuts or empties.** Damage after the snapshot: the log is cut at
  the damage and the leader sends the rest. Damage under the snapshot, or a
  snapshot that does not read: the snapshot file and the whole log are
  removed and the leader sends its snapshot and log, as to a new node. The
  hard state is kept, so the node cannot vote twice in a term.
- **A recovering node votes for nobody and never campaigns** until it has
  fetched back what it lost: its commit index has reached the log end it had
  before the repair, or the leader has nothing more to send it. A durable
  `recovering` file says so across restarts. This is the rule of the paper:
  with damage on a minority, every committed entry is on a node that is not
  recovering, which refuses any candidate that lacks it.
- **A cluster of one refuses to start on damage.** There is no copy.
- **No format change.** The two marker files are new, and only a build that
  has this ADR reads them. A downgrade during recovery is not supported.

## Alternatives

- **Tell damage from a tear exactly,** with a "synced up to here" mark in
  every frame (a new level, 8 bytes a frame): left as the stated limit.
- **Repair a hole,** keeping the valid frames after the damage: needs a log
  with holes in every reader. Rejected.
- **Repair at run time,** without a restart: the broker cannot take back what
  it applied. Rejected.
- **Cut at the damage even under the snapshot.** The log would end before
  its snapshot; every check that ties them (checkSnapshot, commit) would
  have to change. Emptying the log reuses the install path.

## Consequences

- A repair under the snapshot re-sends the whole log: the cost of replacing a
  node. Damage after it costs the log after the damage.
- During recovery a cluster of three has two nodes that vote; a second
  failure at that moment stops elections (not data). Damage and membership
  changes together can leave two voters, so the simulator does not combine
  them.
- The scrub's read load is bounded and runs on the core thread.
- `samsa_recovering`, `samsa_storage_damage_repaired_total` and
  `samsa_scrubbed_bytes_total` show it; `/readyz` is false while recovering.
