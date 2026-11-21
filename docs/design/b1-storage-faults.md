# B1: storage faults beyond crashes, scrub, repair from peers

Production plan item 13, 2026-11-15. Roadmap B1. Builds on the storage
log (`docs/design/phase-1.md`), Raft recovery and snapshots
(`docs/design/a1-snapshots.md`), and the simulated disk.

## 1. What and why

A disk can change data that was synced and acknowledged: a bit flips, a
sector reads back zeros, a write lands in the wrong place. Today Samsa
meets that badly:

- **Found at start in a sealed segment:** the Raft start-up scan reads the
  log from the snapshot and fails with `ErrCorrupt`. The node refuses to
  start. Safe, but it stays down until someone deletes its data.
- **Found at run time:** a read that fails its checksum sets `n.fatal` and
  the node stops, with the same result at the next start.
- **In the active segment, found at start:** recovery cuts the segment at
  the first bad frame, however many valid frames follow. If the bad frame
  was rot in synced data, the node forgets entries it acknowledged, and then
  votes. That is the failure "Protocol-Aware Recovery for Consensus-Based
  Storage" (Alagappan et al., FAST 2018) shows in real systems: a quorum
  can lose a committed entry.

A replicated log can repair this: the data is on other nodes. What it must
not do is let a node that lost entries it acknowledged help elect a leader
before it has them back.

Goal, from the plan: with corruption on a minority of nodes no committed
data is lost and the cluster repairs itself; the naive mutation is caught
losing data.

## 2. The fault model, and its limits

The simulator injects, into synced data at rest, a run of 1 to 64 bytes
overwritten with random bytes, or a 512-byte block zeroed. A misdirected
write, seen from the file, is the same thing.

Not covered, and said plainly:

- **The active segment.** A bad frame there is indistinguishable from a
  crash tear: a crash leaves garbage blocks in the unsynced tail, and a
  valid frame can follow a garbage one, so "valid frames after the bad
  one" does not mean synced data was damaged (the simulated disk does
  exactly this). Treating it as damage would send every node of a cluster
  that restarted together into recovery at once, which deadlocks. Recovery
  keeps cutting the active segment at the first bad frame. Rot in the
  active segment, which is at most one segment of the newest data, can
  therefore still make a node forget what it acknowledged. The first limit
  of the report, and what a per-frame "synced up to here" mark (a new
  format level) would fix.
- **The hard state file** (term and vote), the **snapshot file**, the
  **install marker.** Each is one small file with a checksum, replaced
  atomically. A node whose copy fails its checksum still refuses to start.
  A lost vote can let a node vote twice in a term, which needs its own
  design.
- **More than a minority** of nodes damaged at once.
- **A single-voter cluster:** nobody to repair from. A damaged node of one
  refuses to start, with a message that says so.

## 3. Design

### 3.1 Damage: a bad frame in a sealed segment

A sealed segment was synced whole before its successor was created, so a
bad frame in it, or one that does not lead to the next segment's base, is
damage and cannot be a tear. `storage.Log.FirstDamage(from)` scans the log
from offset `from` and returns the offset where the first bad frame of a
sealed segment starts, which is the end of the last good frame before it,
and the offset where the log ended (`End`, the next segment's base or the
log's end). The torn tail of the active segment is cut as before and is
never damage. `Log.Scrub(cursor, budget)` is the same scan in steps (3.4).

### 3.2 Repair happens at start

Detection can come at any time: the start-up scan, a read for a fetch, the
scrub. At run time a detection stops the node as a failed read does today
(`n.fatal`, message "restart to repair"); the process exits and its
supervisor starts it again. Repair is code in `raft.Start`, where nothing
is applied yet and the log is read from the snapshot:

1. When the scan of the log from the snapshot fails with `ErrCorrupt`,
   Raft asks `FirstDamage(from)` where. If the answer is not a sealed
   segment's damage (some other `ErrCorrupt`, as a header without a term),
   the node refuses to start, as now.
2. If the damage lies below the snapshot's index nothing is read there at
   start (the state is in the snapshot) and the log is left alone: the
   leader of a follower that needed those entries stops on them, and
   repairs the same way.
3. Otherwise Raft writes the **recovery marker** (3.3) with `until = End`,
   syncs it and its directory, and only then calls
   `TruncateSuffix(damage)`, which removes the damaged frame and everything
   after it in every segment. Every entry kept was read and checked.
4. If the node is the only voter, it stops with a message.
5. The scan continues over the shortened log and the node starts as a
   follower, recovering.

### 3.3 Recovering

The marker is a small file in the Raft directory holding `until`, a log
offset, with a checksum, replaced atomically as the hard state is. While it
exists the node is recovering, and:

- it **does not grant a vote or a pre-vote**, and does not campaign
  (`onRequestVote`, the pre-vote path and `campaign` check a flag). A
  candidate gets fewer votes, never more;
- it **does not lead**: it starts as a follower and never campaigns;
- it **follows the leader** as any follower does: the leader backs up to
  where the logs match and sends the rest; its acknowledgements are true,
  since they cover only entries it holds;
- it ends when its commit index is at least `until - 1`. Then every entry
  it ever held in a sealed segment is committed and in its log. The marker
  is removed and the directory synced; a crash in between leaves the
  marker, which only delays.

At start an existing marker is read, and `until` is the larger of it and
any new `End`. A marker that fails its checksum counts as present with
`until` infinite: the node stays out of votes until restarted without it,
which an operator does with `samsa check`'s repair message.

**Why it is safe** (the paper's argument for this protocol). Suppose entry
e was committed with node A among its acknowledgers, and A has lost e. Any
election needs a majority of voters. A votes for nobody until it holds e
again. If fewer than a majority of voters are recovering or down at once
(damage on a minority), every majority of the other nodes overlaps with the
set that holds e in at least one node that is not recovering and holds e,
and that node refuses any candidate whose log is behind e. A node that
catches up leaves recovery only after e is committed and in its own log.

### 3.4 Scrub

Damage at rest should be found before it is needed. A timer
(`timerScrub`, every second) reads sealed segments from the oldest, one
frame at a time, checking each checksum, for at most `-scrub-bytes-per-second`
(default 8 MiB). When it reaches the end of the last sealed segment it waits
`-scrub-interval` (default 24 h; the simulator uses seconds) and starts over.
A bad frame stops the node as in 3.2. The scrub changes nothing the log
holds.

### 3.5 What a read does

`raft.Read` at serving time returns `ErrCorrupt` today and the broker sets
`n.fatal`. That stays; the message says it is damage and where.

### 3.6 Counters

`Metrics`: `Recovering` (gauge), `DamageRepaired` and `ScrubbedBytes`
(counters); `/metrics` exports `samsa_recovering`,
`samsa_storage_damage_repaired_total`, `samsa_scrubbed_bytes_total`;
`/ready` is false while recovering.

## 4. The simulator

- **`Disk.Corrupt(name, off, n, kind)`** changes bytes of the synced copy and
  the live copy of a file at once.
- **The injector**, a new fault kind `faultRot`, picks one node of a
  cluster of at least three, a sealed segment of its Raft log, a frame in it,
  and damages the frame. It runs only when no node is recovering and none
  was damaged since the last time all nodes were healthy.
- **A node that stops on damage** is restarted as if the process was
  killed; the model's epoch does not change. No acknowledged write may be
  lost.
- **Invariant S7:** a node whose recovery marker exists grants no vote or
  pre-vote. Checked in the monitor against `Recovering()` and the vote
  messages each node sent.
- **Invariant S8:** recovery ends only with commit at or above `until - 1`.
- S1 to S6 and the history checks run unchanged: the claim "no committed
  data lost" is their sum.
- **Coverage counters:** damage injected, nodes repaired, scrubbed bytes.

## 5. Mutations

1. `MutVoteWhileRecovering`: a recovering node votes. S7 at once.
2. `UnsafeTruncateAtDamage`: cut at the first bad frame, no marker, the
   roadmap's "naive truncation". The loss of a committed entry.
3. `UnsafeRecoveredEarly`: ignore the marker at start. S7.
4. `UnsafeScrubOff`: the scrub reads nothing. Not a safety bug; a unit test
   shows damage then waits for a read.

## 6. Alternatives

- **Repair a hole**, keeping the valid frames after the damage and fetching
  only the missing entries. The log would have a hole that `Read`,
  `TruncateSuffix`, the scan and the index must all know, and a crash in the
  middle must resume. Cutting costs refetching a suffix, which the leader
  sends anyway.
- **Repair at run time.** The broker applies from the log and cannot take
  back what it applied. A restart rebuilds the state machine from the snapshot.
- **A synced-up-to mark in every frame**, to tell damage from a tear in the
  active segment exactly. It changes the frame format (a new level) and
  costs eight bytes a frame; left as the stated limit.
- **A node that stays recovering forever if all nodes are damaged.** It is
  the case where the paper's argument fails; nodes stall rather than vote.
  That is the correct failure.

## 7. Second review

1. *Can a torn tail look like damage?* Yes if the rule were "bad frame with a
   valid one after it" in the active segment: the simulated disk leaves a
   garbage block followed by a valid one in unsynced data. The first draft
   did that, with a forward search; on review it would send every node of a
   cluster that restarts together into recovery at once: a deadlock. The
   design now calls damage only a bad frame in a sealed segment, and the
   active segment is the stated limit (section 2).
2. *Can the repair loop?* After the cut the node holds entries from the
   snapshot to the damage; the leader sends the rest. If the damage is in
   the leader's log, the leader stops at its next read or scrub, restarts as
   a recovering follower and another node leads. A second damaged copy of
   the same offset on the new leader is two damaged nodes.
3. *Does recovering hurt a healthy restart?* No: the marker exists only
   after damage, never after a tear or a normal restart.
4. *What if `until` is far ahead?* The node needs `until - 1` committed, a
   fetch of the whole suffix; if it is below the leader's log start the
   leader sends a snapshot, which also moves commit.
5. *Snapshot installed while recovering:* it sets commit to its index; if
   that is at least `until - 1`, recovery ends.
6. *Can the scrub hurt clients?* At most 8 MiB a second between other
   events on the core thread. The disk interface does the read.
7. *Does the leader's own damage stop it from stepping down properly?* It
   stops, so its leases end with its process; followers elect a new one.
8. *Entries below the snapshot:* never read at start, so never found there;
   found when sent to a follower or by the scrub, and handled by the same
   restart.
9. *Format level:* a new file only a new build reads; no entry, snapshot or
   frame changes. A downgrade while recovering would let the old build vote;
   not supported.
10. *What if the damage is in the segment that holds the snapshot's last
    entry?* `checkSnapshot` reads it and fails with `ErrCorrupt`; the node
    refuses to start as before. This needs one more case in the first
    step of 3.2 and is left as a stated limit.

## 8. As built (2026-11-15)

What differs from sections 3 to 7:

- **Damage is repaired by cutting, or by emptying the log.** Section 3.2's
  cut at the damage works only for damage after the snapshot. Samsa's
  data lives in the Raft log: the state references batches by log index,
  and live batches sit far below the newest snapshot. So damage under the
  snapshot, or in the snapshot's own last entry, matters, and a cut there
  would leave a log shorter than its snapshot. The repair for that case
  removes the snapshot file and empties the log (`Log.Reset(0)`); the
  leader sends its snapshot and log again, as to a new node. The hard state
  (term and vote) stays. Damage after the snapshot still costs only the log
  from there on.
- **A damage marker.** A run that finds damage (the scrub, or any read that
  fails its checksum) first writes `damaged` and then stops. The next start
  finds the first damage again by reading the whole log, writes the
  recovery marker, repairs, and removes `damaged`. The start-up scan also
  repairs damage it meets itself, above the snapshot.
- **Recovery ends sooner than 3.3 said.** `until - 1` committed is not
  enough: the node's old log may have held entries that were never
  committed and are gone from the leader's. Recovery ends when the node's
  commit reaches `until - 1`, or when the leader sends an AppendEntries
  with nothing more than the node already holds (so the node holds the
  leader's whole log). Found by seed 417.
- **Not a single voter, by the peer list.** A node that joined has no peer
  list; "alone" is `len(-peers) == 1`.
- **Simulator:** damage is never combined with membership changes: a
  configuration of two voters has no minority (seed 417 deadlocked on
  exactly that). The scrub runs at 1 MiB a second, once a second; when the
  run ends, damage not yet found is found by one more scrub, and the nodes
  are given time to repair before logs are compared.
- **Mutation scores (200 seeds each):** vote while recovering 10 (S7);
  ignore the marker at start 1 (S3); cut at damage and go on 5. Only a
  quarter of seeds have damage at all, and a mutation needs an election
  or a lost entry at the wrong moment.
- **Limits that remain:** the active segment (section 2); the hard state,
  snapshot and install files (a node whose snapshot file fails its
  checksum still refuses to start); misdirected writes are modelled as
  overwritten bytes only; damage on more than a minority; no repair for a
  cluster of one.

## 9. Addendum, 2026-11-21: a truncation into a sealed segment

**Found by** the nightly simulator run (seeds 623263227, 623265521; BUGS.md
S14): `recovery of node 1 failed: raft: snapshot file is corrupt or does not
match the log: snapshot at 15, log holds 0 to 12`.

**Problem.** `TruncateSuffix` at an offset inside a sealed segment removed
the later segments and made that segment the active one again. From then
on its old frames, synced when it was sealed, were in the active segment,
which this design leaves out of scope (section 6): every start scans it as
a possible torn tail. Damage in those old frames, met after a crash, was
cut away as a tear: synced, committed entries were gone without the
`recovering` marker, and with a snapshot above the cut the node could not
start at all. The simulator damages sealed segments only, so it damaged
the segment while it was sealed, and the truncation then moved the damage
out of scope.

**Decision.** A truncation into a sealed segment, at a frame other than its
first, leaves it sealed: it is cut at the frame, its index is rewritten to
end there, and a new, empty active segment starts at the cut offset. The
old frames stay where the scrub reads them and where a bad frame is damage.
A cut at a segment's first frame leaves nothing old in it, and that segment
is reused as before. No format change: a sealed segment is still valid to
its last byte and its successor starts where it ends.

**Alternatives.**
- *Keep reusing the segment, and rewrite its index to mark the synced
  prefix.* Recovery would then have to treat the indexed prefix of the
  active segment as damage and the rest as a possible tear. The index is
  sparse, so the boundary is inexact unless the index format changes, and
  it puts two rules into one segment.
- *Tell damage from a tear by content* (a valid frame after the bad one):
  rejected in section 7, item 1; the simulated disk produces exactly that
  after a torn write.

**Second review.**
1. *A crash in the middle of the truncation.* After the cut and its sync,
   and before the new segment's name is synced, the cut segment is the last
   one and is scanned as active at the next start: the old behaviour, for
   the length of a few system calls, not until the next roll. A crash
   between the index rewrite and the new segment leaves an index that
   matches the cut file. Neither loses a synced frame.
2. *More segments.* A truncation below the active segment happens only when
   a follower drops a suffix that began in an older segment: rare, and each
   one adds a single small segment, which `TruncatePrefix` removes like any
   other.
3. *Readers of the cut segment.* Its size, index and file handle are those
   of any sealed segment; `Read` opens it on demand, `FirstDamage` and the
   scrub read it.
4. *A mutation.* `reactivateSealed` brings the old behaviour back, and
   `TestTruncateIntoSealedSegmentKeepsItSealed` fails with it: synced
   entries are cut after the crash.
