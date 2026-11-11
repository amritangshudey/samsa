# A1 design — snapshots and log truncation

Status: built, 2026-11-11 (both parts; see sections 6 and 7). Roadmap section 7, A1. Done in two parts, each
its own PR: **(a) local snapshots**, which bound start-up work; **(b)
retention and log truncation**, which bound disk use and need
InstallSnapshot for followers behind the cut.

## 1. The problem

A start today does two things in proportion to everything ever written:

1. `raft.Start` reads every entry of the log to learn its term
   (`r.terms`).
2. The broker re-applies every entry from index 0 once the node commits
   again.

And nothing is ever deleted, so disk use grows without bound.

## 2. Part (a): local snapshots

**Contents.** A snapshot at index *S* holds:

- *S*, the last entry it includes;
- the terms of entries 0..*S* as runs `(first index, term)`: a log has few
  terms, so this is small, and with it `Start` reads only entries after *S*;
- the broker's state machine after applying 0..*S*, as bytes Raft does not
  interpret: topics, each partition's batch index (offset, log index,
  maximum timestamp per batch), groups, members, assignments, committed
  offsets. Not record data, which stays in the log.

**Who owns it.** Raft, as in the Raft paper: it is the thing that will
later have to send it to a follower. `raft.SaveSnapshot(index, state)` and,
after `Start`, `raft.Snapshot()` returning the loaded index and state. The
broker decides when, and encodes and decodes its own state (`state.go`,
sorted iteration only, so equal states give equal bytes).

**When.** The broker takes one after every `SnapshotEvery` applied entries
(default 10 000; 0 disables). Synchronous: the core is single-threaded and
the state is small next to the log. *shortcut:* a pause per snapshot that
grows with the number of batches; write it incrementally if that is ever
measured to matter.

**The file.** One file, `snapshot`, replaced like the hard state: write
`snapshot.tmp`, sync it, rename, sync the directory. Magic, version,
the fields above, a CRC-32C over everything. Lengths are checked against
the file size before allocating. A file that fails its checksum cannot
come from a crash under the disk contract (ADR 0002), so it stops the node,
as a corrupt hard state does.

**The rules that make it safe.**

1. *S* must be committed: the broker applies only committed entries, and
   snapshots its applied state. So no leader can ever truncate entry *S*.
2. **The log must be synced up to *S* before the snapshot is written.** A
   follower can apply entries it has appended and not yet synced (the
   message that carries them also carries the commit index), and under
   `interval` and `never` a single node does so all the time. Without the
   sync, a power cut could leave a snapshot that is ahead of the log. This
   is the mutation for part (a): `UnsafeSnapshotUnsynced`.
3. On start, Raft checks that *S* < the log's end and that the terms in the
   snapshot match the entries it still reads: a snapshot ahead of the log
   stops the node with an error instead of serving state it cannot back.
4. After `Start`, the commit index is at least *S*, and the broker's
   applied index is *S* + 1.

**What checks it.** The simulator restarts nodes constantly and, at the
end, restarts all of them and compares state hashes with those before (I6);
every replica comparison (S4) and every client answer after a restart now
goes through a snapshot. Seeds draw `SnapshotEvery` from {0, 1, 7, 100}.
A unit test round-trips random states through encode and decode (equal
hash, equal structure), and fuzzing covers the snapshot decoders.

**Start-up benchmark.** A test builds a log of many entries on the
simulated disk and times `OnStart` with and without a snapshot; it must
show start-up work bounded by the entries after the snapshot, not by the
log.

## 3. Part (b): retention and truncation

### 3.1 Where the cut is decided

**Retention is a command.** Nodes cut their logs at different moments, but
what clients see must not depend on which node answers. So the leader,
when its log exceeds `RetentionBytes`, proposes `Trim(C)`: C is the base
of one of its own segments, chosen so that at most `RetentionBytes` remain
from C on, and C ≤ the commit index. Applying `Trim(C)` (deterministic,
on every replica) sets the state's log start to C and drops every batch
reference whose log index is below C; each partition's start offset
becomes its first remaining batch, or its end if none remains.

*shortcut:* size only, global; retention by time needs a clock decision
in the proposer and is left out.

**Kafka's answers** (checked against `FetchResponse.json` and `Errors.java`
in apache/kafka, 2026-11-11): Fetch v5+ carries `LogStartOffset`; a fetch
below it answers `OFFSET_OUT_OF_RANGE` (1), as one above the end does;
ListOffsets with timestamp -2 (earliest) answers the start offset.

### 3.2 When a node deletes

A node deletes log segments only below the log start recorded in **its own
newest snapshot**. That snapshot was taken after `Trim` was applied, so it
holds no reference below the cut: a restart from it never needs a deleted
entry. Right after applying a `Trim`, the node takes a snapshot so that the
deletion is not delayed by a whole interval. Segments are deleted oldest
first, syncing the directory after each, so that a crash leaves a
contiguous log. **Mutation `UnsafeTrimEarly`: delete as soon as `Trim` is
applied**, before a snapshot covers it; a restart then finds a snapshot
whose batches or following entries are gone.

### 3.3 A follower behind the cut: InstallSnapshot

When a follower's next index is below the leader's log start C, the leader
cannot send entries. Record data is in the log, not the snapshot, so the
follower needs both: the leader's newest snapshot (index S ≥ C − 1), then
the entries from C by ordinary AppendEntries.

- **Leader:** sends `InstallSnapshot{term, stamp, cut C, offset, chunk,
  done}` with chunks of its snapshot file, one message in flight per
  follower, as for AppendEntries. A response that asks for an offset other
  than the next restarts from there.
- **Follower:** accepts it only from the leader of its term (it counts as
  contact, for the lease) and only while it really is behind (C − 1 >
  its last index, and S > its commit index); anything else is a stale copy
  and is answered with what it has. Chunks are written in order to
  `install.tmp`; out of order, it asks for the offset it expects.
- **On the last chunk:** the file is decoded and checked, made durable and
  renamed to `install` (the marker). Then the log is reset to an empty log
  starting at C. Entries below C are now known only as terms (from the
  snapshot's runs) and as committed. The follower answers "match C − 1",
  and the leader continues with AppendEntries from C.
- **While the marker exists** the node is *installing*: it does not apply
  anything, so the broker keeps its old state and serves nothing new. It
  votes and appends as a node whose log ends where its log ends, which is
  no less safe than before the install: it forgets only entries below C,
  all committed, and keeps their terms.
- **Promotion:** once its log is durable past S, the node renames `install`
  to `snapshot`. The broker then restores the snapshot's state and applies
  from S + 1.
- **A restart while installing** finds the marker, resets the log to C if
  it does not start there (a crash between rename and reset), and promotes
  at once if the log already reaches past S.

### 3.4 What checks it

- Raft random schedules: nodes trim below their snapshots, followers are
  brought back by InstallSnapshot; every existing check, reading committed
  entries from each node's log start.
- Simulator: `RetentionBytes` drawn per seed (off, small, tiny), chunk
  size drawn small. The history checker learns one new rule: **a
  partition's start offset never moves backwards, records at or after it
  never change, and only offsets below it may become unavailable**, and
  only through `OFFSET_OUT_OF_RANGE` or an earliest offset that moved.
  Clients that read below the start go back to the earliest offset.
- Mutation `UnsafeTrimEarly`, which must be caught.

## 4. Alternatives rejected

- *Snapshot inside the log* (as a command): the log would still need
  reading from the start to find it.
- *Snapshot by the broker alone, Raft unaware:* Raft would still read the
  whole log for terms, and part (b) needs Raft to send it anyway.
- *Snapshots taken in the background* (copy-on-write state): no goroutines
  in the core, and nothing yet says the pause matters.
- *Keep the terms of all entries in a side file instead:* another file to
  keep consistent, for something the snapshot can carry.

## 5. Review record

Second pass, looking for the ways a snapshot could lie:

1. A snapshot of state that includes unsynced entries (rule 2) — found
   while writing this; the mutation keeps it found.
2. A snapshot from before a power cut, with a log that then lost entries
   *after* *S* and gained different ones: harmless, *S* is committed and
   the entries after it are replayed from the log as it is.
3. A group's soft state (rounds, session timers) must not be in the
   snapshot; it is not now (it lives in `softGroup`), and restart already
   rebuilds it.
4. The no-op entries of Raft are counted in the applied index but change
   no state; the snapshot index is a Raft index, so they need nothing.
5. `UnsafeNoFencing` and other switches live in `state`, not in its
   encoding: decoding must restore them from the config, not from disk.

Second pass on part (b):

6. Trimming locally, without a command, would make two replicas answer
   ListOffsets differently for the same applied index, and the state
   hashes (S4) would differ. Hence `Trim` is a command.
7. A follower that installs forgets entries it may have acknowledged.
   They are below C, so committed, and their terms are kept: an election
   compares last index and term, and both only grow. Recorded here as an
   argument, checked by the random schedules.
8. A stale InstallSnapshot arriving after the follower caught up must not
   reset its log: hence the "really behind" test.
9. The leader may cut again while sending a snapshot: the snapshot it
   sends stays valid (S only grows; C is fixed in the message), but if C
   moves past what the follower will ask for next, the follower simply
   needs another install.

## 6. As built (part a)

- **Start-up is not yet bounded by the snapshot interval.** Measured by
  `BenchmarkRestart` (simulated disk, 64 KiB segments): 50 000 entries
  restart in 1.6 ms with snapshots against 55 ms without (34 times less),
  but the time still grows with the log, by about 30 ns an entry against
  1 100. What remains is the snapshot itself: one batch reference per
  batch ever written, and one term per entry expanded in memory. Both
  shrink only when old batches can be dropped, which is part (b).
- Storage recovery checksums the whole active segment at every start; that
  is bounded by the segment size, not by the log.
- The simulator counts starts from a snapshot (`SnapshotStarts`) and
  requires them in `TestSim`.

## 7. As built (part b)

Where the code differs from section 3, and why:

- **InstallSnapshot carries the snapshot whole, not a file in chunks on
  disk.** The follower buffers chunks in memory and writes the `install`
  marker (snapshot bytes, cut, CRC) once the last arrives. *shortcut:* the
  memory is the snapshot's size; stream to `install.tmp` if snapshots grow
  past a few MiB. Chunks are at most `MaxPayloadBytes` less the entry
  header: the first version used `MaxAppendBytes` alone, the decoder
  refused the larger chunks, and no install ever finished (found by the
  Raft random schedules, seed 8).
- **"Really behind" is a term match, not an index test.** A follower
  answers "not needed" only if it holds entry C − 1 with the leader's term
  for it (`prevTerm` in the message). Comparing indexes alone livelocked a
  follower whose log had diverged below C: it kept refusing the snapshot
  and refusing the entries (random schedules, seed 1053).
- **The broker's state carries `logStart` and each partition's `start`**
  (snapshot version 2; version 1 still decodes). The leader proposes a
  Trim at most once per cut while serving, and again after a change of
  leader, since the earlier proposal may have been lost.
- **Produce v5+** now answers `LogStartOffset` = -1, the schema's default
  (ProduceResponse.json, checked 2026-11-11), instead of a 0 that became
  false once the log could be cut. Fetch carries the real value.
- **What checks it:** the Raft random schedules compact and install
  (thousands of installs in 3000 seeds; mutation "compact past the
  snapshot" caught); the simulator draws `RetentionBytes` from {off, off,
  64 KiB, 8 KiB} and requires trims and installs in `TestSim`; the history
  checker's rule R1 (the start never moves back, never passes the end,
  nothing is returned below it, and only records below it may vanish) has
  its own `TestModel` cases; mutation `UnsafeTrimEarly` is caught at seed 1
  (a restart finds a snapshot its log cannot back). `TestRetentionTrimsTheLog`
  checks fetch, ListOffsets and restart on one node.
- **The checker trusts the start.** R1 lets any record below the reported
  start vanish; a broker that raised its start for no reason would pass.
  Retention is a size bound the model does not compute. *shortcut:* model
  the cut if a bug is ever suspected there.
- **I6 restarts run with retention off:** a restarted leader may
  legitimately trim again, which changes the state hash. The first run of
  the suite reported that as a violation (seeds 55, 498, 975); it was the
  check, not the broker.
- Retention by size only, one bound for the whole log, not per topic.
