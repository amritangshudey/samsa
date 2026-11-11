# A1 design — snapshots and log truncation

Status: draft, 2026-11-11. Roadmap section 7, A1. Done in two parts, each
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

## 3. Part (b): retention and truncation (sketch; detailed in its PR)

- **Retention** is global (one log): by total size or by age of the oldest
  batch. The cut *C* is the first entry still wanted; it must be ≤ *S*+1 so
  that the state for everything below it is in the snapshot.
- **The cut** deletes whole sealed segments below *C*. Partitions' start
  offsets move up: ListOffsets earliest and Fetch below the start answer
  as Kafka does (`OFFSET_OUT_OF_RANGE`), verified against the protocol
  documentation first.
- **Raft with a log that starts at *C*:** terms known from the snapshot;
  a follower whose next entry is below the leader's *C* is sent
  InstallSnapshot (state in chunks) and then the retained log from *C* by
  ordinary AppendEntries.
- **Mutation:** "truncate past the snapshot index". The history checker
  must also learn that records below the start are gone by design, and
  check that nothing above it ever is.

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
