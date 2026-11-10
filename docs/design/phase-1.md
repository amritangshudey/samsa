# Phase 1 design — Storage

Status: approved 2026-11-10 under the standing delegation (see CLAUDE.md).
Review findings are in section 6.

## 1. Concepts

**Append-only log.** Records are only ever added at the end and are named by
a dense 64-bit offset. Sequential writes are the cheapest thing a disk does,
and "the end of the log" is the only place where a crash can leave damage.

**Segments.** One ever-growing file cannot be deleted from the front and is
awkward to recover. The log is a sequence of files, each named after the
offset of its first record. Only the last one (the active segment) is
written; the rest are sealed and immutable.

**Offset index.** To read offset N we must find a byte position. A sparse
index stores (offset, position) roughly every 4 KiB of log. A lookup finds
the nearest entry at or before N, then walks forward a few frames.

**Checksums and torn writes.** A crash during a write can leave a prefix of
the write, or blocks of it, or garbage (ADR 0002). Each frame carries a CRC
so that recovery can tell a complete frame from a damaged one.

**fsync and acknowledgement.** `write` only moves bytes to the OS cache.
Data is safe after `fsync`. The fsync policy is therefore the durability
promise: a record acknowledged before its sync can be lost.

**Recovery.** On open, the log must return to a state where every frame is
whole and offsets are contiguous. It does this by scanning the active
segment and cutting off everything from the first bad frame.

## 2. Proposed design

### 2.1 Frame format

All integers big-endian (as in Kafka's protocol, so one convention
throughout the code).

```
offset  size  field
0       4     crc      CRC-32C of bytes 4..end of payload
4       4     length   payload length in bytes
8       8     base     offset of the first record in the frame
16      4     count    number of offsets the frame occupies (>= 1)
20      n     payload  opaque
```

The payload is opaque to storage, and one frame occupies `count`
consecutive offsets. Storage never looks inside. (Decided while this phase
was in progress, ADR 0005: the broker stores one command per frame with
`count` 1, and the log's offsets are command indexes.)

CRC-32C (Castagnoli) because it has hardware support on arm64 and x86-64 and
is what Kafka's own record batches use.

A frame read from disk is valid only if: `length <= MaxPayloadBytes`, the
whole frame lies inside the file, `count >= 1`, `base + count` does not
overflow, and the CRC matches. `length` is checked before any allocation.

### 2.2 Files

```
<dir>/00000000000000000000.log     segment, named by base offset
<dir>/00000000000000000000.index   index of a sealed segment
```

Index file: `magic u32 | n u32 | n × (relOffset u32, position u32) | crc u32`.
`relOffset` is the frame's base minus the segment's base.

The index of the active segment lives only in memory. It is written to disk
once, when the segment is sealed. An index file is never trusted: it is used
only if its size, magic and CRC are right and its entries are strictly
increasing and inside the segment. Otherwise it is rebuilt by scanning the
segment and written again. So a torn index file costs time, not correctness,
and it needs no atomic-rename dance.

### 2.3 Operations

```go
Open(disk, dir, cfg) (*Log, error)       // creates or recovers
(*Log) Append(payload, count) (base, error)
(*Log) Sync() error
(*Log) Read(offset, maxBytes) ([]Batch, error)
(*Log) StartOffset() / EndOffset() / DurableOffset()
(*Log) Close() error
```

- **Append** writes one frame at the end of the active segment with a single
  `WriteAt`. If the frame would push the segment past `SegmentBytes` and the
  segment is not empty, it rolls first.
- **Roll**, in this order: sync the active segment; write and sync its index
  file; create the new segment file; `SyncDir`. After that the old segment
  is sealed. Rolling always syncs, whatever the policy, so that sealed
  segments are always fully durable and never need scanning on recovery.
- **Sync** syncs the active segment and advances `DurableOffset` to
  `EndOffset`.
- **Read** returns whole frames, starting with the frame that contains
  `offset`, from one segment, until `maxBytes` of payload is reached. It
  always returns at least one frame if one exists, even if larger than
  `maxBytes` (otherwise a large frame could never be read). Every frame's
  CRC is checked on read; a mismatch is `ErrCorrupt`, never bad data.
  `offset == EndOffset` returns nothing; outside the log is
  `ErrOffsetOutOfRange`.

### 2.4 Fsync policy

`Config.SyncBytes`:

- `0`: sync inside every `Append` (nothing unsynced is ever acknowledged);
- `n > 0`: sync inside `Append` once `n` unsynced bytes have accumulated;
- `< 0`: never sync on append.

The log has no clock, so "sync every T milliseconds" is not its business:
the broker arms a timer and calls `Sync`. `DurableOffset` lets the broker
decide what may be acknowledged: with group commit it appends several
requests, calls `Sync` once, and acknowledges everything below
`DurableOffset`.

### 2.5 Recovery (in Open)

1. List the directory; the `.log` files sorted by base offset are the
   segments. No files: create segment 0 and `SyncDir`.
2. For each sealed segment (all but the last): load its index, or rebuild it.
3. Scan the last segment from the start, frame by frame. A frame is accepted
   if it is valid (2.1) and its `base` equals the end of the previous frame
   (the segment's base for the first). Stop at the first frame that is not.
4. If bytes remain after the last accepted frame, truncate them and sync.
   The truncate must be durable before any new append, or a later crash could
   resurrect the cut bytes behind newly written ones.
5. `EndOffset` = end of the last accepted frame; `DurableOffset` = the same,
   because everything that survived is on disk and now synced.

Recovery writes nothing if nothing is damaged, so running it twice gives the
same result as once. A crash during recovery leaves a state that recovery
handles again.

Frames that were never synced but happen to survive a crash are kept. They
were never acknowledged, but keeping them is harmless on a single node
(Kafka does the same). Phase 5 revisits this for replication.

### 2.6 Failure handling

Any error from a write, sync or truncate poisons the log: every later call
returns that error. The broker returns it from its handler and the node
stops (ADR 0002, rule 5).

### 2.7 The simulated disk

`internal/simsys.Disk` implements `sys.Disk` in memory.

- **File data.** Each file has its content as of the last `Sync` (durable)
  and its current content (live).
- **Namespace.** Creates, removes, renames and mkdirs go into one ordered
  journal. `SyncDir(d)` commits the journal up to the last entry that touches
  `d`.
- **Crash.** A random prefix of the uncommitted journal is committed; the
  rest is undone. For every surviving file, each block that differs between
  durable and live becomes the new bytes, the old bytes or random bytes, and
  the length lands anywhere between the durable and the live length. With
  some probability all unsynced data is simply lost, or all of it kept, so
  the plain cases are covered too.
- **Crash point.** The test arms the disk to crash at the N-th operation.
  That operation and all later ones return `ErrCrashed`; if the operation
  was a sync, it has taken effect or not at random. The caller then discards
  the log object, calls `Restart`, and reopens.
- **Injected sync errors**, with a configurable probability, to test
  poisoning.

Block size is configurable and small in tests (so frames tear in the middle).

### 2.8 The real disk

`internal/realsys.Disk` over `os.Root`, which confines every path to the
data directory. `File.Sync` is `os.File.Sync`. Verified in the Go 1.27
source (`internal/poll/fd_fsync_darwin.go`): on macOS it issues
`F_FULLFSYNC`, which forces the drive to flush, where plain `fsync` does
not. `SyncDir` opens the directory and syncs it.

### 2.9 Tests

- Table-driven unit tests for frame encode/decode, index encode/load,
  append/read, roll, out-of-range, oversized payload.
- Fuzz targets: frame decoder, index loader, and recovery of a segment made
  of arbitrary bytes (must not panic, must be idempotent).
- **Crash property test** (many seeds): random appends, syncs, reads, crashes
  at random operations, and reopens, checked against a model:
  1. every offset below the `DurableOffset` observed before the crash is
     still readable;
  2. everything readable is byte-identical to what was appended at that
     offset;
  3. offsets are contiguous from `StartOffset` to `EndOffset`;
  4. opening again without a crash changes nothing.
- A deliberately broken variant (roll without `SyncDir`) must be caught by
  the property test, to show the test can fail.
- The same unit tests run against the real disk in a temp directory.

## 3. Alternatives rejected

- **Store Kafka record batches directly, no frame of our own.** Saves 20
  bytes per batch and a CRC pass. Rejected: storage could not be built or
  tested before the Kafka format exists, and its recovery would depend on
  parsing a format designed for another purpose.
- **Kafka-style memory-mapped index, appended to as the segment grows.**
  Rejected: needs mmap (a syscall the core may not make) and makes the index
  part of crash recovery. An index written once at seal is immutable.
- **Recovery checkpoint / clean-shutdown marker** to skip the scan of the
  active segment. Rejected for now: one more file whose own crash behaviour
  must be right. The scan is bounded by `SegmentBytes`.
- **Per-record checksums.** The unit of writing is the frame; a finer
  checksum protects nothing more.
- **Write-ahead intent or double-write of the tail.** Unneeded: an
  append-only log's only in-flight write is its tail, and the CRC identifies
  it.
- **Fsync timer inside the log.** Would require a clock in storage.

## 4. Acceptance (from PLAN.md)

As listed in 2.9, plus: fuzzing finds no panics; the suite passes on the
real disk.

## 5. Limits stated up front

- No retention or deletion of old segments (not in the plan).
- No truncation of a log suffix by offset; added in Phase 5 if Raft needs it.
- All sealed indexes are held in memory (about 2 MiB per GiB of log).
- Recovery time is proportional to the active segment's size.
- A random block that happens to form a valid frame with the right base
  offset would be accepted. The chance is about 2^-32 per torn tail.
- Corruption of already-synced data is detected on read (`ErrCorrupt`) but
  not repaired.

## 6. Review record

Second pass, looking for ways to lose an acknowledged record:

1. **Roll without a directory sync loses synced data.** If the new segment's
   directory entry is not durable, a crash removes the file together with
   records that were synced into it. The roll order in 2.3 fixes this, and a
   test proves the property test notices if it is removed.
2. **An unsynced truncate can resurrect a torn tail.** Recovery step 4 now
   requires the sync.
3. **Sealed segments unsynced under the "never" policy** would make
   "scan only the last segment" unsound. Roll always syncs.
4. **A stale but checksum-valid frame.** Could an old frame reappear at the
   right place with the right base? Only from durable content, and recovery
   has truncated and synced that away before new appends. The contiguity
   check (`base` must equal the expected offset) covers the remaining cases.
5. **Journal model is kind.** Real filesystems do not all promise that
   directory operations persist in order. Recorded as a model limit.
6. **Trusting `length` from a torn header** could allocate gigabytes. It is
   bounded by `MaxPayloadBytes` and by the file size first.
