# 0002 — Disk durability contract

Status: accepted, 2026-11-10

## Context

Storage correctness is a statement about what survives a crash. It can only
be tested if "what the disk may do" is written down once and both the storage
engine and the simulated disk are held to it. A simulated disk kinder than a
real one gives false confidence.

## Decision

The contract is the doc comment on `sys.Disk`, and that comment is the
authority. In summary:

1. Data is durable only after `File.Sync` returns nil.
2. After a crash, unsynced regions hold new, old or arbitrary bytes per
   block; writes are neither atomic nor ordered; length and truncation are
   uncertain.
3. Directory entries are durable only after `SyncDir` on the parent.
4. `Rename` is atomic across a crash.
5. A failed sync is fatal to the node.

## Alternatives

- **Assume ordered, prefix-only loss** (a crash loses a suffix of what was
  written). Simpler recovery, but real filesystems do not promise it.
- **Retry a failed fsync.** Rejected: on Linux a failed fsync may mark dirty
  pages clean, so a later success does not mean the data was written
  (the 2018 PostgreSQL "fsyncgate" finding).
- **Zeros only in torn regions.** Rejected in review: some filesystems can
  expose stale blocks, and modelling arbitrary bytes costs nothing because
  recovery relies on checksums either way.

## Consequences

- Every record needs a checksum; recovery must not trust lengths read from
  an unsynced region without bounding them.
- Creating or rolling a segment needs a directory sync.
- Not modelled: corruption of synced data, misdirected writes, a device that
  acknowledges a sync it did not perform. These are stated limitations.
- Still to verify in Phase 1: what `os.File.Sync` does on macOS
  (`F_FULLFSYNC` or plain `fsync`).
