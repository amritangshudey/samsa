# C2 report, part (b): key compaction

Branch `compaction-b`, 2026-11-15. Production plan item 12, part (b).
Design: `docs/design/c2b-compaction.md` (and "As built"). ADR 0029.

## What was built

- **Topic configs** `cleanup.policy` (`delete`, `compact`,
  `compact,delete`), `delete.retention.ms` (default 24 h,
  `-delete-retention-time`) and `min.compaction.lag.ms`. Format level 3.
- **`cmdCompact`:** replaces one batch of a partition with a copy that
  holds fewer records and keeps its offsets, or removes it. Apply checks
  the header and that the reference is unchanged.
- **The compactor** (`broker/compact.go`) on the serving leader: a soft
  key map per compacted partition (16 bytes of SHA-256 of the key, highest
  offset, count, tombstone flag, bounded by `-compaction-map-bytes`) built
  by reading the log, and sweeps that rewrite batches with dead records.
- **Produce:** a compacted topic refuses a record without a key
  (INVALID_RECORD) and a compressed batch (UNSUPPORTED_COMPRESSION_TYPE).
- **Gaps:** the snapshot reader, Trim and Fetch handle a first batch that
  begins after the start and a fetch inside a gap.
- **Simulator:** topic `tc`, checks C1 to C5, five mutations, coverage
  counter "records compacted".
- **Tests:** wire (split and rewrite a batch, fuzzed), broker unit tests
  (latest value kept, tombstone goes after its retention, lag, refusals,
  apply of the command), `TestCompaction` with franz-go on the real binary
  (kill -9 and restart), `TestBenchCompaction`.

## Results

**Simulator:** 4,000 seeds pass in 118 s with the compacted topic in half
of them (2,000 in 40 s on the final code); 60,383 records compacted
over the first 1,000.

**Mutations** (200 seeds each, first catch's invariant):

| Mutation | Caught by | First catch |
|---|---|---|
| compact away the latest record of a key | 67 of 200 | C1 |
| compact a record younger than `min.compaction.lag.ms` | 61 | C3 |
| remove a tombstone before `delete.retention.ms` | 8 | C1 |
| remove a tombstone, leave older records of its key | 26 | C5 |
| tell keys apart by their first byte | 82 | C1 |

The tombstone-early score is low: a tombstone must still be the latest
record of its key and young when a check sees it.

**Real client** (`TestCompaction`): 6,000 updates of 20 keys and a
tombstone; after compaction a franz-go consumer from offset 0 reads 19
records, the latest of each key but the deleted one, at ascending offsets
that are the original ones; the same after `kill -9` and a restart.

**Benchmark** (`TestBenchCompaction`): single node, `-sync always`, 1 KiB
records without compression, 100,000 keys, 90% of the updates to 10% of
the keys, one minute of load, one run each:

| Run | Throughput | Records compacted | Copied by sweeps and cleaner | Log after catching up |
|---|---|---|---|---|
| default policy | 170,483 rec/s, 174.6 MB/s | - | 0 | 10,687 MB (kept) |
| `compact` | 124,493 rec/s, 127.5 MB/s | 7,378,199 of 7,478,179 | 9,904 MB | 263 MB, 40 s after the load stopped |

- **Throughput fell by 27%** on this machine at this load. The leader
  reads the log back, decodes every record and hashes every key on the
  one thread that also serves clients.
- **Amplification is high:** the bytes written again by the compactor and
  the cleaner were 1.3 times the client bytes. A hot key's batch is
  rewritten each sweep until nothing in it is dead.
- **Live data** is about 100 MB (100,000 keys of 1 KiB), so 263 MB is
  within the cleaner's budget of two times live plus a segment.
- **The method is weak:** one run each, one minute, one laptop. During
  the load the log grew past the budget; it came back 40 s after the load
  stopped.

## Problems met

- **BUGS.md C2b-1:** the snapshot reader refused a partition whose first
  batch begins after its start. Found by the first 300 seeds.
- **BUGS.md C2b-2:** Trim moved a start past gaps, and expiry applied to
  a compact-only topic (an edit that did not apply, found by R9).
- **BUGS.md C2b-3:** a fetch inside a gap (found by reading the code).
- **BUGS.md M14, M15 (simulator):** the model ordered records by id, and
  missed a tombstone nobody had the acknowledgement of. Two false alarms
  among 2,000 seeds; both fixed in the model, and both seeds are in
  `regressions.txt`.
- **The design had four errors**, recorded in "As built": the format
  level, the sweep versus the cleaner, the tombstone count, and the places
  gaps reach.

## How to run it

```sh
go test ./internal/wire ./internal/broker -run 'Compact' -v
make sim SEEDS=1000
go test ./internal/sim -run 'TestMutationSuite/(compact|remove_a_tombstone|tell_keys)' -v
make build && cd integration && SAMSA_BIN=../bin/samsa go test -run TestCompaction -v .
cd integration && SAMSA_BENCH_COMPACTION=1 SAMSA_BIN=../bin/samsa go test -run TestBenchCompaction -v .
kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name t --add-config cleanup.policy=compact
```

## What is untested or weak

- **C3 and the removal checks stop after a power loss** that may have cut
  acknowledged data (the model is not sure what it may expect).
- **Compression:** compressed batches are refused, so most default clients
  (franz-go, librdkafka with their default codecs) must turn it off to
  write to a compacted topic. Java's default works. A decompressor is a
  decision for the developer (it adds `compress/*` to the core).
- **A partition with more keys than `-compaction-map-bytes` holds** is not
  compacted (a warning, once). Untested beyond a unit-scale check.
- **A tombstone sweep reads the whole partition every 30 s.**
- **Batches without timestamps (-1)** are never compacted for lag, and
  their tombstones are never removed.
- **A leader change rebuilds the key maps** by reading every compacted
  partition: unmeasured with a large log.
- **Seek by time in a compacted partition** is as imprecise as before
  (first batch with a large enough maximum).
- **Rollback to a build without compaction** after a compacted topic
  exists is not possible (an old node stops on command 18).
- **`kafka-configs.sh` and the Java client** were not run against a
  compacted topic (the real-client test is franz-go).

## Ten questions

1. **Why does apply know no keys?** A key map in the state machine would be
   in every snapshot and hash and grow with the keys. The leader decides;
   the simulator checks its decisions (C1 to C5), as for retention.
2. **What stops a compaction from removing the only copy of a key?** It
   removes a record only if the map holds a higher offset of the key
   that it read from the log; a stale command (the batch moved) does
   nothing; and the final read checks that every acknowledged record is
   there or replaced.
3. **Why does a tombstone wait for the count to reach one?** If it went
   while an older put of the key sat in a batch not yet rewritten, readers
   would see the put again. `UnsafeTombstoneOrphan` is that bug; C5 finds
   it in 26 of 200 seeds.
4. **Why must a rewritten batch keep its last offset?** The offsets after
   it are assigned and clients have seen them; the span stays, with gaps.
   Kafka does the same and every client accepts it.
5. **Why not rewrite only in the head segment?** Dead records count as
   referenced bytes, so the cleaner would not start; the sweep shrinks
   them first.
6. **What is the cost?** A quarter fewer records per second at 128 MB/s on
   one node in the benchmark, and 1.3 times the client bytes written again.
7. **Why are compressed batches refused?** The core cannot read the keys
   inside, and has no decompressor.
8. **Why is the format level 3?** Command 18 and the new config values
   would stop an older node; ADR 0024 says to raise the level.
9. **What does a fetch inside a gap do?** It starts at the next batch whose
   last offset is at or above the offset; a client that reads past the
   end of a gap gets the next record.
10. **What do the simulator's checks not cover?** After a power loss that
    may have cut acknowledged data, removals are not checked; unanswered
    writes blind C3's supersession test for their key (acks=0 is not used
    on the compacted topic).
