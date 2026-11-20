# C20 report: tiered object storage (and unified design with diskless topics)

Date: 2026-11-20

## Summary

Implemented unified object storage (Item 20, tiered storage; shared foundation
for Item 21, diskless topics) with zero external runtime dependencies:

- **Core boundary (`internal/sys/sys.go`)**: added asynchronous `ObjectStore`
  (`Put`, `Get`, `Delete`) and `ObjectHandler.OnObjectDone`.
- **S3 SigV4 & file backend (`internal/realsys/s3.go`, `internal/realsys/loop.go`)**:
  stdlib-only AWS Signature Version 4 HTTP client (path-style `PUT`, ranged `GET`,
  `DELETE`) plus `file://` local-directory backend on a bounded worker pool
  posting completions to `Loop`.
- **Simulated faulty object store (`internal/simsys/object.go`)**: deterministic
  in-memory store injecting `LostPut`, `LostGet`, `TornRead` (bit-flip/truncation),
  and `MaxDelay` latency.
- **Broker (`internal/broker/tier.go`, `state.go`, `snapshot.go`, `retain.go`,
  `handlers.go`, `node.go`, `metrics.go`)**:
  - `formatLevel = 8`, `snapshotVersion = 13`, `cmdTier = 31`.
  - `batchRef` carries `{objKey, objOffset, objLen, objCRC}`.
  - `clean()` packs closed Raft segments into objects, uploads via `ObjectStore.Put`,
    commits `cmdTier` only after upload confirmation, and trims local segments.
  - `fetch()` prefetches uncached remote batches asynchronously, verifies CRC32C
    (retrying transient/torn reads), and caches in bounded `objCache`.
  - `gcObjects()` deletes unreferenced objects after `DeleteRecords` / retention.
  - CLI flags in `cmd/samsa/main.go`: `-object-store-endpoint`, `-object-store-bucket`,
    `-object-store-region`, `-object-store-access-key`, `-object-store-secret-key`,
    `-object-cache-bytes`, `-segment-bytes`.

## Measurements & Verification

- **Integration (`TestTieredObjectStorage` against in-process S3 SigV4 server)**:
  - Produced 120 × 2 KiB records (~240 KiB) with `-segment-bytes 65536`:
    `puts=1 objects=1 objBytes=62400 localBytes=256447`.
  - Broker restart (cold cache) ready in **31.65 ms**; all 120 records verified via
    `franz-go` with **30 S3 ranged GETs**.
  - Note: measurements are local against an in-process HTTP server, not real cloud S3 latency/pricing.
- **Unit tests**: `TestS3StoreHTTPAndFile`, `TestTieringLifecycleFaultsAndGC` (including
  snapshot v13 round-trip, torn-read CRC retry, and object GC): PASS.
- **Simulator**:
  - `TestDeterminism`: PASS.
  - `TestMutationSuite/delete_local_segment_before_upload_confirmed` (`UnsafeDeleteBeforeUpload`):
    caught by **119 of 200 seeds**, first at seed 1 (`I2: the leader answered OFFSET_NOT_AVAILABLE`).
  - `TestSim -seeds=3000`: verified with faulty object store (`LostPut=0.15`, `LostGet=0.10`,
    `TornRead=0.15`, `MaxDelay=20ms`).
