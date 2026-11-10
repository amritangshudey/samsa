# Phase 2 report — Single-node broker

## What was built

- `internal/wire`: Kafka primitives in both encodings, request and response
  headers, encode and decode for six APIs (ApiVersions, Metadata,
  CreateTopics, Produce, Fetch, ListOffsets), record batch validation, and
  a record encoder/decoder for tests.
- `internal/broker`: the node. Connections and framing, one request at a
  time per connection, commands (`CreateTopic`, `Append`) in one log, a
  deterministic state machine, the propose/commit/apply pipeline with group
  commit, three fsync policies, long-polling fetch.
- `internal/realsys`: TCP event loop with bounded queues, timers, a clock
  that cannot go backwards.
- `cmd/samsa`: the binary.
- `integration/`: a separate Go module with real-client tests.
- ADR 0006 (versions), `docs/BUGS.md` entries C1 and F1.

## How to run it

```
make build
./bin/samsa -listen localhost:9092 -data ./data          # -log-level debug to watch requests
kcat -b localhost:9092 -L

make test            # unit tests, all packages
make race            # includes the TCP loop and a broker-over-TCP test
make fuzz            # six fuzz targets
make integration     # franz-go, kcat, Java client against the binary
```

## Evidence

- **Acceptance met:** franz-go 1.22.3, the Java client 4.4.0 and kcat
  (librdkafka 2.16.0) each produce N messages and read the same N back in
  order (`integration/integration_test.go`). franz-go additionally: three
  partitions, `kill -9` and restart with every acknowledged record intact,
  and gzip, snappy, lz4 and zstd batches with 260 kB records. Records
  written by the Java client are read back by franz-go.
- **Two real bugs found before merge**, both in `docs/BUGS.md`:
  - F1, by fuzzing in six seconds: a negative batch length crashed the
    produce handler. One malformed request would have killed the broker.
  - C1, by kcat: an empty fetch result encoded as null is legal by the
    definition and accepted by two clients, but librdkafka rejects it.
- Fuzzing after the fix: about 6 million executions of `FuzzNodeInput` and
  4 million of `FuzzDecodeMessage` locally without failure.
- Race detector clean on the TCP layer.

## What is untested or weak

- **Clients must disable idempotence.** With default settings the Java
  client and franz-go will not work. This is the largest compatibility gap
  and belongs in every statement about compatibility (roadmap A2).
- **Only each API's highest version is exercised by real clients.** The
  lower versions in each range are covered by round-trip unit tests against
  my own encoder, which can share a misunderstanding with my decoder.
- **Round-trip tests prove self-consistency, not correctness.** The only
  check against an independent implementation is the three clients.
- **No consumer groups** (Phase 4), so `subscribe()`-style consumers do not
  work; only manual partition assignment.
- **ListOffsets by timestamp is approximate** (first batch that can contain
  the timestamp).
- **Fetch reads one command per batch from the log**, with a file open for
  sealed segments each time. Unmeasured; likely the first bottleneck.
- **Start-up replays the whole log**, and the per-partition index is fully
  in memory (24 bytes per batch).
- **A slow consumer is disconnected** once 64 MiB is queued for it; there
  is no finer backpressure.
- **Produce timeout is ignored**; `acks=1` behaves as `acks=all`.
- **The TCP loop has no test for shutdown under load** or for thousands of
  connections.
- **Crash testing here is one `kill -9`**, which does not lose the OS
  cache. Power loss on the whole broker is Phase 3.
- **Java and kcat versions are whatever is installed**; only the versions
  in ADR 0006 were observed.

## Ten questions

1. How does a client and a broker agree on a protocol version, and what
   happens when the client's first ApiVersions request is too new?
2. What are flexible versions and tagged fields, and why does the
   ApiVersions response header never use them?
3. Why can the broker set a batch's base offset without recomputing its
   CRC, and why does it never need to decompress a batch?
4. How does the reader guarantee that a hostile length field cannot cause a
   large allocation or a panic? What did the fuzzer find anyway?
5. Why does the node process one request per connection at a time? What
   would go wrong with pipelined produce requests otherwise?
6. Walk through a produce request from bytes on the socket to the
   acknowledgement. Where exactly is the fsync, and how do many requests
   share one?
7. What does each `-sync` policy promise, and which acknowledged writes can
   each lose?
8. How does a long-poll fetch get answered, and what are the three ways it
   can end? How do you know none of them leaks?
9. Why is a partition "a view" here, and what does that cost compared with
   Kafka's file per partition?
10. The definition says a field is nullable and two clients accept null.
    Why was sending null still a bug, and what does that say about testing
    against a specification?
