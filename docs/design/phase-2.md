# Phase 2 design — Single-node broker

Status: approved 2026-11-10 under the standing delegation. Review findings
are in section 8.

Sources for every protocol fact below, read on 2026-11-10 from the Apache
Kafka repository (`trunk`): the message definitions in
`clients/src/main/resources/common/message/*.json`; `Errors.java`;
`DefaultRecordBatch.java`; `Type.java`, `TaggedFields.java` and
`ByteUtils.java` for the primitive encodings. Nothing is from memory.
What clients are observed to send is recorded in ADR 0006 once the
integration tests run.

## 1. Concepts

**Framing.** A Kafka connection carries length-prefixed messages: a 4-byte
big-endian size, then that many bytes. TCP gives a byte stream with no
boundaries, so the broker accumulates bytes per connection and cuts frames.

**Request header.** API key (which operation), API version, correlation id
(echoed in the response so the client can match it), client id.

**Versioning.** Every API has its own version number, and the encoding of a
request depends on it. A client first sends ApiVersions; the broker replies
with the range it supports per API; the client then uses, for each API, the
highest version both sides support.

**Flexible versions.** From some version on, each API switches to a compact
encoding: lengths are varints holding length+1, and every structure ends
with "tagged fields" (count, then tag, size, bytes for each) so that fields
can be added without a new version.

**Record batch.** Producers send records packed in batches. A batch has a
61-byte header (base offset, length, leader epoch, magic = 2, CRC-32C,
attributes, last offset delta, timestamps, producer id/epoch/sequence,
record count) followed by the records, possibly compressed. The CRC covers
everything from the attributes on, so the broker can set the base offset
without recomputing it. The broker never needs to look inside the records.

**Ordering on a connection.** Responses must come back in the order of the
requests. Producers pipeline several requests on one connection.

**Long polling.** A fetch says "give me at least `MinBytes`, wait up to
`MaxWaitMs`". With nothing to return, the broker parks the request and
answers when data arrives or the time is up.

## 2. Which versions

One table (`wire/api.go`) drives both ApiVersions and request validation.

| API | key | Supported | Why this range |
|---|---|---|---|
| ApiVersions | 18 | 0–4 | 3 and 4 are flexible and identical on the wire; clients probe with their highest and fall back |
| Metadata | 3 | 1–8 | 8 is the last non-flexible version |
| CreateTopics | 19 | 2–4 | 2 is Kafka 4's baseline, 4 the last non-flexible |
| Produce | 0 | 3–8 | 3 is the first with record batch format 2; 8 the last non-flexible |
| Fetch | 1 | 4–11 | 4 is the first that requires format 2; 11 is the last non-flexible and the last that names topics by name (13 switches to topic ids) |
| ListOffsets | 2 | 1–5 | 1 is the baseline; 5 the last non-flexible |

The rule: for each API, from Kafka 4's own minimum up to the highest
non-flexible version. A client uses min(its max, our max), which must be at
least the client's own minimum. Kafka 4 removed the versions below these
minimums, so current clients all support them.

Flexible encoding is still implemented in the primitives, because
ApiVersions 3+ needs it, and Phase 4 and later can raise ranges using it.

Produce is advertised from version 0 although versions 0–2 are rejected.
Kafka itself does this (KAFKA-18659, noted in `ProduceRequest.json`):
librdkafka decides which features to enable from the advertised range.

An ApiVersions request with an unsupported version gets a version-0
response with `UNSUPPORTED_VERSION` and the full list, so the client can
retry (KIP-511, noted in `ApiVersionsResponse.json`). Any other request
with an unknown API or unsupported version closes the connection.

## 3. Architecture

ADR 0005 and `docs/ROADMAP.md` sections 1 and 2 define the shape. In short:

```
bytes -> frame -> header -> decode -> handler -> propose(command)
                                                      |
                                    append to the log | flush timer (0 delay)
                                                      v
                                         sync -> apply to state -> respond
```

### 3.1 Packages

- `internal/wire`: `Reader` and `Writer` primitives; request header; for
  each API a request and a response struct with `Decode` and `Encode`. Both
  directions are implemented for both, because the simulator's clients
  (Phase 3) speak wire bytes to the node, and because round-trip tests
  (decode(encode(x)) == x) check each direction against the other. Also
  `RecordBatch` header parsing and validation.
- `internal/broker`: the node (`sys.Handler`), commands, the state machine,
  the propose/commit/apply pipeline, delayed fetches.
- `internal/realsys`: TCP server, event loop, wall clock.
- `cmd/samsa`: flags and wiring.
- `integration/`: a separate Go module holding the real-client tests, so
  that franz-go never enters the broker's `go.mod` (ADR 0004).

### 3.2 Decoding safety (architecture rule 4)

- A frame's declared size is checked against `MaxRequestBytes` before any
  byte of it is buffered beyond what has already arrived.
- `Reader` never allocates from a length it has not checked against the
  bytes remaining: an array of N elements needs at least N bytes left.
- `Reader` errors are sticky: after the first, every read returns zero and
  the caller checks once at the end. No bounds check can be forgotten.
- Byte slices returned by `Reader` alias the input. Anything kept past the
  handler call is copied.
- A decode error closes the connection. Nothing panics. Every decoder and
  the node's whole input path have fuzz targets.

### 3.3 Connections

Per connection: an input buffer and a busy flag. While a request from a
connection is being processed (waiting for commit, or parked as a delayed
fetch), later frames from that connection stay in the buffer. This is how
Kafka's own broker behaves, and it gives response ordering and per-
connection produce ordering without a reorder queue. A connection whose
input buffer exceeds twice `MaxRequestBytes` is closed.

### 3.4 Commands and state

```
command = type u8, then fields
  1 CreateTopic: name, partition count
  2 Append:      topic name, partition, one record batch
```

State machine (`state.go`): topics by name; per partition the next offset
and a dense list of batch references (base offset, log index, max
timestamp). `Apply` follows rules R1 to R7 of the roadmap: deterministic,
bounded decoding, unknown command types are errors.

Applying `Append` assigns the batch's base offset (the partition's next
offset) and advances it by the batch's record count. Applying
`CreateTopic` for an existing name reports `TOPIC_ALREADY_EXISTS` and
changes nothing, so two racing creates resolve the same way on every
replay.

A produce request with several partitions, or several batches for one
partition, becomes several commands. Its response is sent when the last of
them is applied.

### 3.5 Commit pipeline and fsync policy

`propose(cmd, done)`: append to the log (no sync), remember `done` for that
index, arm a zero-delay flush timer if none is armed. The event loop
delivers the timer after the requests already waiting, so one sync covers
all of them (group commit).

| `-sync` | committed means | acknowledged write survives a power loss |
|---|---|---|
| `always` (default) | synced | yes |
| `interval` | appended; a timer syncs every `-sync-interval` | no: up to one interval can be lost |
| `never` | appended; only segment rolls sync | no |

When the committed index advances, commands are applied in order and their
waiters completed. On start, the whole log is replayed into the state
machine (everything that survived recovery is committed).

`acks`: `-1` and `1` are both answered after commit. `0` gets no response,
as the protocol specifies. Other values are `INVALID_REQUIRED_ACKS`. The
produce timeout is ignored: on one node commit takes one fsync.

### 3.6 Produce validation

For each batch: at least a full header; `length` consistent with the bytes
present; magic 2 (else `UNSUPPORTED_FOR_MESSAGE_FORMAT`); CRC-32C correct
(else `CORRUPT_MESSAGE`); record count at least 1 and equal to last offset
delta + 1; not a control batch; size within the limit
(`MESSAGE_TOO_LARGE`). Compressed batches are stored as they are; the
broker does not decompress, so it does not validate the records inside.

### 3.7 Fetch

For each partition asked: unknown is `UNKNOWN_TOPIC_OR_PARTITION`; an
offset beyond the end or below zero is `OFFSET_OUT_OF_RANGE`; otherwise
whole batches starting with the one containing the offset, up to the
partition's and the request's byte limits, the first batch always. The
stored batch's base offset field is overwritten with the assigned offset on
the way out. High watermark = last stable offset = the applied end.

If the total is under `MinBytes`, no partition had an error and
`MaxWaitMs` > 0, the request is parked with a timer. Applying an append to
one of its partitions re-evaluates it. Timer expiry answers with whatever
there is. Closing the connection discards it and cancels the timer.

Fetch sessions (version 7+) are declined by always answering session id 0,
which tells the client to keep sending full requests.

### 3.8 The real I/O layer

One goroutine accepts; per connection one goroutine reads and one writes;
one goroutine runs the node. Reads become events on a bounded channel, so a
busy node stops reading sockets and TCP pushes back. `Send` appends to a
per-connection queue with a byte limit; overflow closes the connection.
Timers are `time.AfterFunc` posting events; a generation number per timer
id discards cancelled and replaced ones. Zero-delay timers are kept in a
local list that is served only when the event channel is empty, which is
what makes group commit happen. `Now` is the wall time at start plus
monotonic elapsed time, so it cannot go backwards.

## 4. Tests

- Table-driven tests for primitives, header, each codec (round trip), batch
  validation, state machine, each handler.
- Fuzz: each request decoder; each response decoder; record batch parser;
  command decoder; the node's connection input as a whole.
- Node tests on the simulated disk with a scripted clock and network: no
  goroutines, no sockets.
- Real-I/O test with the race detector: the node behind the TCP loop,
  driven by this repository's own encoder.
- Integration (acceptance): franz-go, the Java client and kcat each create
  or use a topic, produce N messages and consume the same N in order,
  against the built binary. Restart in the middle: messages survive.

## 5. Alternatives rejected

- **Generate the codecs from Kafka's JSON definitions.** Fewer hand-written
  lines and every version for free. Rejected for now: a generator is a
  sizeable program in its own right, and supporting every version means
  handling every version's semantics (topic ids, fetch sessions, leader
  epochs). A narrow range per API is less code to defend. Worth revisiting
  if the supported ranges need to grow a lot.
- **Support the newest (flexible) versions only.** Fewer branches, but
  version 13 of Produce and Fetch need topic ids and newer Fetch versions
  assume more of the replication protocol.
- **A response queue per connection instead of one-at-a-time.** Allows
  concurrent processing per connection, which a single-threaded node
  cannot use anyway.
- **Auto-create topics on Metadata.** Not asked for; topics are created
  explicitly.
- **Accept idempotent producers without enforcing sequences**, so that
  default client configurations work. Rejected: the client would believe
  it has a guarantee the broker does not give.

## 6. Known limits (to be stated in the report and README)

- Clients must disable idempotence (`enable.idempotence=false` in Java,
  `DisableIdempotentWrite` in franz-go). Roadmap item A2 removes this.
- No consumer groups until Phase 4.
- ListOffsets by timestamp returns the base offset of the first batch whose
  max timestamp qualifies, not the exact record.
- Replication factor must be 1. Topic deletion and configs are unsupported.
- Start-up replays the whole log.

## 7. Acceptance (from PLAN.md)

As in section 4, plus: every decoder fuzzed in CI; malformed and oversized
input never panics or allocates without bound; race detector clean.

## 8. Review record

Second pass, as a client author trying to break the broker:

1. **Pipelined produce requests could be answered out of order** if each
   were completed independently. One request at a time per connection
   (3.3) removes the possibility.
2. **A parked fetch could leak** when its connection closes or be answered
   twice when a timer and new data coincide. Each parked fetch has exactly
   one owner record, removed on the first of: completion, timeout, close.
3. **Acknowledging before sync under the default policy** would break the
   central invariant. The waiter is completed only by apply, and apply
   only runs below the committed index (roadmap rule R4).
4. **A fetch must not see unapplied appends.** Partition state changes only
   in apply, so reads cannot see them.
5. **A slice aliasing the connection buffer kept in a command** would be
   overwritten by the next read. Commands are encoded into fresh buffers.
6. **A large declared array length** could allocate before failing. The
   reader bounds every count by the bytes remaining.
7. **Replay must tolerate what a previous build wrote.** Command types are
   versioned by their first byte; unknown types fail loudly.
8. **Idempotence** is a real compatibility gap with default client
   settings and must be stated wherever compatibility is claimed.
