# Plan

Phases 0–3 are the MVP. Each phase starts with `docs/design/phase-N.md`
(approved before coding) and ends with a report and a stop for approval.

Phase 4 onwards, and the work beyond Phase 7, are specified in detail in
`docs/ROADMAP.md`, which wins where it differs from this file. The
architecture all phases share (one command log, partitions as views) is
ADR 0005.

Anything stated here about Kafka's wire protocol is a planning assumption.
Per architecture rule 5 it is verified against the official documentation in
the phase that implements it.

## Decisions made before Phase 0 (2026-11-10)

1. **Name:** Samsa. Module path `github.com/amritangshudey/samsa`.
2. **Go toolchain:** Go 1.27 via Homebrew, pinned in `go.mod`.
3. **Dev tools** (Homebrew, not module dependencies): `golangci-lint`,
   `kcat`, `openjdk`.

## Cross-cutting design questions

These shape several phases. Each is settled in the design doc of the phase
named, then recorded as an ADR. My current recommendation is given so you can
push back early.

| Question | Settled in | Recommendation |
|---|---|---|
| How does the core talk to the outside: synchronous calls on injected interfaces, or a pure `step(event) -> effects` function? | Phase 0 | Synchronous injected interfaces, as rule 1 states. Simpler to read and debug; the simulator injects faults at each call. |
| On-disk record format: store Kafka record batches as received, or wrap opaque payloads in our own frame? | Phase 1 | Decided: our own frame around an opaque payload. Since ADR 0005 the payload is a command, one of whose kinds carries a record batch. |
| What do simulated clients send: Kafka wire bytes or direct calls into the core? | Phase 3 | Wire bytes over the simulated network, so the decoders and request handling are inside the simulated system. |
| Raft topology: one Raft group for the cluster, or one per partition? | Phase 5 | Decided early (ADR 0005): one group, replicating one command log. |
| Kafka replicates with ISR, not Raft. What do `acks` and the high watermark mean here? | Phase 5 | A produce is acknowledged only after Raft commit, whatever `acks` says (stronger than requested is permitted). Fetch never returns beyond the commit index. |
| Consumer-group state is built in Phase 4 on a single node, then must be replicated in Phase 5. | Phase 4 | Write the coordinator as a state machine driven by a log of commands from the start, so Phase 5 swaps the local log for the Raft log. |

## Phase 0 — Foundation

**Tasks**
- Repository layout, `go.mod`, `.gitignore`, licence.
- Makefile with the targets listed in CLAUDE.md.
- GitHub Actions: build, test, race, lint, short fuzz run.
- Lint configuration, and the core-purity check: core packages may import
  only an allowlist of standard-library packages, may use nothing from
  `time` except `Duration`, and may not contain `go` or `select` statements.
- The core interfaces: Clock (now, timers), Disk (open, read, write, sync,
  rename, remove, directory sync), Network, Random. Interfaces only, with doc
  comments describing the exact guarantees each implementation must give.
- ADRs: deterministic-core architecture; repository layout; dependency
  policy; how the core does I/O (first row of the table above).

**Acceptance**
- `make build test lint` pass locally and in CI on an empty skeleton.
- The purity check rejects impure code, proven by a permanent table-driven
  test of the checker (it runs under `make test`, not `make lint`).
- The Disk interface doc states what is durable after each call. Phase 1
  depends on that contract.

**Risks**
- A Disk interface that is too high-level cannot express the faults Phase 1
  needs (torn write, lost fsync, lost rename). Design it from the fault list
  backwards.
- CI runs on Linux, development on macOS. Filesystem and fsync behaviour
  differ; the real disk implementation must be tested on both.

## Phase 1 — Storage

**Tasks**
- Record frame format with CRC, and its decoder (fuzzed).
- Segment: append, read by offset, roll at a size limit.
- Sparse offset index per segment; rebuilt from the segment if missing or
  corrupt.
- Fsync policy: every append, every N bytes / interval, or never.
- Recovery on open: scan the last segment, truncate at the first bad frame,
  rebuild the index.
- Simulated disk: tracks which bytes are synced; on crash, unsynced data is
  dropped, partially kept, or torn mid-write, chosen by the seeded RNG.
- Real disk implementation over `os.File`.

**Acceptance**
- Property test against the simulated disk: for random sequences of append,
  sync, crash and reopen, every record whose sync returned before the crash
  is readable afterwards; offsets are contiguous; no record is returned with
  a bad CRC.
- Recovering twice gives the same result as recovering once.
- Fuzzing the frame decoder and the index loader finds no panics.
- The same test suite passes against the real disk in a temp directory
  (without crash injection).

**Risks**
- Fsync semantics on macOS: plain `fsync` does not force data to stable
  storage; `F_FULLFSYNC` does. Verify what Go's `File.Sync` does on darwin
  and record it in an ADR.
- Forgetting directory fsync after creating or renaming a segment. The
  simulated disk must model directory entries as separately syncable.
- A simulated disk that is kinder than a real one gives false confidence.
  The list of faults modelled, and those not modelled (bit rot, misdirected
  writes), is written down explicitly.

## Phase 2 — Single-node broker

**Tasks**
- Capture what franz-go, the Java client and kcat actually negotiate, pick
  the supported version range per API, write the ADR.
- Request framing and headers, including flexible versions (tagged fields,
  compact encodings) where the chosen versions require them.
- Primitive encoders/decoders with length bounds; fuzz target per decoder.
- ApiVersions, Metadata, CreateTopics, Produce, Fetch, ListOffsets.
- Record batch validation on produce (magic, CRC, lengths, record count)
  without decoding individual records more than necessary.
- Real TCP server: one goroutine per connection for I/O, all requests
  funnelled into the single-threaded core.
- Topic and partition metadata persisted and recovered.

**Acceptance**
- Integration test: franz-go creates a topic, produces N messages and
  consumes the same N in order. Same for the Java client and kcat. A client
  that is not exercised by a test is not claimed as supported.
- Every decoder has a fuzz target, run in CI.
- Malformed and oversized requests yield a protocol error or a closed
  connection, never a panic or an unbounded allocation.
- Race detector clean on the TCP layer.

**Risks**
- This is the largest phase. Clients differ in which versions they require
  and how they fall back; recent Java clients have dropped old versions.
  Negotiation is captured from real clients first, not assumed.
- Fetch semantics (long polling with min bytes / max wait, partial batches,
  size limits) are subtle and are where "works with one client" tends to
  break with another. Long polling needs Clock timers in the core.
- franz-go would be the first dependency. Proposal to approve at the time:
  integration tests live in their own Go module so the broker module stays
  dependency-free.
- The Java client and kcat need a JVM and a binary on the machine and in CI.

## Phase 3 — Simulator v1

**Tasks**
- Harness: one seed drives virtual time, the simulated disk, the simulated
  network, the workload and the fault schedule.
- Simulated network for client-to-broker connections.
- Workload generator: clients that create topics, produce and fetch, and
  record what was acknowledged.
- Faults: node crash and restart at arbitrary points; disk faults from
  Phase 1.
- Invariant checkers:
  - no acknowledged write is lost (under the fsync policy that promises it);
  - offsets are monotonic and gap-free per partition;
  - recovery is idempotent;
  - a fetch never returns data that differs from what was produced.
- Determinism check: the same seed run twice produces an identical event
  trace hash.
- `make sim SEEDS=n`, `make sim SEED=x`; CI job over many seeds; failing
  seeds printed with the reproduce command.
- `docs/BUGS.md` started.

**Acceptance**
- The determinism check passes over many seeds in CI.
- At least one deliberately injected bug (for example, acknowledging before
  sync) is caught by an invariant, and the seed reproduces it. This shows
  the checkers can fail.
- Every bug found has a BUGS.md entry and a regression seed.

**Risks**
- Hidden nondeterminism: map iteration, a stray goroutine, wall-clock time
  in a log line. The trace-hash check exists to catch this early.
- Invariants that are too weak pass trivially. Each invariant gets a test
  showing it fires on a known-bad history.
- "No acknowledged write lost" is only true for fsync policies that sync
  before acknowledging. The checker must know the configured policy, and the
  documentation must say what the other policies give up.

**MVP ends here.**

## Phase 4 — Consumer groups

**Tasks**
- FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
  OffsetCommit, OffsetFetch (classic group protocol; versions verified and
  recorded as in Phase 2).
- Group state machine: empty, preparing rebalance, completing rebalance,
  stable, dead. Session and rebalance timeouts on the Clock interface.
- Durable committed offsets and group metadata.
- Simulator workload with group members joining, leaving and timing out.

**Acceptance**
- Integration test: two franz-go consumers in one group split the partitions,
  one is killed, the other takes over and resumes from committed offsets.
  Same scenario with the Java client.
- Simulator invariants: in a stable generation each partition has exactly
  one owner; committed offsets never go backwards without an explicit
  commit; committed offsets survive a crash.

**Risks**
- Rebalance timing is where clients are least forgiving; delayed join
  responses and member timeouts interact.
- Newer clients may prefer the newer consumer group protocol. Verify what
  each test client uses by default and what it falls back to.
- Rework in Phase 5 if group state is not log-driven (see table above).

## Phase 5 — Replication (Raft)

**Tasks**
- Raft from the paper: leader election, log replication, persistence of
  term, vote and log; commit index and apply loop. No membership changes,
  no snapshots unless the design doc shows they are required.
- Decide and document the mapping from Raft log entries to partition
  offsets, and the meaning of `acks` and the high watermark.
- Produce acknowledged on commit; fetch bounded by the commit index;
  Metadata reports the current leader; non-leaders return the appropriate
  Kafka error so clients refresh and retry.
- Topic creation and group state go through the replicated log.
- Real three-node cluster on localhost.

**Acceptance**
- Raft unit tests for election and replication, including the paper's
  log-matching cases (Figure 7) and the commit rule for earlier terms
  (Figure 8).
- Integration test: three nodes on localhost, a client produces while the
  leader is killed, and consumes every acknowledged message afterwards.
- Raft persistent state survives the Phase 1 disk faults in the simulator.

**Risks**
- The classic Raft bugs: committing entries from earlier terms by counting
  replicas, replying before persisting, stale leaders serving reads.
- Client retries after a leader change can duplicate messages. The
  idempotent producer is a non-goal, so duplicates on retry are expected
  behaviour and must be documented, and the "no lost writes" invariant must
  tolerate duplicates without tolerating loss.
- Without snapshots or log truncation the Raft log grows without bound.
  Acceptable only if stated plainly as a limitation.

## Phase 6 — Cluster simulation

**Tasks**
- Simulated network between nodes: partitions (including asymmetric),
  drop, delay, reorder, duplication.
- Fault schedules combining network faults, crashes and disk faults.
- Invariants: at most one leader per term; committed entries are identical
  on all replicas at the same index; no committed write is lost; a consumer
  never observes an uncommitted record; plus all Phase 3 invariants.
- Liveness check: after faults heal, the cluster elects a leader and makes
  progress within a bounded virtual time.
- Many-seed CI job, nightly long run.

**Acceptance**
- Deliberately injected Raft bugs (at least three, from the risk list in
  Phase 5) are each caught by an invariant from a reproducible seed.
- The seed corpus runs clean in CI; every bug found is in BUGS.md.

**Risks**
- Real inter-node transport is TCP, which does not reorder or duplicate
  within a connection. Decide whether the simulator models messages or
  connections, and say what that does and does not prove.
- Fault rates that are too high prevent any progress; too low and bugs stay
  hidden. Fault schedules need quiet periods and tuning.
- Seed runtime on a MacBook Air limits coverage; measure seeds per second.

## Phase 7 — Release

**Tasks**
- Reproducible benchmarks (throughput and latency, single node and three
  nodes, per fsync policy), with the machine and commands recorded.
- README: what it is, what is supported and demonstrated by which test,
  what is not supported, how to run the simulator.
- Architecture document linking the ADRs.
- Release build of the static binary; tagged release.

**Acceptance**
- Every claim in the README points to a test or a benchmark in the repo.
- A fresh clone builds, passes tests and runs the quick-start as written.

**Risks**
- Benchmarks on a laptop are noisy and thermally throttled; report
  conditions and variance, and compare only against the project itself.
