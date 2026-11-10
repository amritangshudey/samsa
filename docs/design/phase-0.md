# Phase 0 design — Foundation

Status: approved 2026-11-10. Approval was delegated by the developer to a
second-pass review by the author; the findings and the changes they caused
are recorded in section 6.

Phase 0 produces no broker behaviour. It fixes the shape that every later
phase must fit into, so the decisions here are the expensive ones to change.

## 1. Concepts

**Deterministic simulation testing (DST).** A distributed system's bugs live
in rare interleavings: a crash between a write and its fsync, a timeout
firing just as a reply arrives. Ordinary tests seldom hit them, and when they
do the failure cannot be reproduced. DST removes every source of
nondeterminism from the system under test, then drives it with a seeded
pseudo-random generator that chooses the workload, the timing and the faults.
The same seed always yields the same execution, so a failure found on seed
4127 is a failure you can step through in a debugger as often as you like.

**The four sources of nondeterminism** in a server are time, the network,
the disk and randomness, plus a fifth that is specific to the runtime:
scheduling (goroutines) and, in Go, map iteration order. The first four are
put behind interfaces. The fifth is removed by making the core
single-threaded and forbidding order-dependent map iteration.

**Why not just test with real threads and a fake clock?** Go's
`testing/synctest` gives goroutines a virtual clock, but the scheduler still
chooses which runnable goroutine goes next, so an execution is not a function
of a seed. Determinism has to be a property of the design.

**What DST does not prove.** It tests the core against a *model* of the disk
and network. Bugs in the real I/O layer, and real-world faults the model
omits, are out of its reach. Those are covered separately by integration
tests and the race detector, and the model's omissions are written down.

## 2. Proposed design

### 2.1 How the core does I/O

A node is a single-threaded object. Two directions:

- **Inbound: the driver calls the node.** Something outside decides what
  happens next (a timer fires, bytes arrive on a connection) and calls one
  handler method. The node runs to completion and returns. Handlers are never
  called concurrently.
- **Outbound: the node calls injected interfaces.** While handling an event
  the node may read the clock, set a timer, send bytes, and read, write or
  sync files. These are plain synchronous calls.

In production the driver is an event loop: goroutines do the socket I/O and
push events onto one channel; one goroutine pops them and calls the node. In
the simulator the driver is a scheduler that picks the next event using the
seeded generator and advances virtual time.

Sketch of the contract (names open to change):

```go
// Called by the driver, one at a time. A non-nil error means the node is
// dead: the driver never calls it again (real: log and exit; simulator:
// treat as a crash).
type Handler interface {
    OnStart() error
    OnTimer(id TimerID) error
    OnConnOpen(c ConnID) error
    OnConnData(c ConnID, p []byte) error // p: any non-empty fragment, valid only during the call
    OnConnClose(c ConnID) error
}

type Instant int64 // nanoseconds since the Unix epoch

type Clock interface {
    Now() Instant // never goes backwards while the process lives
    SetTimer(id TimerID, after time.Duration) // re-arming an id replaces it
    CancelTimer(id TimerID) // OnTimer(id) is not called for a cancelled arming
}

type Network interface {
    Send(c ConnID, p []byte) // never blocks; see backpressure below
    Close(c ConnID)
}

type Random interface {
    Uint64() uint64
}

type Disk interface {
    Open(name string, flag OpenFlag) (File, error)
    MkdirAll(dir string) error
    Remove(name string) error
    Rename(oldName, newName string) error
    List(dir string) ([]string, error) // sorted
    SyncDir(dir string) error
}

type File interface {
    ReadAt(p []byte, off int64) (int, error)
    WriteAt(p []byte, off int64) (int, error)
    Size() (int64, error)
    Truncate(size int64) error
    Sync() error
    Close() error
}
```

Outbound dialling (node to node) is not in the Network interface yet. It is
added in Phase 5 when Raft needs it.

**Backpressure.** `Send` never blocks and returns nothing. The real
implementation keeps a bounded outbound buffer per connection; if it
overflows, the connection is closed and the node hears `OnConnClose`.
Inbound, the event channel is bounded, so a busy node stops reading sockets
and TCP pushes back on clients. Buffer sizes are set in Phase 2.

**The cost, stated plainly:** disk calls block the node. While one fsync
runs, the node handles nothing else. Throughput then depends on batching:
handle all the events that are ready, sync once, then send the
acknowledgements (group commit). The intended mechanism needs no extra
interface: on the first unsynced append the node arms a zero-delay timer,
which the driver delivers after the events already queued. This is designed
in Phase 1/2 and measured in Phase 7. It is the price of a core that reads
top to bottom.

### 2.2 The disk durability contract

This is what the storage engine may assume, and what the simulated disk is
allowed to break. Both implementations must honour it.

1. Bytes written to a file are durable only once a later `Sync` on that file
   has returned nil.
2. After a crash, each unsynced region of a file contains, block by block,
   the new bytes, the old bytes, or arbitrary bytes. Writes are not atomic: a
   crash can leave part of one write. Unsynced writes are not kept in order:
   a later write can survive when an earlier one is lost. The file's length
   may be anything between its last synced length and its largest unsynced
   length; an unsynced `Truncate` may or may not have happened.
3. A file's existence under a name (after create, rename or remove) is
   durable only once `SyncDir` on its parent has returned nil. Before that, a
   crash may show the old or the new directory entry.
4. `Rename` is atomic with respect to crashes: the name refers to the old
   file or the new one, never neither.
5. If `Sync` or `SyncDir` returns an error, nothing can be assumed about
   unsynced data, and retrying proves nothing (on Linux a failed fsync can
   clear the error and drop the dirty pages). The node must stop.

Not modelled in the MVP, and to be listed as such: corruption of data that
was already synced (bit rot), misdirected writes, and a disk that reports
success for a sync it did not perform.

### 2.3 Repository layout

```
cmd/samsa/            main: flags, wiring of the real implementations
internal/sys/         the interfaces above; no implementation
internal/storage/     Phase 1 (core)
internal/wire/        Phase 2: Kafka protocol encoding (core)
internal/broker/      Phase 2: request handling, the node (core)
internal/realsys/     real Clock, Disk, Network, Random and the event loop
internal/simsys/      simulated Clock, Disk, Network, Random
internal/sim/         Phase 3: harness, workload, invariant checkers
docs/                 PLAN.md, design/, adr/, BUGS.md
```

Only `internal/sys` and its doc comments are created in Phase 0. The other
directories appear in the phase that needs them.

Everything is under `internal/`: there is no public Go API to keep stable.

### 2.4 Enforcing purity

Rules 1 and 2 are checked by one test using only the standard library
(`go/parser`, `go/ast`). It parses every package under `internal/` except
`internal/realsys`, and fails on:

- an import that is not on a short allowlist of standard-library packages
  (`bytes`, `encoding/binary`, `errors`, `fmt`, `hash/crc32`, `io`,
  `log/slog`, `math`, `slices`, `sort`, `strconv`, `strings`, `time`, and a
  few like them) or one of this module's own core packages. Adding to the
  list is a deliberate, reviewed change;
- any use of package `time` other than `time.Duration` and its unit
  constants;
- a `go` statement or a `select` statement.

Test files (`_test.go`) are exempt: tests may use temp directories and real
time. The checker has its own table-driven test proving it rejects each kind
of violation, which replaces the one-off demonstration in PLAN.md.

`log/slog` is allowed but stamps records with the real time. The simulator
must install a handler that discards that timestamp, and log output is never
part of the trace hash.

The simulated implementations are checked too: a simulator that is not
deterministic is useless. Its generator is a small hand-written one
(SplitMix64, a few lines) so that no exception to the import ban is needed
and seeds mean the same thing on every Go version.

Being a test, it runs under `make test` and in CI with no extra tooling.

Not mechanically checked: iteration over maps. Detecting it needs type
information. It is covered by convention (sort the keys) and, from Phase 3,
by the trace-hash check that runs each seed twice.

### 2.5 Build, lint, CI

- **Makefile:** `build` (`CGO_ENABLED=0`, so the binary is static), `test`,
  `race`, `lint`, `fuzz`. The `sim` and `integration` targets are added in
  the phases that give them something to run.
- **Lint:** `golangci-lint` with its standard set plus `errorlint` (error
  wrapping) and `revive`'s exported-comment rule (doc comments).
- **CI (GitHub Actions):** on every push and pull request: build, test,
  race, lint, and a short fuzz run, on `ubuntu-latest` and `macos-latest`.
  Both, because the real disk code behaves differently on each.
- **Go version:** 1.27, pinned in `go.mod`; CI reads it from there.

### 2.6 ADRs written in this phase

- 0001 Deterministic core: driver calls handlers, core calls injected
  interfaces
- 0002 Disk durability contract
- 0003 Repository layout and mechanical purity check
- 0004 Dependency policy: standard library first; integration-test
  dependencies isolated from the broker module

### 2.7 Commit plan

1. `chore: initialise module, gitignore, licence`
2. `docs: add CLAUDE.md, plan and phase 0 design`
3. `build: add Makefile`
4. `test: add core purity check` (with a demonstration that it fails)
5. `feat(sys): add Clock, Disk, Network and Random interfaces`
6. `ci: add GitHub Actions workflow and lint configuration`
7. `docs: add ADRs 0001–0004`

## 3. Alternatives rejected

**A pure `step(event) -> []effect` core (sans-I/O).** The core would return
descriptions of the I/O it wants, and results would come back as new events.
This is the purest form and makes disk latency and reordering simulable.
Rejected because every disk read becomes a suspension point: reading a
record turns into a hand-written state machine spread across handlers. The
storage engine would be much harder to write and to explain. Faults at call
boundaries (crash before, during or after any disk call) are still fully
simulable with synchronous calls. What is given up: simulating slow disks
interleaved with other events on the same node.

**Goroutines under a deterministic scheduler.** Write ordinary concurrent
Go and control the scheduler in tests. Rejected: it needs a patched runtime
or a third-party simulator, depends on runtime internals, and rule 2 forbids
goroutines in the core.

**`time.Time` in the Clock interface.** Rejected: it carries a location and
a monotonic reading, and invites `time.Since` and friends, which read the
real clock. A bare `int64` cannot.

**Asynchronous disk interface with callbacks.** Same objection as the pure
step function, for the same benefit. Revisit only if Phase 7 benchmarks show
group commit is not enough.

**Enforcing purity with `depguard` and a custom analyser.** `depguard` can
ban imports but not `go` statements; a custom analyser needs
`golang.org/x/tools`, a dependency. One standard-library test covers both.

**A separate network abstraction for messages now.** Raft will want
message-shaped communication, Kafka clients need byte streams. Designing
both now would be guessing. Streams only; Phase 5 extends it.

**Public `pkg/` packages.** Nothing outside this repository imports the
code. `internal/` keeps the freedom to change everything.

## 4. Acceptance (from PLAN.md)

- `make build test lint` pass locally and in CI on the skeleton.
- A deliberately impure file in a core package makes the purity test fail,
  shown once and then removed.
- The Disk interface documentation states what is durable after each call.

## 5. Decisions (answered 2026-11-10)

1. **GitHub repository:** public, created with `gh`.
2. **Licence:** Apache-2.0.

## 6. Review record

A second pass over this document, reading it as someone who has to build
Phase 1 on top of it. Eight gaps, all fixed above:

1. **No way for the node to stop.** The disk contract says the node must
   stop after a failed sync, but the core may not exit the process. Handlers
   now return an error that the driver treats as node death.
2. **Group commit had no trigger.** A node cannot see that "all ready events
   are handled". Resolved with a zero-delay timer; no new interface.
3. **`Send` could buffer without bound.** Backpressure rules added.
4. **A denylist of imports is the wrong default.** Anything not thought of
   (`context`, `runtime`, `maps`) would slip through. Changed to an
   allowlist, and `select` is banned alongside `go`.
5. **The crash model was too kind.** Unsynced blocks may now hold arbitrary
   bytes, not only zeros, and file length and truncation after a crash are
   specified.
6. **Clock semantics were unspecified.** A wall clock can jump backwards;
   `Now` is now required not to. Timer re-arm and cancel behaviour is stated.
7. **`Disk` could not create directories.** `MkdirAll` added.
8. **`slog` reads the real clock.** Noted, with the rule that keeps it out
   of the trace hash.

Accepted without change: synchronous disk calls, stream-only networking
until Phase 5, and leaving map iteration to convention plus the trace hash.
