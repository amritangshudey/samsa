# A4 design: black-box chaos test of the real binary

Status: reviewed, 2026-11-13. Production plan item 4 (`docs/PRODUCTION.md`),
roadmap A4.

## 1. The problem

The simulator drives the core with a simulated network, disk and clock.
It never runs `realsys`: not the TCP loop, the peer transport, the real
timers, the files or the signal handling. The integration tests do run
that code, but each one checks a single scripted outcome ("the records
come back"), not every answer a client got under faults. A bug in the real
layer, for example a peer frame lost in a way the simulator cannot
produce, would pass both.

## 2. Design

**Processes.** Three `bin/samsa` processes on localhost, `-sync always`,
started by the test (`integration/chaos_test.go`).

**A proxy on every link** (`integration/proxy_test.go`). There is one
user-space TCP proxy per directed peer link (i→j, six of them) and one per
node's client port. No root is needed. The topology comes from the
existing flags alone:

- Node i gets its own `-peers` string. Its own entry carries its real peer
  address, which it listens on. Entry j carries the address of proxy i→j,
  which it dials. The hello names the node id, so nothing compares the
  addresses.
- Every node advertises its client proxy (`-advertise`) and lists the
  other nodes' client proxies, so clients only ever reach a node through
  its proxy.

A proxy can **cut** (close every connection and refuse new ones),
**delay** (hold each chunk for a time), **reset** (close every connection
now, accept new ones) and **heal**.

**Faults.** A scenario is a list of steps drawn from a seed. The seed is
printed, and the test can be run again with `CHAOS_SEED`:

| Fault | Done by |
|---|---|
| kill a node, restart it later | SIGKILL; the same data directory |
| isolate the leader from its peers, clients still reaching it | cut both directions of its four peer links |
| cut one peer link, one direction | one proxy |
| slow links | delay 5–50 ms on a random half of the links |
| reset connections | reset on every proxy |
| restart everything | SIGTERM each node in turn (graceful stop) |

The whole cluster is healed between steps, and it is never left with more
than one node down.

**Clients and the history.** Real franz-go clients drive the load:

- Two producers use `kgo` with defaults (idempotent). Each record carries
  an id from `sim.Model.Send`.
  - An acknowledged record is reported with `Acked` at its offset.
  - A record that failed is reported with `Unknown`.
- Two readers send raw `kmsg.FetchRequest`s (Fetch v11) to a node picked
  at random, through `Broker(id).Request`. They do not go through
  `kgo`'s consumer, which would hide which node answered and what was
  returned.
  - Before sending, a reader captures the model's floor, start and
    knowledge (the causality rule).
  - The raw record bytes go to `sim.Model.Fetched`, exactly as in the
    simulator.

The model is the simulator's own (`internal/sim`), not a copy. The
integration module imports it, which Go permits because its import path
is under the samsa module's path; a `replace` points at `../`. The model
is single-threaded, so the test holds one mutex around every call. The
model is strict: under `sync always` with SIGKILL, every acknowledged
record must survive.

**The end.** Every fault is healed and every node runs. Then one pass
reads every partition from offset 0 (`BeginFinal`, `CheckFinal`): every
acknowledged record must be read, once, at its offset.

**The broken build.** `go build -tags unsafe` adds a hidden flag,
`-unsafe`, taking a list of the broker's mutation switches by name
(`cmd/samsa/unsafe.go`). The release build has no such flag. The test
builds `bin/samsa-unsafe` and runs the leader-isolation scenario with
`-unsafe no-lease,no-check-quorum`. The isolated old leader keeps
answering reads from its old state while the majority elects a new
leader and acknowledges new writes. A read sent to it after such an
acknowledgement returns a high watermark below the model's floor, and
the model fails the run.

## 3. What checks it

- `TestChaos`: the seeded scenario for `CHAOS_DURATION` (default 40 s in
  `make integration`; `make chaos` runs 10 minutes). It requires the
  model to pass, the final read to be complete, and at least one
  acknowledged write and one checked read in each step. Without that
  last check, a test whose clients stalled would pass.
- `TestChaosCatchesBrokenBuild`: the broken build fails the model, within
  three isolations.
- Proxy unit test: bytes pass both ways; cut refuses; heal accepts again.

## 4. Alternatives rejected

- *`iptables`/`pf` or network namespaces.* Need root, and differ between
  macOS and Linux.
- *Toxiproxy.* A dependency and a separate process; the needed behaviour
  is about 100 lines.
- *Checking with a new, simpler checker.* The point is that the same
  checker judges both, so a rule cannot be right in one and missing in
  the other.
- *Only `no-lease` in the broken build.* With CheckQuorum, the isolated
  leader steps down after one election timeout, about when the majority
  elects a new leader. A stale read then needs a write acknowledged by
  the new leader and a read reaching the old one inside a window of a few
  milliseconds. The simulator, which controls time, catches `no-lease`
  alone; a real-time test would catch it only by luck. Turning off
  CheckQuorum too keeps the window open. The test is about whether the
  real layer lets the checker see a stale read at all, not about how
  narrow the window is.

## 5. Review

Second pass, looking for what breaks:

1. **A proxy adds its own failures.** A proxy that loses bytes inside a
   connection would be a fault TCP never produces, and the broker could
   rightly fail. So proxies forward whole reads in order, and fail only
   by closing connections, which TCP does produce.
2. **Ports are reused between the free-port probe and the listen.** Each
   proxy listens on `:0` and keeps its listener, so its address is
   known. Nodes' real addresses are still probed (`freeAddr`). The race
   already exists in the cluster tests and has not hit.
3. **kgo retries hide outcomes.** A producer error after kgo's retries is
   recorded as `Unknown`, so the model allows the record to be either
   present or absent. With idempotence, a record that is present appears
   once. A duplicate would be reported by the final read.
4. **Clocks.** The model uses no time. The lease uses each node's
   monotonic clock, and the processes share one machine, so clock drift
   is not tested here (ADR 0013 covers the assumption).
5. **Raw fetches through `Broker(id)`.** franz-go connects to the address
   in metadata, which is the proxy, so the cut applies to the reader too.
   During leader isolation, the clients still reach the old leader, which
   is the point of that scenario.
6. **The final read can race a late write.** Producers are stopped and
   their outstanding records resolved before `BeginFinal`.
7. **CI time.** 40 s of chaos plus startup is added to `make
   integration`. The 10-minute run is `make chaos`, by hand.

**Found while building** (after the review):

8. *The model assumed the simulator's batches.* It checked that a batch
   read back is exactly a batch that was sent, with consecutive ids.
   franz-go forms its own batches and mixes ids from separate `Send`
   calls. `Model.Rebatched()` drops those two checks only. Each record is
   still placed, checked for damage and duplication, and checked against
   the floor. `TestRebatched` shows the switch is what makes the
   difference.
9. *Compressed batches.* franz-go compresses by default, and the checker,
   like the broker, never decompresses (`wire.DecodeBatch`). The chaos
   producers send uncompressed. Compression has its own real-client test
   (`TestFranzGoCompressedAndLarge`).
10. *A follower's `NOT_LEADER_OR_FOLLOWER` is no answer.* The simulator's
    clients drop such a response before the model sees it; the chaos
    readers now do the same.
11. *A catch proves nothing if the real build fails too.* As in the
    simulator's mutation suite, `TestChaosCatchesBrokenBuild` first runs
    the same isolation scenario on the real build, which must pass.
12. *Replication factor.* A topic on three nodes needs replication factor
    3 or -1, so the test sends -1 rather than using the single-node
    helper.
13. *Random clients do not reliably produce the stale read.* The first
    version of the catch test isolated the leader and relied on the
    random producers and readers. It caught the broken build in 3 of 4
    runs. A stale read needs a write acknowledged by the new leader and a
    read reaching the old one in the same window, and the producers often
    kept sending to the old leader, which names itself leader under the
    mutation. Each isolation now runs a probe: wait for the other two
    nodes to name a new leader, write one record through it, then read
    from the old leader. The real build runs the same probe.
