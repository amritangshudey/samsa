# B4b report: metrics, health, `samsa check`, config file

Branch `b4b-operations`, 2026-11-13. Production plan item 3, part (b).
Design: `docs/design/b4-operations.md`, section 5 and its "as built"
notes. Decision: [ADR 0017](../adr/0017-operations-interfaces.md).

## What was built

- **Metrics** (`internal/broker/metrics.go`, `cmd/samsa/metrics.go`):
  - `Node.Metrics()` returns the node's counters (requests by API key,
    bytes, connections accepted and dropped) and gauges (Raft role, term,
    committed, applied, log range, snapshot; topics, partitions, groups).
  - `-metrics-listen` serves them in the Prometheus text format, written
    by hand. The loop publishes a copy every 250 ms through
    `Loop.Every`, and the HTTP handlers read only that copy.
- **Health**:
  - `/healthz` answers 503 when the copy is more than a second old.
  - `/readyz` answers 200 when the node serves as leader, or follows a
    known leader with everything it knows committed applied. Otherwise it
    answers 503 with the reason: failed, stopping, elected but not yet
    serving, no leader, or still applying.
- **`samsa check -data dir`** (`internal/broker/check.go`,
  `cmd/samsa/check.go`):
  - It runs the node's own recovery, then reads every entry, which checks
    each checksum, and replays every command.
  - It runs on a disk wrapper that records writes instead of making them,
    so the check changes nothing. A write that recovery would make is
    printed as `repair needed:`, and an error as `corrupt:`.
  - Exit status 0 means a start would find nothing to fix.
- **Data directory lock**: `realsys.OpenDisk` takes an exclusive `flock` on
  `LOCK`. A second node, or a check, on a held directory fails with "in
  use". There was no lock before, so two nodes could share a directory.
- **Config file**: `-config file` reads `key = value` lines, one per flag.
  Flags on the command line win. An unknown key, a repeated key, a bad
  value or a line with no `=` stops the start.

## Results

| | |
|---|---|
| `TestCheckFindsCorruption` | a byte flipped in a sealed segment, a hard state with a flipped byte and a snapshot cut in half: each is reported, with the error naming what is wrong; clean data passes |
| `TestOperations`, real binary | config file applied; `/healthz` and `/readyz` 200; `/metrics` counts the produce and the topic; `check` refuses a running node's directory; after SIGKILL it passes; with bytes appended to the active segment it reports the cut and leaves the file unchanged; a misspelt key stops the start |
| `TestMetrics` | counters and gauges after 150 produces; not ready while stopping; every API key fits the counter array |
| `TestLoadConfig` | command line wins; five kinds of bad file refused |
| `TestOpenDiskLocksTheDirectory` | a second open fails; open after close works |

## How to run it

    go test ./internal/broker -run 'Check|Metrics'
    go test ./cmd/samsa ./internal/realsys
    make integration     # TestOperations
    ./bin/samsa -metrics-listen 127.0.0.1:9100 -data d &
    curl 127.0.0.1:9100/metrics; curl 127.0.0.1:9100/readyz
    kill %1; ./bin/samsa check -data d

## What is untested or weak

- **No latency histograms.** There are counters and gauges only: request
  latency, fsync time and apply lag are not measured. They would need
  histograms written by hand.
- **No error counts by API.** Most errors sit per partition inside a
  response, so a top-level count would mean something different for each
  API. Only connections dropped for bad input are counted.
- **Check replays entries past the commit point.** An entry there that
  cannot be applied is reported as corrupt, even though a new leader
  might have replaced it. A valid leader never writes one, so this is
  deliberate.
- **Check does not know the node's segment size.** It uses the defaults.
  A node run with a non-default segment size or batch bound is not a
  supported setup yet, because neither is a flag.
- **The lock is advisory and local.** It does not protect a data
  directory on a network filesystem shared by two machines.
- **`/healthz` measures the loop, not the disk.** A disk that hangs inside
  an fsync stops the loop, so `/healthz` fails, but only after a second.
- **Metrics are not covered by the simulator.** They are read-only copies,
  but a wrong gauge would pass every invariant.

## Ten questions

1. Why can the HTTP handler not call `node.Metrics()` itself?
2. Why does the loop publish on a ticker rather than after each event?
3. What exactly does `/healthz` prove, and what does `/readyz` add?
4. Why is a follower that has not applied everything it knows committed
   not ready, and what would a client see if it were?
5. Why does `check` run recovery itself instead of a separate reader of
   the formats?
6. How does a check that changes nothing still learn what recovery would
   repair?
7. Why can a byte flipped in a sealed segment pass `storage.Open`, and
   what in `check` catches it?
8. Why `flock`, and not a PID file? What does `flock` not protect
   against?
9. Why is an unknown config key an error, not a warning?
10. Why does `check` replay uncommitted entries, and when could that
    report a problem a start would never hit?
