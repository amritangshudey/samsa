# A4 report: black-box chaos test of the real binary

Branch `a4-chaos`, 2026-11-13. Production plan item 4.
Design: `docs/design/a4-chaos.md`.

## What was built

- **A proxy on every link** (`integration/proxy_test.go`). Each node's
  client port and each of the six directed peer links has a user-space
  TCP proxy. It can cut a link, delay it, reset its connections, or heal
  it. It fails only as TCP does: by closing connections, never by
  dropping bytes inside one.
- **Topology from existing flags.** Each node gets its own `-peers`
  string, in which every other node's peer address is the proxy for that
  link. `-advertise` names the node's client proxy. The broker needed no
  change.
- **`TestChaos`** (`integration/chaos_test.go`). Three real processes
  run under faults drawn from a seed:
  - SIGKILL and restart; SIGTERM and restart;
  - isolating a node from its peers; cutting one link in one direction;
  - slow links; connection resets.
  - Two franz-go producers (defaults, idempotent) and two readers that
    send raw Fetch requests to random nodes run throughout.
  - Every answer goes to the simulator's own `sim.Model`. At the end, one
    pass reads everything back.
  - After each fault the cluster must make progress again within 20 s.
- **`TestChaosCatchesBrokenBuild`.** `bin/samsa-unsafe` is built with
  `-tags unsafe` and has a hidden `-unsafe` flag that turns on the
  broker's deliberate bugs. The test isolates the leader three times; at
  each isolation it writes through the new leader and reads from the old
  one. The real build must pass this scenario, and the build with
  `no-lease,no-check-quorum` must fail it.
- **`sim.Model.Rebatched()`**. A real client forms its own batches, so
  the model skips the batch-shape checks and keeps every per-record
  check.
- `make chaos` runs `TestChaos` for `CHAOS_DURATION` (default 10m), and
  `CHAOS_SEED` replays a scenario. `make integration` runs it for 40 s.

## Results

| | |
|---|---|
| `TestChaos`, 10 minutes | passed: 315 faults, 931,031 records acknowledged, 69,855 fetches checked, final read complete |
| `TestChaos`, 30 s runs while building | passed after the three test-side fixes below; no broker bug found |
| `TestChaosCatchesBrokenBuild`, with the probe | caught on the first isolation in every run; 13 of 13 runs; the real build passed all 13 |
| Without the probe (first version) | caught in 3 of 4 runs, so it was replaced (design, finding 13) |
| Test-side bugs fixed | wrong replication factor; compressed batches the checker cannot read; a follower's NOT_LEADER passed to the model; batch-shape checks a real client cannot meet |

No broker bug was found. In about 11 minutes of faults across all runs,
every answer met the same rules the simulator enforces.

## How to run it

    make build
    cd integration && SAMSA_BIN=$PWD/../bin/samsa go test -run 'TestChaos|TestProxy' -v .
    make chaos CHAOS_DURATION=30m
    CHAOS_SEED=1794589733441160000 make chaos   # replays the fault sequence

## What is untested or weak

- **A seed replays the faults, not the run.** The steps and their
  timings come from the seed, but thread scheduling, TCP and the clients
  do not. A failure may not happen again on replay. Only the simulator
  reproduces a run exactly.
- **No power loss.** SIGKILL keeps the OS page cache, so unsynced writes
  survive. Power loss is tested only in the simulator.
- **One client library.** Only franz-go drives the load. The Java client
  and librdkafka are tested elsewhere, without faults.
- **Uncompressed only.** The checker cannot read compressed batches.
- **No groups, admin commands, retention or idempotent-producer
  checks.** The chaos history covers produce and fetch. Producers run
  idempotent, so a duplicate would show up in the final read, but I8 and
  I9 are not checked here.
- **The broken build turns off two safety steps.** With CheckQuorum on,
  a lease bug alone leaves a window of milliseconds that a real-time test
  hits only by luck. The simulator catches `no-lease` alone.
- **Clocks are shared.** All three processes run on one machine, so clock
  drift between nodes is not tested.
- **CI time.** `make integration` grows by about 75 s.

## Ten questions

1. What does this test reach that the simulator cannot, and what does
   the simulator reach that this cannot?
2. Why may a proxy close connections but never drop bytes inside one?
3. How does each node get its own view of the network without any
   change to the broker?
4. Why does the test use the simulator's model and not a simpler checker?
5. Why does `Rebatched` keep the per-record checks and drop only the
   batch-shape checks? What would a broker bug that splits or merges
   batches look like, and is it still caught?
6. Why are reads sent as raw Fetch requests to random nodes rather than
   through kgo's consumer?
7. Why must the real build pass the same scenario that catches the
   broken one?
8. Why did the random clients miss the stale read one time in four, and
   why does the probe not?
9. Why does the broken build need both `no-lease` and `no-check-quorum`?
10. What does `CHAOS_SEED` reproduce, and what does it not?
