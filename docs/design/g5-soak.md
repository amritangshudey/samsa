# G5 design: soak and resource bounds

Status: reviewed, 2026-11-14. Production plan item 6 (`docs/PRODUCTION.md`):
"24 h run, memory and disk tracked; start-up re-measured with retention.
Must show: flat memory, bounded disk, numbers in the repo."

## 1. The problem

Every test so far runs for seconds or minutes. Some faults only show over
hours:

- **Leaks:**
  - soft state that is never freed: connections, groups, producer ids,
    held fetches, timers;
  - file descriptors left open by segment rotation;
  - memory kept alive by a slice that only grows.
- **Unbounded disk:** retention that falls behind, snapshots or index
  files that are never deleted.
- **Start-up time** that grows with the life of the data, not its size.

Rule: the docs claim nothing that no test shows. "Memory is flat" needs a
run long enough to see a slope, and numbers in the repository.

## 2. The run

`make soak` (`TestSoak`, `SAMSA_SOAK=1`) starts one real node from the built
binary:

- `-sync always`;
- `-retention-bytes 256 MiB`;
- `-producer-expiry 10m`, so that expiry happens many times in a day;
- `-metrics-listen`;
- its log at info level, in a file.

The workload runs for `SAMSA_SOAK_DURATION` (24 h):

| Part | What | Exercises |
|---|---|---|
| Producer | 500 records/s of 1 KiB, acks all, idempotent; the client is replaced every 5 min | appends, segment rotation, retention trims, snapshots; a new producer id every 5 min, which must expire 10 min after it stops |
| Two consumers in one group | poll from the start, auto-commit; each leaves and rejoins every 7 min | fetch, offset commits, rebalances, member churn |
| Connection churn | a new client every second: connect, ping, close | per-connection state, file descriptors |

The run writes 500 KiB/s, about 41 GiB a day. Retention keeps the disk
near 256 MiB.

**Why one node.** A cluster adds peer connections and follower state.
Both are worth soaking, but a single node first separates the broker's
own leaks from Raft's. A three-node soak is a follow-up.

**Rejected: a much higher rate.** It finds throughput limits, not leaks,
and it wears the laptop's SSD. The benchmark already covers throughput.

## 3. Samples and bounds

Every minute, the run records into `samples.csv`:

- RSS, from `ps`;
- data directory bytes, from walking it;
- open files (`/proc/PID/fd`, or `lsof`);
- the node's own gauges: connections, log start, last index, snapshot;
- the workload's counts: produced, consumed, produce errors, offset
  gaps.

The first quarter of the run is warm-up. The Go heap and the page cache
grow to their working size, and retention reaches steady state after
about 9 minutes. The bounds:

| Bound | Rule | Why this form |
|---|---|---|
| Flat memory | peak RSS of the last quarter ≤ peak RSS of the second quarter + 10 % + 16 MiB | comparing peaks over equal windows absorbs the GC sawtooth. The quarters are 12 h apart in a 24 h run, so a leak adds 12 times its hourly rate. At 100 MiB RSS the allowance is 26 MiB, so leaks above about 2 MiB/h fail. That is the limit of this test |
| Bounded disk | max data directory ≤ retention + 3 segments (64 MiB each) | retention removes whole segments: up to one segment over, plus the active one, plus snapshot and index |
| Open files | peak of the last quarter ≤ peak of the second + 16 | rotation must close what it opens |
| No gaps | every offset a consumer receives follows the highest one seen on its partition, or repeats | a hole means lost data or retention outrunning the consumer |
| Throughput | at least 90 % of the target rate produced | a node that slows to a crawl is not "up" |

At the end, the run stops the node with SIGTERM and restarts it on the
soaked data. It measures the time to `/readyz` returning 200. That is the
start-up time "re-measured with retention" which the plan asks for. The
log holds about 256 MiB at that point, whatever the run's length.

The run writes `summary.md`: min, median, max and last of each measure,
the verdict and the restart time. Both files are copied to `docs/soak/`
with the date and the commit.

## 4. Second review

1. **Logging into the test's memory.** The integration harness keeps every
   broker log line in a buffer (`broker.Write`). At debug level that is
   gigabytes a day. The soak starts its own node and logs at info level
   to a file.
2. **The 5 s readiness wait of `waitUntil`** is too short for a restart on
   a full log. The soak waits up to 2 minutes and fails if the node is
   not ready by then.
3. **A gap check shared by two consumers.** After a rebalance, a member
   starts from the last commit, so repeats are normal. The check keeps
   the highest offset seen per partition across both members. Only a
   jump past it counts as a gap.
4. **RSS is not the heap.** It includes memory the Go runtime has not
   returned to the OS. The bound is on RSS, the number an operator sees.
   A flat RSS is the stronger claim, and the one asked for.
5. **The memory bound's sensitivity is written down** (table above), so
   that "flat" means "no leak above about 2 MiB/h in 24 h", not more.
6. **Disk wear.** 41 GiB written in a day is far below a laptop SSD's
   rated endurance (hundreds of TB). The rate is chosen to make
   retention trim roughly every two minutes, not to stress the drive.
7. **Asking first.** The run needs the developer's machine awake for a day
   (`caffeinate -i make soak`). PRODUCTION.md lists this as a known
   blocker, so the run waits for the developer's go-ahead.
8. **The first trial run tested nothing on disk.** Every value was 1 KiB
   of zeros, and franz-go compresses by default. 300 000 records left
   21 MiB, and retention never cut a segment (`log_start` stayed 0),
   though the run passed. Values are now random, and produced without
   compression, so the bytes written are the bytes the design counts.
   The lesson for the real run: check that `log_start` moves before
   believing the disk bound.
9. **A ten-minute run is not a soak.** The harness is validated on a short
   run with 64 MiB retention, so that trims happen within it. Those
   numbers prove the harness works, not the bounds.
