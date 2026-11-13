# A3 design: shrinking, timeline, coverage and mutation score

Status: reviewed, 2026-11-13. Production plan item 5 (`docs/PRODUCTION.md`),
roadmap A3.

## 1. The problem

A failing seed prints its setup and the violation. The trace has every
event, often tens of thousands of lines over seconds of virtual time. Most
of the faults in it had nothing to do with the failure. Finding the few
that matter is manual work, and so is reading the trace.

Two numbers are also missing. Nobody knows which interesting states the
seeds reach: a leader killed mid-commit, a split vote, a torn write cut at
recovery. And the mutation suite stops at the first seed that catches each
bug, which says that a bug can be caught, not how often.

## 2. Design

### 2.1 Shrinking

The roadmap asks for "the schedule as an explicit list derived from the
seed". Rewriting the simulator to draw every fault up front would change
what every existing seed does, including those in `regressions.txt` and
`BUGS.md`. Instead:

- **Every fault gets a sequence number.** Each call to `chaos` (power
  loss, kill, disk crash, failed sync, graceful stop) and each partition
  that `partition` starts is numbered in the order it happens. The run
  records each one in `Result.Faults` (`{Seq, At, What}`).
- **A fault can be skipped without moving any draw.** `Config.Skip` is a
  set of sequence numbers. A skipped fault still makes every random draw
  it would have made (target, kind, delay to the next fault), and then
  does not act. The world's random stream, the clients' streams and the
  disks' streams stay aligned, so a run that skips nothing is the
  original run, event for event.
- **Knobs that keep alignment.** `Config.Calm` zeroes the drop and
  duplicate probabilities and the client disconnects. `Chance(p)` draws
  once whatever `p` is, so these too move no draw.
- **The shrinker** (`sim.Shrink`), given a failing seed:
  1. Runs the seed, learns its violation's rule tag (`I1`, `S6`, ...) and
     its faults.
  2. Tries `Calm`.
  3. Runs delta debugging over the faults: it skips halves, then
     quarters, down to single faults. A candidate is kept when it still
     fails with the same rule tag.
  4. Repeats step 3 until no single fault can be removed, within a
     budget of 500 runs.

  The result is the setup, the faults left (time, node, what) and the
  violation, with a replay command: `make sim SEED=n SKIP=1-5,7`.
- A run that no longer fails the same way is not kept, even if it fails
  differently. Shrinking must not swap one bug for another.

### 2.2 Timeline

`make sim SEED=n TIMELINE=t.html` (with an optional `SKIP`) writes one
self-contained HTML file with inline SVG, no scripts from the network:

- One lane per node, one per client.
- A node's lane is shaded while the node is down. Leadership is a
  coloured band with the term.
- Faults are marked on the lane they hit. Partitions are hatched spans on
  every lane they cut.
- Peer messages are arrows from sender to receiver. They are drawn only
  in the window before the violation (default 2 s of virtual time), or
  the file would hold tens of thousands.
- Client lanes show acknowledged writes and checked reads as ticks. The
  violation is a red line across all lanes with its message.

Events are captured where the world already calls `note`, plus send and
delivery of peer messages, as a typed list (`timelineEvent`). Without
`TIMELINE`, nothing is captured.

### 2.3 Coverage

The `Stats` counters already count most states (power losses, recoveries
cut, installs, handovers, trims, refusals). Three are added:

- `LeaderFaults`: a fault hit the node that was serving.
- `SplitVotes`: a term ended with no leader.
- `LeaderIsolated`: a partition cut the leader off from a majority.

`TestSim` prints the totals and lists every counter that stayed at zero
as "never reached". `TestSimCoverage` asserts that 300 seeds reach every
listed state, so a change that stops a fault from happening fails a
test, instead of quietly making the simulator weaker.

### 2.4 Mutation score

`TestMutationSuite` runs every mutation on the same 200 seeds and reports
"caught by k of 200". A mutation caught by none of them is then searched
for up to seed 2000, as now, and its score is reported as 0 of 200 plus
its first catch. Each mutation's first catching seed is still rerun
without the mutation. The suite already runs on every push (`make test`
in the `test` job), and a PR is merged only on green CI (CLAUDE.md), so a
mutation that stops being caught blocks the merge. GitHub does not
enforce this: `main` has no branch protection (checked 2026-11-13).
Turning it on is a repository setting for the owner, not a code change.

## 3. What checks it

- `TestSkipNothingIsTheSameRun`: for 50 seeds, a run with an empty
  `Skip` and the original run have the same event hash (`w.hash`, which
  every `note` feeds).
- `TestShrink`: a mutation's catching seed shrinks to fewer faults, still
  fails with the same rule, and the replay command reproduces it.
- `TestTimeline`: the HTML of a failing seed holds the violation and a
  lane per node and client, and stays under 5 MB.
- `TestSimCoverage`, and the mutation score table as above.

## 4. Alternatives rejected

- *Drawing the whole schedule first.* Changes every seed's meaning and
  splits the simulator into a planner and an executor. Skipping, with
  draws kept, gives the same shrinking for a fraction of the change.
- *Shrinking the workload too* (clients, requests). Clients draw from
  their own streams, so idling one keeps the others aligned. But a shrunk
  workload changes the shape of the history, and the faults are what a
  reader needs cut down. Left for later if needed.
- *A JavaScript timeline viewer.* A static SVG opens anywhere, diffs in
  review, and needs no build.
- *Turning on branch protection from here.* It is a setting of the
  owner's repository, outside the code; it is recommended in the report.

## 5. Review

Second pass, looking for what breaks:

1. **A skipped fault can still change the run.** Skipping a kill changes
   which node is up, so later draws mean different things: the "target"
   draw picks the same index, but another node may now be leader. That is
   fine. A candidate is judged only by whether it still fails the same
   way, and the shrunk schedule is the one that was run.
2. **Rule tags.** Some violations have no tag ("did not all come back",
   the liveness check). For those the whole message, up to the first
   number, is the key.
3. **Partitions are started by `partition` and healed by a timer.** A
   skipped partition must still schedule its heal draw. The heal of a
   partition that was never made is a no-op.
4. **The disk crash in recovery** (`CrashAfter` from `chaos`) is a
   numbered fault. The crash during recovery that `start` itself draws
   (world.go, the 25 % in `start`) is numbered too, so it can be removed.
5. **Budget.** A seed with 300 faults needs about 2·300·log(300) runs in
   the worst case. The 500-run budget stops earlier and reports the best
   result found so far. At about 40 ms a run, that is 20 s.
6. **Timeline size.** Messages are limited to the window. Lanes are
   capped at 5 nodes and 20 clients, as the simulator never exceeds
   them.
7. **Determinism of capture.** The timeline is written after the run
   from the captured list. Capture draws nothing and feeds nothing into
   the hash, so a run with `TIMELINE` is the same run.

**Found while building** (after the review):

8. *Only the draws that choose a fault are kept.* A skipped kill leaves a
   node running, which then sends messages and draws network delays the
   original run did not. So alignment holds until the first skipped
   fault, not after it. "Skip nothing is the same run" is exact. It was
   checked against `origin/main` for 50 seeds (equal trace hashes) and is
   kept by `TestSkipNothingIsTheSameRun`.
9. *Shrinking has a limit.* Because the faults share the world's random
   stream, removing an early fault reshapes everything after it. A failure
   that needs a long chain of faults may not survive any cut. Results:
   - Seed 1, acks before sync: 11 faults to 1, in 8 runs.
   - Seed 17, transfer keeps the lease: 4 to 1, in 16 runs.
   - Seed 24, no lease: all 3 needed.
   - Seed 38, retention start: none needed.
   - Seed 25, truncating on a stale message: 22 of 22 kept, in 54 runs.

   A separate random stream for faults would fix seed 25's case, but it
   changes every existing seed; that is left for when a real failure
   needs it.
10. *`go test` owns `-skip`.* It is the test-name filter, so the flag is
    `-skip-faults`. `make sim SKIP=...` hides this.
11. *Coverage found a gap at once.* `ExpiredRefusals`, a producer that
    expired and came back, is zero in 5000 seeds. Expiry is 15 s and the
    workload runs 10 s, so it cannot happen. The expiry cannot be
    shortened below `answerBound` plus `giveUp` (12 s) without breaking
    the reasoning in `sim.go`. Longer runs for expiry seeds would cost
    simulator time. The gap is recorded in `docs/PRODUCTION.md`;
    `TestJavaProducerAfterExpiry` still covers the behaviour with a real
    client.
12. *The coverage totals are summed by reflection* in the test (not in
    the core), so a new counter can never be left out of the "never
    reached" list.
13. *Mutation scores* (200 seeds each): vote before persisting 150, vote
    by length 3, truncate on stale 10, acks before sync 141, serve early
    34, no lease 2, no fencing 85, snapshot before sync 24, no sequence
    check 46, transfer keeps lease 16, expire early 30, trim early 50,
    retention moves start back 2. The suite takes 65 s, as before.
