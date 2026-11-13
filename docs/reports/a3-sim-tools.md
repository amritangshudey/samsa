# A3 report: shrinking, timeline, coverage and mutation score

Branch `a3-sim-tools`, 2026-11-13. Production plan item 5.
Design: `docs/design/a3-sim-tools.md`.

## What was built

- **Numbered faults.** Every fault is numbered in the order it happens and
  listed in `Result.Faults`: each fault `chaos` injects, each partition,
  each disk crash drawn during recovery. `Config.Skip` skips faults by
  number. A skipped fault still makes the draws that chose it, so a run
  that skips nothing is the original run, event for event.
  `Config.Calm` zeroes message drops and duplicates without moving any
  draw.
- **Shrinking**: `sim.Shrink` and `make shrink SEED=n [MUTATION=...]`. It
  calms the network, then runs delta debugging over the faults. A
  candidate is kept only if it fails by the same rule. It prints the
  setup, the faults left and a replay command:
  `make sim SEED=n CALM=1 SKIP=1-11`.
- **Timeline**: `make sim SEED=n TIMELINE=t.html` writes one HTML page with
  two inline SVG panels.
  - The first panel shows the whole run: node down spans, leader bands
    with their terms, faults (skipped ones hollow), partitions, and each
    client's acknowledged writes and checked reads.
  - The second panel shows the 2 s before the violation, with every peer
    message as an arrow.
  - The page adapts to light and dark themes, and ends with the list of
    faults.
- **Coverage**: three new counters (faults on the serving leader, leaders
  isolated, terms without a leader). `TestSim` sums every counter and
  prints those never reached; the existing threshold check covers the new
  ones.
- **Mutation score**: every mutation runs on the same 200 seeds, and the
  suite reports "caught by k of 200". Only a mutation that none of them
  catches is searched further, up to seed 2000.

## Results

| | |
|---|---|
| Skip nothing | trace hashes equal to `origin/main` for 50 seeds; `TestSkipNothingIsTheSameRun` |
| Seed 1, acks before sync | 11 faults to 1 in 8 runs: one one-way partition; the final restart of every node then loses entries acknowledged as committed (S3). The same schedule passes without the mutation |
| Seed 17, transfer keeps lease | 4 faults to 1 in 16 runs: a graceful stop of the leader (S6) |
| Seed 38, retention start | no fault needed (R1 from retention alone) |
| Seed 24, no lease | 3 of 3 needed |
| Seed 25, truncate on stale | not shrunk: 22 of 22 after 54 runs (see below) |
| Timeline of seed 1, shrunk | 114 kB; the restarts and the violation are visible on one screen |
| Mutation scores (of 200) | vote before persisting 150, acks before sync 141, no fencing 85, trim early 50, no sequence check 46, serve early 34, expire early 30, snapshot before sync 24, transfer keeps lease 16, truncate on stale 10, vote by length 3, no lease 2, retention start back 2 |
| Coverage gap found | `ExpiredRefusals` never reached in 5000 seeds (below) |

## How to run it

Note, 2026-11-14: adding the SASL APIs (item 7) changed the advertised API
list, and with it every seed, because the simulated network draws once per
delivered fragment. The seed-1 example below no longer fails. The same
demonstration is now seed 4: 39 faults shrink to 1, rule S3
(`TestShrink`).

    make shrink SEED=1 MUTATION="acknowledge entries before they are synced"
    make sim SEED=1 CALM=1 SKIP=1-11 MUTATION="acknowledge entries before they are synced" TIMELINE=t.html
    make sim SEEDS=1000          # the totals end with "never reached: ..."
    go test ./internal/sim -run TestMutationSuite -v | grep score

## What is untested or weak

- **Shrinking fails on long chains.** Faults share the world's random
  stream, so removing an early one reshapes the rest of the run. Seed 25
  needs a chain and did not shrink at all. Giving faults their own stream
  would fix that, but it changes every existing seed.
- **The acceptance asked for a seed from BUGS.md.** Those bugs are fixed,
  so their seeds no longer fail. The demonstrations use the mutation
  suite's catching seeds, which are deliberate versions of the same kinds
  of bug.
- **A coverage gap that is not fixed here.** A producer that expired and
  came back is never simulated: expiry is 15 s and the workload runs
  10 s. Fixing it means longer runs for some seeds, or rethinking the
  bound that keeps expiry above 12 s. It is recorded under item 1 in
  `docs/PRODUCTION.md`.
- **Low mutation scores.** No lease and retention moving the start back
  are caught by 2 seeds in 200, and vote by length by 3. Each is caught,
  but a small change to the workload could lose it. The score now makes
  that visible.
- **Branch protection is off.** The mutation suite runs on every push and
  merges wait for green CI by rule, but GitHub does not enforce it on
  `main`. Turning on "require status checks" for the `test` job is the
  owner's setting.
- **The timeline is checked by a test for its content, not its looks.**
  It was looked at by eye for one seed.

## Ten questions

1. Why number faults instead of drawing the whole schedule first?
2. What exactly does "a run that skips nothing is the original run" rely
   on, and how was it checked?
3. Why must a shrunk candidate fail by the same rule and not just fail?
4. Why did seed 25 not shrink, and what would change that?
5. Why is `Calm` safe to try first, and what does it keep aligned?
6. What does the seed-1 timeline show that the trace did not make
   obvious?
7. Why does the timeline draw messages only in a window?
8. What does a mutation score of 2 in 200 tell you, and what should you
   do about it?
9. Why could `ExpiredRefusals` never happen, and why is the fix not just
   a shorter expiry?
10. Why are the coverage totals summed by reflection in the test and not
    in the core?
