# Handoff: where the project stands

Written for whoever works on Samsa next, a person or another coding tool.
**It is kept current:** the last thing done at the end of every item and every
merged PR is to update this file (CLAUDE.md says so). If the date below is old,
check `git log` and `docs/PRODUCTION.md` before trusting it.

**Last updated: 2026-11-21, the nightly run of 2026-11-21 read (PR "nightly 2026-11-21"): its integration failure was the sample application (M26, fixed), and GitHub cut its simulator job off, so its seeds ran locally; PR #54 (part 2) merged as `28978aa`.**

## 1. Read these first, in this order

1. `CLAUDE.md`: the architecture rules (pure core, no goroutines or clocks in
   `internal/` except `realsys`), the process, the conventions. Binding.
2. `docs/PRODUCTION.md`: the plan. Section 4 is the original order, section 6
   the status of every item, section 7 the plan aimed at Kafka's problems
   (items 18 to 24). **The status table is the source of truth.**
3. `docs/ARCHITECTURE.md`: the code as built. `docs/adr/`: every decision,
   with the alternatives. `docs/BUGS.md`: every bug the simulator or a test
   found, with seed and root cause. `docs/reports/`: one per item, with the
   untested and weak parts and ten questions.
4. `docs/guides/runbook.md` and `configuration.md`: how it is run.

## 2. The rules that cost something to forget

- **Never claim** "production ready" or "best". The goals are goals.
- **Ask the owner before adding any dependency**, test-only ones included.
  The module has none (standard library only); `integration/` is a separate
  module for client libraries.
- **Every item** gets a design doc with a recorded second review, tests first,
  a mutation for each safety step, an ADR, BUGS.md entries, a report with ten
  questions, and a PR that is merged only on green CI (`gh pr merge N --merge`).
  Never force-push, never push to `main`.
- **Every commit builds and passes tests.** Never weaken an invariant to
  make a test pass: find out why it fails.
- **Verify Kafka wire formats against the official schemas** (the JSON files
  in the Apache Kafka repository), not memory (rule 5).
- Commits end with the tool's co-author line; PR bodies with its generated-with
  line.
- **The owner's name, email and details are private**: never in a request to
  an outside service.

## 3. What the owner has approved (standing)

- "Approve yourself and continue coding till there is a blocker or the project
  is done; I will review later." Stop only for a real blocker.
- Merging green PRs, and pushing everything, tags included.
- Apache Kafka's tarball for the CLI integration tests (installed), and
  running Kafka's broker for the item 17 comparison benchmark
  (2026-11-15).
- A newer Kafka Java client (4.1 or later) for tests of items 18 and 22
  (2026-11-15).
- An S3-compatible server for item 20's tests, **on condition that the object
  store is a pluggable connector**: in-memory fake, local directory, and
  S3-compatible by settings, so the owner can try real S3 later by changing
  settings (2026-11-15).
- Owner approved test-only Go client dependencies (`IBM/sarama`,
  `segmentio/kafka-go`) for item 16 on 2026-11-21.

## 4. State on 2026-11-21

**All roadmap items (1–24) are merged to `main`** and tags `v0.1.0` through
`v1.0.0` are pushed (release notes in `docs/releases/`); see
`docs/PRODUCTION.md` section 6 for each item. The CI run of every push is
green (build, unit, race, fuzz, lint, 3,000 simulator seeds, exhaustive
Raft search, real clients).

**The nightly run (30,000 seeds) has failed every night since 2026-11-13**
and nobody looked: 33 failing seeds in eight nights.

- 2026-11-13 (4 seeds) and 2026-11-14 (1): three were the old S6 checker
  (M10) and one the transfer-lease bug (S8), both fixed on 2026-11-14,
  before the next run; the fifth was the first sighting of S15.
- 2026-11-15 to 2026-11-20 (28 seeds): a review on 2026-11-21 replayed the
  24 that the logs print on `v1.0.0`; 15 still failed with the same
  schedule. 11 were the simulator's own mistakes (`docs/BUGS.md` M22, M23,
  M24), fixed in PR #53. **4 were two real bugs**, 2 seeds each: storage
  (S14) and membership (S15), both fixed in PR "review findings, part 2".
  Every seed is in `internal/sim/regressions.txt`.
- 2026-11-21, the first run on items 20 and 21 (`fb6fd51`, before the S14
  and S15 fixes), failed without a seed. GitHub shut the simulator job's
  runner down after 17 minutes (`The runner has received a shutdown
  signal`, exit 143), and the job prints its seeds only at the end. The
  same 30,000 seeds (base 623340000) ran locally on `28978aa`, in six
  processes of 5,000: the first 20,000 passed, and the rest was still
  running when this was written. No invariant failed, but one seed,
  623351498, held 4 GB: tiering uploaded the same segment again and
  again, and nothing checked that tiering progresses (`docs/BUGS.md` S16,
  fixed in the next PR). The integration job failed once, in the sample
  application of `TestVerdictCatchesCommitBeforeProcess` (`docs/BUGS.md`
  M26), fixed in PR "nightly 2026-11-21".

The tiered and diskless code (items 20, 21) has had 3,000 seeds per push
and 20,000 of the nightly's, run locally, but S16 kept most of it idle: a
segment of more than about 70 batches was never tiered, so the tiered read
path was hardly simulated. No full nightly run on GitHub yet.

**Soak (item 6):** done. Run 1 (before the memory-leak fix, `docs/BUGS.md` L1)
failed on memory only (peak RSS 173 MiB). Run 2 on the fixed build (`7d877ab`)
finished 2026-11-16 01:38 IST and **passed**: 43,196,136 records produced and
consumed (41.2 GiB), 0 produce errors, 0 gaps, RSS 18–32 MiB (25 MiB at 24 h),
data directory 29–256 MiB (198 MiB at 24 h), open files 16–26, and restart on
the soaked data ready in 63 ms (`docs/reports/g5-soak.md`).

**A local change was stashed, not committed:** `git stash list` shows an
edit of `internal/broker/handlers.go` that removed the C21-7 re-check of
remote batches. Simulator seed 924 fails with it (`I2: the leader answered
OFFSET_NOT_AVAILABLE`) and passes without it: do not apply it.

## 5. Next, in order

1. **Fixed (BUGS.md S14, design b1 section 9):** a truncation into a sealed
   segment made it the active segment again, so rot in its old, synced
   frames was cut at the next start as a torn tail (seeds 623263227,
   623265521). The segment now stays sealed and appends go to a new one.
2. **Fixed (BUGS.md S15, design c1 finding 10 amended):** a spare that was
   added, led, and appended its own removal did not stand after a restart,
   because its commit index was gone and the configuration in force at -1
   is empty for a spare; the other voter needed its vote (seeds 623233840,
   623282885). A node now stands while any configuration from its commit
   index on names it.
3. **In progress, the next PR (BUGS.md S16):** a `cmdTier` too large for a
   log entry was refused, and the segment uploaded again for ever. With it
   fixed, tiering will run in far more seeds than before: expect the
   simulator to find more in that code.
4. Watch the nightly run of 2026-11-22: the first full one on items 20 and
   21, the first after S14, S15 and M26, and the first that can open the
   issue of item 5.
5. **Done:** a failed nightly run opens an issue titled "Nightly simulator
   run failed" with its failing seeds, or comments on the one still open
   (`.github/workflows/ci.yml`, job `sim`). Close the issue once its seeds
   are fixed or explained; the next failure then opens a new one. Not yet
   seen working: it runs only on a failing scheduled run.
6. Optional follow-ups (not blocking v1.0): multi-Raft sharding (revisit ADR
   0039 if the single-Raft-group write ceiling becomes a bottleneck),
   Python/JS native client tests in CI, and automated web UI compatibility
   checks.

## 6. Lessons that are not in the code

- **A green push is not a green project.** The nightly run (30,000 seeds) failed
  for eight nights while every push was green, because only the nightly run
  draws enough seeds to meet one-in-ten-thousand bugs. Look at
  `gh run list --workflow CI --event schedule` at the start of every session,
  and replay every failing seed on the current commit before trusting it.
- **Edits that silently do nothing** happened twice (a string replacement that
  missed its target by a tab, and BSD `sed -i` with a newline). Check that an
  edit applied, especially a guard like `if !deletes(t)`.
- **A CI run is not started by every push** (a merge commit did not start
  one): `gh run list --branch B` shows it; an empty commit starts one.
- **Timing tests on CI** are slower than on the laptop: size them for CI
  (the all-kinds fault test took 108 s locally and failed on CI before it was
  split).
- **The simulator is the judge.** Most real bugs of the last items were found
  by it, a few by real-process tests, a few of its own bugs (the M entries in
  BUGS.md) by false alarms: investigate a false alarm before changing the
  check, and record it.
- **Seeds change** when a feature adds a random draw to every client; the
  seeds in `internal/sim/regressions.txt` still run but may no longer show their
  original bug.
- The machine is an Apple Silicon MacBook Air; `make integration` needs kcat, a
  JDK (`/opt/homebrew/opt/openjdk/bin/java`) and the Kafka CLI tarball.

## 7. Commands

`make build test lint`; `make sim SEEDS=1000` (or `SEED=n` to replay, with a
trace); `go test ./internal/sim -run TestMutationSuite -v`; `make integration`;
`make image`; `make soak`; `make release VERSION=vX.Y.Z`. Full list in
`CLAUDE.md`. To rerun a night's seeds: `go test ./internal/sim -count=1
-run '^TestSim$' -timeout 2h -seeds=5000 -base-seed=B` for B, B+5000, …,
B+25000, where B is that night's `-base-seed` (days since 1970 × 30,000).
