# Handoff: where the project stands

Written for whoever works on Samsa next, a person or another coding tool.
**It is kept current:** the last thing done at the end of every item and every
merged PR is to update this file (CLAUDE.md says so). If the date below is old,
check `git log` and `docs/PRODUCTION.md` before trusting it.

**Last updated: 2026-11-21, compacted topics stay in the log under object storage, and S19 to S22 and M27 fixed (PR #57, "tiering: compacted topics, S18 to S22"); PR #56 (S16, S17 and T1) merged as `38f3777`.**

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
  processes of 5,000: all passed. No invariant failed, but one seed,
  623351498, held 4 GB: tiering uploaded the same segment again and
  again, and nothing checked that tiering progresses (`docs/BUGS.md` S16).
  The new check T1 then found a second stall in 8 of 3,000 seeds (S17).
  Both are fixed in PR #56 (`38f3777`). The integration job
  failed once, in the sample
  application of `TestVerdictCatchesCommitBeforeProcess` (`docs/BUGS.md`
  M26), fixed in PR "nightly 2026-11-21".

Until S16 was fixed, the simulator never tiered a segment of more than
about 70 batches, so items 20 and 21 had far less simulation than their
seed counts say. With the fixes, 3,000 seeds pass with T1, locally; no
full nightly run on GitHub on them yet. Until S18 the swarm never drew
tiering together with compaction or disk rot, and no document said why.
Allowing the pair with compaction, 9 of 600 seeds failed, on two real bugs
(S18, S19). 5,000 seeds of that fix then found two more (S20, S21), and
5,000 seeds of their fix one in it (S22). CI's 3,000 seeds then all
passed, and the suite failed its own coverage floor for rot repair: 297
damages found against 300. The rot injector stopped for good once
tiering deleted its damage before anything found it (M27). With the
injector fixed, the same 3,000 seeds find and repair 439, and 10,000
seeds pass locally (1 to 5,000 and 623370000 to 623374999), finding and
repairing 1,599 damages. `make test` passes on every commit of the PR,
the mutation suite and the regressions included. `make sim` fails its
coverage floor on the commits from the draw change to the one before
M27's fix.

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
3. **Fixed (BUGS.md S16, S17; design c20 section 6):** a `cmdTier` too
   large for a log entry was refused and the segment uploaded again for
   ever; and a leader cut off while its `cmdTier` waited never tiered
   again. T1 now checks that the settled leader tiers every sealed
   segment, and the mutation "keep the tiering flag when serving again"
   that T1 can still fail (its first catch is seed 1139: 85 s of the
   mutation suite). Tiering now runs in far more seeds than before:
   expect the simulator to find more in that code.
4. **Fixed (BUGS.md S18, S19; ADR 0042; design c20 section 7):** with
   object storage, compaction rewrote tiered batches and snapshots stopped
   decoding, so a node could neither restart nor install one. Also, a fetch
   in a compaction gap before a tiered batch was answered
   OFFSET_NOT_AVAILABLE for ever. Compacted topics now stay in the log, as
   in Kafka (KIP-405). The swarm draws tiering with compaction and rot,
   and the mutation "tier a compacted topic and rewrite its tiered
   batches" keeps the pair drawn. Drawing rot with tiering showed that the
   rot injector stopped for good once its damage was deleted unfound
   (M27, fixed).
5. **Fixed (BUGS.md S20, S21, S22; ADR 0043; design c20 section 8, a1
   section 8):** a follower fetch that waited for an object read was
   answered during a snapshot install, and the node stopped. And a snapshot
   transfer started again with every newer snapshot, so a follower that
   needed one never caught up. The fetch now checks again when its reads
   complete. The leader finishes the snapshot it began, and compacts
   nothing while a follower that still answers installs it. The first
   version of that went on after the log start moved, and sent an older
   snapshot with a newer cut (S22, found before merge). None of the three
   has a mutation: the simulator met each in 1 to 3 of 5,000 seeds. Unit
   tests guard them.
6. **The owner's decision:** S18 and S20 are in `v1.0.0`, and S21 is in
   every release since `v0.1.0`. S18 touches only clusters with object
   storage and a compacted topic.
   - Cut `v1.0.1` with the fixes?
   - Repair logs that S18 already damaged? That means decoding a batch that
     is both in an object and at a log index as tiered. It changes the
     snapshot decoder and the state hash: design it first. Until then,
     such a log still breaks its snapshots once trimmed past.
7. Refuse `cleanup.policy=compact` for a diskless topic, and for a topic
   that was already tiered, as KIP-405 does (ADR 0042, alternative 3). For
   now both are accepted, their batches in objects are never compacted,
   and nothing tells the user.
8. Watch the nightly run of 2026-11-22. It is the first full one on items
   20 and 21, the first with tiering drawn with compaction and rot, the
   first after S14 to S22 and M26, and the first that can open the issue
   of item 9.
9. **Done:** a failed nightly run opens an issue titled "Nightly simulator
   run failed" with its failing seeds, or comments on the one still open
   (`.github/workflows/ci.yml`, job `sim`). Close the issue once its seeds
   are fixed or explained; the next failure then opens a new one. Not yet
   seen working: it runs only on a failing scheduled run.
10. The simulator never reaches `ExpiredRefusals` (0 in 23,000 seeds, and
   on `main` too; `make sim` logs "never reached"). That is its check that
   an idempotent producer refused after its state expired starts again
   (`internal/sim/idempotent.go`). Either the broker never refuses there,
   or no producer is silent long enough: find out which, and make a seed
   reach it.
11. Optional follow-ups (not blocking v1.0): multi-Raft sharding (revisit ADR
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
- **Watch what a run costs, not only what it reports.** S16 failed no
  invariant; it showed only as one seed that took 2 minutes and 4 GB (a
  heap profile named the code). A check that the system makes progress,
  not only that what it does is right, is what made it a failure (T1).
- **A combination the swarm never draws is a claim that it cannot fail.**
  The draws said `tier = Chance(0.35) && !rot && !compact` with no reason
  given, while the broker tiered compacted topics. Drawing the pair found
  S18 and S19 at once. Every `&& !x` in the draws must give its reason
  (rot's does: design B1, section 7).
- **When a coverage floor fails, find what stopped before touching the
  floor.** CI's 3,000 seeds found 297 damages against a floor of 300.
  Lowering the floor or drawing rot more often would have passed. The cause
  was an injector that stopped for good (M27), and fixing it found 439.
- **What fits Raft may not fit the broker.** The broker's state names
  record data in the log from its own log start, below the snapshot's
  index, and `compactTo` keeps the log only for the newest snapshot. Raft
  sees only the index. S21's first fix sent an older snapshot with a cut
  that fit Raft and not the broker (S22). Check any change to what Raft
  sends or keeps against `compactTo`.
- **A request that waits must check again before it answers.** S13's check
  ran when a fetch arrived. Item 20 added a wait after it, and a follower
  answered from a log that an install had reset meanwhile (S20).
  `completeFetch`, the older wait, already checked again.
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
