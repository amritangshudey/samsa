# Handoff: where the project stands

Written for whoever works on Samsa next, a person or another coding tool.
**It is kept current:** the last thing done at the end of every item and every
merged PR is to update this file (CLAUDE.md says so). If the date below is old,
check `git log` and `docs/PRODUCTION.md` before trusting it.

**Last updated: 2026-11-21, review of the nightly simulator runs.**

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

**The nightly run (30,000 seeds) has failed every night since 2026-11-15**
and nobody looked: 28 failing seeds in six nights. A review on 2026-11-21
replayed the 24 that the logs print on `v1.0.0`: 15 still fail with the same
schedule. Of those:

- 11 were the simulator's own mistakes, fixed in PR "review findings, part 1"
  (`docs/BUGS.md` M22, M23, M24; seeds in `internal/sim/regressions.txt`).
- **4 are two real bugs, open**: 2 seeds each (section 5, items 1 and 2).

The tiered and diskless code (items 20, 21) merged after the last nightly
run, so it has had 3,000 seeds per push and no nightly run yet.

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

1. **Storage cuts synced entries after a lost segment** (seeds 623263227,
   623265521: `recovery of node 1 failed: raft: snapshot file is corrupt or
   does not match the log: snapshot at 15, log holds 0 to 12`). A crash loses
   the newest segment (legitimately: Raft had cut and rewritten that suffix and
   the new file was not yet synced), so the sealed segment before it becomes
   the active one again. `storage.recoverActive` then reads latent damage in
   it as a torn tail and truncates entries that were synced long ago, against
   the promise of `storage.Open` ("it never discards a record that was
   synced"). With a snapshot above the cut the node cannot start; without one
   it would forget entries it may have acknowledged, without the `recovering`
   marker that keeps it from voting. Direction: a last segment that has an
   index file was sealed and synced, so a bad frame in it is damage; report it
   and repair through `cutAt` (marker first, snapshot dropped when above the
   cut) instead of truncating. Needs a design note, a mutation and a BUGS.md
   S entry.
2. **No leader after membership changes** (seeds 623233840, 623282885, both
   `changes=true`): with faults stopped the cluster elects nobody for 60 s;
   in 623233840 every node stays in term 3 while only the node with the
   shortest log keeps asking for votes. Root cause unknown: start with
   `make sim SEED=623233840 TIMELINE=t.html`.
3. Watch tonight's nightly run, the first on items 20 and 21. A local run of
   30,000 seeds on `v1.0.0` was killed by the machine (16 GB, other load)
   after 54 minutes, so it says nothing.
4. Make a failed nightly run impossible to miss (an issue opened by the
   workflow, or a check of `gh run list --event schedule` at the start of
   every session).
5. Optional follow-ups (not blocking v1.0): multi-Raft sharding (revisit ADR
   0039 if the single-Raft-group write ceiling becomes a bottleneck),
   Python/JS native client tests in CI, and automated web UI compatibility
   checks.

## 6. Lessons that are not in the code

- **A green push is not a green project.** The nightly run (30,000 seeds) failed
  for six nights while every push was green, because only the nightly run
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
`CLAUDE.md`.
