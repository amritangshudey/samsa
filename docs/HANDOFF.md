# Handoff: where the project stands

Written for whoever works on Samsa next, a person or another coding tool.
**It is kept current:** the last thing done at the end of every item and every
merged PR is to update this file (CLAUDE.md says so). If the date below is old,
check `git log` and `docs/PRODUCTION.md` before trusting it.

**Last updated: 2026-11-15, 20:30 IST.**

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
- Still to ask about: test-only dependencies for item 16 (the client matrix).

## 4. State on 2026-11-15

**Done and merged to `main`:** items 1 to 13 and 24, 23a (see the status table),
including retention and compaction (ADR 0026, 0027, 0029), storage damage repair
(ADR 0030), profiles and `samsa doctor`, and four more fault kinds.

**Open PRs, in this order of merging** (each is based on the one before):
- #36 `packaging`: container image (`make image`), systemd unit, runbook,
  security review, owner-only data files.
- #37 `follower-fetch`: item 19, fetch from the nearest replica (KIP-392).
  After #36 merges, merge `origin/main` into it and push (an empty commit
  starts CI if a merge push does not).

**Soak (item 6):** run 1 (before the memory-leak fix) failed on memory only.
Run 2, on the fixed build, started 2026-11-15 01:38 IST and ends about
2026-11-16 01:40 IST. At 18.8 h: 33.7 million records, no errors, no gaps,
peak memory 33 MiB (run 1: 173 MiB). It runs from `~/soak-run2/`
(`samsa` binary, `out/` results, `src/` a git worktree) under `caffeinate`.
**When it ends:** copy `summary.md` and `samples.csv` to `docs/soak/run2/`;
run 1's output is on branch `soak-run1-data` (`docs/soak/run1/`); write
`docs/reports/g5-soak.md` comparing both (mention that other heavy work ran on
the same machine); PR, merge on green; then `git worktree remove` the source
worktree; then tags (v0.3 and onward, after checking CHANGELOG-style notes in
`docs/releases/`). Do not run the Kafka benchmark during a soak.

## 5. Next, in order

1. **18a topic ids.** Verified schemas are in
   `docs/design/c18a-topic-ids-schemas.md`: Metadata v9 to 12, Fetch v12 and 13,
   CreateTopics v5 to 7, DeleteTopics v6. State needs a topic id per topic
   (snapshot and format level change). Needed by 18 and 22.
2. **18 KIP-848 consumer groups** (ConsumerGroupHeartbeat 68, Describe 69;
   server-side range and uniform assignors), then **22 share
   groups** (75 to 78) after 18; **15 transactions**; **23b** fault testing for
   them.
3. **20 and 21 object storage:** design and prototype first, one design for
   tiered and diskless, behind the pluggable connector (section 3). Open
   questions are in `docs/PRODUCTION.md` section 7.
4. **14 sharding**, decided by ADR after 21; **16** client matrix (ask about
   dependencies); **17** the Kafka comparison benchmark (needs a quiet machine,
   after the soak) and tags.

## 6. Lessons that are not in the code

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
