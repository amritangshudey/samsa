# Handoff: where the project stands

Written for whoever works on Samsa next, a person or another coding tool.
**It is kept current:** the last thing done at the end of every item and every
merged PR is to update this file (CLAUDE.md says so). If the date below is old,
check `git log` and `docs/PRODUCTION.md` before trusting it.

**Last updated: 2026-11-16, 02:56 IST.**

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

## 4. State on 2026-11-16

**Done and merged to `main`:** items 1 to 13, 18a, 19, 23a, 24, and item 17's
packaging/runbook/security review (see the status table in
`docs/PRODUCTION.md`), including retention and compaction (ADR 0026, 0027,
0029), storage damage repair (ADR 0030), fetch from the nearest replica
(ADR 0031, PR #37), profiles and `samsa doctor` (PR #35), four more fault kinds
(PR #35), packaging/runbook/security review (PR #36), the 24-hour soak
(`docs/reports/g5-soak.md`, `docs/soak/run1/`, `docs/soak/run2/`, PR #39), and
topic IDs and Kafka 3.1 protocol versions (`docs/reports/c18a-topic-ids.md`,
ADR 0032, format level 4, snapshot v9).

**Soak (item 6):** done. Run 1 (before the memory-leak fix, `docs/BUGS.md` L1)
failed on memory only (peak RSS 173 MiB). Run 2 on the fixed build (`7d877ab`)
finished 2026-11-16 01:38 IST and **passed**: 43,196,136 records produced and
consumed (41.2 GiB), 0 produce errors, 0 gaps, RSS 18–32 MiB (25 MiB at 24 h),
data directory 29–256 MiB (198 MiB at 24 h), open files 16–26, and restart on
the soaked data ready in 63 ms (`docs/reports/g5-soak.md`). Release notes for
v0.3.0 to v0.6.0 are in `docs/releases/`.

## 5. Next, in order

1. **18 KIP-848 consumer groups** (ConsumerGroupHeartbeat 68, Describe 69;
   server-side range and uniform assignors; plus DescribeTopicPartitions 75,
   OffsetCommit v8, OffsetFetch v6–8, FindCoordinator v3–4, ListOffsets v6–7,
   Produce v9), then **22 share groups** (76 to 79) after 18; **15
   transactions**; **23b** fault testing for them.
2. **20 and 21 object storage:** design and prototype first, one design for
   tiered and diskless, behind the pluggable connector (section 3). Open
   questions are in `docs/PRODUCTION.md` section 7.
3. **14 sharding**, decided by ADR after 21; **16** client matrix (ask about
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
