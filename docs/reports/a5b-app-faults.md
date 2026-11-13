# A5b report: application fault testing

Branch `a5b-app-faults`, 2026-11-14. Production plan item 5b.
Design: `docs/design/a5b-app-faults.md`. ADR 0018. Guide:
`docs/guides/testing-applications.md`.

## What was built

- **`samsa -faults seed=N[,rate=R][,kinds=...]`.** The node injects three
  Kafka-level faults, drawn from a seeded stream of its own, once per
  eligible request:
  - `lostack`: a produce commits, then the connection closes unanswered;
  - `notleader`: produce, fetch or commit refused as by a non-leader;
  - `rebalance`: a heartbeat of a stable group starts a rebalance.

  Each fault is logged (`fault injected kind=... fault=n`) and counted
  (`samsa_faults_injected_total`). The binary warns at start that faults
  are on. It keeps the spec in the data directory's `FAULTS`.
- **`samsa verdict -data D -in T -out T2 [-group G] [-expect ...]`.** It
  reads a stopped node's data with the same loading code as
  `samsa check`. It matches input records to output records by key, and
  reports each input as once, duplicated, lost (the group committed past
  it, nothing was output) or not yet processed. It lists up to 20
  examples of each and the replay command. Exit status: 0 pass, 1 fail,
  2 cannot judge.
- **A sample application** (`integration/apps_test.go`): a franz-go
  consumer that copies `in` to `out`, with the commit-before-process bug
  switchable.
- **A guide** for JUnit 5, Go and pytest.

## Results

| | |
|---|---|
| Commit before process, `seed=5,rate=0.2,kinds=rebalance` | FAIL on the first run, three runs out of three: 518, 518, 520 of 600 lost, each listed with its offset and key, and the replay command |
| Commit after process, `seed=5,rate=0.1` (all kinds) | PASS three of three; 169–268 duplicates (at-least-once); 44–51 s |
| Same, `rate=0.05` (what the test uses) | PASS two of two and in the final run; 22–28 s; all three kinds injected (checked from the broker log) |
| Faults off | no draw (`TestFaulterIsASeededStream`); the simulator suite passes unchanged |
| Unit tests | each kind at rate 1; the verdict on a hand-built pipeline with every class; four refusals (unknown topic, duplicate key, missing key, compressed batch) |

## How to run it

    make build
    ./bin/samsa -listen 127.0.0.1:9092 -data /tmp/app -faults seed=42
    # ... run the application, then stop the broker ...
    ./bin/samsa verdict -data /tmp/app -in orders -out invoices -group billing
    cd integration && go test -run TestVerdictCatchesCommitBeforeProcess -v .

## What is untested or weak

- **A seed does not fix the schedule** with real, concurrent clients.
  Which request is the *k*-th eligible one varies, so a rerun usually,
  not always, fails the same way. The three buggy runs lost 518, 518 and
  520 records.
- **Three of the plan's fault kinds are missing**: session expiry, slow
  or empty fetches, retention cutting offsets.
- **The JUnit and Python examples in the guide are not run by any test.**
  Only the Go loop is.
- **Compressed records are refused.** Most production producers compress
  by default. Applications must switch it off for the test. A
  decompressor in the core is a dependency decision (gzip is in the
  standard library, the others are not).
- **Single node only.** The verdict replays entries past the commit
  point, as `samsa check` does. On a single node with `-sync always`
  that holds nothing acknowledged. A cluster with faults has no test.
- **Only franz-go** has run under the faults. The Java client's reaction
  to a lost ack (it retries, idempotent by default) is expected but not
  tested here.
- **Branch protection is still off on `main`.** Merges wait for green CI
  by rule, not by setting. Enabling "require status checks" is the
  owner's setting.

## Ten questions

1. Why do faults have their own random stream instead of the node's?
2. What does "the same seed gives the same faults" guarantee, and what
   does it not?
3. Why does each eligible request draw exactly once, even when the
   fault cannot apply?
4. Why is `notleader` built from the same refusal a real non-leader
   sends?
5. Why does a lost ack close the connection, and why is it not counted
   as a dropped connection?
6. How does the verdict tell "lost" from "not yet processed", and why
   does the difference matter in a CI log?
7. Why match inputs to outputs by key, and what does it demand of the
   application?
8. Why is the verdict offline, and what would a live one cost?
9. Why did the fixed consumer first stall, and what does that teach
   about client settings in fault tests?
10. Why must a passing run show that every fault kind was injected?
