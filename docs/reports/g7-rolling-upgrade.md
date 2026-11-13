# G7 report: rolling upgrade

Branch `upgrade`, 2026-11-14. Production plan item 10.
Design: `docs/design/g7-rolling-upgrade.md`. ADR 0024.

## What was built

- **Snapshots in the oldest version that holds them:**
  - state version 4 when there are no ACLs, credentials or level;
  - Raft version 1 when there is no configuration.
- **A cluster format level** (`internal/broker/level.go`):
  - reports from each node (`msgLevel`);
  - `cmdSetLevel`, which the leader proposes once every voter supports
    the level;
  - UNSUPPORTED_VERSION for ACL, credential, forwarding and voter changes
    below level 1;
  - a learner must report the level before it becomes a voter.
- **Mutation `UnsafeRaiseLevelEarly`** (`-unsafe raise-level-early` in an
  unsafe build).
- **`TestRollingUpgrade`** (integration):
  1. Three nodes on the previous release.
  2. A producer writes one record at a time throughout.
  3. Each node is restarted with SIGTERM on the new binary.
  4. After each restart, the old nodes must still run.
  5. A leader on the new build must refuse a credential change while any
     old node remains, and accept it after the last.
  6. Every acknowledged record is read back dense, in order and once.
- **CI builds the previous tag** (`git describe --tags HEAD^`) for the
  test, and `SAMSA_REQUIRE_UPGRADE=1` makes a missing old binary a
  failure.
- **Simulator check U1:** a settled run reaches its build's level.

## Results

- **`TestRollingUpgrade` from v0.2.0:** 6 of 6 local runs pass after the
  fixes below, in about 8 s each. Between 150 and 400 records were
  acknowledged during the upgrade, with no producer error.
- **With `-unsafe raise-level-early`,** the test fails: both old nodes
  stop with `undecodable command in log: unknown type 15 at index 9`.
- **`TestLevelWaitsForEveryVoter`:** with node 3 silent, as v0.2.0 is, the
  level stays 0 and CreateAcls is refused. After node 3 reports, the level
  is 1 on every node and CreateAcls succeeds. With the mutation, the level
  rises at once, which the test reports.
- **`TestSnapshotVersionFollowsContent` and `TestSnapshotEncoding`** pin
  the version choices.
- **3000 simulator seeds pass with U1,** as do the existing mutation
  checks. Unit tests and lint are clean.

## Problems met while writing the test

- **A brand-new franz-go producer gave up** when a stopping node closed
  its connection during ApiVersions. franz-go reads that as a TLS
  mismatch. The test now upgrades a running producer. Applications that
  start a producer during a rolling restart may see this error: it is in
  the client, and Kafka brokers close connections on shutdown too.
- **Only the leader answers admin writes,** so a gated request reaches the
  gate only on a new leader. The test moves leadership off an old node by
  restarting that node on the old binary.

## How to run it

```sh
git worktree add /tmp/prev v0.2.0 && (cd /tmp/prev && go build -o /tmp/samsa-prev ./cmd/samsa)
go build -o /tmp/samsa ./cmd/samsa
cd integration && SAMSA_BIN=/tmp/samsa SAMSA_OLD_BIN=/tmp/samsa-prev go test -run TestRollingUpgrade -v .
```

## What is untested or weak

- **Only one upgrade path is tested,** v0.2.0 to this build. The CI job
  always tests from the previous tag, so each release is tested against
  the one before. Skipping a release is not tested.
- **Downgrade is not supported,** and a downgrade attempt is not tested.
  An old binary on level-1 data stops at the first command it cannot
  read. It does not misread.
- **The level rises automatically.** There is no window to roll back
  binaries after the last node is upgraded, unlike Kafka's
  `kafka-features.sh`.
- **A fresh cluster refuses gated requests for up to about a second
  after its first election.** Kafka clients do not retry
  UNSUPPORTED_VERSION.
- **The test uses three nodes and a single producer,** and no consumer
  group runs during the upgrade.
- **Formats outside the log are not covered:**
  - the hard state;
  - the producer id block file;
  - the segment format.

  None changed since v0.2.0, and the test reads v0.2.0's data with the new
  binary. Nothing yet forces a review when one changes.

## Ten questions

1. Which bytes could v0.2.0 not read, and which of them did the new build
   write without any new feature being used?
2. Why can a snapshot be written in an older version, and why is the
   choice still deterministic?
3. Why must the level be in the log rather than negotiated per message?
4. What does the leader need before it proposes `cmdSetLevel`, and why do
   reports expire?
5. What does a node do if the log holds a level above its build's?
6. Why does AddRaftVoter wait for the learner's level report?
7. In the test, why is leadership moved off an old node before the gated
   request is sent?
8. What happens in the broken build (`raise-level-early`), and which
   check catches it?
9. Why does the test start upgrading only after the producer has written
   once?
10. Compare this with Kafka's `metadata.version` and `kafka-features.sh`:
    what does explicit finalization buy, and what does it cost?
