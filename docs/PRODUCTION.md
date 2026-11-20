# Production plan

Asked for on 2026-11-11: "complete the project and make it production
ready", with every feature including the former non-goals, "better and
unique and best in the market". This document says what that means in
terms that can be checked, in what order it is built, and what each step
must show before it counts.

## 1. What "production ready" means here

A claim is worth what its evidence is worth, so the goal is a list of
gates. v1.0 is tagged when every gate is met and linked to its test.

| Gate | Evidence required |
|---|---|
| G1 Default clients work | Java, librdkafka and franz-go with **default settings** (idempotence on) produce and consume; no flag needed |
| G2 No acknowledged write lost, no duplicate | simulator invariants under crashes, partitions and disk faults; chaos test on real processes; both mutation suites green |
| G3 Survives its own faults | corrupt synced data on a minority is repaired, not served or truncated (B1) |
| G4 Operable | metrics, health and readiness, graceful shutdown, rolling restart without errors visible to clients, offline `samsa check`, documented runbook |
| G5 Bounded resources | disk bounded by retention and compaction; memory bounded under a soak test of 24 h with a stated load; start-up bounded by snapshot interval, measured |
| G6 Secure by configuration | TLS for clients and peers, SASL (PLAIN, SCRAM-SHA-256/512), ACLs; peers authenticate each other; a test for every refusal |
| G7 Changeable while running | add, remove and replace nodes without downtime; rolling upgrade between two releases tested in CI; on-disk formats versioned |
| G8 Honest numbers | benchmark method, hardware and raw results in the repo; a comparison with Apache Kafka on the same machine, same method |
| G9 Standard tools work | `kafka-topics`, `kafka-consumer-groups`, `kafka-configs`, kcat and one web UI run their common commands; compatibility table generated from test results |
| G10, G11 | cost and scaling measured; new protocols under the simulator (section 7) |

What is **not** promised even at v1.0, and is said so in the README: scale
beyond what one Raft group gives until sharding (item 14) or diskless
topics (item 21) land; Kafka Streams/Connect guarantees.

## 2. The real problem it solves

**Teams cannot test how their own applications behave when Kafka
misbehaves.** The bugs that lose or duplicate messages in production are
in application code: a consumer that commits before it finishes
processing, a producer retry that writes twice, a rebalance in the middle
of a batch. They show only under faults, and today's tools do not make
those faults at the protocol level, on demand, and repeatably:
Testcontainers and Redpanda give a healthy broker; network chaos tools
(Toxiproxy, Litmus) cut connections but cannot say "commit this write and
lose its acknowledgement"; Kafka's own fault injector and the
deterministic simulators at WarpStream and Aiven test the broker, not the
user's application. (Searched 2026-11-11; no tool found that does this.
That is a search result, not proof that none exists.)

Samsa already has both halves of the answer inside it: a fault injector
driven by a seed, and a checker that knows the ground truth of every
record. The product is to turn them outwards:

- **`samsa -faults seed=N,...`**: a real broker for a test suite or CI job
  that injects Kafka-level faults from a seed: an acknowledgement lost
  after the write committed, a leader change mid-request, a rebalance
  forced mid-batch, a session expiry, slow or empty fetches, offsets cut
  by retention. The same seed gives the same faults.
- **A verdict on the application**, from the broker's own knowledge: for
  a declared pipeline (input topic → the application → output topic), was
  every input record processed exactly once, at least once, or lost; did a
  group commit past records it was never sent; did it reprocess after a
  rebalance. Printed as a report and as an exit code for CI.
- One static binary that starts in milliseconds, so it runs inside any
  test without Docker.

So the pitch is one sentence: **the Kafka you run your tests against,
which breaks on purpose, the same way every time, and tells you whether
your application lost or duplicated anything.** The production-grade
broker below is what makes that verdict trustworthy, and is the second
use: a small, single-binary Kafka for places a JVM cluster does not fit.

## 3. What makes it different

Not features: Kafka has more. The difference is evidence, and it is
already the project's core:

- every release ships with its simulator seed count, mutation score and
  fault-coverage report, so "tested" is a number;
- any bug reproduces from a seed, shrinks to a readable schedule, and
  renders as a timeline (item 5);
- the same history checker judges the simulator and real processes
  (item 4);
- small configurations are checked exhaustively, not sampled (item 11);
- one static binary, no JVM, no ZooKeeper or separate controller.

"Best in the market" stays a goal, never a claim (CLAUDE.md). Where
Samsa loses to Kafka or Redpanda, G8 makes the repo say so.

## 4. Order and milestones

Each item follows ROADMAP section 8: design doc with a recorded second
review, tests first, a mutation switch for each safety step, PR on green
CI, report. Each milestone is a tagged release.

### v0.2 — correct with default clients

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 1 | Idempotent producer | A2 | invariant "no record twice in a partition" under retries, leader changes and resets; Java client with defaults; mutation "skip the sequence check" caught |
| 2 | Raft hardening: PreVote, CheckQuorum, leadership transfer, graceful shutdown | B3 | a returning partitioned node does not depose a healthy leader (scenario fails without PreVote); SIGTERM hands over leadership, clients see no error |
| 3 | Operations: Prometheus metrics, health/readiness, admin APIs (DescribeCluster, DescribeConfigs, DeleteTopics, DeleteRecords, CreatePartitions), `samsa check`, config file | B4 | the standard Kafka CLI tools run against it; `check` finds planted corruption |

### v0.3 — proven on real processes, and the feature nobody else has

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 4 | Black-box chaos test of the real binary | A4 | shared checkers pass on real histories; a deliberately broken build is caught |
| 5 | Simulator tools: seed shrinking, HTML timeline, fault coverage, mutation score in CI | A3 | a seed from BUGS.md shrinks to a readable schedule; the suite reports how many seeds catch each deliberate bug, not only the first |
| 5b | **Application fault testing**: `-faults` mode and the pipeline verdict (section 2) | new | a sample consumer with a commit-before-process bug is reported as losing records, with the seed; the fixed one passes; a guide for JUnit, Go and Python tests |
| 6 | Soak and resource bounds: 24 h run, memory and disk tracked; start-up re-measured with retention | G5 | flat memory, bounded disk, numbers in the repo |

### v0.4 — secure

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 7 | TLS (clients and peers, mutual between peers), SASL PLAIN and SCRAM | C5 | real clients over TLS+SCRAM; wrong credentials and plaintext refused; standard library only |
| 8 | ACLs (replicated commands; the Kafka ACL APIs) | — | every API has an allowed and a denied test; default-deny mode |
| 8b | Replicated SCRAM credentials (AlterUserScramCredentials, DescribeUserScramCredentials) and forwarding admin writes from followers to the leader | — | a credential change takes effect on every node without a restart; `kafka-acls.sh` and `kafka-configs.sh` work against any node of a cluster |

### v0.5 — changeable

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 9 | Cluster membership changes (single-server), node replacement | C1 | simulator adds and removes nodes under faults; mutation "two changes at once" caught |
| 10 | Rolling upgrade: format versions, upgrade test between releases in CI | G7 | N-1 to N with clients running, no errors |

### v0.6 — complete log semantics

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 11 | Exhaustive checking of small configurations | B2 | search completes in its bound; a mutation found with a shortest counterexample |
| 12 | Per-topic retention (time and size) and key compaction | C2 | ADR with measurements first; invariant "the latest value of every key survives" |
| 13 | Storage faults beyond crashes, scrub, repair from peers | B1 | no committed data lost with corruption on a minority; naive truncation mutation caught |

### v1.0 — everything

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 14 | Sharding across Raft groups | C3 | throughput grows with nodes, measured; a second project in size, decided by ADR when reached |
| 15 | Transactions | C6 | read-committed consumers never see aborted data, under faults; Java transactional producer |
| 16 | Compatibility matrix: more clients and versions, table generated from results | B5 | **needs test-only dependencies: ask first** |
| 17 | Kafka comparison benchmark, packaging (container image, systemd unit), runbook, security review | G4, G8 | published numbers; a stranger can deploy from the docs |

## 5. Known blockers ahead

- Item 16, and the standard Kafka CLI tools in item 3, need tools or
  modules not yet installed (rule: ask before adding any dependency).
- Item 6 needs a machine left running for 24 hours.
- Item 17's comparison needs Apache Kafka installed locally.
- Item 14 may be judged too large; that is decided in its ADR, with the
  developer, after item 21.
- Items 18, 20 and 22 need test-only dependencies (section 7,
  "Blockers to settle early").

Blockers are written to `docs/QUESTIONS.md` as they arrive.

## 6. Status

| Item | Status |
|---|---|
| 1 Idempotent producer | part (a) done (`docs/reports/a2-idempotent-part-a.md`). Part (b), expiry: done (`docs/reports/a2-idempotent-part-b.md`) |
| 2 Raft hardening | done (`docs/reports/b3-raft-hardening.md`) |
| 3 Operations | part (a), admin APIs: done (`docs/reports/b4a-admin-apis.md`). Part (b), metrics, health, `samsa check`, config file: done (`docs/reports/b4b-operations.md`) |
| 4 Black-box chaos test | done (`docs/reports/a4-chaos.md`) |
| 5 Simulator tools | done (`docs/reports/a3-sim-tools.md`). Found: the simulator never lets an expired idempotent producer come back (expiry 15 s, runs 10 s); open, covered meanwhile by `TestJavaProducerAfterExpiry` |
| 5b Application fault testing | done (`docs/reports/a5b-app-faults.md`, guide `docs/guides/testing-applications.md`). Three of the section 2 fault kinds (lost ack, not leader, forced rebalance); the other four (session expiry, empty and slow fetches, offsets out of range) came with item 23a (`docs/reports/a23a-fault-kinds.md`). Not done: offsets cut by an actual retention in the verdict |
| 6 Soak | done (`docs/reports/g5-soak.md`, `docs/soak/run1/`, `docs/soak/run2/`): harness (`make soak`); run 1 found Raft memory growing 8 bytes per entry forever (BUGS.md L1), fixed in PR 28; run 2 on the fixed build passed 24 h (43.2M records, 0 errors, 0 gaps, RSS 18–32 MiB, data dir ≤ 256 MiB, restart ready in 63 ms) |
| 7 TLS and SASL | done (`docs/reports/c5-tls-sasl.md`). Credentials from a file (restart to change); replicated credentials (API 51) moved to item 8b |
| 8 ACLs | done (`docs/reports/c8-acls.md`). certificate principals not supported; forwarding added in 8b |
| 8b Credentials and forwarding | done (`docs/reports/c8b-credentials-forwarding.md`). Found and fixed BUGS.md S8 in Raft leadership transfer. Forwarded ACL changes are authorized on the follower's applied state |
| 9 Membership changes | part (a), Raft, broker APIs and simulator: done (`docs/reports/c1-membership-part-a.md`). "Two changes at once" caught by the random Raft schedules, the erratum by a replayed counterexample. Found and fixed BUGS.md S9. Part (b), the real binary: done (`docs/reports/c1-membership-part-b.md`): `-join`, peer addresses in the configuration, `samsa node add/remove`, three to four to three processes under a producer. Not built: DescribeQuorum, directory ids (ids must not be reused with a new disk) |
| 10 Rolling upgrade | done (`docs/reports/g7-rolling-upgrade.md`): a cluster format level raised only once every voter supports it, and snapshots written in the oldest version that holds them. `TestRollingUpgrade` upgrades three processes from v0.2.0 under a producer, in CI. With the level raised early (`-unsafe raise-level-early`), the old nodes stop on an unknown command |
| 11 Exhaustive checking | done (`docs/reports/b2-exhaustive.md`, ADR 0025): three Raft nodes, breadth-first, every schedule up to depth 10 (`go test`) and 12 (CI, with one power loss or loss per node), both configurations; no violation. `AckBeforeSync` found with its shortest counterexample, 11 events. A one-node reduction and a vacuous bound found and removed (BUGS.md M11). Bounds are short; no sound reduction yet |
| 12 Retention and compaction | part (a), per-topic retention: done (`docs/reports/c2-retention-part-a.md`, ADR 0026, ADR 0027). `retention.ms` and `retention.bytes` per topic, set with CreateTopics or `kafka-configs.sh --alter`; live batches copied forward out of the head segment; R9 in the simulator catches `UnsafeTrimLive`. Part (b), key compaction: done (`docs/reports/c2-retention-part-b.md`, ADR 0029, format level 3); `cleanup.policy=compact` keeps the latest record of every key, C1 to C5 in the simulator, five mutations caught |
| 13 Storage faults | done (`docs/reports/b1-storage-faults.md`, ADR 0030): scrub, `damaged` and `recovering` markers, repair from the other nodes, S7 in the simulator, three mutations caught. Limits: the active segment, the small files, a cluster of one |
| 19 Fetch from the nearest replica | done (`docs/reports/c19-follower-fetch.md`, ADR 0031): `-peers id=...@rack`, KIP-392 on Fetch v11, followers serve committed data, OFFSET_NOT_AVAILABLE; real franz-go consumer reads from its own rack's node; mutation caught in 21 of 200 seeds. Racks of nodes added later are not known |
| 18a Topic IDs and protocol versions to Kafka 3.1 | done (`docs/reports/c18a-topic-ids.md`, ADR 0032, format level 4, snapshot v9): 16-byte UUID per topic in state; ApiVersions v5, Metadata v9–12, Fetch v12–13, CreateTopics v5–7, DeleteTopics v6 by id; recreated topic gets a fresh id and old id answers UNKNOWN_TOPIC_ID; mutation `UnsafeReuseTopicID` caught in 66 of 200 seeds; franz-go, Java 4.4.0 and librdkafka negotiate the new versions |
| 23a More fault kinds | done (`docs/reports/a23a-fault-kinds.md`): `expire`, `emptyfetch`, `slowfetch`, `outofrange`, opt in with `kinds=` or `kinds=all`; a sample application is caught under `expire` and passes under all |
| 24 Profiles and doctor | done (`docs/guides/configuration.md`): `-profile dev|test|prod`, `samsa doctor` with sources and risks. A prod node `doctor` finds nothing risky in needs eleven settings, not five |
| 14 Sharding across Raft groups | decided by ADR 0039 (`docs/adr/0039-sharding-decision.md`): deferred for v1.0 after items 19–21 (follower fetch + tiered storage + diskless topics address read, disk, and replication bottlenecks without multi-Raft complexity) |
| 15 Transactions | done (`docs/reports/c15-transactions.md`, ADR 0035, format level 7, snapshot v12): InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn, WriteTxnMarkers, KIP-890 epoch bump, `read_committed` LSO & aborted-txn filtering; Java transactional producer & franz-go tests; simulator invariants T1–T5 & mutations caught |
| 16 Compatibility matrix | done (`docs/reports/b5-client-matrix.md`, `docs/compatibility.md`, ADR 0041, `make matrix`): generated from live tests across `twmb/franz-go`, `IBM/sarama`, `segmentio/kafka-go`, Apache Kafka Java 4.4.0, `kcat`/`librdkafka`, and Kafka 4.4.0 CLI tools |
| 17 Packaging, runbook, security review, Kafka comparison | done: container image (`make image`, 18 MB, `packaging/Dockerfile`), systemd unit and sample config (`packaging/`), runbook (`docs/guides/runbook.md`), security review (`docs/reports/g8-security-review.md`), and Apache Kafka 4.4.0 KRaft comparison benchmark (`docs/reports/g8-kafka-comparison.md`, ADR 0040, `make bench-kafka`) |
| 18 KIP-848 consumer groups | done (`docs/reports/c18-kip848.md`, ADR 0033, format level 5, snapshot v10): ConsumerGroupHeartbeat (68), ConsumerGroupDescribe (69), DescribeTopicPartitions (75), range & uniform server-side assignors; Java 4.4.0 `group.protocol=consumer` test; simulator invariant & mutation caught |
| 20 Object storage (tiered) | done (`docs/reports/c20-object-storage.md`, ADR 0037, format level 8, snapshot v13): pluggable `ObjectStore` (`file://`, zero-dep S3 SigV4, simulator fake), `cmdTier`, remote batch cache, two-pass GC; 3,000-seed simulator under faulty object store & mutation caught |
| 21 Diskless topics | done (`docs/reports/c21-diskless-topics.md`, ADR 0038, format level 9): per-topic `diskless.enabled=true`, `cmdAppendRef` (~97 B in Raft), leaderless produce forwarding from any broker, 97.7%–99.99% local Raft disk reduction |
| 22 Share groups (KIP-932) | done (`docs/reports/c22-share-groups.md`, ADR 0034, format level 6, snapshot v11): ShareGroupHeartbeat (76), ShareGroupDescribe (77), ShareFetch (78), ShareAcknowledge (79), record locks, acquisition timeout, redelivery; Java 4.4.0 share consumer test; simulator invariant & mutation caught |
| 23b Fault testing (v2) | done (`docs/reports/c23b-app-faults.md`, ADR 0036): `-faults` verdicts and fault injection for transactions, KIP-848 consumer groups, and KIP-932 share groups |

## 7. Kafka's problems, and the plan's answer to each

Asked for on 2026-11-15: "make the future development plans of samsa in
such a way that it solves the problems of kafka". ADR 0028 records the
decision, its reasons, and the review of the first draft (checked
against the KIP pages and kafka.apache.org/protocol on 2026-11-15).

**What the review changed, in short.** Kafka is fixing most of these
problems itself: tiered storage is production-ready since 3.9, the new
group protocol since 4.0, fetch from the nearest replica since 2.4;
share groups and diskless topics are accepted. So implementing them is
catching up, not overtaking. What Samsa can add that Kafka does not
have is the same as in section 3: each of these features run under the
simulator with a mutation that must be caught, and `-faults` verdicts
for applications that use them. The plan below says which rows are
"same as Kafka" and which are "more than Kafka".

| Kafka problem | Kafka's own answer | Samsa's answer | Compared with Kafka | Item |
|---|---|---|---|---|
| Cross-zone traffic and disks cost more than the machines | KIP-392 (done), tiered storage (done), diskless topics (accepted, being designed) | the same three steps; one design doc for object storage decides how many mechanisms are built | same, later; simpler if one mechanism serves both | 19, 20, 21 |
| Adding or replacing a broker copies data for hours | tiered storage shortens it | a new node copies the snapshot and the local tail only | same; measured | 20 |
| Hundreds of settings, JVM, ZooKeeper migration | KRaft only since 4.0 | one binary, Raft built in; profiles and `samsa doctor` | more: already simpler | 24 |
| A rebalance stops the whole consumer group | KIP-848 (done) | the same protocol, under the simulator | same protocol; more evidence | 18a, 18 |
| No queues | share groups, KIP-932 (accepted) | the same protocol, state in the one Raft log, under the simulator | same protocol; more evidence | 22 |
| Applications lose or duplicate records; they are never tested against broker faults | none | `-faults` and the verdict, for every fault kind and every consumer type | **more: Kafka has nothing here** | 23a, 23b |
| Exactly-once is hard in the broker too | KIP-890 (approved) | transactions built to the protocol current clients negotiate, under the simulator | same protocol; more evidence | 15 |
| Partitions are expensive | none | a partition is a reference in state, not files. **But every write goes through one leader** until item 21 or 14 | more for partition count, less for write throughput; both measured | 17 |

### New gates (added to section 1)

| Gate | Evidence required |
|---|---|
| G10 Cost and scaling are measured | bytes between racks per byte produced and consumed; local disk per GB retained; time to replace a node; each before and after, and against Kafka in item 17. Measured locally: object-store latency and prices are not real ones, and the report says so |
| G11 New protocols are held to the old standard | every new group or storage mechanism has a simulator invariant, a mutation that is caught, and a real client test |

### New items

| # | Item | Must show | Estimate |
|---|---|---|---|
| 23a | Fault testing, the open kinds from 5b: session expiry, slow and empty fetches, offsets cut by retention (possible since 12a) | a sample application with each bug is caught with its seed; the fixed one passes; guide updated | 2–3 days |
| 19 | Fetch from the nearest replica (KIP-392): `-rack` per node; the leader names a preferred read replica (Fetch v11, already offered); a follower serves only what it knows is committed and answers OFFSET_NOT_AVAILABLE above that | a consumer with `client.rack` reads from its own rack's node; simulator: no uncommitted record ever served by a follower; mutation "follower serves its whole log" caught; bytes between racks measured. Also takes read load off the one leader | 2–3 days |
| 18a | Topic ids: a UUID per topic in state; Metadata v10–12, Fetch v12–13, CreateTopics v5–7, DeleteTopics by id. **Needed by 18 and 22** ("the new consumer rebalance protocol works only with topic ids") | a topic deleted and recreated gets a new id and a client with the old id is refused; current Java and franz-go clients negotiate the new versions; snapshot and format level raised | 3–4 days |
| 18 | KIP-848 consumer groups: ConsumerGroupHeartbeat (68), ConsumerGroupDescribe (69), server-side range and uniform assignors, next to the classic protocol | a Java consumer with `group.protocol=consumer` works; a member joining does not pause the others' fetching (measured against the classic protocol); invariant "no partition owned by two members at once"; mutation "assign before the old owner revoked" caught | 5–7 days |
| 24 | Configuration: `-profile dev|test|prod`; `samsa doctor` lists every setting in use, its source and its risk | every flag documented with its default and when to change it; a prod node needs at most five settings | 1–2 days |
| 20 | Object storage, design first: **one design doc and prototype for tiered and diskless together.** Open questions it must answer: (1) the copy-forward cleaner (12a) rewrites live batches, which would re-upload them: retention for stored objects must delete or rewrite objects, not copy through Raft; (2) a lagging follower or new node must read old entries from objects, so snapshots must reference them; (3) is tiering worth building if diskless references do the same job. Then the build: closed data in object storage (S3 API, standard library only), read back on fetch | simulator with a faulty object store (lost, slow, duplicated, torn writes): no committed record lost or changed; node replacement time and local disk measured before and after; mutation "delete locally before the upload is confirmed" caught | 1 week design and prototype, 1–2 weeks build |
| 21 | Diskless topics, if item 20's measurements hold: a per-topic choice; brokers write batches to object storage and Raft orders only references. Kafka's design (KIP-1150, 1163, 1164) adds a separate coordinator for the ordering; here the Raft log already is one | latency, metadata throughput and bytes between racks measured and stated plainly; R-invariants unchanged in the simulator; any broker accepts a produce for a diskless topic | 1–3 weeks |
| 15 | Transactions (was in the plan): built to the versions current clients negotiate, including KIP-890's epoch bump | as before: read-committed consumers never see aborted data, under faults | 4–7 days |
| 22 | Share groups (KIP-932), after 18: ShareGroupHeartbeat (75), ShareGroupDescribe (76), ShareFetch (77), ShareAcknowledge (78); record locks, timeout, redelivery, delivery count. Share state is commands in the Raft log, so Kafka's inter-broker share-state APIs are not needed | Java share consumer works; invariant "every record is acknowledged once or delivered again"; mutation "drop a lock without redelivery" caught | 6–9 days |
| 23b | Fault testing for transactions, KIP-848 groups and share groups: verdicts for each | a sample application with each class of bug is caught with its seed | 2–3 days |

### Milestones from here

| Release | Items, in order | Why this order |
|---|---|---|
| v0.6 | 6 re-soak, 12b compaction, 13 storage faults | open work; correctness that the rest stands on |
| v0.7 — modern clients | 23a, 19, 18a, 18, 24 | 23a is the one thing Kafka lacks, and cheap; 19 is small and needs nothing new; topic ids unlock the rest |
| v0.8 — object storage | 20, 21 | the largest saving and the largest risk; design and measure before building |
| v0.9 — semantics | 15, 22, 23b | queues need 18; fault testing follows what it tests |
| v1.0 | 14 sharding (decided by ADR after 21), 16, 17 | 17's benchmark covers G10 against Kafka |

### Blockers to settle early

- **A Kafka Java client of 4.1 or later** for the integration tests of
  18 and 22 (test-only dependency: ask first). The release where share
  groups left preview was not verified; item 22's design doc checks it.
- **An S3-compatible server** for item 20's integration tests (ask
  first). Unit and simulator tests use an in-process fake.

### What is weak in this plan

- Everything is measured on one laptop. "Cross-zone cost" is bytes
  between nodes with different `-rack`, not a bill. Object-store latency
  against a local server is far below a real one.
- The problem list is from knowledge up to mid-2026 plus the KIP pages,
  not from a user survey.
- The new items add roughly 8–11 weeks to the 4–6 left before. The
  estimates are guesses from items 1–12a, and miss by the bugs the
  simulator finds.
- KIP-1163 (the diskless write path) could not be read on 2026-11-15
  (HTTP 404); item 20's design doc must read it.
