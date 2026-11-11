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

What is **not** promised even at v1.0, and is said so in the README: scale
beyond what one Raft group gives until sharding (item 14) lands; Kafka
Streams/Connect guarantees; tiered storage.

## 2. What makes it different

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

## 3. Order and milestones

Each item follows ROADMAP section 8: design doc with a recorded second
review, tests first, a mutation switch for each safety step, PR on green
CI, report. Each milestone is a tagged release.

### v0.2 — correct with default clients

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 1 | Idempotent producer | A2 | invariant "no record twice in a partition" under retries, leader changes and resets; Java client with defaults; mutation "skip the sequence check" caught |
| 2 | Raft hardening: PreVote, CheckQuorum, leadership transfer, graceful shutdown | B3 | a returning partitioned node does not depose a healthy leader (scenario fails without PreVote); SIGTERM hands over leadership, clients see no error |
| 3 | Operations: Prometheus metrics, health/readiness, admin APIs (DescribeCluster, DescribeConfigs, DeleteTopics, DeleteRecords, CreatePartitions), `samsa check`, config file | B4 | the standard Kafka CLI tools run against it; `check` finds planted corruption |

### v0.3 — proven on real processes

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 4 | Black-box chaos test of the real binary | A4 | shared checkers pass on real histories; a deliberately broken build is caught |
| 5 | Simulator tools: seed shrinking, HTML timeline, fault coverage, mutation score in CI | A3 | a seed from BUGS.md shrinks to a readable schedule |
| 6 | Soak and resource bounds: 24 h run, memory and disk tracked; start-up re-measured with retention | G5 | flat memory, bounded disk, numbers in the repo |

### v0.4 — secure

| # | Item | Roadmap | Must show |
|---|---|---|---|
| 7 | TLS (clients and peers, mutual between peers), SASL PLAIN and SCRAM | C5 | real clients over TLS+SCRAM; wrong credentials and plaintext refused; standard library only |
| 8 | ACLs (replicated commands; the Kafka ACL APIs) | — | every API has an allowed and a denied test; default-deny mode |

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

## 4. Known blockers ahead

- Item 16, and the standard Kafka CLI tools in item 3, need tools or
  modules not yet installed (rule: ask before adding any dependency).
- Item 6 needs a machine left running for 24 hours.
- Item 17's comparison needs Apache Kafka installed locally.
- Item 14 may be judged too large; that is decided in its ADR, with the
  developer.

Blockers are written to `docs/QUESTIONS.md` as they arrive.

## 5. Status

| Item | Status |
|---|---|
| 1 Idempotent producer | design in progress |
| 2–17 | not started |
