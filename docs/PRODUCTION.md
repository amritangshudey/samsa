# Production plan

Asked for on 2026-11-11: "complete the project and make it production
ready", with every feature, including the former non-goals. This is the
order, chosen so that each item builds on the ones before it, and the
status of each. Every item follows the working method of ROADMAP section 8
(design doc with review, tests first, mutation for each safety step, PR on
green CI, report).

| # | Item | Roadmap | Why here | Status |
|---|---|---|---|---|
| 1 | Idempotent producer | A2 | the Java client's default; transactions need it | next |
| 2 | Raft hardening: PreVote, CheckQuorum, leadership transfer, graceful shutdown | B3 | a rejoining node must not disrupt a healthy cluster | |
| 3 | Operations: Prometheus metrics, admin APIs (describe cluster/configs, delete topics, delete records, create partitions), `samsa check` | B4 | what an operator touches first | |
| 4 | Black-box chaos test of the real binary | A4 | the real I/O layer, which the simulator cannot reach | |
| 5 | Simulator tools: shrinking, timeline, fault coverage, mutation score in CI | A3 | | |
| 6 | TLS and SASL (PLAIN, SCRAM), then ACLs | C5 | needed for any real deployment; standard library only | |
| 7 | Cluster membership changes | C1 | add and replace nodes | |
| 8 | Per-topic retention and key compaction | C2 | ADR with measurements first | |
| 9 | Transactions | C6 | needs 1 | |
| 10 | Storage faults beyond crashes, repair from peers | B1 | | |
| 11 | Exhaustive checking of small configurations | B2 | | |
| 12 | Compatibility matrix with more clients | B5 | needs new test-only dependencies: ask first | |

Still out: tiered storage, Kafka Streams/Connect guarantees.

Blockers are written to `docs/QUESTIONS.md`.
