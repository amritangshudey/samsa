# ADR 0028: Aim the rest of the plan at problems Kafka users have today

Date: 2026-11-15. Status: accepted (asked for by the developer: "make the
future development plans of samsa in such a way that it solves the
problems of kafka"). Plan: `docs/PRODUCTION.md`, section 7.

## Context

Items 1–12a built a correct, Kafka-compatible broker and the fault-testing
mode (item 5b). The remaining items (13–17) completed Kafka features one
by one, which makes Samsa a smaller Kafka, not a better answer to what
Kafka users struggle with. The developer asked for the plan to target
those struggles instead.

The problems below are from knowledge up to mid-2026, not a user survey.
The KIP facts were checked against the KIP pages and
kafka.apache.org/protocol on 2026-11-15 (see "Review"). Each item's
design doc still verifies its wire formats before building (CLAUDE.md
rule 5).

1. **Cost in the cloud.** Each record is copied to three brokers in
   different zones; cross-zone traffic is billed, and for busy clusters
   often costs more than the machines. Fast local disks are expensive.
   Kafka's answers: fetch from the nearest replica (KIP-392, since 2.4),
   tiered storage (KIP-405, production-ready since 3.9), diskless topics
   (KIP-1150, accepted; its parts, KIP-1163 and 1164, still in design).
   Other systems (WarpStream, AutoMQ, Bufstream) write to object storage
   directly.
2. **Slow scaling.** Brokers own data on disk, so adding or replacing one
   copies partitions for hours.
3. **Hard to operate.** Hundreds of settings, JVM tuning, the move off
   ZooKeeper (KRaft only from 4.0).
4. **Rebalances stop whole consumer groups.** The new protocol (KIP-848,
   GA in 4.0) makes them incremental and server-driven; many deployed
   clients still use the classic one.
5. **No queue semantics.** No per-record acknowledgement, at most one
   consumer per partition in a group. Share groups (KIP-932, accepted)
   add them.
6. **Exactly-once is hard.** Transaction bugs (KIP-890) and, more often,
   application bugs: committing before processing, retries that write
   twice.
7. **Partitions are expensive.** Each is files and a replication stream;
   counts are chosen up front.
8. **Applications are not tested against broker faults** (section 2 of
   the plan; item 5b is the start).

## Decision

- **Keep Samsa's protocol Kafka's.** Every answer must work with
  standard clients: where Kafka already defined the protocol (KIP-848,
  KIP-932, KIP-392), implement that protocol, not a new one.
- **Finish what is open first:** the soak re-run (6), compaction (12b),
  storage faults (13). They are correctness, and the new items depend on
  them.
- **Be plain about what is catching up.** Kafka has fixed or is fixing
  problems 1, 2, 4, 5 and part of 6 itself. Implementing its answers
  makes Samsa equal there, later. Samsa adds two things Kafka lacks:
  every feature under the simulator with a mutation that must be caught,
  and `-faults` verdicts for applications (problem 8). The plan marks
  each row "same" or "more".
- **New items and their order** are in `docs/PRODUCTION.md`, section 7:
  23a and 19 first (cheap, and 23a is the unique part), topic ids (18a)
  and KIP-848 (18), configuration (24), object storage (20, 21),
  transactions (15), share groups (22), their fault testing (23b), then
  sharding (14, decided after 21), 16 and 17.
- **Object storage through the S3 API with the standard library only**
  (request signing is HMAC-SHA256, in `crypto`). Tests use an in-process
  fake with faults; a real S3-compatible server for integration tests is
  a dependency, so ask first. Nothing needs a cloud account: the project
  still runs only locally (CLAUDE.md), against fakes or a local server.
- **Tiered storage leaves the non-goals** (CLAUDE.md), as the developer's
  request covers it.

## Review of the first draft (2026-11-15)

The developer asked for a review. Checked against the KIP pages and the
protocol's API key table; found and changed:

1. **Topic ids were missing.** KIP-848 "works only with topic ids", and
   share-group state is keyed by topic id. Samsa has none (Metadata stops
   at v8). Added item 18a before 18.
2. **Share groups build on KIP-848's coordinator.** Item 22 now depends
   on 18. Its API keys are 75–78, not as first remembered.
3. **The draft called tiered storage "in progress".** It is
   production-ready since Kafka 3.9. The draft so overstated how far
   ahead these items put Samsa; the plan now says "same" or "more" per
   row.
4. **Tiering conflicts with the copy-forward cleaner (ADR 0026):** the
   cleaner rewrites live batches at the tail, which would upload them
   again. Tiered and diskless storage also overlap. Item 20 now starts
   with one design doc and prototype for both, and may conclude that
   only one mechanism is built.
5. **Fault testing came last, though it is the only row where Kafka has
   nothing.** Split: the fault kinds open since 5b come first (23a), the
   ones for new semantics after them (23b).
6. **KIP-392 detail:** a follower must answer OFFSET_NOT_AVAILABLE above
   what it knows is committed; the estimate rose, and a mutation was
   added.
7. **No gates for the new aims.** Added G10 (cost and scaling measured)
   and G11 (new protocols under the simulator).
8. **Samsa's own ceiling was understated:** every write goes through one
   Raft leader. The table now says so next to "partitions are cheap".

Not verified: the release where share groups left preview; KIP-1163
(HTTP 404 that day); the later parts of the long KIP pages.

## Alternatives

- **Keep the old plan (13–17 only).** A smaller Kafka with better
  evidence. Rejected: the developer asked for the problems to be the aim.
- **Object storage first, everything else after.** It is the largest
  saving for cloud users, but the largest change, and a local-only
  project tests it against fakes. Cheap protocol wins (18, 19, 24) come
  first so each step ships something.
- **A new protocol for queues or diskless writes.** Rejected: standard
  clients are the project's first goal; Kafka has defined both.
- **Drop sharding.** Not decided: if diskless topics leave Raft carrying
  only references, one group may go far enough. Item 21 measures it.

## Consequences

- The plan grows by about seven items, roughly 8–11 weeks of work
  (`docs/PRODUCTION.md`, section 7, has the estimates). v1.0 moves out.
- Diskless topics trade latency for cost: a write is acknowledged after
  the object store has it, typically hundreds of milliseconds. They must
  be a per-topic choice, with local topics staying the default.
- Samsa's single Raft log is a strength here: partitions are references
  in state, not files (problem 7), and a diskless batch is just a
  reference to an object instead of a log index. It is still a ceiling
  for metadata throughput, measured in item 21.
- Nothing here is claimed solved until its "must show" is met and
  measured. Where Kafka or others do better, item 17 says so.
