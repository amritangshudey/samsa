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

The problems below are from knowledge up to mid-2026, not a fresh survey.
Each item's design doc must check its problem and the Kafka proposal it
cites (KIP) against the official text (CLAUDE.md rule 5) before building.

1. **Cost in the cloud.** Each record is copied to three brokers in
   different zones; cross-zone traffic is billed, and for busy clusters
   often costs more than the machines. Fast local disks are expensive.
   Kafka's answers in progress: tiered storage (KIP-405), fetch from the
   nearest replica (KIP-392), diskless topics (KIP-1150). Other systems
   (WarpStream, AutoMQ, Bufstream) write to object storage directly.
2. **Slow scaling.** Brokers own data on disk, so adding or replacing one
   copies partitions for hours.
3. **Hard to operate.** Hundreds of settings, JVM tuning, the move off
   ZooKeeper (KRaft only from 4.0).
4. **Rebalances stop whole consumer groups.** The new protocol (KIP-848)
   makes them incremental and server-driven.
5. **No queue semantics.** No per-record acknowledgement, at most one
   consumer per partition in a group. Share groups (KIP-932) add them.
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
- **New items, in this order** (numbers continue from 17; the order is
  the plan's, not the numbers'):
  - 18 the consumer group protocol of KIP-848 (problem 4);
  - 19 fetch from the nearest replica, KIP-392 (problem 1, consumer
    traffic);
  - 20 tiered segments in object storage (problems 1 and 2);
  - 21 diskless topics, decided by ADR after a prototype (problem 1,
    producer traffic, and 7);
  - 22 share groups, KIP-932 (problem 5);
  - 23 fault testing for the new semantics, and the open fault kinds of
    item 5b (problems 6 and 8);
  - 24 one-page configuration: profiles and `samsa doctor` (problem 3).
- **Items 14–17 stay,** reordered: transactions (15) before share groups;
  sharding (14) is decided after item 21, because diskless topics put
  only metadata through Raft and may remove the reason for it; 16 and 17
  last, and 17's benchmark adds cost per GB and node-replacement time.
- **Object storage through the S3 API with the standard library only**
  (request signing is HMAC-SHA256, in `crypto`). Tests use an in-process
  fake with faults; a real S3-compatible server for integration tests is
  a dependency, so ask first. Nothing needs a cloud account: the project
  still runs only locally (CLAUDE.md), against fakes or a local server.
- **Tiered storage leaves the non-goals** (CLAUDE.md), as the developer's
  request covers it.

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

- The plan grows by about seven items, roughly 6–9 weeks of work
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
