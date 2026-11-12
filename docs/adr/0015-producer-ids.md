# ADR 0015: producer ids come from per-node blocks on disk

Date: 2026-11-11. Status: accepted. Design: `docs/design/a2-idempotent-producer.md`.

## Context

The idempotent producer needs an id that no other producer ever has: the
broker tells a retry from new data by (producer id, epoch, sequence), so
two producers with one id would have their batches mistaken for each
other's duplicates and silently dropped.

Two facts constrain where ids can come from, both read in the Kafka
source on 2026-11-11:

- The Java client sends `InitProducerId` for a non-transactional producer
  to **any** node (`Sender.java`, `leastLoadedNode`), and handles
  `NOT_COORDINATOR` by looking up a transaction coordinator, which makes
  no sense for it. So a node cannot answer "ask the leader".
- Samsa allows acknowledging before fsync on a single node (`-sync
  interval|never`, ADR 0012). A power loss may then take back the end of
  the log. Anything derived from the log can repeat.

## Decision

Each node allocates ids itself:

    id = node id (16 bits) | block (28 bits) | index in the block (19 bits)

The block number is kept in the file `producer-block` in the data
directory, replaced atomically and synced **regardless of the sync
policy**. A node reserves the next block at every start, before it answers
any request, and again when a block's 524 288 ids are used up. The node id
must be below 65 536.

Ids are unique by construction: the node id separates nodes, the block
separates the lives of one node. Nothing about them is replicated.

## Alternatives

- **A counter in the replicated state machine**, incremented by a command
  (the roadmap's outline). Only the leader could answer, and after a power
  loss under `interval` or `never` the counter can go back and hand out an
  id twice.
- **Random 63-bit ids.** No file and three lines of code, and a collision
  is very unlikely. Rejected because uniqueness would be a probability the
  simulator cannot check, underneath a guarantee called "exactly once".
- **(Raft term, counter)**, unique per leader with no file. Followers
  cannot answer.
- **A boot counter inside Raft's hard-state file.** Changes a released
  file format for a purpose that is not Raft's.

## Consequences

- One more small durable file, written once per start. It is not a
  command, so its crash safety is checked on its own: the simulator's
  power losses and failed syncs hit it at every start, and invariant I9
  (no id handed out twice) watches the result.
- A start that cannot write the file fails, as a start that cannot write
  the hard state does.
- 2^28 blocks per node: a node restarted every second lasts eight years.
  At the end it refuses to start instead of repeating an id.
- Transactions (plan item 15) will map a transactional id to a producer id
  in the replicated state; the id itself can still come from the leader's
  own block.

## API versions

InitProducerId (key 22) is supported in versions 0 to 4, flexible from 2.
The three test clients negotiate version 4: the Java client 4.4.0,
franz-go 1.22.3 and librdkafka 2.16.0, as logged by the integration tests.
Version 5 and later are not implemented.
