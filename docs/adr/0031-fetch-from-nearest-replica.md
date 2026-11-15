# ADR 0031: Fetch from the nearest replica

Date: 2026-11-15. Status: accepted. Design: `docs/design/c19-follower-fetch.md`.

## Context

Consumers in a multi-zone cluster read from leaders, and cross-zone traffic
is billed (ADR 0028, problem 1). Kafka's answer is KIP-392: the consumer
names its rack, the leader names a replica in it, the consumer reads there.
Every Samsa node already applies every committed entry.

## Decision

- Implement KIP-392 on Fetch v11 (already offered): `RackId` in, `PreferredReadReplica`
  out; OFFSET_NOT_AVAILABLE from a follower that has not applied an offset.
- Racks are in `-peers` (`id=client/peer@rack`) and reach Metadata and
  DescribeCluster. They are not in the log.
- A follower serves only fetches of v11 or later, from applied state, while it
  hears its leader, is not recovering and is not receiving a snapshot.
- The leader names the lowest-numbered caught-up member in the consumer's
  rack, never itself, and sends no records with the name.
- Old clients are unchanged: below v11 a follower still refuses.

## Alternatives

- Racks in the log or the member info (a node added later would have one):
  more machinery for the less common case.
- Followers that proxy to the leader: reintroduces the cross-zone hop it is
  meant to save.

## Consequences

- Reads from a follower can be stale by the time it takes to apply: a deleted
  topic or a moved log start reaches it later.
- A node added with `samsa node add` has no rack and is never named.
- The consumer sees one retry when it lands on a replica that is behind.
- Bytes between racks can be measured (`samsa_follower_fetch_bytes_total`); a
  bill cannot.
