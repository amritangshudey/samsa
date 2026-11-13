# ADR 0024: A cluster format level gates new formats

Date: 2026-11-14. Status: accepted. Design: `docs/design/g7-rolling-upgrade.md`.

## Context

A rolling upgrade runs old and new builds in one Raft group. Since
v0.2.0, the new build can write:

- commands 11 to 15;
- state snapshot versions 5 to 7;
- Raft snapshot version 2;
- configuration entries.

v0.2.0 cannot read any of these. It stops on an unknown command or a
snapshot it cannot decode. Every node snapshots periodically, so the
snapshot problem arose without any new feature being used.

## Decision

- **Snapshots are written in the oldest version that holds their
  content:**
  - state version 4, unless there are ACLs, credentials or a level;
  - Raft version 1, unless there is a configuration.

  Later fields are always appended, so the older version is a prefix.
- **A cluster format level is replicated state,** set by `cmdSetLevel`.
  This build's `formatLevel` is 1. Nodes report theirs to the leader every
  second until the cluster reaches it. A build that never reports, such
  as v0.2.0, counts as 0.
- **The leader raises the level only when every voter in force has
  reported it within 3 s.** A learner must report the cluster's level
  before AddRaftVoter makes it a voter.
- **Below level 1, writes of a level-1 format are refused with
  UNSUPPORTED_VERSION:**
  - CreateAcls and DeleteAcls;
  - AlterUserScramCredentials;
  - forwarding;
  - AddRaftVoter and RemoveRaftVoter.
- **A build that meets a level above its own stops** with "run a newer
  release". It does not misread. The level never goes down.

## Alternatives

- **Kafka's explicit finalization** (`kafka-features.sh`,
  UpdateFeatures). It leaves a window to roll back before using the new
  format, at the cost of an API and an operator step. This can be added
  later by gating `cmdSetLevel` on a request.
- **Document "use no new feature mid-upgrade".** Snapshots break the
  cluster anyway, and the rule cannot be checked.
- **Per-node negotiation.** The log outlives the nodes that wrote it, so
  the decision must be in the log.

## Consequences

- **Every change to a replicated format must raise `formatLevel`** and
  gate what writes it. A review item for future work: the decode tests
  already exercise each version.
- **A fresh cluster runs at level 0 for up to about a second after its
  first election.** Gated requests made in that window are refused with
  UNSUPPORTED_VERSION, a code Kafka clients do not retry.
- **Downgrading below the cluster's level is not possible.**
