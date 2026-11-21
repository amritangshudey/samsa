# ADR 0042: Compacted topics stay in the log under object storage

**Status:** Accepted  
**Date:** 2026-11-21  
**Item:** 20 (object storage), after `docs/BUGS.md` S18; amends ADR 0037

## Context

Compaction rewrites a batch in place. It reads the batch, removes the
records that a later record of the same key replaced, and proposes
`cmdCompact` with the log index where it read the batch. The apply changes
the batch only if it is still at that index. An object is immutable, and
ADR 0037 gives a tiered or diskless batch log index 0. So a `cmdCompact`
of a tiered batch matched it, and left it both in its object and at a log
index. That batch broke every snapshot after the trim passed it (S18).

Item 20 tiered every topic. Its design never considered compaction, and the
simulator never drew the two together. Kafka's tiered storage leaves them
apart. KIP-405 says "It does not support compact topics with tiered
storage". It also says that a tiered topic "can not change its retention
policy from delete to compact".

## Decision

1. With object storage, the tierer skips a topic whose `cleanup.policy`
   includes `compact`. Its batches stay in the log.
2. The cleaner copies those batches forward as it does without object
   storage, by the same rule, but with only them counted as live:
   - It copies them only once the log exceeds `RetentionSlack` times their
     live bytes, plus a segment.
   - A head segment that holds none of them is tiered and trimmed at once,
     as before.
3. Compaction never rewrites a batch that is in an object. Such batches
   belong to a topic that was tiered or diskless before it became
   compacted. They stay as they are, and compaction works on the rest of
   the partition.

## Alternatives

- **Compact tiered batches by turning them back into log batches.** This
  would let compacted topics live in object storage, as their data grows.
  But `cmdCompact` names a batch by its log index, which is 0 for every
  tiered batch, so the command would need a new format that names the
  object. The key map would also have to read objects asynchronously,
  and every compacted batch would be uploaded again. It is a feature to
  design with its own simulator coverage, not part of a bug fix.
- **Relocate compacted batches at once in every round, without the rule.**
  Then compacted live data larger than a segment would be copied around
  for ever: each copy fills the segment being written, which is sealed and
  copied again. The rule exists to prevent that (see `clean`).
- **Refuse compaction for a diskless topic, and refuse switching a tiered
  topic to `compact`, as KIP-405 does.** That would be clearer for users.
  But it needs checks across configs at create and at alter time, and it
  is not needed for correctness once rule 3 holds. It is left as a
  follow-up: until then, such data is not compacted where it lies in
  objects.

## Consequences

- ADR 0037's consequence that local disk is bounded by the active segment
  no longer holds when there are compacted topics. Local disk then holds
  their live data, up to `RetentionSlack` times it plus a segment, as
  without object storage. While a head segment waits under that rule,
  segments after it are not tiered either. That is bounded by the same
  rule.
- The simulator now draws tiering together with compaction, and with disk
  rot. T1 (tiering progresses) passes for a head segment that holds a
  compacted topic's batches. The mutation "tier a compacted topic and
  rewrite its tiered batches" checks that the pair stays drawn.
- A diskless compacted topic is accepted but never compacted, and nothing
  tells its user so. The same holds for the tiered part of a topic made
  `compact` after it was tiered. The key map stops at the first such batch
  that is not in the cache, so compaction of that partition waits for
  reads to bring it in.
