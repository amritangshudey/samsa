# A2 part (a) report: idempotent producer

Branch `a2-idempotent`, 2026-11-12. Production plan item 1, part (a).
Design: `docs/design/a2-idempotent-producer.md`; ids: ADR 0015.

## What was built

- **InitProducerId** (key 22, versions 0–4). Any node answers, leader or
  not. Ids are `node id | block | index`, where the block number is synced
  to `producer-block` before any of its ids is used, whatever the sync
  policy. A transactional producer is refused with `INVALID_REQUEST` until
  transactions exist.
- **Sequence state** per partition and producer: the epoch and the last
  five batches (first and last sequence, base offset). It is checked and
  updated at apply, so it is replicated, in the state hash and in
  snapshots (version 3). A retry gets the first copy's offset and appends
  nothing. A gap or an older epoch is refused with 45 or 47. A producer
  unknown to the partition must start at 0.
- **Simulator**: an idempotent producer (`internal/sim/idempotent.go`).
  It keeps up to three batches in flight, re-sends unanswered batches
  unchanged, and after two seconds gives up and moves the partition to a
  new epoch. It sometimes starts a new session to get a new id. New
  checks: I8 (never refused a sequence or epoch error) and I9 (no id
  handed out twice). New mutation: `UnsafeNoSequenceCheck`.
- **Integration**: the Java client, franz-go and kcat now run with
  idempotence on; all three negotiate InitProducerId v4. The cluster test
  now fails on any duplicate instead of counting them.
- Also: S5 in the Raft monitor checks that a node never forgets a vote
  across restarts. It was already on the branch and is committed
  separately.

## Results

| | |
|---|---|
| Simulator, 5000 seeds | clean after the fix: 2.27 M idempotent records acknowledged, 371 k retries, 17 k ids, 3 k epoch bumps |
| Bug found | **S6**: seeds 683 and 2706 broke I8. A batch got in ahead of the one before it, which a node had refused as not leader just before it became leader. Fixed by dropping Kafka's rule 4. Both seeds are in `regressions.txt` |
| Mutation | retries appended twice are caught at seed 4 as I1/I4 (one record at two offsets) |
| Cluster test, two leader kills | 0 duplicate records (it used to count them) |

## How to run it

    make test                       # unit tests, the mutation suite
    make sim SEEDS=5000             # about 4 minutes on the MacBook Air
    make sim SEED=683               # the S6 seed
    make integration                # the three clients with idempotence on

## What is untested or weak

- **State is never dropped**: about a hundred bytes per producer session
  and partition, for ever. Part (b) adds expiry, and it must keep the S6
  rule for producers it has not expired.
- **Idempotent producers run only in `sync always` simulations.** Under
  `interval` and `never` a retry may re-send a record that a power loss
  removed after it was read, which the model reports as I3. The id block
  file does not depend on the policy, and strict runs do have power losses
  and failed syncs, so I9 still sees crashes.
- The simulated producer bumps the epoch per partition and never reaches
  epoch 32767, so asking for a new id on overflow is not exercised.
- The sequence wrap at 2^31 − 1 is unit tested on a partition directly,
  not through the broker or the simulator.
- Clients asking for InitProducerId v5 or later are not supported. None
  of the three asks for it.
- Not compared against a real Kafka broker. "Kafka's rules" means the
  rules read in its source (design section 2).

## Ten questions

1. Why are ids made per node from a block file instead of by a replicated
   counter? What goes wrong with the counter under `-sync never`?
2. Why is the block file synced even when the sync policy is `never`?
3. Why are sequences checked at apply rather than by the leader before it
   proposes?
4. A retry is answered with the offset of an earlier log entry. Why is
   that correct even if the first copy came from an older term?
5. What exactly happened in S6, and why does Kafka's rule 4 exist there
   but not here?
6. Why is the epoch kept per partition and not per producer?
7. Why is the window five batches, and what does the simulator's limit of
   three in flight have to do with it?
8. What does a refused or duplicate Append do to the log and to the
   state? Why does the entry stay in the log?
9. Why must the producer state be in the snapshot? Which seed setting
   checks that?
10. Why does the simulated producer give up after two seconds, and why can
    no record be written twice after it does?
