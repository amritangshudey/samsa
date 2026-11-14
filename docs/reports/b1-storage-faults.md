# B1 report: storage faults, scrub, repair from peers

Branch `storage-faults`, 2026-11-15. Production plan item 13. Design:
`docs/design/b1-storage-faults.md` (and "As built"). ADR 0030.

## What was built

- **`storage.Log.Scrub`**: reads sealed segments in steps, checks each
  frame, reports the first bad one without changing anything.
- **The scrub in the broker** (`broker/scrub.go`): once a second, within
  `-scrub-bytes-per-second` (8 MiB), then rests `-scrub-interval` (24 h). Damage,
  or any read that fails its checksum, writes a `damaged` marker and stops the
  node.
- **Repair at start** (`raft/recovery.go`): the next start finds the damage
  again, writes a `recovering` marker, then cuts the log at the damage (damage
  after the snapshot) or removes the snapshot file and empties the log
  (damage under it). The leader sends the rest as to a new node. The hard
  state stays.
- **Recovering** (`recovering` file): grants no vote or pre-vote, never
  campaigns, until its commit reaches the log end it had, or the leader has
  nothing more for it. `/readyz` says so; metrics `samsa_recovering`,
  `samsa_storage_damage_repaired_total`, `samsa_scrubbed_bytes_total`.
- **Simulator:** `Disk.Corrupt`; fault kind `faultRot` (one node at a time,
  never with membership changes, in a cluster of three or more); invariant
  S7 (a recovering node casts no vote); three mutations; coverage counters.
- **Tests:** storage (rot found by `FirstDamage` and the scrub, a torn tail
  is not damage), Raft (a damaged follower recovers, refuses votes, ends
  recovery; the mutation elects on its vote; a single node refuses),
  `TestStorageRotRepair` on three real processes.

## Results

**Simulator:** 4,000 seeds pass in 169 s. In 1,000 of them, 147 damages
were injected, 118 found by the scrub or the end-of-run scrub and repaired
by a restart, and 705 MB of log scrubbed; the rest were cut away by a
snapshot or never reached before the run ended.

**Mutations** (200 seeds each):

| Mutation | Caught by | First catch |
|---|---|---|
| a recovering node votes | 10 of 200 | S7, seed 31 |
| ignore the recovery marker at start | 1 | S3 (a committed entry replaced), seed 67 |
| cut the log at damage and carry on, no marker | 5 | "asked to replace committed entry", seed 73 |

The scores are low: a quarter of the seeds have damage, and a mutation needs an
election or a lost entry at the wrong moment. "Ignore the marker" is caught
once in 200 seeds: it shows as a loss only if a vote follows.

**Real processes** (`TestStorageRotRepair`, 21 s): three nodes, 150,000 records
of 1 KiB; 11 bytes in the middle of node 2's oldest segment overwritten. Its
scrub finds it within a second and the process exits; the restart empties
and refetches (142 MB of 152 MB after 20 s); 3,000 more records are
acknowledged meanwhile; with the third node killed, all 153,000 records are
read from the other two.

## Problems met

- **BUGS.md B1-1:** recovery waited for entries that were never committed
  (seed 417); it now also ends when the leader has nothing more to send.
- **BUGS.md B1-2:** damage under the snapshot restarted the node for ever; the
  design had assumed such entries unneeded. The log is the data store. Now a
  durable marker, and an emptying repair.
- **BUGS.md B1-3:** damage in the snapshot's last entry stopped the start.
- **BUGS.md M16, M17 (simulator):** the final comparison read unfound damage;
  damage with a two-voter configuration deadlocks.
- **The first design had an error that review found:** "a bad frame with a valid
  frame after it" is also what a crash tear looks like in the simulated
  disk, which would put a cluster restarting together into recovery at once.
  Damage became "a bad frame in a sealed segment".

## How to run it

```sh
go test ./internal/storage ./internal/raft -run 'Damage|Recover|Torn' -v
make sim SEEDS=1000
go test ./internal/sim -run 'TestMutationSuite/(vote_while|ignore_the_recovery|cut_the_log)' -v
make build && cd integration && SAMSA_BIN=../bin/samsa go test -run TestStorageRotRepair -v .
```

## What is untested or weak

- **The active segment is not covered.** Rot there looks like a crash tear and
  is cut at the first bad frame, so a node can still forget acknowledged
  entries that were in the newest segment. A "synced up to here" mark in
  each frame would fix it (a new format level).
- **The hard state, snapshot and install files** are single checksummed
  files: a failing one stops the node as before.
- **More than a minority** of nodes damaged, and **a cluster of one**, are not
  repaired. Damage together with a membership change is not simulated.
- **Cost:** repairing damage under the snapshot re-sends the whole log, as
  replacing a node does; untimed here beyond the one real run.
- **Misdirected and lost writes** are modelled as overwritten or zeroed bytes
  only.
- **The scrub's load** is bounded by its rate, not measured against client
  throughput.
- **A downgrade to a build without this** while a node is recovering would let
  it vote.

## Ten questions

1. **Why does a damaged node not vote?** If it acknowledged an entry that
   then committed and lost it, a candidate that lacks the entry could win
   with its vote. Without its vote every majority holds an intact copy.
2. **Why is damage only a bad frame in a sealed segment?** A crash leaves
   garbage then valid blocks in the unsynced tail; in a sealed segment all of
   it was synced, so nothing there can be a tear.
3. **Why does repair happen at start?** The broker applies from the log and
   cannot take back what it applied; a restart rebuilds the state from the
   snapshot.
4. **Why empty the log for damage under the snapshot?** The log holds the
   batches clients read, and a cut there would leave a log shorter than its
   snapshot. Emptying uses the path that brings in a new node.
5. **How does recovery end?** When the commit index reaches the log end the
   node had, or the leader has nothing more to send it.
6. **Why did the first end condition fail?** Part of the old log was never
   committed and is gone, so the commit index could not reach it (seed 417).
7. **What stops two nodes from being repaired at once?** The simulator
   injects one at a time. In a real cluster nothing does: it is the stated
   assumption (damage on a minority).
8. **Why is the damage marker needed?** The scrub may find damage the start-up
   scan never reads (under the snapshot); the restart must know to look.
9. **What does a cluster of one do?** It refuses to start and says there is no
   copy.
10. **What can still lose data?** Rot in the active segment on a node whose
    acknowledgement was needed for a commit, followed by that node's vote.
