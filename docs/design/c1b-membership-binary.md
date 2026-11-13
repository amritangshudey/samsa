# C1 part (b): membership changes in the real binary

Production plan item 9, part (b). Part (a), Raft, the broker APIs and the
simulator, is in `c1-membership.md` and ADR 0022. This part makes the
binary able to do what the simulator already does: a node joins a running
cluster of real processes, and a voter leaves it.

## 1. What is missing

- **The transport's address book is fixed.** `realsys.Loop.Peers` takes
  every peer address once, at start. It has no queue for a node that is
  not in `-peers`, and it drops an inbound connection whose id it does not
  know.
- **The configuration carries no peer addresses.** A voter's info holds
  its client listener only. A bootstrap voter has no info at all until the
  first change writes the address the leader knows.
- **No `-join` flag.** `broker.Config.Join` exists, but `cmd/samsa` never
  sets it.
- **No tool.** Kafka's `kafka-metadata-quorum.sh add-controller` reads a
  Kafka controller's properties and its meta.properties directory id, so
  it cannot describe a Samsa node (part (a), section 1).

## 2. Design

### Addresses travel in the configuration

- **Each voter's info names its listeners:** `CLIENT` for Kafka clients,
  as now, and `PEER` for other nodes.
- **A bootstrap voter's info is filled from `-peers`.** `broker.Peer`
  gains `PeerAddr`, and `described()` writes both listeners when the
  first change is proposed. From then on, the log names every voter's
  address.
- **The new voter's listeners come from AddRaftVoter,** as in part (a).

### The core tells the transport

- **`sys.Network` gains `PeerAddr(id, addr)`:** "messages for id go to
  addr from now on".
- **The broker calls it:**
  - for every voter whenever the configuration in force changes (it
    tracks `raft.ConfIndex`);
  - for the learner when AddRaftVoter starts.

  It is called for the `-peers` entries before any other, so that a node
  that joins can answer the leader before it holds a configuration.
- **The simulator's network routes by id,** so its `PeerAddr` does
  nothing.
- **The real loop keeps the address per id behind its peer mutex:**
  - The sender goroutine reads the address at each dial, so a change
    takes effect at the next connection.
  - An unknown id gets a queue and a sender, started at once if the loop
    runs, else at start.
  - Inbound connections are accepted from any id the book holds.
- **Removed voters keep their entry.** Raft stops sending to them once
  their removal commits, so the sender sits idle. That costs one goroutine
  per node ever known, at most 64 (the configuration's bound) plus the
  `-peers` list.

### `-join`

- **`-join` takes the same `-peers` list,** this node included: every
  address it may need. It does not bootstrap a configuration from it
  (`Config.Join`).
- **Restarting with `-join` is safe.** The log, not the flag, decides the
  configuration once there is one.
- **`-join` requires `-peers`** and `-sync always`, like any multi-node
  run.

### `samsa node add` and `samsa node remove`

```sh
samsa node add -bootstrap h:9092,h:9093 -id 4 -client h:9095 -peer h:19095
samsa node remove -bootstrap h:9092 -id 3
```

- **The tool sends AddRaftVoter v1 or RemoveRaftVoter v0** to each
  bootstrap address in turn until one answers with something other than
  NOT_CONTROLLER. It retries REQUEST_TIMED_OUT (another change, or the
  learner still catching up) until `-timeout`.
- **It reports the error code and message.**
- **It speaks plain TCP without authentication.** With `-acl`, the
  anonymous principal needs ALTER on the cluster. TLS and SASL for the
  tool are left out (`shortcut:` in the code).

### Replacing a node

1. Remove the dead voter.
2. Add the new one under a **new id**.

Reusing an id with an empty disk is unsafe: the node would have forgotten
its vote, and could vote twice in one term. Kafka guards against this
with directory ids (KIP-853). Samsa keeps it an operator rule for now,
written into the operations guide. The tool refuses the id of a voter
already in the configuration (DUPLICATE_VOTER), but it cannot know former
voters.

### The test

`integration/membership_test.go` runs against `SAMSA_BIN`, with a
franz-go producer and consumer running throughout:

1. Start three processes.
2. Start a fourth with `-join`.
3. Run `samsa node add`. Wait until Metadata lists four brokers.
4. Run `samsa node remove` for node 1, the bootstrap leader at the start.
5. Stop node 1. Metadata lists three brokers.
6. Every acknowledged record is read back in order, with no gap.

## 3. Alternatives

- **Peer addresses only in a new flag,** with the configuration holding
  client addresses only. Every node's flags would have to change whenever
  a node joins, which is the problem membership changes solve.
- **The transport reads the configuration itself.** The loop cannot read
  core state from its goroutines (rule 1), so the core must say it.
- **DescribeQuorum (55) now.** It is useful for operators, but no test
  here needs it. Deferred: listed in the report as not built.
- **Directory ids now.** They change the request (VoterDirectoryId),
  Raft's votes and the meta file. Deferred: the operator rule above
  covers it, and the report says so.

## 4. Second review

1. *Can a joining node answer the leader?* It knows every `-peers`
   address from the start, the leader's among them. A leader that was
   itself added later is in the configuration, which the joiner learns
   only by replication from that leader. So the joiner's `-peers` must
   name the current voters. The operations guide says so, and the test
   names them.
2. *Does accepting any known id let a learner disturb elections?* A
   learner with no configuration does not stand (`mayStand`, part (a)).
3. *Is `PeerAddr` a purity problem?* It is a call on an interface the
   core already holds, with plain data. No goroutine or I/O enters the
   core.
4. *Is a bootstrap voter without info a problem after this change?* Only
   until the first change. Before it, there is no node that does not have
   the `-peers` list. The first change writes every address.
5. *A sender goroutine for an id whose address changes mid-connection*
   keeps the old connection until it fails. That happens only when an id
   is reused at a new address, which section 2 forbids with a new disk.
   With the same disk, the old process is gone, so the connection breaks.
