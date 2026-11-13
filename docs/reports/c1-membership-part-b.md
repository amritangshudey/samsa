# C1 report, part b: membership changes in the real binary

Branch `membership-binary`, 2026-11-14. Production plan item 9, part (b).
Design: `docs/design/c1b-membership-binary.md`. ADR 0023.

## What was built

- **Peer addresses in the configuration:**
  - a `PEER` listener per voter;
  - `sys.Network.PeerAddr`, which the broker calls as the configuration
    changes;
  - a real transport whose address book grows while it runs.
- **`-join`:** start a node that waits to be added.
- **`samsa node add` and `samsa node remove`.** They send AddRaftVoter v1
  and RemoveRaftVoter v0, retry while another change runs, and find the
  leader through Metadata.
- **`TestMembershipThreeFourThree`** (integration). It runs three
  processes, then four, then three, with a producer writing throughout.
  Then a second original node is killed: the cluster must still accept
  writes, which it can only if the added node counts as a voter. Every
  acknowledged record is read back, dense and in order, with no
  duplicate.

## Results

- **The integration test passes 6 of 6 runs locally:**
  - one run, then five in a row;
  - each in 6 to 12 s;
  - in every run, the node removed was the leader when the test began
- **`TestPeerAddrLearnedLater`:**
  - no message crosses before the address is known;
  - both directions work after.
- **`TestVoterChanges` now checks that every node, the joiner included,
  learns every peer address.** It fails if the broker stops telling the
  transport (checked by disabling the calls).
- **`go test ./...`, `make lint`:** clean. The simulator is unaffected,
  because its network ignores addresses. 1000 seeds pass.

## How to run it

```sh
go build -o /tmp/samsa ./cmd/samsa
cd integration && SAMSA_BIN=/tmp/samsa go test -run TestMembershipThreeFourThree -v .
```

Manually, after the README's three-node quick start:

```sh
./bin/samsa -node-id 4 -listen 127.0.0.1:9095 -data d4 -join \
  -peers $P,4=127.0.0.1:9095/127.0.0.1:9195 &
./bin/samsa node add -bootstrap 127.0.0.1:9092 -id 4 -client 127.0.0.1:9095 -peer 127.0.0.1:9195
./bin/samsa node remove -bootstrap 127.0.0.1:9092 -id 1
```

## What is untested or weak

- **The producer during the changes acknowledges only about 10 records,**
  one at a time, because the changes take about a second. The test shows
  no failure across a change. It is not a load test.
- **Directory ids are not built.** A node that keeps its id but loses its
  disk could vote twice in one term. The README tells operators to
  replace a node under a new id. Nothing enforces it.
- **No DescribeQuorum.** `kafka-metadata-quorum.sh` does not work. There
  is no command to see a learner's lag.
- **The tool speaks plain TCP without SASL or TLS.** On a cluster with
  `-acl`, only User:ANONYMOUS can be given ALTER on the cluster for it.
- **A joining node must be given the current voters' peer addresses.** If
  the leader is a node added after the joiner's `-peers` list was
  written, the joiner cannot answer it.
- **A single node started without `-peers` has no peer listener,** so it
  cannot grow. Start it with a one-entry `-peers` list.
- **Peer TLS with membership changes is untested.** The certificate names
  are not checked against ids, as before (ADR 0019).

## Ten questions

1. Why must the core tell the transport about addresses, rather than the
   transport reading the configuration?
2. Why does a joining node need `-peers` at all, if addresses are in the
   log?
3. Where does a bootstrap voter's `PEER` address come from, and when is it
   first written to the log?
4. Why does the transport accept inbound connections only from ids in its
   book, and when does a leader add a joiner to it?
5. What does `samsa node remove` do when the node it asks is not the
   leader?
6. Why does the test kill a second original node at the end, and what
   would happen if node 4 were still a learner?
7. Why is reusing a node id with an empty disk unsafe, and how do
   KIP-853's directory ids prevent it?
8. What happens to the sender goroutine of a removed voter?
9. Why does `-join` require `-sync always`?
10. What would DescribeQuorum add for an operator, and why could it be
    left out here?
