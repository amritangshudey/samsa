# ADR 0023: Peer addresses travel in the configuration

Date: 2026-11-14. Status: accepted. Design: `docs/design/c1b-membership-binary.md`.

## Context

Part (a) of item 9 put the voters in the Raft log (ADR 0022). The real
binary still had a fixed address book from `-peers`, so a node could join
only in the simulator, whose network routes by id.

## Decision

- **Each voter's info names a `PEER` listener next to `CLIENT`.** A
  bootstrap voter's info comes from `-peers` when the first change is
  proposed. A new voter's comes from AddRaftVoter.
- **`sys.Network` gains `PeerAddr(id, addr)`.** The broker calls it:
  - for the `-peers` entries at start;
  - for the learner when AddRaftVoter starts;
  - for every voter whenever `raft.ConfIndex` moves.

  The real loop keeps the book behind its peer mutex and starts a sender
  for each new id. The simulator ignores it.
- **`-join` reuses the `-peers` list** for addresses, without
  bootstrapping a configuration from it.
- **`samsa node add|remove` sends AddRaftVoter and RemoveRaftVoter.** It
  finds the leader through Metadata when a node answers NOT_CONTROLLER.

## Alternatives

- **A flag per node for peer addresses.** Every node would need new flags
  whenever one joins.
- **The transport reads the configuration itself.** It runs on other
  goroutines, and the core's state is single-threaded (ADR 0001).
- **Kafka's `kafka-metadata-quorum.sh add-controller`.** It needs a Kafka
  controller's properties file and directory id (ADR 0022).

## Consequences

- **A joining node must be told the current voters' addresses** in
  `-peers`, because it must answer the leader before it holds a
  configuration.
- **Removed voters keep a sender goroutine that sits idle,** at most one
  per id ever known.
- **The `sys.Network` interface grew a method.** Every implementation (the
  real loop, the simulator and the broker's test fake) implements it.
- **Ids are not reused with a new disk.** Directory ids, KIP-853's guard,
  are not built.
