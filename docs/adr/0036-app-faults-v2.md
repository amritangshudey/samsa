# 0036: Application fault testing for transactions, KIP-848 groups, and share groups

Date: 2026-11-17  
Status: Accepted (C23b)

## Context

`samsa -faults` and `samsa verdict` (ADR 0028, item 23a) only covered classic
consumer groups and non-transactional producers. With transactions (C15),
KIP-848 consumer groups (C19), and share groups (C22) implemented, `verdict`
needed to understand transactional markers/aborts and share-group delivery
state, and the fault injector needed to cover their protocol RPCs.

## Decision

1. **Verdict reads `read_committed` state**: `records()` in
   `internal/broker/verdict.go` skips control batches, records at or above
   `p.lso()`, and records covered by `p.aborted`. Aborted or uncommitted
   transactions never count as valid outputs.
2. **Share group progress in `-group`**: `Judge` auto-detects `st.shares[p.Group]`
   when present, treating records below `spso` or marked `shareAcked` /
   `shareArchived` as finished (`lost` if absent from `out`), so `samsa verdict`
   keeps the single `-group` flag across classic, KIP-848, and share groups.
3. **Reuse existing fault kinds (`notleader`, `lostack`, `rebalance`, `expire`,
   `emptyfetch`)** on the new protocol RPCs rather than inventing new fault kind
   names. In KIP-848 and share heartbeats, `rebalance` returns an empty
   assignment for one heartbeat rather than an error code, matching the real
   KIP-848 revocation flow.

## Consequences

- No Raft log format or snapshot version change is required.
- Existing `-faults seed=N[,rate=R][,kinds=...]` specs automatically exercise
  transactional, KIP-848, and share-group applications.
- Fault-off runs remain bit-identical because `faulter` draws only when
  `Rate > 0` and the kind is enabled.
