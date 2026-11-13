# ADR 0018: injected faults and an offline verdict for applications

Date: 2026-11-14. Status: accepted. Design: `docs/design/a5b-app-faults.md`.

## Context

Production plan section 2: users should be able to test their own
applications against Kafka-level faults, and get a verdict on whether the
application lost or duplicated records. The core decides every answer and
holds every record. It is single-threaded and pure (ADR 0003).

## Decision

- **Faults are decided in the core**, at the request they hit, by a
  splitmix64 stream of their own, seeded by `-faults seed=N`. Each
  eligible request draws once.
  - Kinds: `lostack` (commit, then close without answering), `notleader`
    (refuse as a non-leader does, ADR 0011) and `rebalance` (start a round
    from a heartbeat of a stable group).
  - The zero configuration draws nothing.
- **The verdict is offline.** `samsa verdict` loads a stopped node's data
  with the same code as `samsa check` and reads the input and output
  topics.
  - It matches inputs to outputs by record key, and uses the group's
    committed offsets to tell "lost" (committed past, never output) from
    "not yet processed".
  - Exit status: 0 pass, 1 fail, 2 cannot judge.
- **The fault spec of the last start is kept in `FAULTS`** in the data
  directory, written by the binary, so that a verdict can print the
  replay command.

## Alternatives

- **Faults drawn from the node's `sys.Random`.** Rejected: switching faults
  on would move every Raft timeout and change every simulator seed.
- **A fault proxy outside the broker** (like the chaos test's TCP proxy).
  Rejected: a proxy cannot commit a write and then lose its answer, or
  start a rebalance. It sees bytes, not meaning.
- **A live verdict over HTTP.** Rejected: it needs a second reader of the
  node's state, which ADR 0003 forbids or makes costly. Stopping the
  broker at the end of a test is cheap.
- **Matching inputs to outputs by value or by an injected header.**
  Values are often transformed. A header requires the application to
  copy it, which is no easier than copying the key, and keys are already
  what most pipelines keep.

## Consequences

- A seed fixes the fault stream, not which request gets each fault: with
  concurrent clients the order of eligible requests varies. Reruns usually
  fail the same way; exact replay needs the simulator.
- Applications under test must produce uncompressed records with unique,
  present keys. The verdict refuses anything else, with the reason.
- Three of the plan's fault kinds exist; session expiry, slow or empty
  fetches and retention cuts are follow-ups.
