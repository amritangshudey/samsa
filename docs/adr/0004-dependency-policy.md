# 0004 — Dependency policy

Status: accepted, 2026-11-10

## Context

The broker is meant to be understood end to end by one person, and its core
must be deterministic. Every dependency is code that has not been read and
may use time, goroutines or randomness.

## Decision

- The broker module uses the standard library only, unless the developer
  approves a specific dependency in advance.
- Real Kafka clients needed for integration tests (franz-go first) will live
  in a separate Go module inside the repository, so the broker's `go.mod`
  stays free of them. To be confirmed when Phase 2 needs it.
- Developer tools (`golangci-lint`, `kcat`, a JDK) are installed with
  Homebrew locally and pinned by version in CI. They are not module
  dependencies.

## Alternatives

- **Use an existing Kafka protocol library for encoding.** Rejected: writing
  and fuzzing the decoders is part of the project's point.
- **Use an existing Raft library.** Rejected for the same reason (Phase 5).

## Consequences

- More code to write, notably the wire protocol and CRC-checked framing.
- No supply-chain surface in the shipped binary.
