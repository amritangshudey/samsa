# 0003 — Repository layout and the mechanical purity check

Status: accepted, 2026-11-10

## Context

ADR 0001 only holds if no core code reads the clock, starts a goroutine or
touches the OS. Review does not catch a stray `time.Now()` in a log line; a
machine must.

## Decision

Layout:

```
cmd/samsa/          wiring of the real implementations
internal/sys/       the interfaces
internal/realsys/   real implementations and the event loop (exempt)
internal/...        everything else is core and must be pure
```

`internal/purity_test.go` parses every non-test Go file under `internal/`
except `internal/realsys` and fails on:

- an import outside an allowlist of standard-library packages and this
  module's own core packages;
- any use of package `time` other than `Duration` and its unit constants;
- a `go` or `select` statement.

The simulated implementations and the simulator harness are core and are
checked. The checker has its own table-driven test.

## Alternatives

- **Denylist of imports.** Rejected in review: anything not thought of
  (`context`, `runtime`) passes silently.
- **`depguard` plus a custom analyser.** `depguard` cannot see statements,
  and an analyser needs `golang.org/x/tools`, a dependency.
- **Public `pkg/` packages.** No external importer exists.

## Consequences

- Needing a new standard-library package in the core means editing the
  allowlist, which is the intended friction.
- `_test.go` files are exempt.
- Not checked: iteration over maps (needs type information). Covered by
  convention and, from Phase 3, by running each seed twice and comparing
  trace hashes.
- `log/slog` is allowed although it timestamps records with the real clock.
  The simulator must drop that timestamp and keep logs out of the trace hash.
- The check is syntactic. A local variable named `time` causes a false
  positive; reflection or `unsafe` tricks are not on the allowlist.
