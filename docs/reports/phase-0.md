# Phase 0 report — Foundation

Merged as PR #1 on 2026-11-10. CI green on Linux and macOS.

## What was built

- `internal/sys/sys.go`: `Handler`, `Clock`, `Network`, `Disk`, `File`,
  `Random`, with the durability contract in the `Disk` doc comment. No
  implementations.
- `internal/purity_test.go`: fails on any off-allowlist import, any use of
  `time` beyond `Duration`, and any `go` or `select` statement in core
  packages. It has its own 12-case table test.
- Makefile, `golangci-lint` configuration, GitHub Actions on Linux and macOS.
- `CLAUDE.md`, `docs/PLAN.md`, `docs/design/phase-0.md`, ADRs 0001–0004.

## How to run it

```
make build test race lint fuzz
./bin/samsa        # prints a placeholder line
```

## What is untested or weak

- Nothing implements the interfaces yet; Phase 1 may force changes to `Disk`.
- Map iteration order is not checked mechanically.
- The purity check is syntactic: a local variable named `time` gives a false
  positive.
- `make fuzz` and `make race` have nothing meaningful to run yet.
- The static-binary claim was checked only by setting `CGO_ENABLED=0`; the
  Linux binary was not inspected.
- The design review was a second pass by the same author, not an independent
  reviewer.

## Ten questions

1. What are the sources of nondeterminism in a server, and how does this
   design remove each?
2. Why does Go's `testing/synctest` not give deterministic simulation?
3. Why do handlers call the disk synchronously instead of returning effects,
   and what can the simulator no longer test as a result?
4. If fsync blocks the whole node, how will the broker get acceptable
   throughput?
5. After a crash, what can an unsynced region of a file contain, and why does
   the model allow arbitrary bytes?
6. Why must a directory be synced after creating or renaming a file?
7. Why is a failed fsync fatal instead of retried?
8. Why an import allowlist and not a denylist, and why is `select` banned?
9. Why is `Instant` an `int64` and not `time.Time`, and why must `Now` never
   go backwards?
10. What does the simulator not prove, and how are those gaps covered?
