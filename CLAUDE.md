# Project: Samsa

Named after Gregor Samsa, the man in Kafka's *Metamorphosis* who wakes up as
a bug. The simulator's job is to find them. Go module:
`github.com/amritangshudey/samsa`.

A Kafka-compatible message broker written from scratch in Go, shipped as a
single static binary. Its distinguishing feature is correctness demonstrated by
deterministic simulation testing (FoundationDB / TigerBeetle style).

Sole developer, Apple Silicon MacBook Air. Everything runs locally; no cloud
infrastructure. The developer must be able to defend every design decision in
an interview, so explain the why, not only the what.

The phase plan lives in `docs/PLAN.md`; Phase 4 onwards and the work beyond
the plan are specified in `docs/ROADMAP.md`. Read both before starting work.
The shared architecture: every state change is a command in one log, applied
in order to a deterministic state machine (ADR 0005).

## Goals

- Unmodified Kafka clients (franz-go, the official Java client, kcat) can
  produce and consume against the broker.
- The core is a deterministic state machine. A simulator runs a whole cluster
  in one process under injected faults; any failure reproduces from a seed.

## Non-goals (do not build unless asked)

Transactions, idempotent producer, log compaction, ACLs/SASL/TLS, tiered
storage, Kafka Streams/Connect guarantees, cluster membership changes.

## Architecture rules (non-negotiable)

1. Core logic is single-threaded per node and does no I/O directly. Clock,
   network, disk and randomness are interfaces passed in.
2. Inside the core: no goroutines, no `time.Now`, no global `rand`, no
   dependence on map iteration order (sort keys before iterating whenever
   order can reach output, disk, network or a decision).
3. Two implementations of each interface: real (TCP, files, wall clock) and
   simulated (virtual time, in-memory disk and network with fault injection).
4. Never trust input from the network: bound every length before allocating,
   return protocol errors, never panic on malformed bytes.
5. Verify Kafka protocol details against the current official protocol
   documentation (https://kafka.apache.org/protocol and the message JSON
   schemas in the Apache Kafka repository) before implementing. Choose API
   versions that the test clients actually negotiate, and record the choice in
   an ADR. Do not rely on memory for wire formats.

Rules 1 and 2 are enforced mechanically by `internal/purity_test.go`: every
package under `internal/` except `internal/realsys` may import only an
allowlist of standard-library packages, may use nothing from `time` except
`Duration`, and may not contain `go` or `select` statements (ADR 0003).

## Process

**Standing instruction (2026-11-10):** the developer delegated approvals:
"approve yourself and continue coding till either there is any blocker or
the project is done, keep everything documented and I will review all
later". So: do not stop between phases. For each phase still write the
design doc, review it critically as a second pass (record the findings in
the doc), open a PR, merge only on green CI, and write the end-of-phase
report to `docs/reports/phase-N.md`. Stop only for a real blocker. The
"wait for approval" lines below describe the original process and apply
again if the developer withdraws this instruction.

- Phases 0–3 are the MVP. **Stop after every phase and wait for approval.**
- At the start of each phase, write `docs/design/phase-N.md`: the concepts
  involved, the proposed design, the alternatives rejected and why. **Wait
  for approval before coding.**
- Write tests first. Table-driven unit tests; Go native fuzzing for every
  decoder; the race detector on the real-I/O layer.
- Small commits using Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`,
  `refactor:`, `chore:`, `ci:`). Every commit builds and passes tests. Never
  commit to `main` with failing CI.
- **Ask before adding any dependency**, including test-only modules and dev
  tools. Prefer the standard library.
- Record every significant decision as an ADR in `docs/adr/`
  (`NNNN-short-title.md`: Context, Decision, Alternatives, Consequences).
- Maintain `docs/BUGS.md`: for each bug the simulator finds, record the seed,
  the violated invariant, the root cause and the fix.
- Never claim compatibility or performance that a test or reproducible
  benchmark in the repo does not demonstrate.
- At the end of each phase, report: what was built, how to run it, what is
  untested or weak, and ten questions the developer should be able to answer
  about it.

## Code conventions

- Wrap errors with context: `fmt.Errorf("open segment %d: %w", base, err)`.
- `log/slog` for structured logging. The core receives its logger; it never
  uses the global one.
- Doc comment on every exported identifier.
- Decoders return errors, never panic, and check every length against both
  the remaining input and a configured maximum before allocating.
- Anything in the core that needs time, randomness, disk or network takes the
  interface as a parameter or struct field. No package-level state.
- A simulator failure message always includes the seed and the exact command
  that reproduces it.

## Commands

| Command | Does |
|---|---|
| `make build` | Build the static binary to `bin/samsa` |
| `make test` | `go vet` and unit tests, including the core-purity check |
| `make race` | Tests with the race detector |
| `make lint` | `golangci-lint` |
| `make fuzz` | Run every fuzz target for `FUZZTIME` (default 10s) each |
| `make integration` | Real clients (franz-go, kcat, Java) against the binary; needs kcat and a JDK |
| `make sim SEEDS=n` | Run n seeds of the simulator (default 1000); failures print their seed |
| `make sim SEED=x` | Replay one seed with its full event trace |

Run the broker: `./bin/samsa -listen localhost:9092 -data ./data`
(`-sync always|interval|never`, `-log-level debug` to see each request).

## Local tools

Installed with Homebrew: `go` (1.27), `golangci-lint`, `kcat`, `openjdk`.
The JDK is keg-only and not on PATH: use `/opt/homebrew/opt/openjdk/bin/java`.

## Layout

See ADR 0003. `cmd/samsa` wires the real implementations; `internal/sys`
holds the interfaces; `internal/realsys` is the only impure package under
`internal/`; everything else there is core: `storage` (the log), `wire`
(Kafka encoding), `broker` (the node), `simsys` (simulated disk), `sim`
(the simulator: world, clients, and the history checker in `history.go`).
`integration/` is a separate Go module so that client libraries never enter
the broker's `go.mod`.
