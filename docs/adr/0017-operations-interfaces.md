# ADR 0017: metrics, health, offline check and the data directory lock

Date: 2026-11-13. Status: accepted. Design: `docs/design/b4-operations.md`,
section 5.

## Context

An operator needs to see what a node does (metrics), to let an
orchestrator know whether it is alive and whether it can serve (health),
to find out whether a data directory is sound before starting a node on it
(check), and to keep its settings in a file. The node is a single-threaded
core with no I/O of its own (ADR 0003); nothing may read it from another
goroutine.

## Decision

- **Metrics** are a value (`broker.Metrics`) the node returns on its own
  goroutine. The real loop calls a hook every 250 ms (`Loop.Every`) that
  stores a copy behind an `atomic.Pointer`; the HTTP handlers read only
  that copy. The Prometheus text format is written by hand.
- **Health**: `/healthz` fails when the copy is over a second old (the
  loop is stuck). `/readyz` reports the node's own judgement (`Ready`,
  `NotReady`).
- **Check** reuses recovery: `broker.Check` runs the Raft start, decodes
  the snapshot, reads every entry and replays every command, on a disk
  wrapper that records writes instead of making them. A recorded write is
  a repair a start would make.
- **Lock**: `realsys.OpenDisk` holds an exclusive `flock` on `LOCK` for as
  long as the disk is open.
- **Config file**: `key = value`, one per flag; the command line wins; an
  unknown or repeated key is an error.

## Alternatives

- *A Prometheus client library.* A dependency for about 40 lines of text
  output.
- *Publishing after every event.* A copy per request with no reader, and
  it would not show a loop that is alive but idle.
- *A separate reader of the on-disk formats for check.* Two readers drift;
  the one that matters is the one a start uses.
- *A PID file instead of flock.* A PID file outlives a killed process and
  must be judged stale; a flock is released by the kernel when the process
  ends, however it ends.
- *YAML or TOML.* No parser in the standard library, and every setting is
  a scalar flag.

## Consequences

- Metrics are up to 250 ms old.
- `check` replays entries past the commit point too; a command there that
  cannot be applied is reported though a new leader might have replaced
  it. Valid leaders never write such commands, so this is reported as
  corruption on purpose.
- `flock` is advisory and local: it does not protect a data directory on
  a network filesystem shared by two machines.
- Windows is not supported (`syscall.Flock`); the release targets are
  macOS and Linux.
