# Security review (production plan item 17, part)

2026-11-15. A review by the author, with the help of the model that wrote much
of the code, of what is in this repository. It is **not an independent audit**,
and it finds what its author knew to look for. What was read, what was found,
what was fixed, and what is left.

## What was reviewed

1. **Network input (architecture rule 4).** Every decoder checks lengths
   against the remaining input and a limit before allocating, and returns an
   error instead of panicking. Fuzz targets (11, run by `make fuzz`):
   `wire` (messages, request header, batches), `storage` (recovery, index),
   `raft` (messages, snapshots, the state machine's input), `broker` (whole-node
   input, state decoding), `scram` (the server exchange). A request frame is
   limited to 32 MiB (`MaxRequestBytes`), a batch to 1 MiB; the buffer of a
   connection that has sent more than twice the frame limit is dropped.
2. **Transport.** TLS 1.2 or later for clients and for peers; mutual TLS is
   optional for clients and, with the three `-peer-tls-*` flags, for peers.
   The standard library only: the module has no dependencies (`go.mod`
   lists none); the tests that use client libraries are a separate module.
3. **Authentication.** SASL PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 (`scram`),
   comparisons with `crypto/subtle`, iterations between 4096 and a maximum.
   A password sent over a plaintext listener is warned about at start.
4. **Authorization.** ACLs are replicated commands; with `-acl`, a resource
   without an ACL is denied unless `-allow-if-no-acl`; every API has an
   allowed and a denied test.
5. **The data directory and the files Samsa writes.**
6. **What an operator can mis-set**, through `samsa doctor`.

## Findings

| # | Finding | Severity (author's view) | Status |
|---|---|---|---|
| 1 | The data directory and its files were created 0755 and 0644: any local user could read the records, the SCRAM verifiers and the ACLs in the log and snapshots | medium on a shared machine | **Fixed:** created 0700 and 0600. An existing directory keeps its modes; the runbook says to `chmod` it |
| 2 | **Peer traffic is not authenticated unless the three `-peer-tls-*` flags are set.** Anyone who can reach a peer port can send Raft messages: votes, AppendEntries, snapshots | high if the peer port is reachable | **Documented, not removed:** `samsa doctor` marks it with `!`, the runbook and sample config set it, a `prod` node warns. Peer authentication on by default would break local clusters and tests; left to the operator |
| 3 | `/metrics`, `/healthz`, `/readyz` are plain HTTP with no authentication | low (counters; no data) | Off by default (`-metrics-listen`); the runbook binds it to localhost |
| 4 | **No quotas, no limit on connections**: a client that can connect can fill the disk or open connections until the process runs out of descriptors | medium | **Open.** Retention bounds the disk only if set; the unit sets `LimitNOFILE=65536` |
| 5 | SCRAM credentials from `samsa user add` default to 4096 PBKDF2 iterations, the minimum, which is Kafka's default; current guidance for password hashing is far higher | low to medium (an attacker who has the verifier file) | **Open.** `-iterations` raises it; the doctor does not mention it |
| 6 | PLAIN over a plaintext listener sends the password in the clear | known | Warned at start; the `prod` profile's doctor marks plaintext |
| 7 | Compression: batches are stored and served as sent; a compressed batch is never expanded by the broker, so no decompression bomb | none found | n/a |
| 8 | Fault injection (`-faults`) and the deliberate-bug build (`-tags unsafe`, `bin/samsa-unsafe`) drop data on purpose | by design | The first warns at start and `doctor` marks it; the second is a separate binary, not built by `make release` |
| 9 | The `damaged` and `recovering` markers (ADR 0030): a forged file in the data directory can make a node stop voting or repair itself | needs write access to the directory | Same trust as the log itself |
| 10 | A damaged hard-state file stops the node: it cannot guess its term | availability, not safety | By design |

## What was not reviewed

- The Go standard library and the operating system.
- Denial of service by a slow or malicious client holding resources for a
  long time (parked fetches are bounded by `max.wait`; join and sync rounds by
  their timeouts) beyond what the simulator's vandals send.
- Side channels, memory scrubbing, the TLS configuration beyond the minimum
  version (cipher suites are Go's defaults).
- Supply chain: the build is reproducible with `-trimpath`, but the release
  binaries are not signed.
- Any real attacker. No penetration test has been done.

## What should be done before anyone depends on it

1. Findings 4 and 5: quotas or at least a connection limit; a higher default
   iteration count with a measured cost.
2. Sign the release checksums.
3. An independent review, which this one is not.
