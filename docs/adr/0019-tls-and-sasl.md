# ADR 0019: TLS in the I/O layer, SASL in the core, credentials in a file

Date: 2026-11-14. Status: accepted. Design: `docs/design/c5-tls-sasl.md`.

## Context

Production plan item 7: TLS for clients and between nodes (mutual between
nodes), and SASL PLAIN and SCRAM, using only the standard library. The core
is pure (ADR 0003). Kafka's SASL APIs were read from the official schemas
and sources on 2026-11-14 (design, section 2).

## Decision

- **TLS lives in the I/O layer.**
  - `cmd/samsa` wraps the client listener (`-tls-cert`, `-tls-key`,
    optional `-tls-client-ca`) and the peer listener (`-peer-tls-*`).
  - `realsys.Loop.PeerTLS` dials peers with mutual TLS.
  - TLS 1.2 or later. Any certificate from the peer CA is a cluster
    member; the node id is not bound to the certificate.
- **SASL lives in the core.** Authentication state is per connection.
  - Before authentication only ApiVersions, SaslHandshake and
    SaslAuthenticate are accepted; anything else closes the connection.
  - Failures answer with Kafka's error code and text, then close.
  - Versions: SaslHandshake 1 only, SaslAuthenticate 0–2.
  - Mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
  - SessionLifetimeMs is 0: no re-authentication.
- **The core may import** `crypto/hmac`, `crypto/pbkdf2`, `crypto/sha256`,
  `crypto/sha512`, `crypto/subtle`, `encoding/base64` and `hash`. They are
  deterministic computations. `crypto/rand` stays out: nonces come from
  `sys.Random`.
- **Credentials are a file** of SCRAM credentials (salt, stored key, server
  key), the same on every node, read at start.
  - `samsa user add` writes it from a password on stdin.
  - PLAIN is checked against the SCRAM credentials, so no password is
    stored anywhere.

## Alternatives

- **TLS inside the core**, as a byte-stream state machine. Rejected: it
  would mean writing a TLS implementation or wrapping `crypto/tls` around
  a fake connection. TLS adds nothing to the protocol's semantics, and
  the I/O layer is where the standard library's TLS fits.
- **SaslHandshake version 0.** Rejected: it switches the connection to raw
  token framing, a second framing in the reader for clients that no
  longer exist. All three test clients send version 1.
- **Credentials as replicated commands** (AlterUserScramCredentials, API
  51). Deferred to the ACL work (item 8), which needs the same replicated
  security state. Until then, a change needs a restart of every node.
- **Plain-text passwords for PLAIN,** as Kafka's JAAS file holds them.
  Rejected: deriving from the SCRAM salt costs one PBKDF2 per PLAIN login
  and keeps every password off the disk.
- **Binding the node id to the peer certificate.** Not done: it adds a
  naming rule for operators, and protects only against a compromised
  member posing as another, not against what it can do as itself.

## Consequences

- Plaintext clients, clients that do not trust the broker's CA, and
  clients without valid credentials are refused. This is shown with
  franz-go, the Java client and kcat.
- PLAIN logins cost a PBKDF2 derivation on the node's single thread.
  Prefer SCRAM.
- Adding the two APIs lengthened the ApiVersions response, which moved
  every simulator seed. Any change to the advertised API list will do
  that.
- With SASL off, the node draws no randomness for it, and nothing else
  changes.
