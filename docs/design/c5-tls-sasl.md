# C5 design: TLS and SASL

Status: reviewed, 2026-11-14. Production plan item 7 (`docs/PRODUCTION.md`):
"TLS (clients and peers, mutual between peers), SASL PLAIN and SCRAM. Must
show: real clients over TLS+SCRAM; wrong credentials and plaintext refused;
standard library only." Roadmap C5.

## 1. What is needed and where it goes

| Concern | Layer | Why there |
|---|---|---|
| TLS, client listener | `cmd/samsa` wraps the listener with `tls.NewListener` | TLS is I/O: certificates are files, handshakes block on the network. The loop reads `net.Conn`s and never sees the difference |
| TLS, between nodes | `realsys` (`Loop.PeerTLS`), with a TLS-wrapped peer listener | the peer transport dials. The dialer needs the config; the core never knows |
| SASL | core (`internal/broker/sasl.go`) | authentication is protocol state per connection, decided by request and answer, like every other request |
| SCRAM computation | `internal/scram`, pure | HMAC and hashing are deterministic; the nonce and the key for unknown users come from `sys.Random` |
| Credentials | a users file, read at start (`-sasl-users`), written by `samsa user add` | see section 4 |

The core imports `crypto/hmac`, `crypto/pbkdf2`, `crypto/sha256`,
`crypto/sha512`, `crypto/subtle`, `encoding/base64` and `hash`. These are
pure functions of their input: no clock, no randomness, no I/O. They are
added to the purity allowlist with this ADR as the reason. `crypto/rand`
is not added: randomness reaches the core only through `sys.Random`.

## 2. The protocol (verified, rule 5)

The sources are the message schemas `SaslHandshake{Request,Response}.json`
and `SaslAuthenticate{Request,Response}.json`, plus `Errors.java`,
`SaslServerAuthenticator.java` and `ScramSaslServer.java`, all from the
Apache Kafka repository on trunk, read on 2026-11-14.

- **SaslHandshake (17), version 1 only.** Versions 0 and 1 have the same
  fields. Version 0 means the tokens that follow are raw, size-prefixed,
  with no Kafka header. Supporting that needs a second framing in the
  connection reader. No client of the last six years uses it: franz-go,
  librdkafka and Java all send version 1 (measured, below). Advertised
  range 1–1.
- **SaslAuthenticate (36), versions 0–2.** Flexible from 2. The response
  has ErrorCode, ErrorMessage (nullable), AuthBytes (*not* nullable), and
  SessionLifetimeMs from version 1. SessionLifetimeMs is always 0:
  re-authentication (KIP-368) is never required. Kafka returns the same
  when it is not configured.
- **Before authentication** only ApiVersions and SaslHandshake are
  accepted, plus SaslAuthenticate after a handshake. Anything else closes
  the connection with no answer: Kafka throws, and has no response for
  that case.
- **Failures answer, then close.** An unsupported mechanism gets
  UNSUPPORTED_SASL_MECHANISM (33) with the enabled list. Out-of-order
  requests (authenticate first, a second handshake) get
  ILLEGAL_SASL_STATE (34). Bad credentials get SASL_AUTHENTICATION_FAILED
  (58) with Kafka's own text, which names neither the user nor the
  reason: "Authentication failed during authentication due to invalid
  credentials with SASL mechanism M".
- **SCRAM, as Kafka does it:**
  - no channel binding (`p=` is refused);
  - an authorization id must equal the user name;
  - extensions in the client-first message (Kafka's `tokenauth`) are
    ignored;
  - iterations are bounded to 4096–16384.

  An unknown user gets a full exchange. Its salt and keys are derived
  from a key the node draws at start, so the failure comes at the proof,
  like a wrong password, and the salt is stable for that name. The client
  cannot tell which users exist.
- **PLAIN** (RFC 4616): `authzid NUL authcid NUL password`, with the
  authorization id empty or equal to the user.

API versions the clients negotiated (broker debug log, `TestTLSAndSASL`,
`TestTLSAndSASLOtherClients`):

| Client | SaslHandshake | SaslAuthenticate |
|---|---|---|
| franz-go v1.22.3 | 1 | 2 |
| Java (Kafka 4.4 console tools; its authenticator sends these with an empty client id, seen in a Java-only run) | 1 | 2 |
| librdkafka 2.16.0 (kcat) | 1 | 1 |

## 3. TLS

- **Client listener:** `-tls-cert`, `-tls-key`; TLS 1.2 or later. With
  `-tls-client-ca`, clients must show a certificate that CA signed (mutual
  TLS for clients). A plaintext client on a TLS listener fails the
  handshake and gets nothing.
- **Peers:** `-peer-tls-cert`, `-peer-tls-key`, `-peer-tls-ca`, all three or
  none. Both directions are mutual TLS: the listener requires and
  verifies a client certificate from the CA, and the dialer verifies the
  peer's certificate against the CA, for the host in `-peers`.
- **What a peer certificate proves:** that the holder is a member of the
  cluster, not which node it is. The node id in the connection's first
  four bytes is still believed. Binding the id to the certificate (say, a
  SAN `node-3`) would stop one compromised node from posing as another. It
  would not stop it from doing harm as itself, and it adds a naming rule
  for operators. Not done; recorded.

## 4. Credentials

Kafka keeps SCRAM credentials in the cluster metadata and changes them
with AlterUserScramCredentials (API 51). Here, at first, they live in a
file, the same on every node:

    alice SCRAM-SHA-256 4096 <salt> <storedKey> <serverKey>
    alice SCRAM-SHA-512 4096 <salt> <storedKey> <serverKey>

- `samsa user add -file F -name alice` reads the password on stdin. It
  writes both SCRAM credentials with fresh random salts, replaces the
  user's previous lines, and sets the file to mode 0600. **No password is
  ever stored.**
- PLAIN checks the password by deriving the stored key from it, with the
  user's salt, and comparing in constant time. A file stolen from a node
  does not give anyone a password, or a way to log in. The stored key is
  the hash of the client key, and SCRAM needs the client key itself.
  Kafka's PLAIN, by contrast, keeps passwords in its JAAS configuration.
- **Changes need a restart.** Credentials as replicated commands (API 51)
  would make changes live and consistent. That is a follow-up, and it
  belongs with ACLs (item 8), which need the same machinery.

## 5. Costs and limits

- **PBKDF2 on the core's thread.** PLAIN derives on every login, with an
  unknown user costing the same as a known one. That is a few
  milliseconds at 4096 iterations, and the node does nothing else
  meanwhile. A flood of PLAIN logins slows the node. SCRAM moves that
  cost to the client: the server only computes HMACs. Recommended:
  SCRAM; PLAIN only over TLS. Kafka runs authentication on network
  threads. Here that would mean an off-loop verifier, which is a real
  change to the loop. Recorded, not done.
- **SASL without TLS** works, as Kafka's SASL_PLAINTEXT does. The node
  logs a warning at start: data, and PLAIN passwords, cross the network
  readable.
- **No re-authentication, no OAUTHBEARER, no GSSAPI.**
- **Simulator:** SASL is off in the simulator. Its state lives only on the
  connection and never reaches the log, so it cannot affect replication
  safety. The node draws randomness for SASL only when it is enabled
  (`TestSASLOff`), so with SASL off the runs are unchanged. One thing did
  change: the two new APIs lengthen the ApiVersions response, which moves
  every seed (review, finding 3).

## 6. Tests

| Test | Shows |
|---|---|
| `scram.TestRFC7677Example` | the server reproduces RFC 7677's example exchange, byte for byte |
| `scram.TestExchange`, `TestClientAgainstServer`, `TestFirstRefuses`, `FuzzServer` | both hashes; wrong password and unknown user fail the same way; malformed first messages refused; no fuzzed input authenticates |
| `broker.TestSASLScram`, `TestSASLPlain`, `TestSASLRefusals`, `TestSASLOff` | the state machine: each refusal answers and closes; non-SASL requests before login close unanswered; SASL off draws nothing |
| `wire` round trip, fuzz, `TestSaslAuthBytesNeverNull` | encodings at every version |
| `cmd/samsa.TestUsersFile` | no password in the file, mode 0600, replacement, load errors |
| `integration.TestTLSAndSASL` | franz-go over TLS: SCRAM-SHA-256, SCRAM-SHA-512 and PLAIN log in and round-trip a record. Refused: wrong password, unknown user, no SASL, plaintext, a client that does not trust the broker. No password in the broker log |
| `TestTLSAndSASLOtherClients` | the Java client (console producer and consumer) and kcat over SASL_SSL with SCRAM; a wrong password is refused with a SASL error |
| `TestClientCertificates` | `-tls-client-ca`: with a certificate, in; without, out |
| `TestPeerTLS` | a three-node cluster over mutual TLS replicates; a certificate from another CA cannot complete a handshake on a peer port |

## 7. Second review

1. **A nil AuthBytes was encoded as null.** The Java client refused every
   failure response: "non-nullable field authBytes was serialized as
   null". It then reported a lost connection instead of the
   authentication error. franz-go and librdkafka did not mind. The
   field is not nullable in the schema. `Writer.Bytes` writes nil as
   null, which is right for nullable fields, so the SASL encoders now
   pass an empty slice. Found by the Java test; regression
   `TestSaslAuthBytesNeverNull`.
2. **The first failure tests went through a producer,** which retries
   until its deadline and reports the deadline, not the cause. A failed
   login looked like a timeout and would have passed for the wrong
   reason. The refusal checks now send one Metadata request and require
   `SASL_AUTHENTICATION_FAILED` itself.
3. **The simulator's seeds moved.** The two new APIs lengthen the
   ApiVersions response. The simulated network draws once per delivered
   fragment, so every seed after the first ApiVersions exchange is a
   different run. `TestShrink`'s seed 1 stopped failing under its
   mutation. That is not a lost catch: the mutation suite, over 200
   seeds, still catches every mutation. `TestShrink` now uses seed 4
   (39 faults shrink to 1, rule S3), and the reason is in a comment
   there. This is the same root cause the A3 report gives for shrinking
   failing on long chains: one random stream for everything.
4. **A failed login must not let the next request in.** The handler
   answers and then closes. It runs inside `pump`, which stops at a
   closed connection, so a request already buffered behind the failed
   login is never dispatched. `TestSASLRefusals` checks the close on each
   path.
5. **Unknown users.** Failing at the client-first message for an unknown
   user is simpler. It was considered and rejected, because it would tell
   a client which users exist. Kafka continues with a fake credential
   (`ScramSaslServer`). This does too, and the fake salt is stable per
   name, so asking twice does not reveal it either.
6. **Timing.** Proofs and PLAIN keys are compared with
   `subtle.ConstantTimeCompare`. PLAIN for an unknown user still costs a
   derivation.
7. **The test certificates** are made per run by the standard library,
   valid for a day, ECDSA P-256. Nothing is committed to the repository,
   so there are no keys to leak and none that expire in CI.
8. **No mutation switch.** Roadmap rule 6 asks for one for every safety
   step that the simulator checks. Authentication is not in the
   simulator. Its negative cases are explicit tests against three real
   clients, which is the stronger evidence here.
9. **CI found two seeds failing S6** (1946, 2496) once the seeds moved.
   Both were the monitor, not the broker: a leader of an older term,
   elected by a late vote answer, while a newer leader held its lease.
   Raft allows that; such a leader cannot commit or lease. S6 now compares
   terms. `docs/BUGS.md` M10.
