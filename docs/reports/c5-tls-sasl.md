# C5 report: TLS and SASL

Branch `c5-tls-sasl`, 2026-11-14. Production plan item 7.
Design: `docs/design/c5-tls-sasl.md`. ADR 0019.

## What was built

- **TLS for clients:** `-tls-cert`, `-tls-key`, TLS 1.2 or later. With
  `-tls-client-ca`, client certificates are required.
- **Mutual TLS between nodes:** `-peer-tls-cert`, `-peer-tls-key`,
  `-peer-tls-ca`; `realsys.Loop.PeerTLS` for the dialing side.
- **SASL in the core:** SaslHandshake v1 and SaslAuthenticate v0–2, read
  from the official schemas. Mechanisms: PLAIN, SCRAM-SHA-256 and
  SCRAM-SHA-512.
  - Before authentication, only ApiVersions and the SASL requests are
    accepted.
  - Every failure answers with Kafka's code and text, then closes the
    connection.
  - Unknown users get a full SCRAM exchange with a stable fake
    credential.
- **`internal/scram`:** the pure SCRAM server, plus a client for tests. It
  reproduces RFC 7677's example exchange.
- **`samsa user add`:** writes SCRAM credentials from a password on stdin,
  file mode 0600. PLAIN checks passwords against the stored SCRAM keys,
  so no password is stored.
- **Purity allowlist:** gained the deterministic crypto packages (not
  `crypto/rand`).

## Results

| | |
|---|---|
| franz-go v1.22.3 over TLS | SCRAM-SHA-256, SCRAM-SHA-512 and PLAIN log in; produce and fetch work |
| Java client (Kafka 4.4 console producer and consumer), SASL_SSL, SCRAM-SHA-512, PEM truststore | three records produced and consumed; a wrong password gives `SaslAuthenticationException` |
| kcat (librdkafka 2.16.0), SASL_SSL, SCRAM-SHA-256 | produce and consume; a wrong password is refused |
| Refused | wrong password (SCRAM and PLAIN), unknown user, no SASL, plaintext to a TLS listener, a client that does not trust the broker's CA, a client without a certificate under `-tls-client-ca`, a peer certificate from another CA |
| Three-node cluster over mutual TLS | elects, replicates, takes 50 records |
| No password in the broker's log | checked in `TestTLSAndSASL` |
| Bug found by the Java client | a nil AuthBytes was encoded as null; Java refused every failure response. Fixed and covered by a regression test |
| Simulator | SASL off draws nothing. The longer ApiVersions response moved every seed; `TestShrink` moved from seed 1 to seed 4 (39 faults to 1). The mutation suite still catches every mutation |

## How to run it

    ./bin/samsa user add -file users -name alice   # type the password
    ./bin/samsa -listen 127.0.0.1:9093 -data d -tls-cert broker.pem -tls-key broker-key.pem \
        -sasl-mechanisms SCRAM-SHA-512 -sasl-users users
    kcat -b 127.0.0.1:9093 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 \
        -X sasl.username=alice -X sasl.password=... -X ssl.ca.location=ca.pem -L
    cd integration && go test -run 'TLS|SASL|ClientCertificates' -v .

## What is untested or weak

- **Credentials need a restart to change.** They are not yet replicated
  (AlterUserScramCredentials, API 51); that goes with ACLs in item 8.
- **PLAIN costs one PBKDF2 on the node's only thread** per login, a few
  milliseconds. A flood of PLAIN logins slows the node. SCRAM puts that
  work on the client.
- **The node id on a peer connection is believed.** The certificate
  proves cluster membership, not which node is speaking.
- **No re-authentication, no OAUTHBEARER, no GSSAPI, no SaslHandshake
  version 0.**
- **No certificate reload:** a renewed certificate needs a restart.
- **Not run in the simulator.** Authentication never reaches the log, but
  its interaction with leader changes (a client re-logging in to the new
  leader) is shown only by the real-client tests.
- **The 24-hour soak** (item 6) ran without TLS; TLS connection churn has
  not been soaked.

## Ten questions

1. Why is TLS in the I/O layer but SASL in the core?
2. What does the server do for an unknown SCRAM user, and why not fail at
   once?
3. Why can a stolen users file not be used to log in, even with PLAIN
   enabled?
4. Why is SaslHandshake version 0 not supported, and what would it take?
5. What exactly does a peer certificate prove, and what does it not?
6. Why does a failed login answer before closing, and how is a request
   buffered behind it kept from running?
7. Why did adding two APIs change every simulator seed?
8. What did the Java client find that franz-go and kcat did not, and why
   did they not mind?
9. Why were the first refusal tests misleading, and how were they fixed?
10. What would replicated credentials change, and why do they belong with
    ACLs?
