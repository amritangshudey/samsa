# 0006 — Which Kafka API versions the broker speaks

Status: accepted, 2026-11-10

## Context

Each Kafka API is versioned separately. A client asks the broker for its
supported ranges (ApiVersions) and then uses, per API, the highest version
both support. Kafka 4.0 removed many old versions, so current clients have
minimums well above zero. Architecture rule 5 requires choosing versions
that the test clients actually negotiate, from the official definitions.

## Decision

Support, for each API, from Kafka 4's own minimum up to the highest
non-flexible version:

| API | Key | Accepted | Advertised |
|---|---|---|---|
| Produce | 0 | 3–8 | 0–8 |
| Fetch | 1 | 4–11 | 4–11 |
| ListOffsets | 2 | 1–5 | 1–5 |
| Metadata | 3 | 1–8 | 1–8 |
| ApiVersions | 18 | 0–4 | 0–4 |
| CreateTopics | 19 | 2–4 | 2–4 |

Source of every layout: `clients/src/main/resources/common/message/*.json`
in `apache/kafka` at `trunk`, read on 2026-11-10. The table lives in code as
`wire.APIs`.

Produce is advertised from 0 because Kafka does the same: librdkafka
derives feature support from the advertised range (KAFKA-18659).

## What the clients were observed to do

From the broker's debug log during `make integration` on 2026-11-10:

| Client | ApiVersions | Produce | Fetch | ListOffsets | Metadata | CreateTopics |
|---|---|---|---|---|---|---|
| franz-go 1.22.3 | 5, then 4 | 8 | 11 | 5 | 8 | 4 |
| Java client 4.4.0 | 5, then 4 | 8 | 11 | 5 | 8 | 4 |
| librdkafka 2.16.0 (kcat) | 3 | 8 | 11 | 5 | 8 | not used |

Every client used our maximum for every API, so the lower versions in each
range are exercised only by unit tests. franz-go and the Java client first
try ApiVersions 5, receive `UNSUPPORTED_VERSION` in a version-0 response,
and retry with 4: the fallback path is exercised by real clients.

CI installs kcat from Ubuntu's packages, which carries an older librdkafka
than the Homebrew one used here, so CI covers a second librdkafka version.

## Behaviour that is not in the definitions

- A fetch with nothing to return must carry an empty record set, not null,
  although the field is declared nullable: librdkafka rejects null
  (`docs/BUGS.md`, C1).
- Fetch sessions (version 7+) are declined by answering session id 0. All
  three clients then send full fetch requests.

## Alternatives

- **Flexible versions only** (the newest of each API). Rejected: Produce
  and Fetch 13 identify topics by id, and newer Fetch versions carry
  replication fields the broker would have to understand.
- **Generate codecs for every version from the definitions.** Rejected for
  now; see `docs/design/phase-2.md`, section 5.

## Consequences

- Clients must disable the idempotent producer, which needs an API the
  broker does not have. With it enabled (the default in the Java client and
  franz-go) they fail at start instead of silently losing a guarantee.
- If a future client release drops the non-flexible versions, the ranges
  must move up. The primitives already implement the flexible encoding.
- No claim is made for clients or client versions outside the table above.
