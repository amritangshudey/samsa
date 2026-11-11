# 0008 — Which versions of the consumer group APIs the broker speaks

Status: accepted, 2026-11-11

## Context

Phase 4 adds the seven APIs of the classic consumer group protocol.
ADR 0006 set the rule for choosing versions: from Kafka 4's own minimum up
to the highest non-flexible version. The roadmap also asks what current
clients do by default, since Kafka now has a second group protocol
(KIP-848) in which the broker computes assignments.

## Decision

| API | Key | Accepted and advertised |
|---|---|---|
| OffsetCommit | 8 | 2–7 |
| OffsetFetch | 9 | 1–5 |
| FindCoordinator | 10 | 0–2 |
| JoinGroup | 11 | 0–5 |
| Heartbeat | 12 | 0–3 |
| LeaveGroup | 13 | 0–3 |
| SyncGroup | 14 | 0–3 |

Layouts are from `clients/src/main/resources/common/message/*.json` in
`apache/kafka` at `trunk`, read on 2026-11-10. The broker does not
advertise ConsumerGroupHeartbeat (key 68), the entry point of KIP-848.

Static membership (`group.instance.id`, in the newest of these versions)
is read and ignored: such a member is treated as a dynamic one.

## What the clients were observed to do

From the broker's debug log during `make integration` on 2026-11-11:

| Client | FindCoordinator | JoinGroup | SyncGroup | Heartbeat | LeaveGroup | OffsetCommit | OffsetFetch |
|---|---|---|---|---|---|---|---|
| franz-go 1.22.3 | 2 | 5 | 3 | 3 | 3 | 7 | 5 |
| Java client 4.4.0 | 2 | 5 | 3 | 3 | not seen (killed, or still running) | 7 | 5 |
| librdkafka 2.16.0 (kcat) | 2 | 5 | 3 | 3 | 1 | 7 | 5 |

- Every client used our maximum, except librdkafka's LeaveGroup. The lower
  versions are exercised by unit tests and by the simulator, whose clients
  pick a random supported version for every request.
- **The Java client's default is still the classic protocol.** With no
  `group.protocol` setting, 4.4.0 ran `ClassicKafkaConsumer` and never sent
  key 68. No fallback was observed because none was needed. What happens
  with `group.protocol=consumer` against this broker is not tested.
- All three go through the `MEMBER_ID_REQUIRED` round trip (JoinGroup 4+).
- Each client ran with its default assignment strategy. Which one that is
  was not inspected: strategies are opaque bytes to the broker.

## Alternatives

- **Flexible versions as well** (JoinGroup 6+, OffsetFetch 6–9, …). Not
  needed by any test client; OffsetFetch 8+ and FindCoordinator 4+ change
  shape to batch several groups per request.
- **Implement KIP-848.** A second protocol with broker-side assignment;
  out of scope (roadmap 3.2).

## Consequences

- Admin tools cannot list or describe groups: DescribeGroups and
  ListGroups are not implemented.
- No claim is made for clients, versions or settings outside the table.
