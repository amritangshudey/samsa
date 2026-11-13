# ADR 0021: Replicated SCRAM credentials, forwarded ACL changes

Date: 2026-11-14. Status: accepted. Design:
`docs/design/c8b-credentials-forwarding.md`.

## Context

Production plan item 8b. Credentials came from a file read at start (ADR
0019), so a change needed a restart of every node. ACL changes had to be
sent to the leader, so `kafka-acls.sh` failed when the admin client picked
a follower (ADR 0020). The wire formats and Kafka's validation were read
from Kafka's sources on 2026-11-14 (design, section 1).

## Decision

- **DescribeUserScramCredentials (50) and AlterUserScramCredentials (51),
  version 0, flexible.** Version 0 is the only one Kafka defines. The Java
  client 4.4 sends it.
- **Credentials are state applied from the log.** One
  `cmdAlterCredentials` holds all of a request's valid changes and applies
  them atomically, as Kafka does. The leader turns each salted password
  into the stored key and server key before proposing, so the log never
  holds a salted password and apply does no PBKDF2. They are kept in
  snapshots (version 6) and in the state hash.
- **Login checks the `-sasl-users` file first, then the replicated
  credentials.** The file owns its names: the API refuses them with
  UNACCEPTABLE_CREDENTIAL. A deleted API user then cannot leave a file
  credential behind it.
- **A follower forwards CreateAcls and DeleteAcls to the leader** over the
  peer channel (`msgForward`). The leader proposes `cmdForwarded{origin,
  tag, command}`. The origin node completes the waiting request from its
  own apply. It answers NOT_CONTROLLER if it knows no leader, if the
  leader refuses, or after 10 s. Only these idempotent commands are
  forwarded. AlterUserScramCredentials already goes to the leader, which
  Metadata names as controller. A retried delete would answer
  RESOURCE_NOT_FOUND for a delete that worked, so it is not forwarded.
- **DescribeClientQuotas (48), versions 0–1, always empty.**
  `kafka-configs.sh --describe` needs it. No quota exists, so the empty
  list is true. AlterClientQuotas is not added, because Samsa would not
  enforce a quota it accepted.

## Alternatives

- **Forward every write.** A forwarded produce or offset commit that times
  out and is retried could apply twice. ACL changes are the ones admin
  clients send to any node, and they are idempotent.
- **Return the leader's result over the network.** This needs a second
  message type and a path for lost answers. The origin's own apply already
  holds the same result.
- **Authorize forwarded changes on the leader.** That would close the
  stale-authorization window (design, section 5, finding 2). It needs the
  principal and host in the message and an error path back. Kept as a
  known difference from Kafka.
- **Reload the users file on a signal.** That still changes one node at a
  time, and is not the Kafka API that tools use.

## Consequences

- `kafka-configs.sh` creates, describes and deletes users, and
  `kafka-acls.sh` works against any node. Both are tested on a
  three-process cluster.
- A follower whose applied state lags can authorize an ACL change that the
  leader's newer state would refuse, for as long as the lag lasts.
- The salted password travels in the request, as in Kafka. Without TLS,
  anyone who can read it can log in.
- Every new API moves the simulator's seeds. Testing this item's new seeds
  found BUGS.md S8 in Raft leadership transfer.
