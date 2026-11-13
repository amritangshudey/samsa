# C8b design: replicated SCRAM credentials, forwarded ACL changes

Status: reviewed, 2026-11-14. Production plan item 8b
(`docs/PRODUCTION.md`): "Replicated SCRAM credentials
(AlterUserScramCredentials, DescribeUserScramCredentials) and forwarding
admin writes from followers to the leader. Must show: a credential change
takes effect on every node without a restart; `kafka-acls.sh` and
`kafka-configs.sh` work against any node of a cluster." It closes the two
gaps that the C5 and C8 reports left open.

## 1. Sources (rule 5)

Read on 2026-11-14 from the Apache Kafka repository (trunk).

- **Schemas:** `AlterUserScramCredentials{Request,Response}.json` (key 51)
  and `DescribeUserScramCredentials{Request,Response}.json` (key 50).
  - Both have version 0 only, flexible from 0.
  - Alter carries Deletions (Name, Mechanism) and Upsertions (Name,
    Mechanism, Iterations, Salt, SaltedPassword). Its response has one
    result per affected user: User, ErrorCode, nullable ErrorMessage.
  - Describe takes a nullable Users array (null or empty means all). Its
    response has a top-level ErrorCode and ErrorMessage, then per user:
    User, ErrorCode, ErrorMessage, and CredentialInfos (Mechanism,
    Iterations).
- **Mechanisms** (`clients/admin/ScramMechanism.java`): UNKNOWN 0,
  SCRAM_SHA_256 1, SCRAM_SHA_512 2.
- **Errors** (`Errors.java`): RESOURCE_NOT_FOUND 91, DUPLICATE_RESOURCE 92,
  UNACCEPTABLE_CREDENTIAL 93, UNSUPPORTED_SASL_MECHANISM 33.
- **Validation and apply** (`ScramControlManager.alterCredentials`):
  - **A user named twice** in one request (in any mix of deletions and
    upsertions) gets DUPLICATE_RESOURCE, and none of its changes is
    applied.
  - **Empty name:** UNACCEPTABLE_CREDENTIAL, "Username must not be empty".
  - **Unknown mechanism:** UNSUPPORTED_SASL_MECHANISM.
  - **Iterations** below the mechanism's minimum (4096) or above 16384:
    UNACCEPTABLE_CREDENTIAL.
  - **Deleting a credential that does not exist:** RESOURCE_NOT_FOUND.
  - **The stored form** is StoredKey = H(HMAC(SaltedPassword, "Client
    Key")) and ServerKey = HMAC(SaltedPassword, "Server Key"). The salted
    password itself is not kept.
  - **The valid changes of one request** are applied as one atomic batch.
- **Which node gets each request** (`KafkaAdminClient`):
  - `alterUserScramCredentials`: `ControllerNodeProvider`, so the node
    that Metadata names as controller. Samsa names the leader, so these
    reach the leader already.
  - `describeUserScramCredentials`: `LeastLoadedNodeProvider`.
  - `createAcls` and `deleteAcls`: `LeastLoadedBrokerOrActiveKController`,
    so any broker. A per-result error fails the call, with no retry.
  - So only CreateAcls and DeleteAcls need forwarding.

## 2. Replicated credentials

- **A command per AlterUserScramCredentials request** (`cmdAlterCredentials`)
  holds its valid deletions and upsertions. It is applied atomically, as
  Kafka applies the batch. The leader converts each salted password into
  the stored key and server key before proposing (`scram.FromSalted`), so
  the salted password never reaches the log.
- **State:** a map from user to its credentials, one per mechanism. It is
  kept in snapshots (version 6) and in the state hash, and iterated in
  sorted order.
- **Login looks in the replicated credentials first, then in the
  `-sasl-users` file.** Both are per-node lookups that change no state.
- **The file owns its names.** AlterUserScramCredentials refuses a name
  that the file defines, with UNACCEPTABLE_CREDENTIAL: "user is defined
  in -sasl-users". Otherwise a delete through the API would leave the
  file's credential in force, and nobody would expect that. The intended
  use is a file that holds only the administrators, with everyone else
  created through the API.
- **Describe** lists file users and replicated users together. A name
  that exists in neither gets RESOURCE_NOT_FOUND, and a name asked twice
  gets DUPLICATE_RESOURCE.
- **Authorization:** Alter needs ALTER on the cluster and Describe needs
  DESCRIBE on the cluster (`ControllerApis`, `KafkaApis`), each refused
  with CLUSTER_AUTHORIZATION_FAILED. Without `-acl`, both are allowed, as
  everything is.
- **Without SASL** these APIs still work: credentials are state, and a
  node started later with SASL uses them.

## 3. Forwarding CreateAcls and DeleteAcls

A follower cannot propose, so it asks the leader to propose for it:

1. **The follower authorizes and validates** the request against its own
   applied state, as for any request. It encodes the commands exactly as
   the leader would.
2. **It sends `msgForward`** to the node it believes leads, on the peer
   channel. The message holds a 64-bit tag drawn from `sys.Random` and
   the command. Broker messages start with the byte 0xB0, and Raft
   messages with their version byte, 1. An older node discards a broker
   message as a Raft message of an unknown version.
3. **The leader**, if it is serving, proposes `cmdForwarded{origin, tag,
   command}`. If it is not serving, it answers `msgForwardRefused{tag}`.
4. **Every node applies `cmdForwarded`** by applying the inner command.
   The tag changes no state. The node whose id is the origin finds its
   waiting request by the tag and completes it with the result of its own
   apply. No result travels back over the network, and a delete's list
   of matched ACLs comes out of the follower's own apply.
5. **The follower answers NOT_CONTROLLER** in three cases:
   - it knows no leader;
   - the leader refuses;
   - nothing applies within `forwardTimeout` (10 s), because the message
     or the leader was lost.

   That covers the same cases as before, but now only when there is a
   real reason.

Why this is safe:

- **ACL creates and deletes are idempotent,** so a forward that times out
  and is retried by the client, and also applies late, does no harm.
  This is why forwarding is limited to these commands. A produce
  forwarded twice would duplicate records.
- **Tags are random.** A tag from a waiter that was lost in a restart
  cannot match a new waiter, except with probability 2⁻⁶⁴.
- **The follower authorizes on its applied state,** which may be a
  little behind the leader's. A principal whose ALTER on the cluster was
  just revoked can still change ACLs through a lagging follower, for as
  long as that follower has not applied the revocation. Kafka authorizes
  at the controller and has no such window. Recorded in section 5,
  finding 2.

## 4. Tests

- **wire:** round trip, truncation and fuzz entries for the four new
  messages.
- **scram:** `FromSalted` gives the same credential as `NewCredential` for
  the same password and salt.
- **broker:**
  - every validation case of section 1;
  - a credential created through the API logs in, and so does one
    replaced;
  - a deleted one no longer logs in;
  - a name from the file is refused;
  - describe, including RESOURCE_NOT_FOUND and DUPLICATE_RESOURCE;
  - authorization allowed and denied for both APIs;
  - snapshot version 6;
  - forwarding on three nodes: an ACL change sent to a follower applies
    everywhere and answers the follower's client. With the leader cut
    off, the follower answers NOT_CONTROLLER after the timeout. With no
    leader, it answers at once.
- **integration, a three-process cluster with SASL and ACLs:**
  - `kafka-acls.sh --add` sent to a follower;
  - `kafka-configs.sh --alter --add-config 'SCRAM-SHA-256=[password=...]'
    --entity-type users --entity-name carol`;
  - carol logs in on every node without a restart;
  - `kafka-configs.sh --describe` lists carol;
  - `--delete-config` locks her out.

## 5. Second review

1. **Is forwarding needed for AlterUserScramCredentials?** No. The Java
   admin client, and so `kafka-configs.sh`, sends it to the controller,
   which Samsa reports as the leader. A client that sends it to a
   follower gets NOT_CONTROLLER. Forwarding it as well looks like one
   more case, but it is not idempotent the way ACL changes are. A
   retried upsert that also applies late is harmless, since it writes
   the same credential twice. A retried delete, though, answers
   RESOURCE_NOT_FOUND to a client whose first delete in fact worked. So
   it is not forwarded.
2. **The stale-authorization window** (section 3) is real but bounded by
   apply lag, usually milliseconds. Closing it means authorizing on the
   leader, which needs the principal and host in `msgForward` and an
   error path back. Recorded in the report as a known difference from
   Kafka.
3. **The salted password crosses the network.** AlterUserScramCredentials
   carries it in the request, as in Kafka. Without TLS it is readable,
   and it is enough to log in. The node logs a warning when SASL runs
   without TLS (C5), and the guide says to use TLS.
4. **Determinism.** The leader computes the stored and server keys before
   proposing, so apply does no PBKDF2 and the keys in the log are what
   every node uses. Apply only copies them. HMAC is deterministic anyway,
   but this keeps apply cheap.
5. **The simulator does not exercise forwarding.** Peer messages of the
   new kind would come only from ACL changes, which no simulated client
   sends (C8, section 3). The forwarding path is pure node logic over the
   same peer channel. It is tested on three nodes in the unit harness,
   with dropped messages and a lost leader, and on three real processes.
   Recorded.

## 6. Found while testing

1. **`kafka-configs.sh --describe --entity-type users` also sends
   DescribeClientQuotas (key 48).** `ConfigCommand.describeClientQuotaAndUserScramCredentialConfigs`
   asks for quotas first, then credentials, and fails on the first error.
   Samsa has no quotas. It now answers key 48 (versions 0–1, flexible from
   1, per `DescribeClientQuotas{Request,Response}.json`, trunk, read
   2026-11-14) with an empty list. Authorization is Kafka's: DESCRIBE_CONFIGS
   on the cluster, or CLUSTER_AUTHORIZATION_FAILED (`KafkaApis.handleDescribeClientQuotasRequest`).
   An empty list is the true answer, since no quota exists. AlterClientQuotas
   is not added: setting a quota Samsa would not enforce would be a lie.
2. **The 3000-seed run found a Raft bug in leadership transfer** (BUGS.md
   S8), unrelated to this item: the new APIs only moved the seeds. A
   leader that abandoned a transfer took its lease back while its
   TimeoutNow could still arrive. Fixed in `internal/raft`, with a
   mutation switch and a unit test; the B3 design is corrected. Only seed
   1219 of 2000 catches the mutation in the simulator, so the unit test is
   its guard.
