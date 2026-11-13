# C8 design: ACLs

Status: reviewed, 2026-11-14. Production plan item 8
(`docs/PRODUCTION.md`): "ACLs (replicated commands; the Kafka ACL APIs).
Must show: every API has an allowed and a denied test; default-deny mode."
Builds on item 7, which supplies the authenticated principal.

## 1. Sources (rule 5)

Everything below was read on 2026-11-14 from the Apache Kafka repository
(trunk), except the operations table, which comes from the 4.0 docs.

- **Schemas:** `CreateAclsRequest/Response.json` (key 30),
  `DeleteAclsRequest/Response.json` (31), `DescribeAclsRequest/Response.json`
  (29).
  - All three accept versions 1–3. Version 0 was removed in Kafka 4.0.
  - Flexible from version 2. Version 3 adds the USER resource type.
  - Pattern types arrive from version 1, default LITERAL (3).
  - In filters, name, principal and host are nullable ("match any").
  - Responses carry ThrottleTimeMs. Create results have ErrorCode and a
    nullable ErrorMessage. Delete results list the matching ACLs, each
    with its own error.
- **Enums** (`AclOperation`, `AclPermissionType`, `ResourceType`,
  `PatternType`):
  - operations: UNKNOWN 0, ANY 1, ALL 2, READ 3, WRITE 4, CREATE 5,
    DELETE 6, ALTER 7, DESCRIBE 8, CLUSTER_ACTION 9, DESCRIBE_CONFIGS 10,
    ALTER_CONFIGS 11, IDEMPOTENT_WRITE 12, CREATE_TOKENS 13,
    DESCRIBE_TOKENS 14, TWO_PHASE_COMMIT 15;
  - permissions: UNKNOWN 0, ANY 1, DENY 2, ALLOW 3;
  - resources: UNKNOWN 0, ANY 1, TOPIC 2, GROUP 3, CLUSTER 4,
    TRANSACTIONAL_ID 5, DELEGATION_TOKEN 6, USER 7;
  - patterns: UNKNOWN 0, ANY 1, MATCH 2, LITERAL 3, PREFIXED 4.
- **Errors** (`Errors.java`): TOPIC_AUTHORIZATION_FAILED 29,
  GROUP_AUTHORIZATION_FAILED 30, CLUSTER_AUTHORIZATION_FAILED 31,
  INVALID_REQUEST 42, TRANSACTIONAL_ID_AUTHORIZATION_FAILED 53,
  SECURITY_DISABLED 54.
- **Matching** (`metadata/.../authorizer/StandardAuthorizerData.java`):
  - **Any matching DENY wins** over any number of ALLOWs.
  - **ALL** matches every operation.
  - **Implied operations, in ALLOW ACLs only:** DESCRIBE is implied by
    DESCRIBE, READ, WRITE, DELETE and ALTER. DESCRIBE_CONFIGS is implied
    by DESCRIBE_CONFIGS and ALTER_CONFIGS. A DENY READ does *not* deny
    DESCRIBE.
  - **Principals:** the ACL's principal must be the request's principal
    or `User:*`.
  - **Hosts:** the ACL's host must be `*`, equal to the client's IP, or a
    CIDR range containing it.
  - **Resource names:** a LITERAL ACL matches its exact name, or every
    name if it is `*`. A PREFIXED ACL matches names that start with it.
    The resource being checked is always literal.
  - **Super users** are allowed everything.
  - **No ACL at all on the resource** gives the configured default
    (`allow.everyone.if.no.acl.found`; default DENIED). If ACLs exist on
    the resource but none matches this principal and host, the result is
    DENIED, whatever the default.
- **Per API**, from `KafkaApis.scala`, `ControllerApis.scala` and the docs
  table, all verbatim except where marked:

| API | Check | Denied |
|---|---|---|
| Produce | WRITE Topic. Nothing more for an idempotent producer: that check is InitProducerId's (corrected in review, finding 1) | TOPIC_AUTHORIZATION_FAILED per partition |
| Fetch (consumer) | READ Topic | TOPIC_AUTHORIZATION_FAILED per partition |
| ListOffsets | DESCRIBE Topic | TOPIC_AUTHORIZATION_FAILED per partition, timestamp and offset −1 |
| Metadata | DESCRIBE Topic | named topics: an entry with TOPIC_AUTHORIZATION_FAILED, zero id, no partitions. All-topics requests: unauthorized topics left out ("do not disclose the existence of topics unauthorized for Describe") |
| OffsetCommit | READ Group, then READ Topic | group: GROUP_AUTHORIZATION_FAILED for the whole request; topic: TOPIC_AUTHORIZATION_FAILED for its partitions; other topics still committed |
| OffsetFetch | DESCRIBE Group, then DESCRIBE Topic | group: GROUP_AUTHORIZATION_FAILED; named topics: TOPIC_AUTHORIZATION_FAILED, offset −1; all-topics requests leave unauthorized topics out |
| FindCoordinator | DESCRIBE Group | GROUP_AUTHORIZATION_FAILED, no node |
| JoinGroup, Heartbeat, LeaveGroup, SyncGroup | READ Group | GROUP_AUTHORIZATION_FAILED |
| DescribeGroups | DESCRIBE Group | that group's entry, GROUP_AUTHORIZATION_FAILED |
| ListGroups | DESCRIBE Cluster, else DESCRIBE each Group | without the cluster permission, only the groups it may describe; never an error |
| CreateTopics | CREATE Cluster, else CREATE Topic | TOPIC_AUTHORIZATION_FAILED, message "Authorization failed." |
| DeleteTopics | DELETE Cluster, else DESCRIBE and DELETE Topic | TOPIC_AUTHORIZATION_FAILED either way (by name) |
| DeleteRecords | DELETE Topic | TOPIC_AUTHORIZATION_FAILED per partition, low watermark −1 |
| CreatePartitions | ALTER Topic | TOPIC_AUTHORIZATION_FAILED |
| InitProducerId | IDEMPOTENT_WRITE Cluster, *or* WRITE on at least one topic (`authorizeByResourceType`, read verbatim) | CLUSTER_AUTHORIZATION_FAILED |
| DescribeConfigs | DESCRIBE_CONFIGS Cluster (broker resources) or Topic, per resource | CLUSTER_ or TOPIC_AUTHORIZATION_FAILED for that resource (`ConfigHelper`) |
| DescribeCluster | none: DESCRIBE Cluster only decides the optional authorized-operations field, which Samsa does not fill (`AuthHelper.computeDescribeClusterResponse`, corrected in review) | — |
| DescribeAcls | DESCRIBE Cluster | CLUSTER_AUTHORIZATION_FAILED; without an authorizer SECURITY_DISABLED |
| CreateAcls, DeleteAcls | ALTER Cluster | CLUSTER_AUTHORIZATION_FAILED per creation or filter; without an authorizer SECURITY_DISABLED |
| ApiVersions, SaslHandshake, SaslAuthenticate | none | — |

Every row was read in `KafkaApis.scala`, `ControllerApis.scala`,
`AclApis.scala`, `AuthHelper.java` or `ConfigHelper.scala` (trunk,
2026-11-14). Creation is validated as `AclControlManager.validateNewAcl`
and `AclApis.handleCreateAcls` do: resource type not UNKNOWN or ANY,
pattern LITERAL or PREFIXED, operation not UNKNOWN or ANY, permission
ALLOW or DENY, a non-empty name, the CLUSTER resource named
`kafka-cluster`, a principal with a colon, a non-empty host, and a valid
CIDR if the host has a slash. Filters follow `ResourcePatternFilter` and
`AccessControlEntryFilter` (MATCH included).

## 2. Design

- **ACLs are replicated state.** CreateAcls proposes one command per valid
  creation (`cmdCreateACL`), DeleteAcls one per filter (`cmdDeleteACLs`).
  - Apply keeps the ACLs as a slice in creation order, without
    duplicates. Creating an existing ACL succeeds and changes nothing, as
    in Kafka. A delete returns the ACLs it removed, for the answer.
  - Commands are validated before they are proposed. An invalid one in
    the log is a bug or a newer format: apply refuses it like any
    undecodable command.
  - Snapshots gain an ACL section (version 5; version 4 snapshots decode
    with no ACLs). The state hash covers the ACLs.
- **Who serves what.** Only the serving leader proposes, so a follower
  answers CreateAcls and DeleteAcls with NOT_CONTROLLER, as it does every
  write (review, finding 3). Any node answers DescribeAcls from its applied
  state, like Metadata.
- **The principal** is `User:<name>` after SASL, else `User:ANONYMOUS`,
  which is what Kafka uses for an unauthenticated client.
- **Hosts** (open question 1): bound to the connection's address.
  `sys.Handler.OnConnOpen` now carries the client's IP as text: the real
  loop takes it from the TCP address, the simulator gives `127.0.0.1`.
  Kafka's host-restricted ACLs work, CIDR included (`net/netip`, added to
  the purity allowlist: parsing, no I/O).
- **The authorizer** is a pure function of the ACL slice, the
  configuration, (principal, host), the operation and the resource
  (`internal/broker/acl.go`). `authorize` is `StandardAuthorizerData`'s
  rules; `authorizeAny` is `Authorizer.authorizeByResourceType`, used only
  by InitProducerId.
- **Configuration:** `-acl` turns it on. `-super-users User:admin,...`
  lists the super users. `-allow-if-no-acl` is Kafka's
  `allow.everyone.if.no.acl.found`. The default is deny: the plan's
  default-deny mode. Without `-acl`, every request is allowed as before,
  and the ACL APIs answer SECURITY_DISABLED. `-acl` without SASL logs a
  warning: everyone is `User:ANONYMOUS`.
- **Order of checks.** Where Kafka checks authorization before existence,
  so does Samsa: a topic the client may not describe gets
  TOPIC_AUTHORIZATION_FAILED whether it exists or not, and an all-topics
  Metadata leaves it out.

## 3. Decisions on the open questions

1. **Hosts:** bound to the connection address (above).
2. **Simulation:** no simulated client changes ACLs. ACL state is applied
   from the log like every other command. The replication argument (same
   log, same state) is R1, which the simulator already checks on 3000
   seeds per CI run. What is new is the encoding, apply and the decision,
   and those have unit tests, a fuzzer (snapshot decoding) and
   `TestACLsReplicate`, which checks the state hash on three nodes after
   an ACL change. A simulated client that changed ACLs would also move
   every seed again for little new evidence. Recorded as untested in the
   report.
3. **Replicated SCRAM credentials (API 51):** their own item, after this
   one, as plan item 8b. They need the users file to become replicated
   state, a migration from the file, and DescribeUserScramCredentials.
   This item is already large.

## 4. Tests

| Test | Shows |
|---|---|
| `TestAuthorize`, `TestAuthorizeAny`, `TestFilterMatches`, `TestValidACL` | each rule of section 1, one case per rule |
| `TestEveryAPIAllowedAndDenied` | 26 cases, every API that checks ACLs: refused with Kafka's code, allowed after exactly the ACL Kafka requires, refused again after DeleteAcls |
| `TestUncheckedAPIs` | ApiVersions and DescribeCluster need nothing; the SASL requests run before any principal exists |
| `TestMetadataHidesTopics`, `TestAllowIfNoACL`, `TestHostACL`, `TestACLAPIs`, `TestACLsOff` | topics hidden; both default modes; CIDR and exact hosts; validation, MATCH filters, deletes, restart; SECURITY_DISABLED |
| `TestACLCommands`, `TestStateEncoding`, `FuzzDecodeState` | apply, duplicates, snapshot version 5 |
| `TestACLsReplicate` | three nodes: an ACL created on the leader holds on a follower; equal state hashes; a follower refuses ACL changes |
| `integration.TestACLsFranzGo` | franz-go as alice: refused, then produces idempotently and consumes in a group with exactly the ACLs, refused again after the delete |
| `integration.TestACLsOtherClients` | `kafka-acls.sh --add`, `--list`, `--remove`; the Java console consumer refused, then allowed; kcat refused |

## 5. Second review

1. **Two rows of the draft table were wrong.** The draft said Produce
   needs IDEMPOTENT_WRITE on the cluster for an idempotent producer, and
   DescribeCluster needs DESCRIBE on the cluster. Reading the handlers
   verbatim showed otherwise. Since KIP-679, InitProducerId accepts
   IDEMPOTENT_WRITE *or* WRITE on any topic, and Produce checks only WRITE
   on the topic. DescribeCluster checks nothing; the permission only
   fills a field Samsa leaves at its default. Both were fixed before
   coding. Rule 5 exists for this: the docs table alone was not enough.
2. **The fuzzer found a C5 bug.** Adding the ACL messages to the fuzz
   kinds moved the corpus, and a null SaslAuthenticate AuthBytes turned
   out unstable through decode and encode. It was fixed on the C5 branch
   before that merged (`docs/design/c5-tls-sasl.md`, finding 10).
3. **ACL changes on a follower fail.** The Java admin client sends
   CreateAcls and DeleteAcls to the least-loaded broker
   (`LeastLoadedBrokerOrActiveKController` in `KafkaAdminClient`), not to
   the controller. A per-result error completes the future exceptionally,
   with no retry (read 2026-11-14). On a cluster, `kafka-acls.sh` therefore
   fails whenever it picks a follower. How often that happens was not
   measured. Kafka forwards these requests to its controller; Samsa has
   no forwarding. Not fixed here: forwarding a write to the leader is a
   general mechanism that every admin API wants. Recorded in the report.
   The tests change ACLs on the leader, or on a single node.
4. **Certificate principals.** With `-tls-client-ca` and no SASL, Kafka
   names a client by its certificate's subject (`User:CN=...`). Samsa
   calls it `User:ANONYMOUS`: the core never sees the certificate. Not
   done; recorded.
5. **IPv6 host text.** Kafka compares a host ACL to Java's
   `getHostAddress`, which writes IPv6 in full. Go writes it compressed
   (`::1`). An exact IPv6 host ACL written in Java's form does not match
   here. CIDR ranges and IPv4 are unaffected. Recorded.
6. **The seeds moved again,** for the same reason as in C5: three more APIs
   in ApiVersions. `TestShrink` moved from seed 4 to seed 5. Seed 3 also
   failed and shrank, but from 1 fault to none, which shows little.
   Seed 5 goes from 175 faults to 1, by rule S3. The 3000-seed run
   passes.
7. **An ALLOW checked against a DENY is order-free.** `authorize` returns
   DENIED on the first matching DENY, but only after the resource check,
   so the slice order never decides an answer. `authorizeAny` defers a
   wildcard ALLOW until every ACL has been seen, because a wildcard DENY
   later in the slice must still win, as in Kafka.
8. **Authorization failures are not logged.** Kafka has an audit log.
   Here a refusal is visible only to the client; each request still
   appears in the debug log. Recorded.
