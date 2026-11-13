# C8 design: ACLs

Status: draft, sources verified, 2026-11-14. Production plan item 8
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
| Produce | WRITE Topic (IDEMPOTENT_WRITE Cluster for an idempotent producer) | TOPIC_AUTHORIZATION_FAILED per partition |
| Fetch (consumer) | READ Topic | TOPIC_AUTHORIZATION_FAILED per partition |
| ListOffsets | DESCRIBE Topic | TOPIC_AUTHORIZATION_FAILED per partition, timestamp and offset −1 |
| Metadata | DESCRIBE Topic | named topics: an entry with TOPIC_AUTHORIZATION_FAILED, zero id, no partitions. All-topics requests: unauthorized topics left out ("do not disclose the existence of topics unauthorized for Describe") |
| OffsetCommit | READ Group, then READ Topic | group: GROUP_AUTHORIZATION_FAILED for the whole request; topic: TOPIC_AUTHORIZATION_FAILED for its partitions; other topics still committed |
| OffsetFetch | DESCRIBE Group, then DESCRIBE Topic | as OffsetCommit (docs table; the handler was not read verbatim) |
| FindCoordinator | DESCRIBE Group | GROUP_AUTHORIZATION_FAILED, no node |
| JoinGroup, Heartbeat, LeaveGroup, SyncGroup | READ Group | GROUP_AUTHORIZATION_FAILED (docs table) |
| DescribeGroups | DESCRIBE Group | that group's entry, GROUP_AUTHORIZATION_FAILED |
| ListGroups | DESCRIBE Cluster, else DESCRIBE each Group | without the cluster permission, only the groups it may describe; never an error |
| CreateTopics | CREATE Cluster, else CREATE Topic | TOPIC_AUTHORIZATION_FAILED, message "Authorization failed." |
| DeleteTopics | DELETE Cluster, else DESCRIBE and DELETE Topic | TOPIC_AUTHORIZATION_FAILED either way (by name) |
| DeleteRecords | DELETE Topic | TOPIC_AUTHORIZATION_FAILED per partition, low watermark −1 |
| CreatePartitions | ALTER Topic | TOPIC_AUTHORIZATION_FAILED |
| InitProducerId | IDEMPOTENT_WRITE Cluster (no transactional id here) | CLUSTER_AUTHORIZATION_FAILED (docs table; the error was not read verbatim) |
| DescribeConfigs | DESCRIBE_CONFIGS Cluster or Topic, per resource | per resource (docs table) |
| DescribeCluster | DESCRIBE Cluster | CLUSTER_AUTHORIZATION_FAILED (docs table) |
| DescribeAcls | DESCRIBE Cluster | CLUSTER_AUTHORIZATION_FAILED |
| CreateAcls, DeleteAcls | ALTER Cluster | per creation or filter |
| ApiVersions, SaslHandshake, SaslAuthenticate | none | — |

Rows marked "docs table" have their operation and resource verified. The
exact error code for those is to be confirmed against a real client in the
tests before it is relied on.

## 2. Design (to be reviewed)

- **ACLs are replicated state.** CreateAcls and DeleteAcls propose
  commands. They are applied in order, kept in snapshots, and included in
  the state hash. Every node authorizes from the same applied set. A node
  that is not serving refuses ACL changes, as it does any other write.
- **The principal** is `User:<name>` from SASL, or `User:ANONYMOUS` without
  it. That is what Kafka uses for an unauthenticated listener.
- **Hosts.** The core does not know a connection's address:
  `OnConnOpen` passes only an id. Two options:
  1. Give `OnConnOpen` the peer IP. This changes the `sys.Network`
     contract and both implementations.
  2. Accept only `*` hosts, and refuse others with INVALID_REQUEST.

  Leaning to (1). Kafka ACLs restricted by host are common, and the
  simulator can give each client an address.
- **Configuration:** `-acl` enables authorization.
  `-super-users User:admin,...` lists the super users.
  `-allow-if-no-acl` matches Kafka's `allow.everyone.if.no.acl.found`;
  the default is deny, the plan's "default-deny mode".
- **Authorizer:** a pure function of (ACL set, principal, host, operation,
  resource). It implements `StandardAuthorizerData`'s rules above, and is
  table-tested against each rule.
- **Snapshot format:** gains an ACL section. Item 10 (upgrades) will need
  format versions anyway. This item adds a version byte if there is none.

## 3. Open questions for the review

1. Bind host matching to the connection address (option 1), or `*` only?
2. Should ACL commands be simulated? The authorizer is pure, so its table
   tests carry the logic. The simulator would add "every node reaches the
   same decision after the same commands". That follows from R1, but it
   is cheap to check with a client that changes ACLs.
3. Replicated SCRAM credentials (API 51) were deferred from item 7 to sit
   with this item. Do them here, or as their own item?
