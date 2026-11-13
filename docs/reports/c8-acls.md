# C8 report: ACLs

Branch `acls`, 2026-11-14. Production plan item 8.
Design: `docs/design/c8-acls.md`. ADR 0020.

## What was built

- **The ACL APIs:** CreateAcls, DeleteAcls and DescribeAcls, versions 1 to
  3, read from the official schemas.
- **ACLs as replicated state:** a command per creation and per delete
  filter. They are kept in snapshots (version 5) and covered by the
  state hash.
- **An authorizer with Kafka's rules** (`internal/broker/acl.go`), pure and
  table-tested rule by rule:
  - DENY wins;
  - ALL;
  - implied DESCRIBE and DESCRIBE_CONFIGS, for ALLOW only;
  - `User:*`;
  - hosts `*`, exact or CIDR;
  - literal, `*` and prefixed names;
  - super users;
  - the no-ACL default.
- **Every API checks what Kafka checks,** with Kafka's error codes, in
  Kafka's order. Topics a client may not describe are hidden.
- **The client's IP address** now reaches the core (`OnConnOpen`), so
  host-restricted ACLs work.
- **Flags:** `-acl`, `-super-users`, `-allow-if-no-acl`. Default deny.

## Results

| | |
|---|---|
| Every API that checks ACLs (26 cases) | refused with Kafka's code; allowed with exactly the ACL Kafka requires; refused again after DeleteAcls (`TestEveryAPIAllowedAndDenied`). Removing one check makes its case fail (tried on Produce) |
| Default deny and `-allow-if-no-acl` | both, including "ACLs exist for someone else, so denied" |
| franz-go v1.22.3 | refused, then an idempotent producer and a group consumer work with exactly WRITE, READ, group READ and IDEMPOTENT_WRITE; refused again after the delete |
| Kafka 4.4 `kafka-acls.sh` | `--add`, `--list` and `--remove` work; the console consumer is refused with an AuthorizationException, then allowed |
| kcat (librdkafka 2.16.0) | a produce without an ACL is refused |
| Three nodes | an ACL created on the leader holds on a follower; equal state hashes; a follower refuses changes with NOT_CONTROLLER |
| Versions negotiated | Java 4.4 and franz-go: version 3 of all three APIs |
| Simulator | 3000 seeds pass; `TestShrink` moved to seed 5 (175 faults shrink to 1, rule S3) |
| Found on the way | two wrong rows in the draft table (Produce, DescribeCluster), corrected from source before coding; a C5 wire bug found by the fuzzer, fixed before C5 merged |

## How to run it

    ./bin/samsa user add -file users -name admin    # and alice
    ./bin/samsa -listen 127.0.0.1:9092 -data d -sasl-mechanisms SCRAM-SHA-256 -sasl-users users \
        -acl -super-users User:admin
    kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --command-config admin.properties \
        --add --allow-principal User:alice --operation Write --topic orders
    go test ./internal/broker -run 'ACL|Authorize|EveryAPI'
    cd integration && go test -run TestACLs -v .

## What is untested or weak

- **ACL changes sent to a follower fail.** The Java admin client sends
  them to the least-loaded broker and does not retry NOT_CONTROLLER.
  Kafka forwards; Samsa does not yet. On a cluster, retry until the
  request reaches the leader.
- **No simulated client changes ACLs.** Replication of ACL commands rests
  on R1 (checked by the simulator for every other command), unit tests,
  and one three-node test.
- **Client certificates do not name a principal.** Without SASL, every
  client is `User:ANONYMOUS`, even under `-tls-client-ca`. Kafka would use
  the certificate subject.
- **IPv6 exact-host ACLs** compare against Go's compressed text, not
  Java's full form. CIDR ranges are unaffected.
- **No audit log** of refusals.
- **Not supported:** transactional-id and delegation-token resources
  (there are no transactions or tokens yet), authorized-operations
  fields (left at their "not requested" default), and replicated SCRAM
  credentials (item 8b).
- **OffsetFetch version 1** has no top-level error, so a group refusal is
  reported per partition there.

## Ten questions

1. Why is a DENY READ not a DENY DESCRIBE, while an ALLOW READ is an
   ALLOW DESCRIBE?
2. A topic has one ACL, for bob. Why is alice denied even with
   `-allow-if-no-acl`?
3. Why does Metadata answer TOPIC_AUTHORIZATION_FAILED for a topic that
   does not exist, instead of UNKNOWN_TOPIC_OR_PARTITION?
4. What does InitProducerId require without a transactional id, and why
   is Produce not checked for IDEMPOTENT_WRITE?
5. Why are ACLs commands in the log rather than a file, like the SASL
   users?
6. What changed in `sys.Handler` and why was that the smaller choice for
   hosts?
7. Why does a follower answer CreateAcls with NOT_CONTROLLER, and what
   does the Java admin client do with that?
8. Why does `authorizeAny` decide a wildcard ALLOW only after the loop?
9. Why did the shrink test's seed move again, and why seed 5 rather than
   seed 3?
10. What would simulating ACL changes add, and why was it not done?
