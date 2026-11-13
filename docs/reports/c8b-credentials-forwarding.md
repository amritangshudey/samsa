# C8b report: replicated SCRAM credentials, forwarded ACL changes

Branch `credentials`, 2026-11-14. Production plan item 8b.
Design: `docs/design/c8b-credentials-forwarding.md`. ADR 0021.

## What was built

- **SCRAM credential APIs:** DescribeUserScramCredentials (50) and
  AlterUserScramCredentials (51), version 0, read from the official
  schemas. Validation follows `ScramControlManager`:
  - a name given twice in one request: DUPLICATE_RESOURCE;
  - an empty name: UNACCEPTABLE_CREDENTIAL;
  - an unknown mechanism: UNSUPPORTED_SASL_MECHANISM;
  - iterations outside 4096–16384: UNACCEPTABLE_CREDENTIAL;
  - deleting a credential that does not exist: RESOURCE_NOT_FOUND.
- **Credentials as replicated state:**
  - one atomic command per request;
  - the leader derives the stored and server keys before proposing;
  - snapshot version 6, and part of the state hash.
- **Login uses both sources.** It checks the `-sasl-users` file first,
  then the replicated credentials. The API refuses names that the file
  defines.
- **Forwarding:** a follower sends CreateAcls and DeleteAcls to the leader
  on the peer channel. It completes its client's request from its own
  apply of `cmdForwarded`. It answers NOT_CONTROLLER without a leader, on
  refusal, or after 10 s.
- **DescribeClientQuotas (48),** always empty, because
  `kafka-configs.sh --describe` needs it.
- **A Raft fix found on the way** (BUGS.md S8): a leader that abandons a
  transfer after sending TimeoutNow now steps down instead of taking its
  lease back. New mutation `MutAbandonKeepsLeading`, caught by
  `TestTimeoutNowAfterAbandon`. The simulator catches it in only one seed
  of 2000, so it is not in the mutation suite.

## Results

- `TestCredentialsAndForwarding` runs on a three-process cluster with SCRAM
  and ACLs, all against a follower:
  - a raw CreateAcls and `kafka-acls.sh --add` succeed, and `--list` shows
    both ACLs;
  - `kafka-configs.sh` creates carol;
  - carol logs in on each of the three nodes without a restart, and sees
    only the topic her ACLs allow;
  - `--describe` lists her;
  - `--delete-config` locks her out of every node.
- Unit tests:
  - every validation case;
  - lifecycle (create, replace, delete, log in);
  - allowed and denied for 50, 51 and 48;
  - replication to a follower;
  - forwarding through a follower, with timeout and refusal;
  - snapshot version 6.
- The simulator: 3000 seeds pass (8.8 million requests, 3485 handovers). The new APIs moved every seed. Seed 1219
  found S8; `TestShrink` moved to seed 6 (385 faults shrink to none).

## How to run it

```sh
make test
(cd integration && go test -run TestCredentialsAndForwarding .)   # needs a JDK and the Kafka tools
samsa -sasl-mechanisms SCRAM-SHA-256 -sasl-users admins -acl -super-users User:admin ...
kafka-configs.sh --bootstrap-server any-node:9092 --command-config admin.properties \
  --alter --add-config 'SCRAM-SHA-256=[password=secret]' --entity-type users --entity-name carol
```

## What is untested or weak

- **Stale authorization on a follower.** A follower authorizes a forwarded
  ACL change against its own applied state. A principal whose ALTER was
  just revoked can still make one change through a follower that has not
  applied the revocation yet. Kafka authorizes at the controller.
- **The simulator does not send ACL or credential requests,** so
  forwarding is tested only in the unit harness and on real processes,
  not under the simulator's faults.
- **SCRAM-SHA-512 through the API** is tested in unit tests only. The
  integration test uses SHA-256.
- **The salted password crosses the network** in AlterUserScramCredentials,
  as in Kafka. Use TLS.
- **No client quotas.** DescribeClientQuotas answers an empty list, and
  AlterClientQuotas does not exist.
- **The simulator rarely makes S8's schedule.** One seed in 2000 catches
  `MutAbandonKeepsLeading`. A transfer must be abandoned after TimeoutNow
  goes out, and then the message must arrive late. A fault that delays
  TimeoutNow on purpose would make the schedule common; that is open.
- **The S8 fix can cost availability.** A graceful stop whose transfer
  target is slow now leaves the cluster without a leader until a normal
  election, about one to two election timeouts. Before the fix the old
  leader went on serving.

## Ten questions

1. Why does the leader compute the stored key and server key before
   proposing, rather than putting the salted password in the log?
2. Why are ACL changes forwarded but AlterUserScramCredentials is not?
   What would go wrong with a retried, forwarded delete?
3. How does the follower learn the result of a forwarded command without
   the leader sending it back?
4. Why is the forward tag random, and what happens to a tag whose waiter
   was lost in a restart?
5. A broker message and a Raft message share the peer channel. How does
   each side tell them apart, and what does an older node do with a broker
   message?
6. Why does the API refuse names from the `-sasl-users` file instead of
   letting the replicated credential override them?
7. Why does `kafka-configs.sh --describe` need DescribeClientQuotas, and
   why is an empty answer honest while AlterClientQuotas would not be?
8. Explain BUGS.md S8. What assumption in the B3 design was false, and why
   does stepping down fix it while a timeout on TimeoutNow would not?
9. When does a leader that abandons a transfer still serve again, and why
   is that safe?
10. What is the stale-authorization window on a follower, how long can it
    last, and what would closing it cost?
