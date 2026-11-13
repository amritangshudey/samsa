# G7: rolling upgrade

Production plan item 10. Must show: N-1 to N with clients running, no
errors; format versions; an upgrade test between releases in CI.

## 1. The problem, measured against v0.2.0

A rolling upgrade restarts one node at a time on the new binary. For a
while, old and new nodes form one Raft group, and every byte one writes,
the other must read. Since v0.2.0 (the last tag), the new binary can
write five things v0.2.0 cannot read:

| What | Written when | What v0.2.0 does with it |
|---|---|---|
| Commands 11–14 (ACLs, credentials, forwarded) | an admin changes ACLs or SCRAM users | `apply` fails on an unknown type: the node stops |
| State snapshot versions 5 and 6 | every snapshot, whatever the state holds | an installed or local snapshot fails to decode: the node stops |
| Raft snapshot version 2 | every snapshot | `decodeSnapshot` rejects it: the node cannot install it |
| Configuration entries (`kindConfig`) | AddRaftVoter, RemoveRaftVoter | AppendEntries with that entry fails to decode and is dropped: the follower never catches up |
| Broker peer messages (0xB0) | forwarding | dropped, as an unknown Raft version (harmless) |

Checked in the v0.2.0 sources: `state.apply` returns `errBadCommand` for
an unknown type, `decodeState` and `decodeSnapshot` reject a newer
version, and `Raft.Step` drops messages that fail to decode and messages
from ids not in `-peers`.

The snapshots are the worst case. Every node snapshots every 10,000
entries regardless of features, so an old follower that installs a new
leader's snapshot stops. With one node restarting and one stopped, a
three-node cluster has no majority.

## 2. Design

### Snapshots: write the oldest version that holds the state

The formats only append fields, so an older version is a prefix:

- **State snapshot:** version 4 when there are no ACLs and no credentials
  (and the level is 0, below). Otherwise the current version.
- **Raft snapshot:** version 1 when the configuration is the bootstrap one
  (no members to store). Otherwise version 2.

A state that holds nothing new is then readable by v0.2.0. Encoders are
deterministic either way.

### Everything else: a cluster format level

Kafka has the same problem and solves it with `metadata.version`, a
finalized feature level in the log. Samsa does a smaller version of it.

- **`formatLevel`** is a constant in each build. It is 1 in this build:
  ACLs, credentials, forwarding and membership changes. v0.2.0 is level 0,
  implicitly. Each future change to a replicated format raises it.
- **The cluster's level lives in the state machine,** set by a new
  command, `cmdSetLevel`. It starts at 0.
- **Every node tells the leader the highest level it supports,** with a
  broker peer message (`msgLevel`). It sends one when it learns of a new
  leader, and again every second until the cluster reaches the node's
  level. v0.2.0 never sends one, so the leader counts it as 0.
- **The leader proposes `cmdSetLevel(L)`** once every voter in force has
  told it L or more, and the cluster is below L. An old node never sees
  the command, because none of the voters is old. A single node raises
  its level as soon as it serves.
- **Anything that writes a newer format checks the level first:**
  - CreateAcls and DeleteAcls;
  - AlterUserScramCredentials;
  - forwarding;
  - AddRaftVoter and RemoveRaftVoter.

  Below the level each needs, the request is refused with
  UNSUPPORTED_VERSION and a message: "the cluster runs at format level 0
  until every node runs a release that supports it". That is Kafka's code
  for a feature that is not finalized.
- **AddRaftVoter waits for the learner to report its level.** It must be
  at least the cluster's; until then the change keeps polling, as for
  catching up, and times out with REQUEST_TIMED_OUT. A node that cannot
  read the log never becomes a voter.
- **The level never goes down.** Downgrading below it is not supported,
  and the notes of each release say so.

### Who could be hurt, and how

- **During a rolling upgrade, old and new nodes run together until the
  last node is replaced.** Nothing they replicate is newer than v0.2.0
  can read: commands are gated, and snapshots fall back.
- **The level is a replicated command,** so all nodes agree on it, after
  restarts and snapshots too. It is stored in the state snapshot (version
  7), which is written only once the level is above 0.

### The test

`integration/upgrade_test.go`, with `SAMSA_OLD_BIN` pointing at the
previous release's binary (skipped without it):

1. Start three nodes on the old binary. Create a topic. A producer and a
   consumer run throughout.
2. Restart each node in turn on the new binary: SIGTERM, wait for exit,
   start the new binary on the same data directory.
3. While at least one old node remains, a credential change
   (AlterUserScramCredentials) is refused with UNSUPPORTED_VERSION, and
   the old nodes keep running. It is a credential change rather than
   CreateAcls because v0.2.0 has no `-acl` flag. As built, the test also
   moves leadership off an old node first, since only the leader answers
   admin writes (report, "Problems met").
4. After the last restart, the credential change succeeds.
5. Every acknowledged record is read back, dense, in order and once. The
   producer saw no error that a Kafka client does not retry.

In CI, the integration job builds the previous tag into a temporary
directory (`git worktree`, then `go build`, no new dependency) and sets
`SAMSA_OLD_BIN`. The checkout fetches tags.

## 3. Alternatives

- **Do nothing and document "do not use new features mid-upgrade".**
  Snapshots alone break it (section 1). And an operator cannot see that a
  follower forwarded an ACL change.
- **Kafka's explicit finalization (`kafka-features.sh upgrade`,
  UpdateFeatures).** It gives a window to roll binaries back before the
  new format is used. It costs another API and an operator step. The
  automatic level can be made explicit later by gating `cmdSetLevel` on a
  request.
- **Negotiate per message, as Raft does with its version byte.** Formats
  in the log outlive the nodes that wrote them, so the decision must be in
  the log, made once.

## 4. Second review

1. *Can the leader count a node as level 1 that is not?* Only if the node
   says so. Peer messages come from cluster members. Without peer TLS, the
   network is trusted as before (ADR 0019).
2. *What if a voter's report is lost?* It repeats every second until the
   cluster's level reaches its own.
3. *A leader that just took over has no reports yet.* It does not propose,
   and gated requests are refused until reports arrive, about one second
   at worst. A fresh single node is level 1 from its first served request.
4. *Could `cmdSetLevel` reach an old node through a snapshot, or through a
   node that was added?* Added nodes must report the cluster's level
   first. A snapshot that holds level 1 is version 7, and no old voter
   remains to install it.
5. *Is "oldest snapshot version that holds the state" deterministic?* The
   choice depends only on the state, so equal states still encode to
   equal bytes.
6. *The simulator runs one build,* so it cannot run an old node. It still
   checks that every run reaches level 1 once settled, and that gated
   requests are refused only below it. The cross-version test is the
   integration test.
7. *The test's "no errors":* franz-go retries NOT_LEADER and
   NOT_CONTROLLER, as for any leader change. The test fails on an error
   that reaches the application.
