# Running Samsa: a runbook

For someone who has to keep a cluster of Samsa nodes running. It says what
each step does and what to look at. It is written from what the code does and
from the tests in this repository; a step that no test runs says so. Samsa has
not run anyone's production, and "production ready" is not a claim made here.

## 1. Install

One static binary, `samsa` (`make build`, or `make release VERSION=vX.Y.Z` for
every platform with checksums). Three ways to run it:

- **systemd** (`packaging/samsa.service`, `packaging/samsa.conf`): the unit
  restarts the node when it exits on a fault, runs it as an unprivileged user,
  and lets it write only its data directory.
- **A container** (`make image`, `packaging/Dockerfile`): an 18 MB image of the
  binary alone. `docker run -d -p 9092:9092 -v samsa-data:/data samsa:dev
  -advertise HOST:9092`. The data directory is owned by user 65532.
- **By hand**: `samsa -config /etc/samsa/samsa.conf`.

Settings are flags, or `key = value` lines in a `-config` file
(`docs/guides/configuration.md`). Run `samsa doctor -config FILE` before the
first start: it lists what is in use and what is risky.

## 2. Bring up a cluster

Three nodes are the smallest cluster that survives the loss of one.

1. On each, write the config: its own `node-id`, the same `peers` list on all
   (`id=clientHost:port/peerHost:port,...`), `advertise` as clients reach it,
   `data`, and the TLS and SASL settings (section 8). Use `profile = prod`.
2. `samsa doctor -config FILE` on each. Lines marked `!` are what to fix.
3. Start them within a minute of each other.
4. **Ready** means `GET /readyz` on `-metrics-listen` answers 200. A node is
   ready when it leads and has applied its first entry, or follows a known
   leader and has applied what is committed. While it is not, the answer is
   503 with the reason in the body.
5. Create topics with any Kafka tool (`kafka-topics.sh --create`). Samsa
   never creates a topic on its own, and the replication factor is the
   cluster size.

A cluster of one works (`-sync always`, no `-peers`), with nothing to fail
over to and nothing to repair damaged data from.

## 3. What to watch

`GET /metrics` (Prometheus text) on `-metrics-listen`. Bind it to localhost or a
private interface: it is plain HTTP with no authentication.

| Metric | Means | Alert when |
|---|---|---|
| `samsa_ready` | 1 when the node can serve its role | 0 for more than a minute |
| `samsa_raft_leader` | 1 on the leader | no node has it, for more than 10 s |
| `samsa_raft_term` | the election term | it keeps rising: elections are not settling |
| `samsa_raft_committed_entries` and `samsa_raft_applied_entries` | commit and apply progress | applied stays below committed |
| `samsa_raft_last_index` minus `samsa_raft_log_start` | entries held | grows without bound: retention is not working |
| `samsa_recovering` | 1 while the node holds no vote after damage to its log | 1 for more than an hour |
| `samsa_storage_damage_repaired_total` | times damage was cut out at start | any increase: look at the disk |
| `samsa_scrubbed_bytes_total` | how much the scrub has read | does not move |
| `samsa_relocated_bytes_total`, `samsa_compacted_records_total`, `samsa_forced_retention_bytes_total` | what retention and compaction cost | `forced` rises: live data exceeds `-retention-bytes` and is being dropped |
| `samsa_connections`, `samsa_connections_dropped_total` | clients, and malformed input | drops climb |
| `samsa_requests_total{api_key=...}` | requests by API | for capacity |

Disk is not exposed as a metric. Watch the data directory's filesystem.

## 4. Rolling restart and upgrade

One node at a time, followers first:

1. `systemctl stop samsa` (SIGTERM). A leader hands leadership to a follower, finishes
   in-flight requests and exits; clients see no error (`TestClusterGracefulStop`).
2. Replace the binary, or change the config.
3. `systemctl start samsa`. Wait for `/readyz` to answer 200 **on that node**
   before touching the next one: until then the cluster has two of three.

Format levels. Each release reads and writes up to a **format level**; the
cluster raises its level only when every voter reports that it supports it. Until
then new features that change what is stored are refused
(`UNSUPPORTED_VERSION`). So during a rolling upgrade, a newer node behaves as
the older ones. **After the level is raised, going back to an older release is
not possible**: it stops on a command it does not know. Keep the previous
binary and take the upgrade one release at a time (`TestRollingUpgrade` runs
v0.2.0 to the next release under a producer; it needs `SAMSA_OLD_BIN`).

## 5. Change the cluster

- **Add a node:** start it with `-join` and `-peers` naming itself and the
  current voters, then `samsa node add -bootstrap HOST:PORT -id N -client
  HOST:9092 -peer HOST:9093`. It joins as a learner, catches up, and becomes a
  voter.
- **Remove a node:** `samsa node remove -bootstrap HOST:PORT -id N`, then stop
  it.
- **Replace a node:** add the new one under a **new id**, wait for it to catch
  up (`samsa_raft_applied_entries` equals the leader's), remove the old one.
  **Do not reuse a node id with an empty disk**: the new node would have
  forgotten its vote and could vote twice in a term. (Damaged data is the one
  case that does reuse an id: see section 6, which keeps the vote.)
- Never change two voters at once; the leader refuses.

## 6. When something is wrong

| Symptom | What it is | What to do |
|---|---|---|
| A node exits right after start with `data directory is in use` | another process holds `LOCK` | stop it; never run two on one directory |
| A node exits with `the raft log is damaged ... restart the node` | the scrub or a read found damaged data on its disk, and wrote a marker | **start it again** (systemd does): the start cuts the damage out and fetches it again from the others. Look at the disk (`dmesg`, SMART) |
| `samsa_recovering` is 1 | the node repaired damage and is fetching what it lost; it votes for nobody meanwhile | wait. It ends when the node holds what it held, or the leader has nothing more to send. If it lasts, check that a leader exists |
| A cluster of three with one node recovering | two nodes vote: a second failure now stops elections (not data) | do not restart another node until `samsa_recovering` is 0 |
| A node refuses to start: `the log is damaged ... only voter` | a cluster of one has no copy | restore from your own backup (section 7) |
| A node refuses to start: hard state or snapshot file is corrupt | one small checksummed file failed | not repaired by the node. Replace the node: add a new one under a new id, remove this one |
| No leader | fewer than a majority of voters are up and in touch | bring nodes back. Do not delete data to "unstick" a node |
| Clients get `NOT_LEADER_OR_FOLLOWER` for a while after a stop | a leader change | clients retry; shorten `metadata.max.age.ms` in tests |
| `UNSUPPORTED_VERSION` on `kafka-configs.sh --alter` | the cluster's format level is below the feature's | finish the rolling upgrade: every voter must run the new release |
| Disk fills | retention is off (the default keeps everything), or live data exceeds the limit | set `retention.ms`/`retention.bytes` per topic, or `-retention-time`, `-retention-bytes-per-partition`; `-retention-bytes` is a last-resort limit that drops live data |
| Writes slow after enabling a compacted topic | the compactor reads and rewrites batches on the core thread (about a quarter fewer records per second at 128 MB/s in the benchmark) | lower the topic's update rate or move it to its own cluster |

`samsa check -data DIR` reads a **stopped** node's data as a start would, writes
nothing, and exits 1 if anything is corrupt or a start would have to repair it.
Use it on a copy before deciding to delete anything.

## 7. Backup and restore

Samsa has **no backup tool**. The copies are the other nodes: three nodes hold
the data three times. What is not covered: a mistake that every node
replicates (a deleted topic), and the loss of the whole cluster.

To keep a copy: stop one node (the cluster keeps running on two), copy its data
directory, start it. To restore a cluster of one from such a copy: stop the
node, put the copy in `-data`, start. Restoring a node of a cluster of three
from a copy of itself would let it vote with a log older than what it
acknowledged: add a new node instead.

## 8. Security settings

- **Clients:** `-tls-cert`, `-tls-key` (and `-tls-client-ca` for mutual TLS);
  `-sasl-mechanisms SCRAM-SHA-256` and `-sasl-users FILE`. Add a user with
  `samsa user add -file FILE -name NAME < password` (the file has mode 0600).
  Users can then be managed with `kafka-configs.sh` (`AlterUserScramCredentials`),
  which replicates to every node.
- **ACLs:** `-acl` with `-super-users User:admin`. With ACLs on, a resource
  without an ACL is denied unless `-allow-if-no-acl`. Manage them with
  `kafka-acls.sh` against any node.
- **Between nodes:** `-peer-tls-cert`, `-peer-tls-key`, `-peer-tls-ca` (mutual).
  **Without them, anyone who can reach a peer port can send Raft messages**:
  firewall it, or use them.
- **Certificates are read at start.** Replace the files and restart the node,
  one at a time (section 4).
- **The data directory is created owner-only (0700, files 0600).** An existing
  directory keeps its modes: `chmod -R go-rwx` it.

## 9. What this runbook does not cover

Multi-datacenter layouts, quotas (there are none), Kafka Connect and Streams
(no guarantees), moving partitions between nodes (every node holds every
partition), and sizing: the benchmarks (`docs/benchmarks.md`, the reports)
are single-machine numbers.
