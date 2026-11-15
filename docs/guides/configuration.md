# Configuring Samsa

Every setting is a command-line flag (`samsa -h` lists them with their
defaults and what they do). The same names go in a file given with
`-config`, one `key = value` per line; the command line wins over the file.

## Profiles

`-profile dev|test|prod` sets defaults for the flags you did not set:

| Profile | Sets | For |
|---|---|---|
| `dev` | `-sync interval`, `-scrub-interval -1s` | a laptop: fast, writes may wait in the operating system's cache |
| `test` | `-sync never`, `-log-level warn`, `-scrub-interval -1s`, `-producer-expiry 10m` | a CI job: nothing is kept for long, nothing is checked at rest |
| `prod` | `-sync always` | a node that keeps data that matters: every acknowledged write is synced |

A cluster of several nodes keeps `-sync always` whatever the profile (a
cluster needs it). A `prod` node logs a warning at start for every risky
setting it finds, and `samsa doctor` lists them.

## `samsa doctor`

`samsa doctor` takes the flags you would start with, resolves them as a start
would (command line, `-config` file, `-profile`, defaults), prints each flag
you set or that is risky, where its value came from, and what is risky about
it, and does not start. Lines marked `!` are not fit for a node that keeps
data that matters. With `-profile prod` it exits 1 if there are any.

```
$ samsa doctor -profile prod -data /var/lib/samsa
  peers                                                 (default)  ! a single node: nothing to fail over to, nothing to repair damage from
  sasl-mechanisms                                       (default)  ! any client that can connect may read and write
  sync                               always             (profile prod)
  tls-cert                                              (default)  ! client traffic is not encrypted
```

## What a production node needs

Eleven settings for a node of a three-node cluster that `samsa doctor` finds nothing risky in:

```
-profile prod  -node-id 1  -data /var/lib/samsa
-peers 1=h1:9092/h1:9093,2=h2:9092/h2:9093,3=h3:9092/h3:9093
-tls-cert node.pem  -tls-key node.key
-peer-tls-cert peer.pem  -peer-tls-key peer.key  -peer-tls-ca ca.pem
-sasl-mechanisms SCRAM-SHA-256  -sasl-users users.txt
```

The plan said five; peers, client security, peer security and storage each
need their own flags. (A plaintext peer port lets anyone who can reach it
send Raft messages: `doctor` marks it.)
The rest (retention, compaction, scrub) have defaults, and topics set their
own retention and cleanup policy (`kafka-configs.sh --alter`).

## What each group of flags is for

| Flags | Group |
|---|---|
| `-listen`, `-advertise`, `-node-id`, `-peers`, `-join`, `-data` | where the node is and who it talks to |
| `-sync`, `-sync-interval` | when a write is acknowledged (always: after fsync) |
| `-retention-*`, `-delete-retention-time`, `-compaction-map-bytes` | how old data goes away (ADR 0026, 0027, 0029) |
| `-scrub-bytes-per-second`, `-scrub-interval` | how damage on disk is found (ADR 0030) |
| `-tls-*`, `-peer-tls-*`, `-sasl-*`, `-acl`, `-super-users`, `-allow-if-no-acl` | security (ADRs 0019, 0020) |
| `-metrics-listen`, `-log-level` | what an operator sees |
| `-faults` | fault injection for testing applications; never in production |
