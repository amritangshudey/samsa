# Item 24 report: profiles and `samsa doctor`

2026-11-15. Production plan item 24 (new in ADR 0028, problem 3: too many
settings). Guide: `docs/guides/configuration.md`.

## What was built

- **`-profile dev|test|prod`** sets defaults for the flags that neither the
  command line nor the `-config` file set. A cluster of several nodes keeps
  `-sync always` whatever the profile.
- **`samsa doctor [flags]`** resolves the flags as a start would, prints
  every flag that was set or is risky, its value, where it came from
  (command line, config file, profile, default) and what is risky about
  it. Lines starting with `!` are not fit for a node that keeps data that
  matters; with `-profile prod`, it exits 1 if there are any.
- **A `prod` node warns at start** with the same report.
- Tests: the profile sets only what was not set, a cluster keeps
  `-sync always`, an unknown profile is refused, the report names sources and
  risks, and a secured three-node setup is not flagged.

## Design notes (a short design, as the item is small)

- **Profiles are defaults, not mode switches.** A flag always means what it
  says; the profile only moves its default, and `doctor` shows that it did.
  The alternative, a profile that changes behaviour in code, would leave the
  user unable to say what a node does from its flags.
- **`doctor` shares the start's resolution** (one code path), so what it
  prints is what the start would run with, not a copy that can drift.
- **Review:** the first draft refused to start `prod` with any risky
  setting. That would refuse `-profile prod` on a laptop to try it out; it
  now warns, and only `doctor -profile prod` exits non-zero, which a
  deployment script can use.

## What does not meet the plan

- The plan said **a prod node needs at most five settings**. A node of a
  three-node cluster that `doctor` finds nothing risky in needs eleven:
  `-profile`, `-node-id`, `-data`, `-peers`, the three `-peer-tls-*`, `-tls-cert`,
  `-tls-key`, `-sasl-mechanisms`, `-sasl-users`. Peers,
  security and storage each need their own.
- **"Every flag documented with its default and when to change it"** is
  `samsa -h` for the default and meaning, and the guide's table by group for
  when. There is no per-flag "when to change it" text for every flag.
- **The risk list** is a short table in `profile.go`, not exhaustive.

## Ten questions

1. **Why defaults and not modes?** A flag must keep its meaning; see above.
2. **Why does a cluster ignore the profile's `-sync`?** A node that forgets
   acknowledged entries breaks Raft (ADR 0012); the start refuses anything
   but `always` in a cluster.
3. **Where do values come from, in order?** Command line, then `-config`
   file, then the profile, then the built-in default.
4. **Why does `doctor` list only what was set or is risky?** Over a hundred
   lines of defaults hide the three that matter.
5. **What does `doctor` not check?** Whether files named by flags exist,
   whether ports are free, whether peers are reachable.
6. **Why is plaintext traffic marked `!`?** A node that keeps data that matters
   and accepts clients on an unencrypted port exposes it.
7. **Why does `test` set `-sync never`?** CI jobs start from nothing; the
   syncs are time spent for nothing. A cluster test keeps `always`.
8. **Does a profile change the on-disk format?** No.
9. **What happens with `-profile` in the config file?** It works: the file is
   read before profiles apply.
10. **Why eleven settings, not five?** See the section above.
