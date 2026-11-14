# Item 23a report: the other fault kinds

2026-11-15. Production plan item 23a (new in ADR 0028; the kinds left open in
5b). Guide: `docs/guides/testing-applications.md`. Design: ADR 0018 (the
fault stream is unchanged) and a short note here.

## What was built

Four fault kinds for `samsa -faults`, each drawn once per request that it can
act on, from the same seeded stream:

- `expire`: a heartbeat of a stable group expires the member's session. The
  member is removed, the group rebalances, the member gets UNKNOWN_MEMBER_ID.
- `emptyfetch`: a fetch that has data is answered with none, the high
  watermark unchanged.
- `slowfetch`: a fetch that has data and a `max.wait` is held for the whole
  wait, not woken by data, and then answered normally.
- `outofrange`: a fetch of a partition that exists is answered with
  OFFSET_OUT_OF_RANGE and the true log start. What the client does is the
  test: `auto.offset.reset=latest` skips what was pending, `earliest`
  reprocesses.

The default kinds stay the first three (`lostack`, `notleader`,
`rebalance`), so existing test setups do not change; the others are named,
or `kinds=all`.

## Results

| | |
|---|---|
| Unit tests | each kind at rate 1 (`TestFaultFetches`, `TestFaultExpire`), the spec (`kinds=all`) |
| Sample app, commit before process, `seed=5,rate=0.2,kinds=expire` | FAIL: records lost, listed (first run of the test) |
| Sample app, commit after process, `seed=5,rate=0.03,kinds=all` | PASS, and all four new kinds appear in the broker log |
| Faults off | no draw; the simulator suite is unchanged |

## What is untested or weak

- **"Offsets cut by retention"** is `outofrange`: the answer is injected, the
  data stays. An actual cut would also change what the verdict can see; the
  verdict still refuses a partition whose start is above 0.
- **`slowfetch` holds a fetch for its `max.wait`:** a client with
  `max.wait` 0 sees nothing.
- **Only the franz-go sample application runs these.** The guide's JUnit and
  Python examples are not run.
- **A seed does not fix the schedule** with concurrent clients (as in 5b).

## Ten questions

1. **Why are the new kinds opt-in?** `outofrange` changes what a correct
   consumer sees of its offsets; making it default would change the
   meaning of existing tests.
2. **Why does `emptyfetch` need data?** An empty answer to an empty fetch
   is not a fault.
3. **Why does `slowfetch` ignore new data?** Otherwise a produce would wake
   it and the delay would vanish.
4. **How does `expire` differ from `rebalance`?** The member loses its
   session: its next request is UNKNOWN_MEMBER_ID, not REBALANCE_IN_PROGRESS.
5. **Why does `outofrange` report the true log start?** Clients reset from
   it; a lie would test the client's reaction to a broker bug.
6. **How is a fault counted?** Only when it acts: a kind that drew and had
   nothing to do is not injected.
7. **Do the faults draw when off?** No: a kind that is not on never draws.
8. **Which kinds can lose records?** `expire` and `rebalance` with commit
   before process; `outofrange` with `latest`.
9. **Does the verdict change?** No.
10. **What is not covered of the plan's list?** An actual retention cut.
