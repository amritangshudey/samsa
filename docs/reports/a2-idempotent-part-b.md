# A2 part (b) report: producer expiry

Branch `a2b-expiry`, 2026-11-12. Production plan item 1, part (b).
Design: `docs/design/a2-idempotent-producer.md`, section 7.

## What was built

- **Each producer remembers the log index of its last accepted batch.**
  It is applied state, hashed and kept in snapshot version 4. A version 3
  snapshot gets the index of the partition's newest batch, which can only
  make expiry late.
- **`ExpireProducers(L)`**, a command that removes from every partition
  the producers whose last batch is at an index below L. The serving
  leader samples `(time, applied index)` every eighth of the expiry time,
  and proposes the command when a sample is old enough. A new leader
  starts with no samples, so expiry can only be late.
- **`-producer-expiry`**, default 24 h (Kafka's default); negative means
  never. The broker logs "expired idempotent producers" with a count.
- **The S6 rule stays**: a producer the partition does not know must start
  at sequence 0. An expired producer that comes back is refused once, and
  then starts a new epoch.
- **Simulator**: each seed picks an expiry of never or 15 s. The producer
  renumbers its queue under a new epoch when it is refused after a
  legitimate expiry. I8 still fails any refusal that comes sooner than the
  expiry allows. New mutation: `UnsafeExpireEarly`.
- **Integration**: `TestJavaProducerAfterExpiry`. With `-producer-expiry
  1s`, the Java producer stays silent for 4 s, and a plain producer keeps
  the broker busy meanwhile. The broker forgets the Java producer, which
  comes back with one refused request and one re-send, asks for no new
  id, and has all 20 records read back once each, in order.

## Results

| | |
|---|---|
| Simulator, 5000 seeds | clean |
| Expiry in the simulator | 6 480 producer ids expired in 1000 seeds; an active producer refused after expiry: 0 times (see below) |
| Mutation | expiring at once is caught at seed 7 by I8 |
| Unit tests | `TestProducerExpiry`, `TestProducerExpirySwitches`, `TestSnapshotV3ProducerIndex` |
| `make test`, `race`, `lint`, `integration` | pass |

## How to run it

    make sim SEEDS=5000
    go test ./internal/broker -run Expiry
    make integration          # includes TestJavaProducerAfterExpiry, about 9 s

## What is untested or weak

- **The simulator never sees an expired producer come back.** Its expiry
  has to stay above the 10 s a request may wait plus the 2 s of retries
  (design review, point 6), and no active producer goes 15 s without
  writing. The unit test and the Java test cover the return.
- **The timing assumption is Kafka's**: a copy of a batch must not be
  applied later than the expiry time after it was sent. If it is, a retry
  with sequence 0 can be appended twice. A day against a two-minute
  delivery timeout is a wide margin, but it is a margin, not a proof.
- Expiry is checked only when the leader handles an event. An idle
  cluster expires nothing until the next request, which is late, never
  early.
- The leader's clock decides. A clock that jumps forward by a day expires
  everybody early. Clocks are not checked for jumps.
- The version 3 snapshot path is tested on a hand-made encoding only.
  Version 3 was never in a release.

## Ten questions

1. Why does the leader decide expiry and put an index in the command,
   instead of every replica checking its own clock?
2. Why is it safe that a new leader starts with no samples?
3. What does a sample `(t, A)` promise about the entries below A?
4. Why are samples taken every eighth of the expiry time? What does that
   cost in precision?
5. Why does a version 3 snapshot take the newest batch's index, and not 0?
6. Why does Samsa refuse an expired producer that continues its sequence,
   when Kafka accepts it? What does the refusal cost the client?
7. Which assumption does expiry add, and what breaks if a client violates
   it?
8. Why does the simulator use 15 s and not 3 s, and what does that leave
   untested?
9. How does the I8 check tell a legitimate expiry from a broker bug?
10. What does `TestJavaProducerAfterExpiry` show that the simulator
    cannot?
