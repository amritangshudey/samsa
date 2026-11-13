# Testing your application against a Kafka that breaks on purpose

Samsa can inject Kafka-level faults into a test run and then tell you
whether your application lost or duplicated any record. This guide shows
the loop for JUnit, Go and Python tests.

What is tested in this repository:

- the broker side: `internal/broker/faults_test.go`,
  `internal/broker/verdict_test.go`;
- the whole loop with a Go application (`integration/apps_test.go`).

The JUnit and Python examples below are not run by CI. They use only the
two commands documented here and standard client settings, but check
them against your own setup.

## The loop

1. Start a single broker with a fresh data directory and faults on:

       samsa -listen 127.0.0.1:PORT -data DIR -faults seed=42

   Pick a free port for PORT. The binary starts in milliseconds and needs
   no Docker and no JVM.
2. Create the input and output topics, and write the input. Every input
   record needs a **unique key**, and the producer must not compress.
3. Run your application against it until it is done.
4. Stop the broker. A kill is fine: every acknowledged write is on disk.
5. Ask for the verdict:

       samsa verdict -data DIR -in orders -out invoices -group billing

   Exit status 0 is a pass, 1 a fail, 2 "cannot judge" (with the reason).

A failing verdict looks like this, from the sample in
`integration/apps_test.go`:

    FAIL: 600 input records of "in", 82 output records in "out"
      processed once 82, duplicated 0, lost 518, not yet processed 0; output records matching no input 0
      faults: seed=5,rate=0.2,kinds=rebalance
    lost: group "app" committed past them, but they were never written to the output (518; the first 20):
      in/0 offset 82 key "k0246"
      ...
    replay the same faults: samsa -faults seed=5,rate=0.2,kinds=rebalance (the faults repeat when the requests arrive in the same order)

## What your application must do for the verdict to work

- **Write each input record's key on its output record(s).** That is how
  the verdict knows which output came from which input.
- **Use unique input keys**, and give every input record one. The test
  writes the input, so this is in your control.
- **Produce uncompressed**:
  - Java: `compression.type=none`, the default;
  - franz-go: `kgo.ProducerBatchCompression(kgo.NoCompression())`;
  - confluent-kafka: `compression.type: none`, the default.

  The verdict refuses compressed batches and says so.
- **Run as one consumer group** and pass it as `-group`. The group's
  committed offsets are what separate "lost" (committed past, never
  output) from "not yet processed".

## The faults

`-faults seed=N[,rate=R][,kinds=a+b]`. `rate` is the chance per eligible
request (default 0.05). `kinds` defaults to all of:

| Kind | Hits | What happens | Catches |
|---|---|---|---|
| `lostack` | produce | the write commits, the answer never arrives (connection closed) | producers that retry without idempotence: duplicates |
| `notleader` | produce, fetch, offset commit | NOT_LEADER_OR_FOLLOWER (NOT_COORDINATOR for a commit), nothing done | error handling that gives up or skips |
| `rebalance` | a heartbeat of a stable group | a rebalance starts; every member's partitions are revoked mid-work | commit before process, work abandoned without care |

The seed fixes the fault stream: the *k*-th eligible request gets the
*k*-th draw. Real clients send requests concurrently, so which request is
the *k*-th varies between runs. A failing seed usually fails again the
same way, but not always to the record.

**Shorten your client's metadata back-off in tests.** After a
`notleader`, a client refreshes metadata before retrying. franz-go waits
up to `MetadataMinAge`, 5 s by default. Set `kgo.MetadataMinAge(250 *
time.Millisecond)`, or your test spends its time waiting. Java
(`retry.backoff.ms`, 100 ms by default) and librdkafka
(`topic.metadata.refresh.fast.interval.ms`) are quicker by default.

Run the verdict on a **single-node** broker. On a cluster, each node
injects its own faults, and a follower's data can hold entries that were
never committed. Neither is covered by a test yet.

## Go

The whole of this is `integration/apps_test.go`; in short:

```go
func TestBillingUnderFaults(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t) // net.Listen on 127.0.0.1:0, take the port, close
	broker := exec.Command("samsa", "-listen", addr, "-data", dir, "-faults", "seed=42")
	if err := broker.Start(); err != nil {
		t.Fatal(err)
	}
	waitForPort(t, addr)

	writeInput(t, addr, "orders", 1000) // unique keys, kgo.NoCompression()
	runBilling(t, addr)                 // your application, until idle

	broker.Process.Kill()
	broker.Wait()
	out, err := exec.Command("samsa", "verdict", "-data", dir,
		"-in", "orders", "-out", "invoices", "-group", "billing").CombinedOutput()
	if err != nil {
		t.Fatalf("verdict:\n%s", out)
	}
}
```

## JUnit 5 (Java)

```java
class BillingFaultTest {
    @TempDir Path dir;

    @Test
    void noInvoiceLostUnderFaults() throws Exception {
        int port;
        try (var s = new java.net.ServerSocket(0)) { port = s.getLocalPort(); }
        Process broker = new ProcessBuilder("samsa",
                "-listen", "127.0.0.1:" + port, "-data", dir.toString(), "-faults", "seed=42")
            .redirectErrorStream(true).redirectOutput(dir.resolve("broker.log").toFile()).start();
        try {
            String bootstrap = "127.0.0.1:" + port;
            awaitPort(port);
            writeOrders(bootstrap, 1000);   // unique keys; compression.type=none
            Billing.run(bootstrap, Duration.ofSeconds(3)); // your application, until idle
        } finally {
            broker.destroyForcibly().waitFor();
        }
        Process verdict = new ProcessBuilder("samsa", "verdict", "-data", dir.toString(),
                "-in", "orders", "-out", "invoices", "-group", "billing")
            .redirectErrorStream(true).start();
        String report = new String(verdict.getInputStream().readAllBytes());
        assertEquals(0, verdict.waitFor(), report);
    }
}
```

## pytest (Python, confluent-kafka)

```python
import socket, subprocess, time

def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

def test_billing_under_faults(tmp_path):
    port = free_port()
    broker = subprocess.Popen(["samsa", "-listen", f"127.0.0.1:{port}",
                               "-data", str(tmp_path), "-faults", "seed=42"])
    try:
        wait_for_port(port)
        bootstrap = f"127.0.0.1:{port}"
        write_orders(bootstrap, 1000)          # unique keys; compression.type none
        run_billing(bootstrap, idle_seconds=3) # your application
    finally:
        broker.kill()
        broker.wait()
    verdict = subprocess.run(["samsa", "verdict", "-data", str(tmp_path),
                              "-in", "orders", "-out", "invoices", "-group", "billing"],
                             capture_output=True, text=True)
    assert verdict.returncode == 0, verdict.stdout + verdict.stderr
```

## Reading a failure

- **lost**: the group committed past these records, but nothing was
  written for them. The classic cause is committing before processing,
  then losing the partition (rebalance) or the process before
  processing. Fix: commit only what has been written, after writing it.
- **duplicated**: written more than once. That is normal under
  at-least-once delivery after a rebalance or a retry. Pass
  `-expect exactly-once` if your application claims more. A producer that
  retries without idempotence after a `lostack` is the usual cause.
- **not yet processed**: nothing was written, and the group did not
  commit past these records. Usually the application stopped before it
  finished. Check the idle timeout and the client's back-off settings
  above.
