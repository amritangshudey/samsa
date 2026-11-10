// Package integration runs real, unmodified Kafka clients against the samsa
// binary. It is a separate Go module so that client libraries never become
// dependencies of the broker (ADR 0004).
//
// Run with `make integration` from the repository root.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// requireClients turns "tool not installed" from a skip into a failure. CI
// sets it, so a missing client can never pass silently there.
var requireClients = os.Getenv("SAMSA_REQUIRE_CLIENTS") != ""

func missing(t *testing.T, what string) {
	t.Helper()
	if requireClients {
		t.Fatalf("%s is required (SAMSA_REQUIRE_CLIENTS is set) but was not found", what)
	}
	t.Skipf("%s not found", what)
}

// broker is a samsa process under test.
type broker struct {
	t    *testing.T
	addr string
	dir  string
	args []string
	cmd  *exec.Cmd
	mu   sync.Mutex
	logs bytes.Buffer
}

func (b *broker) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.logs.Write(p)
}

// startBroker launches the binary on a free port with a fresh data directory.
func startBroker(t *testing.T, args ...string) *broker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	b := &broker{t: t, addr: addr, dir: t.TempDir(), args: args}
	b.start()
	t.Cleanup(func() {
		b.kill()
		b.reportVersions()
		if t.Failed() {
			t.Logf("broker log:\n%s", b.log())
		}
	})
	return b
}

func (b *broker) log() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.logs.String()
}

func (b *broker) start() {
	b.t.Helper()
	bin := os.Getenv("SAMSA_BIN")
	if bin == "" {
		bin = "../bin/samsa"
	}
	args := append([]string{"-listen", b.addr, "-data", b.dir, "-log-level", "debug"}, b.args...)
	b.cmd = exec.Command(bin, args...)
	b.cmd.Stderr = b
	if err := b.cmd.Start(); err != nil {
		b.t.Fatalf("start %s: %v (run `make build` first)", bin, err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if c, err := net.Dial("tcp", b.addr); err == nil {
			c.Close()
			return
		}
		if time.Now().After(deadline) {
			b.t.Fatalf("broker did not start listening on %s:\n%s", b.addr, b.log())
		}
	}
}

// kill ends the process with SIGKILL: no chance to flush or clean up.
func (b *broker) kill() {
	if b.cmd != nil && b.cmd.Process != nil {
		b.cmd.Process.Kill()
		b.cmd.Wait()
		b.cmd = nil
	}
}

var (
	softwareLine = regexp.MustCompile(`msg="client software" client=(\S+) name=(\S+) version=(\S+)`)
	requestLine  = regexp.MustCompile(`msg=request client=(\S+) key=(\d+) version=(\d+)`)
)

// reportVersions logs which API versions each client actually used, taken
// from the broker's debug log. ADR 0006 is written from this output.
func (b *broker) reportVersions() {
	software := map[string]string{}
	seen := map[string]bool{}
	for _, line := range strings.Split(b.log(), "\n") {
		if m := softwareLine.FindStringSubmatch(line); m != nil {
			software[m[1]] = m[2] + " " + m[3]
		}
		if m := requestLine.FindStringSubmatch(line); m != nil {
			who := software[m[1]]
			if who == "" {
				who = "client-id " + m[1]
			}
			seen[fmt.Sprintf("%s: api key %s version %s", who, m[2], m[3])] = true
		}
	}
	var lines []string
	for l := range seen {
		lines = append(lines, l)
	}
	sort.Strings(lines)
	b.t.Logf("negotiated versions:\n  %s", strings.Join(lines, "\n  "))
}

func franz(t *testing.T, b *broker, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(b.addr),
		// The broker does not implement the idempotent producer (a stated non-goal).
		kgo.DisableIdempotentWrite(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func createTopic(t *testing.T, cl *kgo.Client, topic string, partitions int32) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, partitions, 1
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if code := resp.Topics[0].ErrorCode; code != 0 {
		t.Fatalf("create topic: error code %d", code)
	}
}

// consume reads partitions of topic from the start until each has yielded
// the number of records in want, and returns the values per partition.
func consume(t *testing.T, b *broker, topic string, want map[int32]int) map[int32][]string {
	t.Helper()
	offsets := map[int32]kgo.Offset{}
	total := 0
	for p, n := range want {
		offsets[p] = kgo.NewOffset().AtStart()
		total += n
	}
	cl := franz(t, b, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}))
	got := map[int32][]string{}
	next := map[int32]int64{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for read := 0; read < total; {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch: %v (read %d of %d)", errs, read, total)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset != next[r.Partition] {
				t.Fatalf("partition %d: got offset %d, expected %d", r.Partition, r.Offset, next[r.Partition])
			}
			next[r.Partition]++
			got[r.Partition] = append(got[r.Partition], string(r.Value))
			read++
		})
	}
	return got
}

// franz-go: create a topic, produce to three partitions, consume everything
// in order, then kill -9 the broker and check that it all survived.
func TestFranzGoRoundTripAndRestart(t *testing.T) {
	b := startBroker(t)
	cl := franz(t, b)
	const topic, perPartition = "franz", 400
	createTopic(t, cl, topic, 3)

	want := map[int32][]string{}
	var recs []*kgo.Record
	for i := range perPartition {
		for p := range int32(3) {
			v := fmt.Sprintf("p%d-%d", p, i)
			want[p] = append(want[p], v)
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Key: []byte("k"), Value: []byte(v)})
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results := cl.ProduceSync(ctx, recs...)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	// Each record was acknowledged with an offset; within a partition they
	// must be exactly 0..n-1 in send order.
	nextOffset := map[int32]int64{}
	for _, r := range results {
		if r.Record.Offset != nextOffset[r.Record.Partition] {
			t.Fatalf("partition %d: acknowledged offset %d, expected %d", r.Record.Partition, r.Record.Offset, nextOffset[r.Record.Partition])
		}
		nextOffset[r.Record.Partition]++
	}

	counts := map[int32]int{0: perPartition, 1: perPartition, 2: perPartition}
	check := func(when string) {
		t.Helper()
		got := consume(t, b, topic, counts)
		for p := range int32(3) {
			if strings.Join(got[p], ",") != strings.Join(want[p], ",") {
				t.Fatalf("%s: partition %d differs: got %d records, first %q", when, p, len(got[p]), first(got[p]))
			}
		}
	}
	check("before restart")

	b.kill() // SIGKILL: every acknowledged record must already be on disk
	b.start()
	check("after kill -9 and restart")

	// And the log continues where it stopped.
	cl2 := franz(t, b)
	res := cl2.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 1, Value: []byte("after")})
	if err := res.FirstErr(); err != nil || res[0].Record.Offset != perPartition {
		t.Fatalf("produce after restart: offset %d, %v", res[0].Record.Offset, err)
	}
}

func first(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	return xs[0]
}

// Compressed batches are stored and returned as they are; the client
// decompresses. This also checks a large batch.
func TestFranzGoCompressedAndLarge(t *testing.T) {
	b := startBroker(t)
	for name, codec := range map[string]kgo.CompressionCodec{
		"gzip": kgo.GzipCompression(), "snappy": kgo.SnappyCompression(), "lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression(),
	} {
		cl := franz(t, b, kgo.ProducerBatchCompression(codec))
		createTopic(t, cl, name, 1)
		big := strings.Repeat("compressible ", 20_000) // 260 kB in one record
		var recs []*kgo.Record
		for i := range 20 {
			recs = append(recs, &kgo.Record{Topic: name, Value: []byte(fmt.Sprintf("%d:%s", i, big))})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			t.Fatalf("%s: produce: %v", name, err)
		}
		cancel()
		got := consume(t, b, name, map[int32]int{0: 20})
		for i, v := range got[0] {
			if v != fmt.Sprintf("%d:%s", i, big) {
				t.Fatalf("%s: record %d corrupted (%d bytes)", name, i, len(v))
			}
		}
	}
}

// kcat (librdkafka): produce lines to a partition and read them back.
func TestKcatRoundTrip(t *testing.T) {
	kcat, err := exec.LookPath("kcat")
	if err != nil {
		missing(t, "kcat")
	}
	b := startBroker(t)
	createTopic(t, franz(t, b), "kcat", 1)

	var want []string
	for i := range 500 {
		want = append(want, fmt.Sprintf("kcat-line-%d", i))
	}
	run := func(stdin string, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, kcat, append([]string{"-b", b.addr, "-X", "enable.idempotence=false"}, args...)...)
		cmd.Stdin = strings.NewReader(stdin)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("kcat %v: %v\n%s", args, err, stderr.String())
		}
		return string(out)
	}
	run(strings.Join(want, "\n")+"\n", "-P", "-t", "kcat", "-p", "0")
	got := strings.Fields(run("", "-C", "-t", "kcat", "-p", "0", "-o", "beginning", "-e", "-q"))
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("kcat read %d lines, want %d; first %q", len(got), len(want), first(got))
	}
	if out := run("", "-L"); !strings.Contains(out, `topic "kcat" with 1 partitions`) {
		t.Fatalf("kcat -L does not list the topic:\n%s", out)
	}
}

// javaAndJars finds a working JDK and the kafka-clients class path, or
// skips the test (fails it, in CI).
func javaAndJars(t *testing.T) (java, classPath string) {
	t.Helper()
	java = os.Getenv("JAVA")
	if java == "" {
		for _, candidate := range []string{"/opt/homebrew/opt/openjdk/bin/java", "java"} {
			if p, err := exec.LookPath(candidate); err == nil {
				if exec.Command(p, "-version").Run() == nil { // macOS ships a stub that fails
					java = p
					break
				}
			}
		}
	}
	if java == "" {
		missing(t, "a JDK")
	}
	jars, _ := filepath.Glob(".cache/*.jar")
	if len(jars) < 2 {
		missing(t, "the kafka-clients jars (run `make integration` to download them)")
	}
	return java, strings.Join(jars, string(os.PathListSeparator))
}

// The official Java client: admin creates the topic, producer writes,
// consumer reads back. See java/RoundTrip.java.
func TestJavaClientRoundTrip(t *testing.T) {
	java, cp := javaAndJars(t)
	b := startBroker(t)
	const n = 300
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, java, "-cp", cp, "java/RoundTrip.java", b.addr, "java", fmt.Sprint(n))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("java client: %v\n%s", err, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != n {
		t.Fatalf("java client read %d records, want %d", len(lines), n)
	}
	for i, line := range lines {
		if want := fmt.Sprintf("%d java-%d", i, i); line != want {
			t.Fatalf("record %d: got %q, want %q", i, line, want)
		}
	}
	// What Java wrote, franz-go must read identically.
	if got := consume(t, b, "java", map[int32]int{0: n}); got[0][n-1] != fmt.Sprintf("java-%d", n-1) {
		t.Fatalf("franz-go reading Java's records: last is %q", got[0][n-1])
	}
}
