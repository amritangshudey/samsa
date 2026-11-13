package integration

import (
	"context"
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// cluster is a set of samsa processes that form one Raft group.
type cluster struct {
	t     *testing.T
	nodes map[int32]*broker // by node id, 1..n
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// startCluster launches n nodes on free local ports, each with its own
// fresh data directory, with args added to each node's flags.
func startCluster(t *testing.T, n int, args ...string) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: map[int32]*broker{}}
	var peers []string
	for id := int32(1); id <= int32(n); id++ {
		c.nodes[id] = &broker{t: t, addr: freeAddr(t), dir: t.TempDir()}
		peers = append(peers, fmt.Sprintf("%d=%s/%s", id, c.nodes[id].addr, freeAddr(t)))
	}
	for id, b := range c.nodes {
		b.args = append([]string{"-node-id", fmt.Sprint(id), "-peers", strings.Join(peers, ",")}, args...)
		b.start()
	}
	t.Cleanup(func() {
		for id, b := range c.nodes {
			b.kill()
			if t.Failed() {
				t.Logf("node %d log (last 4000 bytes):\n%s", id, last(b.log(), 4000))
			}
		}
	})
	return c
}

func last(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}

// client returns a franz-go client that knows every node's address.
func (c *cluster) client(opts ...kgo.Opt) *kgo.Client {
	var seeds []string
	var any *broker
	for _, b := range c.nodes {
		seeds = append(seeds, b.addr)
		any = b
	}
	return franz(c.t, any, append([]kgo.Opt{kgo.SeedBrokers(seeds...), kgo.ProducerBatchCompression(kgo.NoCompression())}, opts...)...)
}

// leader asks the cluster who leads, until the running nodes agree on a
// running node.
func (c *cluster) leader(cl *kgo.Client) int32 {
	c.t.Helper()
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
		cancel()
		if err == nil && resp.ControllerID > 0 && c.nodes[resp.ControllerID].cmd != nil {
			return resp.ControllerID
		}
	}
	c.t.Fatal("no leader within 15s")
	return 0
}

func (c *cluster) produce(cl *kgo.Client, topic, prefix string, n int, timeout time.Duration) error {
	var recs []*kgo.Record
	for i := range n {
		recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i % 3), Value: fmt.Appendf(nil, "%s-%d", prefix, i)})
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return cl.ProduceSync(ctx, recs...).FirstErr()
}

// readAll reads the three partitions of topic from the start until every
// value "<prefix>-<i>" for i < n has been seen, and checks that offsets are
// dense and that, within a partition, the wanted values first appear in the
// order they were sent. It returns how many records were read twice: the
// producer is idempotent, so a retry across a leader change must not
// write a batch again, and the test fails on any.
func (c *cluster) readAll(topic string, n int, prefixes ...string) (duplicates int) {
	c.t.Helper()
	want := map[string]int{}
	for _, p := range prefixes {
		want[p] = n
	}
	return c.readAllPrefixes(topic, want)
}

// readAllPrefixes is readAll with a count per prefix: values
// "<prefix>-<i>" for i < want[prefix].
func (c *cluster) readAllPrefixes(topic string, want map[string]int) (duplicates int) {
	c.t.Helper()
	total, wanted := 0, 0
	for _, n := range want {
		total += n
	}
	cl := c.client(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {
		0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart(),
	}}))
	defer cl.Close()
	seen := map[string]bool{}
	next := map[int32]int64{}
	lastIndex := map[string]int{} // prefix/partition -> last i seen first-time
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for wanted < total {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			c.t.Fatalf("read %d of %d acknowledged records, then timed out: %v", wanted, total, fetches.Errors())
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset != next[r.Partition] {
				c.t.Fatalf("partition %d: got offset %d, expected %d", r.Partition, r.Offset, next[r.Partition])
			}
			next[r.Partition]++
			v := string(r.Value)
			if seen[v] {
				duplicates++
				return
			}
			seen[v] = true
			var prefix string
			var i int
			if _, err := fmt.Sscanf(strings.Replace(v, "-", " ", 1), "%s %d", &prefix, &i); err != nil {
				c.t.Fatalf("unexpected value %q", v)
			}
			if i < want[prefix] {
				wanted++
			}
			key := fmt.Sprintf("%s/%d", prefix, r.Partition)
			if prev, ok := lastIndex[key]; ok && i < prev {
				c.t.Fatalf("partition %d: %q first appears after %s-%d", r.Partition, v, prefix, prev)
			}
			lastIndex[key] = i
		})
	}
	return duplicates
}

// Three nodes. Kill the leader, twice, with a restart in between: every
// acknowledged record is still there and the cluster keeps accepting
// writes. With only one node left it must refuse them.
func TestClusterSurvivesLeaderKill(t *testing.T) {
	c := startCluster(t, 3)
	cl := c.client()
	first := c.leader(cl)

	// Create the topic; the replication factor is the cluster's to choose.
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "events", 3, -1
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if resp, err := req.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %v %+v", err, resp)
	}

	const n = 300
	if err := c.produce(cl, "events", "a", n, 20*time.Second); err != nil {
		t.Fatalf("produce to a healthy cluster: %v", err)
	}

	c.nodes[first].kill()
	start := time.Now()
	second := c.leader(c.client()) // a fresh client: no cached metadata
	elected := time.Since(start)
	if err := c.produce(cl, "events", "b", n, 30*time.Second); err != nil {
		t.Fatalf("produce after the leader was killed: %v", err)
	}
	t.Logf("leader %d killed; node %d named leader after %v; %d records acknowledged after %v (the producer's own metadata refresh included)",
		first, second, elected.Round(time.Millisecond), n, time.Since(start).Round(time.Millisecond))
	if second == first {
		t.Fatalf("the dead node %d is still named leader", first)
	}
	dups := c.readAll("events", n, "a", "b")

	// The dead node returns with its old disk and must catch up: once the
	// second leader dies too, it is half of the only possible majority.
	c.nodes[first].start()
	c.nodes[second].kill()
	if err := c.produce(cl, "events", "c", n, 30*time.Second); err != nil {
		t.Fatalf("produce after the second leader was killed: %v", err)
	}
	third := c.leader(cl)
	dups += c.readAll("events", n, "a", "b", "c")
	t.Logf("leader %d killed; leader is now %d; %d duplicate records read in total", second, third, dups)
	if dups > 0 {
		t.Fatalf("%d records were written twice by an idempotent producer", dups)
	}

	// One node of three is no majority: nothing may be acknowledged.
	c.nodes[third].kill()
	if err := c.produce(c.client(), "events", "d", 10, 3*time.Second); err == nil {
		t.Fatal("a write was acknowledged with one node of three running")
	}
}

// SIGTERM to the leader while a producer writes: the leader hands over and
// exits cleanly, no produce fails, and nothing is lost or written twice
// (docs/design/b3-raft-hardening.md, 2.4).
func TestClusterGracefulStop(t *testing.T) {
	c := startCluster(t, 3)
	cl := c.client()
	first := c.leader(cl)
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "events", 3, -1
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if resp, err := req.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %v %+v", err, resp)
	}

	// Batches of ten, one after another, for three seconds; the leader is
	// told to stop after one.
	var prefixes []string
	done := make(chan error, 1)
	go func() {
		for end := time.Now().Add(3 * time.Second); time.Now().Before(end); {
			p := fmt.Sprintf("g%d", len(prefixes))
			if err := c.produce(cl, "events", p, 10, 20*time.Second); err != nil {
				done <- fmt.Errorf("batch %s: %w", p, err)
				return
			}
			prefixes = append(prefixes, p)
		}
		done <- nil
	}()
	time.Sleep(time.Second)
	b := c.nodes[first]
	start := time.Now()
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := b.cmd.Wait(); err != nil {
		t.Fatalf("the leader did not exit cleanly on SIGTERM: %v", err)
	}
	b.cmd = nil
	t.Logf("leader %d exited %v after SIGTERM", first, time.Since(start).Round(time.Millisecond))
	if !strings.Contains(b.log(), `"handing over"=true`) {
		t.Fatalf("the leader did not hand over:\n%s", last(b.log(), 2000))
	}
	if err := <-done; err != nil {
		t.Fatalf("a produce failed across the graceful stop: %v", err)
	}
	if next := c.leader(c.client()); next == first {
		t.Fatalf("the stopped node %d is still named leader", first)
	}
	if dups := c.readAll("events", 10, prefixes...); dups > 0 {
		t.Fatalf("%d records written twice", dups)
	}
	t.Logf("%d batches of 10 acknowledged and read back once each", len(prefixes))
}
