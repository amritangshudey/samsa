package integration

import (
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Three processes, then four, then three (docs/design/c1b-membership-binary.md,
// section 2): node 4 joins with -join and `samsa node add`, node 1, the
// first leader, leaves with `samsa node remove`, and a producer writes
// throughout. Then node 2 is killed: {3, 4} is a majority of {2, 3, 4}
// only if node 4 is a voter that counts, so writes must still succeed.
func TestMembershipThreeFourThree(t *testing.T) {
	c := &cluster{t: t, nodes: map[int32]*broker{}}
	peerAddr := map[int32]string{}
	var peers []string
	for id := int32(1); id <= 4; id++ {
		c.nodes[id] = &broker{t: t, addr: freeAddr(t), dir: t.TempDir()}
		peerAddr[id] = freeAddr(t)
		peers = append(peers, fmt.Sprintf("%d=%s/%s", id, c.nodes[id].addr, peerAddr[id]))
	}
	for id := int32(1); id <= 3; id++ {
		c.nodes[id].args = []string{"-node-id", fmt.Sprint(id), "-peers", strings.Join(peers[:3], ",")}
	}
	c.nodes[4].args = []string{"-node-id", "4", "-join", "-peers", strings.Join(peers, ",")}
	t.Cleanup(func() {
		for id, b := range c.nodes {
			b.kill()
			if t.Failed() {
				t.Logf("node %d log (last 4000 bytes):\n%s", id, last(b.log(), 4000))
			}
		}
	})
	joiner := c.nodes[4]
	delete(c.nodes, 4) // clients and leader lookups see only voters until it is one
	for _, b := range c.nodes {
		b.start()
	}
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
	const n = 200
	if err := c.produce(cl, "events", "a", n, 20*time.Second); err != nil {
		t.Fatalf("produce to three nodes: %v", err)
	}

	// A producer writes one record at a time while membership changes.
	var (
		wg    sync.WaitGroup
		stop  = make(chan struct{})
		acked int
		perr  error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		bg := c.client()
		defer bg.Close()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := bg.ProduceSync(ctx, &kgo.Record{Topic: "events", Partition: int32(i % 3), Value: fmt.Appendf(nil, "bg-%d", i)}).FirstErr()
			cancel()
			if err != nil {
				perr = fmt.Errorf("record bg-%d: %w", i, err)
				return
			}
			acked = i + 1
		}
	}()

	joiner.start()
	c.nodes[4] = joiner
	node(t, "add", "-bootstrap", c.nodes[1].addr+","+c.nodes[2].addr+","+c.nodes[3].addr,
		"-id", "4", "-client", joiner.addr, "-peer", peerAddr[4])
	waitBrokers(t, joiner, []int32{1, 2, 3, 4})

	node(t, "remove", "-bootstrap", joiner.addr, "-id", fmt.Sprint(first))
	waitBrokers(t, c.nodes[2], without([]int32{1, 2, 3, 4}, first))
	c.nodes[first].kill()
	delete(c.nodes, first)

	close(stop)
	wg.Wait()
	if perr != nil {
		t.Fatalf("the producer failed while membership changed: %v", perr)
	}
	t.Logf("%d records acknowledged one by one while node 4 joined and node %d left", acked, first)
	if acked == 0 {
		t.Fatal("no record was acknowledged while membership changed")
	}

	// Kill a second original node: the two left are a majority of three
	// only if node 4 is a voter.
	var other int32
	for id := range c.nodes {
		if id != 4 {
			other = id
			break
		}
	}
	c.nodes[other].kill()
	delete(c.nodes, other)
	cl2 := c.client()
	if err := c.produce(cl2, "events", "c", n, 30*time.Second); err != nil {
		t.Fatalf("produce with node %d gone (node 4 must count): %v", other, err)
	}
	if dups := c.readAllPrefixes("events", map[string]int{"a": n, "bg": acked, "c": n}); dups > 0 {
		t.Fatalf("%d records were written twice by an idempotent producer", dups)
	}
}

// node runs `samsa node ...` and fails the test unless it succeeds.
func node(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command(samsaBin(), append([]string{"node"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("samsa node %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	t.Logf("samsa node %s: %s", args[0], strings.TrimSpace(string(out)))
}

// waitBrokers waits until b's Metadata lists exactly ids.
func waitBrokers(t *testing.T, b *broker, ids []int32) {
	t.Helper()
	cl := franz(t, b, kgo.SeedBrokers(b.addr))
	defer cl.Close()
	var got []int32
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
		cancel()
		if err != nil {
			continue
		}
		got = got[:0]
		for _, br := range resp.Brokers {
			got = append(got, br.NodeID)
		}
		slices.Sort(got)
		if slices.Equal(got, ids) {
			return
		}
	}
	t.Fatalf("node at %s lists brokers %v, want %v", b.addr, got, ids)
}

func without(ids []int32, id int32) []int32 {
	return slices.DeleteFunc(slices.Clone(ids), func(x int32) bool { return x == id })
}
