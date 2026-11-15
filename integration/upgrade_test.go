package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A rolling upgrade from the previous release (docs/design/g7-rolling-upgrade.md):
// three nodes on SAMSA_OLD_BIN, a producer writing throughout, each node
// stopped with SIGTERM and started on the new binary in turn. While an old
// node remains, a request that writes a newer format is refused; after
// the last, it succeeds. Nothing acknowledged is lost or doubled, and the
// producer sees no error.
func TestRollingUpgrade(t *testing.T) {
	oldBin := os.Getenv("SAMSA_OLD_BIN")
	if oldBin == "" {
		if os.Getenv("SAMSA_REQUIRE_UPGRADE") != "" {
			t.Fatal("SAMSA_OLD_BIN is not set, and SAMSA_REQUIRE_UPGRADE says CI must run this test")
		}
		t.Skip("SAMSA_OLD_BIN is not set: build the previous release to run this test")
	}
	c := &cluster{t: t, nodes: map[int32]*broker{}}
	var peers []string
	for id := int32(1); id <= 3; id++ {
		c.nodes[id] = &broker{t: t, addr: freeAddr(t), dir: t.TempDir(), bin: oldBin}
		peers = append(peers, fmt.Sprintf("%d=%s/%s", id, c.nodes[id].addr, freeAddr(t)))
	}
	for id, b := range c.nodes {
		b.args = []string{"-node-id", fmt.Sprint(id), "-peers", strings.Join(peers, ",")}
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
	cl := c.client()
	c.leader(cl)
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "events", 3, 3
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if resp, err := req.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic on the old release: %v %+v", err, resp)
	}
	const n = 200
	if err := c.produce(cl, "events", "a", n, 20*time.Second); err != nil {
		t.Fatalf("produce on the old release: %v", err)
	}

	var (
		wg    sync.WaitGroup
		stop  = make(chan struct{})
		acked atomic.Int64
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
			acked.Store(int64(i + 1))
		}
	}()

	// Upgrade a running producer, not one starting up: a client whose
	// very first connection is closed by a stopping node gives up
	// (franz-go reads it as a TLS mismatch).
	for deadline := time.Now().Add(10 * time.Second); acked.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the producer acknowledged nothing in 10s: %v", perr)
		}
	}
	newBin := samsaBin()
	for id := int32(1); id <= 3; id++ {
		// SAMSA_NEW_ARGS adds flags to the new binary: "-unsafe
		// raise-level-early" with a build tagged unsafe must fail here.
		if extra := os.Getenv("SAMSA_NEW_ARGS"); extra != "" {
			c.nodes[id].args = append(c.nodes[id].args, strings.Fields(extra)...)
		}
		restart(t, c.nodes[id], newBin)
		c.leader(cl)
		time.Sleep(2 * time.Second) // the new node reports its level; the old ones must keep running
		for oid, ob := range c.nodes {
			if ob.cmd == nil || ob.cmd.ProcessState != nil {
				t.Fatalf("node %d stopped during the upgrade", oid)
			}
		}
		// Only a leader on the new binary knows the gated API: move
		// leadership off an old node by restarting it on the old binary.
		for try := 0; c.nodes[c.leader(cl)].bin == oldBin; try++ {
			if try == 3 {
				t.Fatal("leadership stayed on an old node")
			}
			restart(t, c.nodes[c.leader(cl)], oldBin)
		}
		code := createTopicV7(t, c, fmt.Sprintf("upgraded-%d", id))
		switch {
		case id < 3 && code != 35:
			t.Fatalf("with an old node left, CreateTopics v7 answered %d, want UNSUPPORTED_VERSION (35)", code)
		case id == 3 && code != 0:
			t.Fatalf("after the upgrade, CreateTopics v7 answered %d, want success", code)
		}
		t.Logf("node %d upgraded; node %d leads; CreateTopics v7 answered %d", id, c.leader(cl), code)
	}

	// Pre-existing topics created on the old release now carry a non-zero
	// deterministic TopicID at format level 4.
	mreq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	name := "events"
	mt.Topic = &name
	mreq.Topics = append(mreq.Topics, mt)
	mctx, mcancel := context.WithTimeout(context.Background(), 10*time.Second)
	mresp, err := mreq.RequestWith(mctx, cl)
	mcancel()
	if err != nil || len(mresp.Topics) != 1 || mresp.Topics[0].ErrorCode != 0 || mresp.Topics[0].TopicID == ([16]byte{}) {
		t.Fatalf("Metadata v12 for pre-existing topic after upgrade: %v %+v", err, mresp)
	}

	close(stop)
	wg.Wait()
	if perr != nil {
		t.Fatalf("the producer failed during the upgrade: %v", perr)
	}
	t.Logf("%d records acknowledged one by one during the upgrade", acked.Load())
	if err := c.produce(cl, "events", "c", n, 20*time.Second); err != nil {
		t.Fatalf("produce after the upgrade: %v", err)
	}
	if dups := c.readAllPrefixes("events", map[string]int{"a": n, "bg": int(acked.Load()), "c": n}); dups > 0 {
		t.Fatalf("%d records were written twice by an idempotent producer", dups)
	}
}

// createTopicV7 asks the leader to create a topic with CreateTopics v7 and
// returns the error code. A leader on the new binary refuses while the
// cluster's format level is below 4, and succeeds with a non-zero TopicID after.
func createTopicV7(t *testing.T, c *cluster, topic string) int16 {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 1, 3
	req.Topics = append(req.Topics, rt)
	var err error
	for range 10 { // a fresh client each time: no stale idea of the leader
		cl := c.client()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var resp *kmsg.CreateTopicsResponse
		resp, err = req.RequestWith(ctx, cl)
		cancel()
		cl.Close()
		if err == nil {
			if resp.Topics[0].ErrorCode == 0 && resp.Topics[0].TopicID == ([16]byte{}) {
				t.Fatalf("CreateTopics v7 succeeded for %q with a zero TopicID", topic)
			}
			return resp.Topics[0].ErrorCode
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("CreateTopics v7: %v", err)
	return 0
}

// restart stops b with SIGTERM, which hands leadership over, and starts
// it again on bin.
func restart(t *testing.T, b *broker, bin string) {
	t.Helper()
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := b.cmd.Wait(); err != nil {
		t.Fatalf("%s did not exit cleanly on SIGTERM: %v", b.bin, err)
	}
	b.cmd = nil
	b.bin = bin
	b.start()
}
