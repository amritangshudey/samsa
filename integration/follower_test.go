package integration

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestFetchFromTheNearestReplica (KIP-392, docs/design/c19-follower-fetch.md):
// three nodes in three racks. A consumer that says it is in a follower's rack
// reads everything from that follower; one in the leader's rack reads from the
// leader. franz-go follows the preferred replica the leader names.
func TestFetchFromTheNearestReplica(t *testing.T) {
	racks := []string{"a", "b", "c"}
	metrics := map[int32]string{}
	c := startClusterWith(t, 3, racks, func(id int32) []string {
		metrics[id] = freeAddr(t)
		return []string{"-log-level", "warn", "-metrics-listen", metrics[id]}
	})
	cl := c.client()
	leader := c.leader(cl)
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "near", 3, -1
	req.Topics = append(req.Topics, rt)
	if resp, err := req.RequestWith(context.Background(), cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %v %+v", err, resp)
	}
	const n = 3000
	if err := c.produce(cl, "near", "r", n, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	// Followers apply on the next heartbeat: wait until they hold everything.
	time.Sleep(time.Second)

	read := func(rack string) int {
		t.Helper()
		m := map[string]map[int32]kgo.Offset{"near": {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}}
		consumer := c.client(kgo.ConsumePartitions(m), kgo.Rack(rack))
		defer consumer.Close()
		total := 0
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for total < n {
			fs := consumer.PollFetches(ctx)
			if ctx.Err() != nil {
				t.Fatalf("rack %s: read %d of %d records, then timed out: %v", rack, total, n, fs.Errors())
			}
			fs.EachRecord(func(*kgo.Record) { total++ })
		}
		return total
	}
	// The metrics are published a little after the fact.
	bytesFrom := func(id int32) float64 {
		var v float64
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			if v = scrapeMetric(t, metrics[id], "samsa_follower_fetch_bytes_total"); v > 0 {
				break
			}
		}
		return v
	}

	// In the leader's rack: nothing is read from a follower.
	leaderRack := racks[leader-1]
	read(leaderRack)
	for id := range c.nodes {
		if got := scrapeMetric(t, metrics[id], "samsa_follower_fetch_bytes_total"); got != 0 {
			t.Fatalf("a consumer in the leader's rack read %v bytes from node %d, which is not the leader", got, id)
		}
	}
	// In a follower's rack: it reads from that follower.
	follower := int32(1)
	if leader == 1 {
		follower = 2
	}
	read(racks[follower-1])
	if got := bytesFrom(follower); got < n {
		t.Fatalf("a consumer in rack %s read only %v bytes from node %d: it did not follow the preferred replica", racks[follower-1], got, follower)
	}
	t.Logf("leader %d; a consumer in rack %s (node %d) read %.0f bytes from its own rack", leader, racks[follower-1], follower, bytesFrom(follower))
}
