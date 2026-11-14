package integration

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// createTopicWith creates a one-partition topic with configs. The node
// raises its format level just after it starts; until then configs are
// refused, so it tries for a while.
func createTopicWith(t *testing.T, cl *kgo.Client, name string, configs map[string]string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, -1
	keys := make([]string, 0, len(configs))
	for k := range configs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		v := configs[k]
		c.Name, c.Value = k, &v
		rt.Configs = append(rt.Configs, c)
	}
	req.Topics = append(req.Topics, rt)
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		resp, err := req.RequestWith(context.Background(), cl)
		if err == nil && resp.Topics[0].ErrorCode == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic %s: %v %+v", name, err, resp)
		}
	}
}

// readAll reads a topic's partition 0 from the start until a poll brings
// nothing, and returns the records as "key=value" in offset order, with the
// offsets alongside.
func readAll(t *testing.T, b *broker, topic string) (records []string, offsets []int64) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.addr), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for quiet := 0; quiet < 2; {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			v := string(r.Value)
			if r.Value == nil {
				v = "<tombstone>"
			}
			records, offsets = append(records, string(r.Key)+"="+v), append(offsets, r.Offset)
			n++
		})
		if n == 0 {
			quiet++
		} else {
			quiet = 0
		}
	}
	return records, offsets
}

// TestCompaction (docs/design/c2b-compaction.md): a compacted topic ends up
// with the latest record of each key, in a real client's eyes, and a
// tombstone goes once delete.retention.ms has passed. Killing the process
// and starting it again changes nothing.
func TestCompaction(t *testing.T) {
	metrics := freeAddr(t)
	b := startBroker(t, "-log-level", "warn", "-metrics-listen", metrics)
	// Records stay uncompressed: the core cannot read keys inside a
	// compressed batch, and refuses one for a compacted topic.
	cl := franz(t, b, kgo.ProducerBatchCompression(kgo.NoCompression()), kgo.MaxBufferedRecords(10000))
	createTopicWith(t, cl, "latest", map[string]string{
		"cleanup.policy": "compact", "delete.retention.ms": "1000", "min.compaction.lag.ms": "0",
	})

	const keys, updates = 20, 300
	want := map[string]string{}
	var recs []*kgo.Record
	for i := range keys * updates {
		k, v := fmt.Sprintf("k%02d", i%keys), fmt.Sprintf("v%d", i)
		want[k] = v
		recs = append(recs, &kgo.Record{Topic: "latest", Key: []byte(k), Value: []byte(v)})
	}
	recs = append(recs, &kgo.Record{Topic: "latest", Key: []byte("k00")}) // a tombstone
	delete(want, "k00")
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// A record without a key is refused.
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: "latest", Value: []byte("no key")}).FirstErr(); err == nil {
		t.Fatal("a record without a key was accepted by a compacted topic")
	}

	check := func(when string) {
		t.Helper()
		var records []string
		var offsets []int64
		// The compactor needs a few of its one-second looks, and a tombstone
		// waits for its delete.retention.ms.
		for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(time.Second) {
			records, offsets = readAll(t, b, "latest")
			if len(records) == len(want) || time.Now().After(deadline) {
				break
			}
		}
		got := map[string]string{}
		for _, r := range records {
			for i := range r {
				if r[i] == '=' {
					got[r[:i]] = r[i+1:]
					break
				}
			}
		}
		if len(records) != len(want) || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: %d records left, want %d (one per key but k00):\n%v", when, len(records), len(want), records)
		}
		for i := 1; i < len(offsets); i++ {
			if offsets[i] <= offsets[i-1] {
				t.Fatalf("%s: offsets do not ascend: %v", when, offsets)
			}
		}
		t.Logf("%s: %d records at offsets %v", when, len(records), offsets)
	}
	check("after compaction")
	if n := scrapeMetric(t, metrics, "samsa_compacted_records_total"); n < keys*updates/2 {
		t.Fatalf("samsa_compacted_records_total is %v after %d updates of %d keys", n, keys*updates, keys)
	}

	b.kill() // every rewrite is in the log: replay gives the same state
	b.start()
	check("after kill -9 and restart")
}
