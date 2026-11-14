package integration

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestBenchRetention measures what copy-forward retention costs
// (docs/design/c2-retention-compaction.md, review question 4): a single
// node, 90% of 1 KiB records to a topic kept 10 s and 10% to one kept
// forever, against the same load with both kept forever. It runs only with
// SAMSA_BENCH_RETENTION=1, for SAMSA_BENCH_RETENTION_FOR (default 1 min).
func TestBenchRetention(t *testing.T) {
	if os.Getenv("SAMSA_BENCH_RETENTION") == "" {
		t.Skip("set SAMSA_BENCH_RETENTION=1 to run the retention benchmark")
	}
	d := envDuration(t, "SAMSA_BENCH_RETENTION_FOR", time.Minute)
	for _, mixed := range []bool{false, true} {
		metrics := freeAddr(t)
		b := startBroker(t, "-sync", "always", "-log-level", "warn", "-metrics-listen", metrics)
		cl := franz(t, b, kgo.MaxBufferedRecords(10000), kgo.ProducerBatchCompression(kgo.NoCompression())) // bytes written are bytes stored
		short := "-1"
		if mixed {
			short = "10000"
		}
		retentionTopic(t, cl, "short", short)
		retentionTopic(t, cl, "long", "-1")
		value := make([]byte, 1024)
		var mu sync.Mutex
		var acked int
		var firstErr error
		start := time.Now()
		for i := 0; time.Since(start) < d; i++ {
			topic := "short"
			if i%10 == 0 {
				topic = "long"
			}
			cl.Produce(context.Background(), &kgo.Record{Topic: topic, Value: value}, func(_ *kgo.Record, err error) {
				mu.Lock()
				defer mu.Unlock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				acked++
			})
		}
		if err := cl.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		if firstErr != nil {
			t.Fatalf("produce: %v", firstErr)
		}
		time.Sleep(3 * time.Second) // a last look by the cleaner, and its snapshot
		relocated := scrapeMetric(t, metrics, "samsa_relocated_bytes_total")
		written := float64(acked) * 1024
		t.Logf("mixed=%v: %.0f rec/s, %.1f MB/s; relocated %.1f MB of %.1f MB written (amplification %.2f); log on disk %.1f MB",
			mixed, float64(acked)/elapsed.Seconds(), written/elapsed.Seconds()/1e6, relocated/1e6, written/1e6,
			(written+relocated)/written, float64(dirBytes(t, filepath.Join(b.dir, "raft")))/1e6)
		cl.Close()
		b.kill()
	}
}

func retentionTopic(t *testing.T, cl *kgo.Client, name, ms string) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, -1
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name, c.Value = "retention.ms", &ms
	rt.Configs = append(rt.Configs, c)
	req.Topics = append(req.Topics, rt)
	// The node raises its format level to 2 just after it starts; until
	// then configs are refused.
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

func scrapeMetric(t *testing.T, addr, name string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	m := regexp.MustCompile(`(?m)^` + name + ` (\S+)$`).FindSubmatch(body)
	if m == nil {
		t.Fatalf("no %s in /metrics", name)
	}
	v, _ := strconv.ParseFloat(string(m[1]), 64)
	return v
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}
