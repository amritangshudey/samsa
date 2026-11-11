package integration

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestBench measures throughput and latency of the built binary with a real
// client. It runs only with SAMSA_BENCH=1 (`make bench`); the method is in
// docs/design/phase-7.md, section 2. With SAMSA_BENCH_OUT set it writes a
// Markdown table there.
func TestBench(t *testing.T) {
	if os.Getenv("SAMSA_BENCH") == "" {
		t.Skip("set SAMSA_BENCH=1 to run the benchmarks")
	}
	throughputFor := envDuration(t, "SAMSA_BENCH_THROUGHPUT", 5*time.Second)
	latencyFor := envDuration(t, "SAMSA_BENCH_LATENCY", 10*time.Second)
	const reps = 3

	var rows []string
	for _, nodes := range []int{1, 3} {
		for _, policy := range []string{"always", "interval", "never"} {
			for _, size := range []int{100, 1024, 10240} {
				var runs []benchRun
				for range reps {
					runs = append(runs, benchOnce(t, nodes, policy, size, throughputFor, latencyFor))
				}
				row := summarise(nodes, policy, size, runs)
				t.Log(row)
				rows = append(rows, row)
			}
		}
	}
	if out := os.Getenv("SAMSA_BENCH_OUT"); out != "" {
		head := "| Nodes | fsync | Record | Throughput, rec/s (min–max) | MB/s | Latency rate, rec/s | p50 ms | p99 ms | p99.9 ms | max ms |\n|---|---|---|---|---|---|---|---|---|---|\n"
		if err := os.WriteFile(out, []byte(head+strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type benchRun struct {
	throughput float64 // records/s, closed loop
	rate       float64 // records/s targeted by the open-loop run
	achieved   float64 // records/s acknowledged in the open-loop run
	latencies  []time.Duration
}

func benchOnce(t *testing.T, nodes int, policy string, size int, throughputFor, latencyFor time.Duration) benchRun {
	var r benchRun
	t.Run(fmt.Sprintf("n%d-%s-%dB", nodes, policy, size), func(t *testing.T) {
		c := startCluster(t, nodes, "-sync", policy, "-log-level", "warn")
		cl := c.client(kgo.MaxBufferedRecords(10000))
		benchTopic(t, cl)
		value := make([]byte, size)
		closedLoop(t, cl, value, time.Second) // warm-up: connections, metadata, page cache
		r.throughput = closedLoop(t, cl, value, throughputFor)
		r.rate = r.throughput / 2
		r.latencies, r.achieved = openLoop(t, cl, value, r.rate, latencyFor)
	})
	return r
}

// benchTopic creates "bench" with three partitions replicated to every node.
func benchTopic(t *testing.T, cl *kgo.Client) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "bench", 3, -1
	req.Topics = append(req.Topics, rt)
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := req.RequestWith(ctx, cl)
		cancel()
		if err == nil && resp.Topics[0].ErrorCode == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("create topic: %v %+v", err, resp)
		}
	}
}

// closedLoop sends as fast as the client's buffer allows for d and returns
// acknowledged records per second.
func closedLoop(t *testing.T, cl *kgo.Client, value []byte, d time.Duration) float64 {
	t.Helper()
	var mu sync.Mutex
	var acked int
	var firstErr error
	start := time.Now()
	for i := 0; time.Since(start) < d; i++ {
		cl.Produce(context.Background(), &kgo.Record{Topic: "bench", Partition: int32(i % 3), Value: value}, func(_ *kgo.Record, err error) {
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
	return float64(acked) / elapsed.Seconds()
}

// openLoop sends at a fixed rate for d. Record i is due at start + i/rate,
// and its latency is measured from that due time, so a stall that delays
// sending is counted (no coordinated omission).
func openLoop(t *testing.T, cl *kgo.Client, value []byte, rate float64, d time.Duration) ([]time.Duration, float64) {
	t.Helper()
	total := int(rate * d.Seconds())
	lat := make([]time.Duration, 0, total)
	var mu sync.Mutex
	var firstErr error
	start := time.Now()
	due := func(i int) time.Time { return start.Add(time.Duration(float64(i) / rate * float64(time.Second))) }
	for next := 0; next < total; {
		for now := time.Now(); next < total && !due(next).After(now); next++ {
			at := due(next)
			cl.Produce(context.Background(), &kgo.Record{Topic: "bench", Partition: int32(next % 3), Value: value}, func(_ *kgo.Record, err error) {
				done := time.Now()
				mu.Lock()
				defer mu.Unlock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				lat = append(lat, done.Sub(at))
			})
		}
		if next < total {
			time.Sleep(time.Until(due(next)))
		}
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if firstErr != nil {
		t.Fatalf("produce: %v", firstErr)
	}
	return lat, float64(len(lat)) / elapsed.Seconds()
}

// summarise reports the run with the median throughput, and the spread of
// throughput over all runs.
func summarise(nodes int, policy string, size int, runs []benchRun) string {
	slices.SortFunc(runs, func(a, b benchRun) int { return cmp.Compare(a.throughput, b.throughput) })
	m := runs[len(runs)/2]
	lat := slices.Clone(m.latencies)
	slices.Sort(lat)
	pct := func(p float64) string {
		return fmt.Sprintf("%.2f", float64(lat[min(len(lat)-1, int(p*float64(len(lat))))])/1e6)
	}
	rate := fmt.Sprintf("%.0f", m.rate)
	if m.achieved < 0.95*m.rate {
		rate += fmt.Sprintf(" (only %.0f achieved)", m.achieved)
	}
	return fmt.Sprintf("| %d | %s | %d B | %.0f (%.0f–%.0f) | %.1f | %s | %s | %s | %s | %s |",
		nodes, policy, size, m.throughput, runs[0].throughput, runs[len(runs)-1].throughput,
		m.throughput*float64(size)/1e6, rate, pct(0.50), pct(0.99), pct(0.999), pct(1))
}

func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return d
}
