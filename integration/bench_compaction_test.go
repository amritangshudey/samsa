package integration

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestBenchCompaction measures what key compaction costs
// (docs/design/c2b-compaction.md, review question 8): a single node, 1 KiB
// records over 100,000 keys, 90% of the updates to 10% of the keys, to a
// compacted topic and, for comparison, to a topic with the default policy.
// It runs only with SAMSA_BENCH_COMPACTION=1, for SAMSA_BENCH_COMPACTION_FOR
// (default 1 min) per run.
func TestBenchCompaction(t *testing.T) {
	if os.Getenv("SAMSA_BENCH_COMPACTION") == "" {
		t.Skip("set SAMSA_BENCH_COMPACTION=1 to run the compaction benchmark")
	}
	d := envDuration(t, "SAMSA_BENCH_COMPACTION_FOR", time.Minute)
	for _, compact := range []bool{false, true} {
		metrics := freeAddr(t)
		b := startBroker(t, "-sync", "always", "-log-level", "warn", "-metrics-listen", metrics)
		cl := franz(t, b, kgo.MaxBufferedRecords(10000), kgo.ProducerBatchCompression(kgo.NoCompression()))
		configs := map[string]string{}
		if compact {
			configs["cleanup.policy"] = "compact"
		}
		createTopicWith(t, cl, "bench", configs)
		value := make([]byte, 1024)
		rng := rand.New(rand.NewSource(1))
		var mu sync.Mutex
		var acked int
		var firstErr error
		start := time.Now()
		stop := make(chan struct{})
		go func() { // progress, to see when the compactor works
			for tick := time.NewTicker(10 * time.Second); ; {
				select {
				case <-stop:
					return
				case <-tick.C:
					t.Logf("  %v: compacted %.0f records, relocated %.1f MB, log on disk %.1f MB", time.Since(start).Round(time.Second),
						scrapeMetric(t, metrics, "samsa_compacted_records_total"), scrapeMetric(t, metrics, "samsa_relocated_bytes_total")/1e6, float64(dirBytes(t, filepath.Join(b.dir, "raft")))/1e6)
				}
			}
		}()
		for time.Since(start) < d {
			k := rng.Intn(10000) // the hot tenth
			if rng.Intn(10) == 0 {
				k = 10000 + rng.Intn(90000)
			}
			cl.Produce(context.Background(), &kgo.Record{Topic: "bench", Key: []byte(fmt.Sprintf("key-%06d", k)), Value: value}, func(_ *kgo.Record, err error) {
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
		close(stop)
		elapsed := time.Since(start)
		if firstErr != nil {
			t.Fatalf("produce: %v", firstErr)
		}
		// The compactor works on with no load: wait until the log stops
		// shrinking, as the time it needs to catch up.
		settle, last, still := time.Now(), int64(-1), 0
		for still < 3 && time.Since(settle) < 10*time.Minute {
			time.Sleep(5 * time.Second)
			if size := dirBytes(t, filepath.Join(b.dir, "raft")); size < last || last < 0 {
				last, still = size, 0
			} else {
				still++
			}
		}
		t.Logf("  caught up %v after the load stopped", time.Since(settle).Round(time.Second))
		written := float64(acked) * 1024
		t.Logf("compact=%v: %.0f rec/s, %.1f MB/s; compacted %.0f records of %d; relocated %.1f MB; log on disk %.1f MB",
			compact, float64(acked)/elapsed.Seconds(), written/elapsed.Seconds()/1e6,
			scrapeMetric(t, metrics, "samsa_compacted_records_total"), acked,
			scrapeMetric(t, metrics, "samsa_relocated_bytes_total")/1e6, float64(dirBytes(t, filepath.Join(b.dir, "raft")))/1e6)
		cl.Close()
		b.kill()
	}
}
