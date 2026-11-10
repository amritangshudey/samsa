package sim

import (
	"flag"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/amritangshudey/samsa/internal/broker"
)

var (
	seeds    = flag.Int("seeds", 200, "number of seeds TestSim runs")
	seed     = flag.Uint64("seed", 0, "run this one seed with a full trace")
	baseSeed = flag.Uint64("base-seed", 1, "first seed of a -seeds run")
)

// runMany runs n seeds from first on all CPUs. Each run is independent and
// single-threaded; only the test fans out.
func runMany(first uint64, n int, mutate func(*broker.Config)) []Result {
	out := make([]Result, n)
	var wg sync.WaitGroup
	next := make(chan int)
	for range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				out[i] = Run(Config{Seed: first + uint64(i), Mutate: mutate})
			}
		}()
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	return out
}

// TestSim is the simulator's entry point: `make sim SEEDS=n` or
// `make sim SEED=x`.
func TestSim(t *testing.T) {
	if *seed != 0 {
		res := Run(Config{Seed: *seed, Trace: os.Stdout})
		t.Logf("seed %d: %s\n%+v", res.Seed, res.Setup, res.Stats)
		if res.Err != nil {
			t.Fatalf("seed %d: %v", res.Seed, res.Err)
		}
		return
	}
	var total Stats
	failed := 0
	for _, res := range runMany(*baseSeed, *seeds, nil) {
		if res.Err != nil {
			if failed++; failed <= 5 {
				t.Errorf("seed %d (%s): %v\nreproduce: make sim SEED=%d", res.Seed, res.Setup, res.Err, res.Seed)
			}
			continue
		}
		s := res.Stats
		total.Requests += s.Requests
		total.RecordsAcked += s.RecordsAcked
		total.RecordsRead += s.RecordsRead
		total.FetchesChecked += s.FetchesChecked
		total.PowerLosses += s.PowerLosses
		total.Kills += s.Kills
		total.SyncFailures += s.SyncFailures
		total.RecoveriesCut += s.RecoveriesCut
		total.Starts += s.Starts
		total.Disconnects += s.Disconnects
	}
	t.Logf("%d seeds, %d failed; totals %+v", *seeds, failed, total)
	// A suite that passes because nothing happened proves nothing.
	if *seeds >= 100 && failed == 0 {
		for name, n := range map[string]int{
			"records acknowledged": total.RecordsAcked, "records read back": total.RecordsRead,
			"power losses": total.PowerLosses, "kills": total.Kills, "failed syncs": total.SyncFailures,
			"recoveries interrupted": total.RecoveriesCut, "client disconnects": total.Disconnects,
		} {
			if n < *seeds/10 {
				t.Errorf("only %d %s in %d seeds: the suite is not exercising what it claims", n, name, *seeds)
			}
		}
	}
}

// TestDeterminism runs seeds twice: same seed, same trace, same counts.
func TestDeterminism(t *testing.T) {
	a, b := runMany(1000, 20, nil), runMany(1000, 20, nil)
	for i := range a {
		if a[i].TraceHash != b[i].TraceHash || a[i].Stats != b[i].Stats || (a[i].Err == nil) != (b[i].Err == nil) {
			t.Errorf("seed %d is not deterministic:\n%08x %+v\n%08x %+v", a[i].Seed, a[i].TraceHash, a[i].Stats, b[i].TraceHash, b[i].Stats)
		}
	}
}

// TestSimCatchesAckBeforeSync breaks the broker's central rule (answer only
// after the write is durable) and requires the simulator to notice. If
// this test fails, a green TestSim means nothing.
func TestSimCatchesAckBeforeSync(t *testing.T) {
	caught := 0
	results := runMany(1, 200, func(c *broker.Config) {
		c.Sync, c.UnsafeAckBeforeSync = broker.SyncAlways, true
	})
	for _, res := range results {
		if res.Err != nil {
			if caught++; caught == 1 {
				t.Logf("seed %d: %v", res.Seed, res.Err)
			}
		}
	}
	t.Logf("broken broker caught by %d of %d seeds", caught, len(results))
	if caught == 0 {
		t.Fatal("no seed noticed a broker that acknowledges before syncing")
	}
}
