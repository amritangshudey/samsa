package sim

import (
	"flag"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/raft"
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
		total.GroupJoins += s.GroupJoins
		total.GroupSyncs += s.GroupSyncs
		total.OffsetCommits += s.OffsetCommits
		total.OffsetFetches += s.OffsetFetches
		total.ZombiesRefused += s.ZombiesRefused
		total.Leaders += s.Leaders
		total.NotLeader += s.NotLeader
		total.ReplicasCompared += s.ReplicasCompared
		total.Partitions += s.Partitions
		total.SnapshotStarts += s.SnapshotStarts
		total.Trims += s.Trims
		total.Installs += s.Installs
	}
	t.Logf("%d seeds, %d failed; totals %+v", *seeds, failed, total)
	// A suite that passes because nothing happened proves nothing.
	if *seeds >= 100 && failed == 0 {
		for name, n := range map[string]int{
			"records acknowledged": total.RecordsAcked, "records read back": total.RecordsRead,
			"power losses": total.PowerLosses, "kills": total.Kills, "failed syncs": total.SyncFailures,
			"recoveries interrupted": total.RecoveriesCut, "client disconnects": total.Disconnects,
			"group joins": total.GroupJoins, "group syncs": total.GroupSyncs, "offset commits": total.OffsetCommits,
			"offset fetches checked": total.OffsetFetches, "zombie commits refused": total.ZombiesRefused,
			"leaders elected": total.Leaders, "requests refused by a non-leader": total.NotLeader, "replica comparisons": total.ReplicasCompared, "partitions": total.Partitions,
			"starts from a snapshot": total.SnapshotStarts, "log trims": total.Trims, "snapshots installed": total.Installs,
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

// The mutation suite (docs/design/phase-6.md, section 4): each switch
// removes one step that correctness depends on, and the simulator must
// notice within maxSeeds. A mutation that is never caught is a missing
// fault or a missing invariant. If this test fails, a green TestSim means
// nothing.
func TestMutationSuite(t *testing.T) {
	if raceEnabled {
		// Thousands of single-threaded runs: the race detector finds
		// nothing here and makes them ten times slower. `make test` runs it.
		t.Skip("skipped under the race detector")
	}
	const maxSeeds = 2000
	for _, tc := range []struct {
		name   string
		mutate func(*broker.Config)
	}{
		// Not here: committing an earlier term's entry by counting replicas
		// (raft.MutCommitAnyTerm, Figure 8). Its code path runs thousands of
		// times in 300 seeds, but doing harm needs five events in a row:
		// two leaders that each crash with an entry nobody else has, the
		// first returning to push its entry to a majority, the message with
		// its own no-op lost, and its death before the next heartbeat. No
		// seed in 2000 lined them up, with 3 and 5 nodes, 20 ms faults,
		// partitions and one entry per message. TestRandomSchedulesCatch-
		// BrokenRaft in internal/raft, which faults far harder, catches it.
		{"grant a vote before it is on disk", func(c *broker.Config) { c.UnsafeRaft = raft.MutVoteWithoutPersist }},
		{"grant a vote comparing log length only", func(c *broker.Config) { c.UnsafeRaft = raft.MutVoteByLength }},
		{"truncate on a message that merely carries fewer entries", func(c *broker.Config) { c.UnsafeRaft = raft.MutTruncateOnStale }},
		{"acknowledge entries before they are synced", func(c *broker.Config) { c.Sync, c.UnsafeAckBeforeSync = broker.SyncAlways, true }},
		{"serve as soon as elected", func(c *broker.Config) { c.UnsafeServeEarly = true }},
		{"answer reads without the lease", func(c *broker.Config) { c.UnsafeNoLease = true }},
		{"accept offset commits from any generation", func(c *broker.Config) { c.UnsafeNoFencing = true }},
		{"snapshot before the log is synced", func(c *broker.Config) { c.UnsafeRaft, c.SnapshotEvery = raft.MutSnapshotUnsynced, 1 }},
		{"delete segments before a snapshot covers them", func(c *broker.Config) { c.UnsafeTrimEarly, c.RetentionBytes = true, 8<<10 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for first := uint64(1); first <= maxSeeds; first += 100 {
				for _, res := range runMany(first, 100, tc.mutate) {
					if res.Err == nil {
						continue
					}
					// Only a seed that passes without the mutation shows
					// the mutation was caught; otherwise it is a bug.
					if clean := Run(Config{Seed: res.Seed}); clean.Err != nil {
						t.Fatalf("seed %d fails without the mutation too: %v\nreproduce: make sim SEED=%d", res.Seed, clean.Err, res.Seed)
					}
					t.Logf("caught at seed %d (%s): %v", res.Seed, res.Setup, res.Err)
					return
				}
			}
			t.Errorf("not caught in %d seeds", maxSeeds)
		})
	}
}

// L1 fires: a cluster that never elects a leader, and a leader that
// refuses every write, are both reported once faults stop. The workload
// runs for a millisecond, so nothing else gets the chance to complain.
func TestLivenessChecksFire(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*broker.Config)
		want   string
	}{
		"no election":    {func(c *broker.Config) { c.ElectionTimeout = time.Hour }, "did not all come back, elect a leader"},
		"writes refused": {func(c *broker.Config) { c.MaxBatchBytes = 16 }, "L1: on a settled cluster, a produce failed"},
	} {
		found := false
		for seed := uint64(1); seed < 50 && !found; seed++ {
			res := Run(Config{Seed: seed, Duration: time.Millisecond, Mutate: tc.mutate})
			if !strings.Contains(res.Setup, "nodes=3") {
				continue
			}
			found = true
			if res.Err == nil || !strings.Contains(res.Err.Error(), tc.want) {
				t.Errorf("%s: seed %d: got %v, want an error containing %q", name, seed, res.Err, tc.want)
			}
		}
		if !found {
			t.Fatalf("%s: no three-node seed below 50", name)
		}
	}
}

// TestRegressions runs every seed in regressions.txt.
func TestRegressions(t *testing.T) {
	data, err := os.ReadFile("regressions.txt")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		field, _, _ := strings.Cut(line, "#")
		if field = strings.TrimSpace(field); field == "" {
			continue
		}
		seed, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			t.Fatalf("regressions.txt: %q is not a seed", line)
		}
		if res := Run(Config{Seed: seed}); res.Err != nil {
			t.Errorf("seed %d: %v\nreproduce: make sim SEED=%d", seed, res.Err, seed)
		}
	}
}
