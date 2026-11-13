package sim

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"slices"
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
	skipFlag = flag.String("skip-faults", "", "with -seed: faults to skip, by number, as \"1-4,7\"")
	calm     = flag.Bool("calm", false, "with -seed: no message drops or duplicates between nodes")
	mutation = flag.String("mutation", "", "with -seed or -shrink: switch on the named mutation of TestMutationSuite")
	shrink   = flag.Uint64("shrink", 0, "shrink this failing seed and print its schedule")
	tlFile   = flag.String("timeline", "", "with -seed: write the run as an HTML timeline to this file")
)

// replayConfig is the Config the -seed, -skip-faults, -calm and -mutation flags
// describe.
func replayConfig(t *testing.T, seed uint64) Config {
	t.Helper()
	cfg := Config{Seed: seed, Calm: *calm}
	var err error
	if cfg.Skip, err = ParseSkip(*skipFlag); err != nil {
		t.Fatal(err)
	}
	if *mutation != "" {
		i := slices.IndexFunc(mutations, func(m mutationCase) bool { return m.name == *mutation })
		if i < 0 {
			t.Fatalf("-mutation: no mutation named %q", *mutation)
		}
		cfg.Mutate = mutations[i].mutate
	}
	return cfg
}

// TestShrinkSeed is `make shrink SEED=x`: the seed's failure cut down to
// the faults it needs.
func TestShrinkSeed(t *testing.T) {
	if *shrink == 0 {
		t.Skip("set -shrink")
	}
	s, err := Shrink(replayConfig(t, *shrink), 500)
	if err != nil {
		t.Fatalf("seed %d: %v", *shrink, err)
	}
	out := s.String()
	if *mutation != "" {
		out += fmt.Sprintf(" MUTATION=%q", *mutation) // the line ends with the replay command
	}
	t.Logf("\n%s", out)
}

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
		cfg := replayConfig(t, *seed)
		cfg.Trace = os.Stdout
		if *tlFile != "" {
			f, err := os.Create(*tlFile)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			cfg.Timeline = f
			t.Cleanup(func() { t.Logf("timeline: %s", *tlFile) })
		}
		res := Run(cfg)
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
		addStats(&total, res.Stats)
	}
	t.Logf("%d seeds, %d failed; totals %+v", *seeds, failed, total)
	if never := neverReached(total); len(never) > 0 {
		t.Logf("never reached: %s", strings.Join(never, ", "))
	}
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
			"idempotent records acknowledged": total.IdempotentAcked, "idempotent retries": total.Retries, "producer ids": total.ProducerIDs, "producers expired": total.ProducersExpired, "leadership handovers": total.Handovers,
			"records deleted": total.RecordDeletes, "partitions added": total.PartitionsAdded, "topics deleted": total.TopicsDeleted,
			"faults on the serving leader": total.LeaderFaults, "leaders isolated": total.LeaderIsolated, "terms without a leader": total.TermsWithoutLeader,
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
	const maxSeeds, scoreSeeds = 2000, 200
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			// The score: how many of the same scoreSeeds seeds catch it.
			caught, first := 0, Result{}
			for _, res := range runMany(1, scoreSeeds, tc.mutate) {
				if res.Err != nil {
					if caught++; caught == 1 {
						first = res
					}
				}
			}
			// None of them: search on for a first catch, as before.
			for from := uint64(scoreSeeds + 1); first.Err == nil && from <= maxSeeds; from += 100 {
				for _, res := range runMany(from, 100, tc.mutate) {
					if res.Err != nil {
						first = res
						break
					}
				}
			}
			if first.Err == nil {
				t.Fatalf("not caught in %d seeds", maxSeeds)
			}
			// Only a seed that passes without the mutation shows the
			// mutation was caught; otherwise it is a bug.
			if clean := Run(Config{Seed: first.Seed}); clean.Err != nil {
				t.Fatalf("seed %d fails without the mutation too: %v\nreproduce: make sim SEED=%d", first.Seed, clean.Err, first.Seed)
			}
			t.Logf("score: caught by %d of %d seeds; first at seed %d: %v", caught, scoreSeeds, first.Seed, first.Err)
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

// mutationCase is one deliberate bug of the suite.
type mutationCase struct {
	name   string
	mutate func(*broker.Config)
}

// mutations is the suite of TestMutationSuite. Not here: committing an
// earlier term's entry by counting replicas (raft.MutCommitAnyTerm, Figure
// 8). Its code path runs thousands of times in 300 seeds, but doing harm
// needs five events in a row: two leaders that each crash with an entry
// nobody else has, the first returning to push its entry to a majority, the
// message with its own no-op lost, and its death before the next heartbeat.
// No seed in 2000 lined them up, with 3 and 5 nodes, 20 ms faults,
// partitions and one entry per message. TestRandomSchedulesCatchBrokenRaft
// in internal/raft, which faults far harder, catches it. Not here either:
// keeping a deleted topic's offsets (UnsafeKeepOffsetsOnDelete). The
// simulator never creates a topic again under a deleted one's name;
// TestDeleteTopicForgetsEverything in internal/broker catches it.
var mutations = []mutationCase{
	{"grant a vote before it is on disk", func(c *broker.Config) { c.UnsafeRaft |= raft.MutVoteWithoutPersist }},
	{"grant a vote comparing log length only", func(c *broker.Config) { c.UnsafeRaft |= raft.MutVoteByLength }},
	{"truncate on a message that merely carries fewer entries", func(c *broker.Config) { c.UnsafeRaft |= raft.MutTruncateOnStale }},
	{"acknowledge entries before they are synced", func(c *broker.Config) { c.Sync, c.UnsafeAckBeforeSync = broker.SyncAlways, true }},
	{"serve as soon as elected", func(c *broker.Config) { c.UnsafeServeEarly = true }},
	{"answer reads without the lease", func(c *broker.Config) { c.UnsafeNoLease = true }},
	{"accept offset commits from any generation", func(c *broker.Config) { c.UnsafeNoFencing = true }},
	{"snapshot before the log is synced", func(c *broker.Config) { c.UnsafeRaft, c.SnapshotEvery = c.UnsafeRaft|raft.MutSnapshotUnsynced, 1 }},
	{"append an idempotent producer's retries again", func(c *broker.Config) { c.UnsafeNoSequenceCheck = true }},
	{"keep serving during a leadership transfer", func(c *broker.Config) { c.UnsafeRaft |= raft.MutTransferKeepsLease }},
	{"expire producers without waiting", func(c *broker.Config) { c.UnsafeExpireEarly = true }},
	{"delete segments before a snapshot covers them", func(c *broker.Config) { c.UnsafeTrimEarly, c.RetentionBytes = true, 8<<10 }},
	{"let retention move a partition's start back", func(c *broker.Config) { c.UnsafeTrimResetsStart, c.RetentionBytes = true, 8<<10 }},
}

// addStats adds every counter of s to total.
func addStats(total *Stats, s Stats) {
	tv, sv := reflect.ValueOf(total).Elem(), reflect.ValueOf(s)
	for i := range tv.NumField() {
		tv.Field(i).SetInt(tv.Field(i).Int() + sv.Field(i).Int())
	}
}

// neverReached names the counters of total that stayed at zero.
func neverReached(total Stats) []string {
	var names []string
	v := reflect.ValueOf(total)
	for i := range v.NumField() {
		if v.Field(i).Int() == 0 {
			names = append(names, v.Type().Field(i).Name)
		}
	}
	return names
}
