package integration

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestSoak runs one node under a steady, churning workload for a long time
// and samples what it holds: memory, disk, open files, connections
// (docs/design/g5-soak.md). It runs only with SAMSA_SOAK=1 (`make soak`).
//
//	SAMSA_SOAK_DURATION  how long (default 24h)
//	SAMSA_SOAK_SAMPLE    sampling period (default 1m)
//	SAMSA_SOAK_RATE      records per second produced (default 500, of 1 KiB)
//	SAMSA_SOAK_RETENTION -retention-bytes (default 256 MiB)
//	SAMSA_SOAK_OUT       directory for samples.csv and summary.md (default: the test's temp dir)
func TestSoak(t *testing.T) {
	if os.Getenv("SAMSA_SOAK") == "" {
		t.Skip("set SAMSA_SOAK=1 to run the soak test")
	}
	duration := envDuration(t, "SAMSA_SOAK_DURATION", 24*time.Hour)
	sample := envDuration(t, "SAMSA_SOAK_SAMPLE", time.Minute)
	rate := envInt(t, "SAMSA_SOAK_RATE", 500)
	retention := envInt(t, "SAMSA_SOAK_RETENTION", 256<<20)
	outDir := os.Getenv("SAMSA_SOAK_OUT")
	if outDir == "" {
		outDir = t.TempDir()
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}

	n := startSoakNode(t, filepath.Join(t.TempDir(), "data"), outDir,
		"-retention-bytes", fmt.Sprint(retention), "-producer-expiry", "10m")
	admin := franz(t, n.broker)
	createTopic(t, admin, "soak", 4)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	w := &soakWorkload{t: t, b: n.broker, rate: rate}
	var wg sync.WaitGroup
	for _, f := range []func(context.Context){w.produce, w.consume, w.consume, w.churnConnections} {
		wg.Add(1)
		go func() { defer wg.Done(); f(ctx) }()
	}

	csv, err := os.Create(filepath.Join(outDir, "samples.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer csv.Close()
	fmt.Fprintln(csv, "elapsed_s,rss_kib,data_bytes,open_files,connections,log_start,last_index,snapshot,produced,consumed,produce_errors,gaps")
	var samples []soakSample
	start := time.Now()
	tick := time.NewTicker(sample)
	defer tick.Stop()
	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick.C:
		}
		s := n.sample(t, time.Since(start))
		s.produced, s.consumed, s.produceErrors, s.gaps = w.produced.Load(), w.consumed.Load(), w.produceErrors.Load(), w.gaps.Load()
		samples = append(samples, s)
		fmt.Fprintf(csv, "%.0f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", s.elapsed.Seconds(), s.rssKiB, s.dataBytes, s.openFiles,
			s.metrics["samsa_connections"], s.metrics["samsa_raft_log_start"], s.metrics["samsa_raft_last_index"],
			s.metrics["samsa_raft_snapshot_index"], s.produced, s.consumed, s.produceErrors, s.gaps)
		csv.Sync() //nolint:errcheck // best effort, so a killed run keeps its samples
		if s.rssKiB == 0 {
			t.Errorf("the node is gone at %s", s.elapsed)
			cancel()
			break
		}
	}
	cancel()
	wg.Wait()

	restart := n.restartTime(t)
	verdict := judgeSoak(samples, int64(retention), rate, duration)
	summary := soakSummary(samples, verdict, restart, duration, rate, retention)
	if err := os.WriteFile(filepath.Join(outDir, "summary.md"), []byte(summary), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + summary)
	for _, f := range verdict.failures {
		t.Error(f)
	}
}

type soakNode struct {
	*broker
	metrics string
	logFile string
}

// startSoakNode starts the binary with its log in a file: a day of logs
// does not belong in the test's memory.
func startSoakNode(t *testing.T, dir, outDir string, args ...string) *soakNode {
	t.Helper()
	n := &soakNode{broker: &broker{t: t, addr: freeAddr(t), dir: dir}, metrics: freeAddr(t), logFile: filepath.Join(outDir, "broker.log")}
	n.args = append([]string{"-metrics-listen", n.metrics, "-log-level", "info"}, args...)
	n.launch(t)
	t.Cleanup(n.kill)
	return n
}

func (n *soakNode) launch(t *testing.T) {
	t.Helper()
	bin := os.Getenv("SAMSA_BIN")
	if bin == "" {
		bin = "../bin/samsa"
	}
	logf, err := os.OpenFile(n.logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	n.cmd = exec.Command(bin, append([]string{"-listen", n.addr, "-data", n.dir}, n.args...)...)
	n.cmd.Stderr = logf
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v (run `make build` first)", bin, err)
	}
	logf.Close()
	// A restart on a full log replays it: give it longer than waitUntil does.
	for deadline := time.Now().Add(2 * time.Minute); !n.ready(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the node was not ready within 2 minutes; log in %s", n.logFile)
		}
	}
}

func (n *soakNode) ready() bool {
	resp, err := http.Get("http://" + n.metrics + "/readyz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// restartTime stops the node gracefully and measures how long a restart
// on the soaked data takes to become ready.
func (n *soakNode) restartTime(t *testing.T) time.Duration {
	t.Helper()
	n.cmd.Process.Signal(syscall.SIGTERM) //nolint:errcheck // Wait reports it
	n.cmd.Wait()                          //nolint:errcheck // a stop by signal is an error to Wait
	start := time.Now()
	n.launch(t)
	return time.Since(start)
}

type soakSample struct {
	elapsed                                 time.Duration
	rssKiB, dataBytes, openFiles            int64
	metrics                                 map[string]int64
	produced, consumed, produceErrors, gaps int64
}

func (n *soakNode) sample(t *testing.T, elapsed time.Duration) soakSample {
	s := soakSample{elapsed: elapsed, metrics: map[string]int64{}}
	pid := strconv.Itoa(n.cmd.Process.Pid)
	if out, err := exec.Command("ps", "-o", "rss=", "-p", pid).Output(); err == nil {
		s.rssKiB, _ = strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	}
	filepath.WalkDir(n.dir, func(_ string, d fs.DirEntry, err error) error { //nolint:errcheck // files come and go
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				s.dataBytes += info.Size()
			}
		}
		return nil
	})
	s.openFiles = openFiles(pid)
	if resp, err := http.Get("http://" + n.metrics + "/metrics"); err == nil {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			name, value, ok := strings.Cut(sc.Text(), " ")
			if ok && !strings.HasPrefix(name, "#") && !strings.Contains(name, "{") {
				s.metrics[name], _ = strconv.ParseInt(value, 10, 64)
			}
		}
		resp.Body.Close()
	}
	return s
}

// openFiles counts the process's open file descriptors: /proc on Linux,
// lsof elsewhere; -1 if neither works.
func openFiles(pid string) int64 {
	if runtime.GOOS == "linux" {
		if fds, err := os.ReadDir("/proc/" + pid + "/fd"); err == nil {
			return int64(len(fds))
		}
	}
	out, err := exec.Command("lsof", "-n", "-P", "-p", pid).Output()
	if err != nil {
		return -1
	}
	return int64(strings.Count(string(out), "\n") - 1) // minus the header
}

// soakWorkload keeps the node busy the way a long-lived deployment does:
// a steady producer, a consumer group that commits, and churn. Producers
// are replaced every five minutes (new idempotent producer ids, which must
// expire), consumers leave and rejoin (rebalances), and short-lived
// connections come and go every second.
type soakWorkload struct {
	t                                       *testing.T
	b                                       *broker
	rate                                    int
	produced, consumed, produceErrors, gaps atomic.Int64
	mu                                      sync.Mutex
	next                                    map[int32]int64 // next offset expected per partition
}

func (w *soakWorkload) produce(ctx context.Context) {
	for ctx.Err() == nil {
		// Uncompressed random values: the bytes on disk are the bytes asked
		// for (zeros compressed to nothing in the first trial run).
		cl, err := kgo.NewClient(kgo.SeedBrokers(w.b.addr), kgo.DefaultProduceTopic("soak"), kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerBatchCompression(kgo.NoCompression()))
		if err != nil {
			w.t.Error(err)
			return
		}
		generation, cancel := context.WithTimeout(ctx, 5*time.Minute)
		w.produceFor(generation, cl)
		cancel()
		cl.Close()
	}
}

func (w *soakWorkload) produceFor(ctx context.Context, cl *kgo.Client) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	perTick := max(1, w.rate/100)
	for {
		select {
		case <-ctx.Done():
			cl.Flush(context.Background()) //nolint:errcheck // counted in the callbacks
			return
		case <-tick.C:
		}
		for range perTick {
			value := make([]byte, 1024)
			for i := 0; i < len(value); i += 8 {
				binary.LittleEndian.PutUint64(value[i:], rand.Uint64())
			}
			cl.Produce(ctx, &kgo.Record{Value: value}, func(_ *kgo.Record, err error) {
				if err == nil {
					w.produced.Add(1)
				} else if ctx.Err() == nil {
					w.produceErrors.Add(1)
				}
			})
		}
	}
}

// consume runs one group member; every 7 minutes it leaves and rejoins.
// Offsets must follow one another per partition; a hole is a gap. A
// repeat after a rebalance (at-least-once) is not.
func (w *soakWorkload) consume(ctx context.Context) {
	for ctx.Err() == nil {
		cl, err := kgo.NewClient(kgo.SeedBrokers(w.b.addr), kgo.ConsumerGroup("soak"), kgo.ConsumeTopics("soak"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		if err != nil {
			w.t.Error(err)
			return
		}
		member, stop := context.WithTimeout(ctx, 7*time.Minute)
		for member.Err() == nil {
			fs := cl.PollFetches(member)
			fs.EachRecord(func(r *kgo.Record) {
				w.mu.Lock()
				if w.next == nil {
					w.next = map[int32]int64{}
				}
				if next, ok := w.next[r.Partition]; ok && r.Offset > next {
					w.gaps.Add(1)
				}
				w.next[r.Partition] = max(w.next[r.Partition], r.Offset+1)
				w.mu.Unlock()
				w.consumed.Add(1)
			})
		}
		stop()
		cl.Close() // commits what was polled, then leaves the group
	}
}

// churnConnections opens a client, asks for metadata, and closes it,
// every second: a leak per connection shows up within hours.
func (w *soakWorkload) churnConnections(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		cl, err := kgo.NewClient(kgo.SeedBrokers(w.b.addr))
		if err != nil {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		cl.Ping(pctx) //nolint:errcheck // a failed ping is churn too
		cancel()
		cl.Close()
	}
}

type soakVerdict struct {
	failures []string
	notes    []string
}

// judgeSoak applies the bounds of docs/design/g5-soak.md, section 3, to
// the samples taken after the first quarter of the run (warm-up).
func judgeSoak(samples []soakSample, retention int64, rate int, duration time.Duration) soakVerdict {
	var v soakVerdict
	if len(samples) < 8 {
		v.failures = append(v.failures, fmt.Sprintf("only %d samples: the run was too short to judge", len(samples)))
		return v
	}
	q := len(samples) / 4
	second, last := samples[q:2*q], samples[len(samples)-q:]
	peak := func(ss []soakSample, f func(soakSample) int64) int64 {
		m := int64(0)
		for _, s := range ss {
			m = max(m, f(s))
		}
		return m
	}
	rss := func(s soakSample) int64 { return s.rssKiB }
	if a, b := peak(second, rss), peak(last, rss); b > a+a/10+16<<10 {
		v.failures = append(v.failures, fmt.Sprintf("memory grows: peak RSS %d MiB in the second quarter, %d MiB in the last", a>>10, b>>10))
	}
	const segment = 64 << 20
	disk := func(s soakSample) int64 { return s.dataBytes }
	if bound, got := retention+3*segment, peak(samples, disk); got > bound {
		v.failures = append(v.failures, fmt.Sprintf("disk not bounded: %d MiB, bound %d MiB (retention + 3 segments)", got>>20, bound>>20))
	}
	files := func(s soakSample) int64 { return s.openFiles }
	if a, b := peak(second, files), peak(last, files); a >= 0 && b > a+16 {
		v.failures = append(v.failures, fmt.Sprintf("open files grow: %d in the second quarter, %d in the last", a, b))
	}
	end := samples[len(samples)-1]
	if end.produced*1024 > 2*retention && end.metrics["samsa_raft_log_start"] == 0 {
		v.failures = append(v.failures, fmt.Sprintf("retention never cut the log although %d MiB were produced: the disk bound was not tested", end.produced>>10))
	}
	if end.gaps > 0 {
		v.failures = append(v.failures, fmt.Sprintf("%d gaps in consumed offsets", end.gaps))
	}
	if want := int64(float64(rate) * duration.Seconds() * 0.9); end.produced < want {
		v.failures = append(v.failures, fmt.Sprintf("produced %d records, want at least 90%% of %d/s for %s", end.produced, rate, duration))
	}
	if end.produceErrors > 0 {
		v.notes = append(v.notes, fmt.Sprintf("%d produce errors (each retried by the client and then failed)", end.produceErrors))
	}
	if end.consumed < end.produced {
		v.notes = append(v.notes, fmt.Sprintf("consumed %d of %d produced: the consumers were behind at the end", end.consumed, end.produced))
	}
	return v
}

func soakSummary(samples []soakSample, v soakVerdict, restart, duration time.Duration, rate, retention int) string {
	var b strings.Builder
	result := "PASS"
	if len(v.failures) > 0 {
		result = "FAIL"
	}
	end := samples[len(samples)-1]
	fmt.Fprintf(&b, "# Soak: %s\n\n%s, %d records/s of 1 KiB, retention %d MiB, %s, Go %s.\n\n",
		result, duration, rate, retention>>20, runtime.GOOS+"/"+runtime.GOARCH, runtime.Version())
	fmt.Fprintf(&b, "| | min | median | max | last |\n|---|---|---|---|---|\n")
	row := func(name string, unit int64, f func(soakSample) int64) {
		vals := make([]int64, len(samples))
		for i, s := range samples {
			vals[i] = f(s) / unit
		}
		last := vals[len(vals)-1]
		slices.Sort(vals)
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d |\n", name, vals[0], vals[len(vals)/2], vals[len(vals)-1], last)
	}
	row("RSS, MiB", 1<<10, func(s soakSample) int64 { return s.rssKiB })
	row("data directory, MiB", 1<<20, func(s soakSample) int64 { return s.dataBytes })
	row("open files", 1, func(s soakSample) int64 { return s.openFiles })
	row("client connections", 1, func(s soakSample) int64 { return s.metrics["samsa_connections"] })
	fmt.Fprintf(&b, "\nProduced %d, consumed %d, produce errors %d, gaps %d. Restart on the soaked data: ready in %s.\n",
		end.produced, end.consumed, end.produceErrors, end.gaps, restart.Round(time.Millisecond))
	for _, f := range v.failures {
		fmt.Fprintf(&b, "\n- FAIL: %s", f)
	}
	for _, n := range v.notes {
		fmt.Fprintf(&b, "\n- note: %s", n)
	}
	b.WriteString("\n")
	return b.String()
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}
