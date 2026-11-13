package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// copyApp is the sample application of docs/guides/testing-applications.md:
// a franz-go consumer in group "app" that copies every record of topic
// "in" to topic "out", keeping its key. When its partitions are revoked in
// the middle of a batch it abandons the rest of the batch, as an
// application must when it no longer owns them.
//
// With commitFirst it has the classic bug: it commits the batch's offsets
// before processing the batch, so an abandoned batch is never processed by
// anyone. Without it, it commits only what it has written.
//
// It runs until it has seen nothing new for idle.
func copyApp(t *testing.T, b *broker, commitFirst bool, idle time.Duration) {
	t.Helper()
	var revoked atomic.Bool
	onRevoke := func(context.Context, *kgo.Client, map[string][]int32) { revoked.Store(true) }
	cl := franz(t, b,
		kgo.ConsumerGroup("app"), kgo.ConsumeTopics("in"), kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.RangeBalancer()), // eager: every rebalance revokes everything
		kgo.HeartbeatInterval(100*time.Millisecond),
		kgo.OnPartitionsRevoked(onRevoke), kgo.OnPartitionsLost(onRevoke),
		kgo.FetchMaxWait(100*time.Millisecond),
		// After NOT_LEADER_OR_FOLLOWER the client waits for fresh metadata,
		// by default at most every 5 s; a test wants that quicker.
		kgo.MetadataMinAge(250*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.NoCompression()), // the verdict reads uncompressed records
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for last := time.Now(); time.Since(last) < idle; {
		revoked.Store(false)
		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fs := cl.PollFetches(pctx)
		pcancel()
		if ctx.Err() != nil {
			t.Fatal("the application did not finish in time")
		}
		recs := fs.Records()
		if len(recs) == 0 {
			continue
		}
		if commitFirst {
			cl.CommitRecords(ctx, recs...) //nolint:errcheck // the bug: whatever happens next, these count as done
		}
		var done []*kgo.Record
		for _, r := range recs {
			if revoked.Load() {
				break // no longer ours: whoever owns the partition now resumes from the committed offset
			}
			out := &kgo.Record{Topic: "out", Partition: r.Partition, Key: r.Key, Value: r.Value}
			if err := cl.ProduceSync(ctx, out).FirstErr(); err != nil {
				break
			}
			done = append(done, r)
			time.Sleep(time.Millisecond) // the work
		}
		if !commitFirst && len(done) > 0 {
			cl.CommitRecords(ctx, done...) //nolint:errcheck // a failed commit means reprocessing, never loss
		}
		last = time.Now() // idle counts from the end of the work, not from the poll
	}
}

// verdict runs `samsa verdict` on a stopped broker's data.
func verdict(t *testing.T, b *broker, args ...string) (string, int) {
	t.Helper()
	bin := os.Getenv("SAMSA_BIN")
	if bin == "" {
		bin = "../bin/samsa"
	}
	cmd := exec.Command(bin, append([]string{"verdict", "-data", b.dir, "-in", "in", "-out", "out", "-group", "app"}, args...)...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &exit):
		return string(out), exit.ExitCode()
	}
	t.Fatal(err)
	return "", 0
}

// runPipeline starts a broker with the given faults, writes the input,
// runs the application, stops the broker and returns the verdict.
func runPipeline(t *testing.T, faults string, commitFirst bool) (out string, code int, log string) {
	b := startBroker(t, "-faults", faults)
	admin := franz(t, b)
	createTopic(t, admin, "in", 3)
	createTopic(t, admin, "out", 3)
	var recs []*kgo.Record
	for i := range 600 {
		recs = append(recs, &kgo.Record{Topic: "in", Partition: int32(i % 3), Key: fmt.Appendf(nil, "k%04d", i), Value: []byte("v")})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	in := franz(t, b, kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err := in.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce the input: %v", err)
	}
	copyApp(t, b, commitFirst, 3*time.Second)
	b.kill()
	out, code = verdict(t, b)
	t.Logf("verdict (exit %d):\n%s", code, out)
	return out, code, b.log()
}

// TestVerdictCatchesCommitBeforeProcess is the acceptance of production
// plan item 5b: under forced rebalances, the application that commits
// before it processes is reported as losing records, with the seed; the
// same application committing after processing passes, under every fault
// kind.
func TestVerdictCatchesCommitBeforeProcess(t *testing.T) {
	t.Run("commit before process loses records", func(t *testing.T) {
		const faults = "seed=5,rate=0.2,kinds=rebalance"
		var out string
		var code int
		// Losing records needs a rebalance to land inside a batch; the
		// seed decides when rebalances happen, the scheduler decides
		// where the application is at that moment. A few runs settle it.
		for range 3 {
			if out, code, _ = runPipeline(t, faults, true); code == 1 {
				break
			}
		}
		if code != 1 || !strings.Contains(out, "lost: group \"app\" committed past them") || !strings.Contains(out, "samsa -faults "+faults) {
			t.Fatalf("want a FAIL naming lost records and the faults, got exit %d:\n%s", code, out)
		}
	})
	t.Run("commit after process passes", func(t *testing.T) {
		out, code, log := runPipeline(t, "seed=5,rate=0.05", false)
		if code != 0 || !strings.HasPrefix(out, "PASS") {
			t.Fatalf("want PASS, got exit %d:\n%s", code, out)
		}
		for _, kind := range []string{"lostack", "notleader", "rebalance"} {
			if !strings.Contains(log, `msg="fault injected" kind=`+kind) {
				t.Errorf("no %s fault was injected: the pass proves less than it says", kind)
			}
		}
	})
}
