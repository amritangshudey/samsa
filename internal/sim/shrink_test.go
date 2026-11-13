package sim

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/amritangshudey/samsa/internal/broker"
)

func ackBeforeSync(c *broker.Config) { c.Sync, c.UnsafeAckBeforeSync = broker.SyncAlways, true }

// A run told to skip a fault that never comes is the original run.
func TestSkipNothingIsTheSameRun(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		a := Run(Config{Seed: seed})
		b := Run(Config{Seed: seed, Skip: map[int]bool{1 << 20: true}})
		if a.TraceHash != b.TraceHash || len(a.Faults) != len(b.Faults) {
			t.Fatalf("seed %d: %08x with %d faults, %08x with %d", seed, a.TraceHash, len(a.Faults), b.TraceHash, len(b.Faults))
		}
	}
}

// shrinkSeed fails with acks before sync and shrinks well (175 faults to
// 1, rule S3, after the ACL APIs moved the seeds on 2026-11-14; seed 4,
// 39 faults to 1, before). Any change to the bytes the simulated clients receive (the
// advertised API list, say) moves every seed, because the network draws
// once per delivered fragment: then pick another one that fails.
const shrinkSeed = 5

// The shrink seed with acks before sync shrinks to fewer faults, fails by
// the same rule, and its replay arguments reproduce it.
func TestShrink(t *testing.T) {
	cfg := Config{Seed: shrinkSeed, Mutate: ackBeforeSync}
	orig := Run(cfg)
	if orig.Err == nil {
		t.Fatalf("seed %d no longer fails with acks before sync: pick another seed", shrinkSeed)
	}
	s, err := Shrink(cfg, 100)
	if err != nil {
		t.Fatal(err)
	}
	if left := len(acting(s.Result)); left >= len(acting(orig)) {
		t.Fatalf("not shrunk: %d faults of %d\n%s", left, len(acting(orig)), s)
	}
	if rule(s.Result.Err) != rule(orig.Err) {
		t.Fatalf("shrunk to another failure: %v, was %v", s.Result.Err, orig.Err)
	}
	skip, err := ParseSkip(FormatSkip(s.Config.Skip))
	if err != nil {
		t.Fatal(err)
	}
	replay := Run(Config{Seed: shrinkSeed, Mutate: ackBeforeSync, Calm: s.Config.Calm, Skip: skip})
	if replay.Err == nil || replay.TraceHash != s.Result.TraceHash {
		t.Fatalf("the replay differs: %v, %08x vs %08x", replay.Err, replay.TraceHash, s.Result.TraceHash)
	}
	t.Logf("\n%s", s)
}

func TestShrinkRefusesAPassingSeed(t *testing.T) {
	if _, err := Shrink(Config{Seed: 1}, 10); err == nil {
		t.Fatal("a passing seed was shrunk")
	}
}

func TestRule(t *testing.T) {
	for msg, want := range map[string]string{
		"I1: record 3 lost":                       "I1",
		"I1/I3: chaos/0 high watermark 0":         "I1/I3",
		"recovery of node 2 failed: raft: x":      "recovery of node ",
		"S3: node 1 committed an entry of term 3": "S3",
		"Ix: not a tag":                           "Ix: not a tag",
	} {
		if got := rule(errors.New(msg)); got != want {
			t.Errorf("rule(%q) = %q, want %q", msg, got, want)
		}
	}
	for _, bad := range []string{"0", "3-1", "x", "1-"} {
		if _, err := ParseSkip(bad); err == nil {
			t.Errorf("ParseSkip(%q) accepted", bad)
		}
	}
}

// The timeline of a failing run names every lane and the violation, and
// writing it does not change the run.
func TestTimeline(t *testing.T) {
	var page bytes.Buffer
	cfg := Config{Seed: 1, Mutate: ackBeforeSync, Calm: true, Skip: map[int]bool{1: true, 2: true, 3: true}}
	plain := Run(cfg)
	cfg.Timeline = &page
	res := Run(cfg)
	if res.TraceHash != plain.TraceHash {
		t.Fatal("the timeline changed the run")
	}
	html := page.String()
	for _, want := range []string{"<svg", "node 1", "node 3", "network", "client 0", "Violation at", "(skipped)"} {
		if !strings.Contains(html, want) {
			t.Errorf("the timeline lacks %q", want)
		}
	}
	if page.Len() > 5<<20 {
		t.Errorf("the timeline is %d bytes", page.Len())
	}
}
