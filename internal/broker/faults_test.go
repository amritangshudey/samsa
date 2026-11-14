package broker

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

func TestParseFaults(t *testing.T) {
	for _, tc := range []struct {
		spec, want, err string
	}{
		{spec: "seed=7", want: "seed=7,rate=0.05,kinds=lostack+notleader+rebalance"},
		{spec: "seed=7,kinds=all", want: "seed=7,rate=0.05,kinds=emptyfetch+expire+lostack+notleader+outofrange+rebalance+slowfetch"},
		{spec: "seed=7,kinds=outofrange+expire", want: "seed=7,rate=0.05,kinds=expire+outofrange"},
		{spec: "seed=7, rate=0.5,kinds=rebalance+lostack+rebalance", want: "seed=7,rate=0.5,kinds=lostack+rebalance"},
		{spec: "rate=0.5", err: "seed is required"},
		{spec: "seed=-1", err: "seed"},
		{spec: "seed=1,rate=0", err: "rate must be"},
		{spec: "seed=1,rate=1.5", err: "rate must be"},
		{spec: "seed=1,rate=NaN", err: "rate must be"},
		{spec: "seed=1,kinds=crash", err: "unknown kind"},
		{spec: "seed=1,seed=2", err: "given twice"},
		{spec: "seed=1,speed=2", err: "unknown key"},
		{spec: "seed", err: "not key=value"},
	} {
		f, err := ParseFaults(tc.spec)
		switch {
		case tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)):
			t.Errorf("ParseFaults(%q) = %v, want error containing %q", tc.spec, err, tc.err)
		case tc.err == "" && err != nil:
			t.Errorf("ParseFaults(%q): %v", tc.spec, err)
		case tc.err == "" && f.String() != tc.want:
			t.Errorf("ParseFaults(%q) = %s, want %s", tc.spec, f, tc.want)
		}
		if tc.err == "" {
			if g, err := ParseFaults(f.String()); err != nil || g.String() != f.String() {
				t.Errorf("round trip of %s: %v, %v", f, g, err)
			}
		}
	}
}

// The same seed gives the same faults for the same requests; the rate is
// roughly kept; a kind that is off never draws.
func TestFaulterIsASeededStream(t *testing.T) {
	draw := func(f Faults, kind string) []bool {
		ft := faulter{cfg: f, state: f.Seed}
		out := make([]bool, 10000)
		for i := range out {
			out[i] = ft.inject(kind)
		}
		return out
	}
	f := Faults{Seed: 42, Rate: 0.1, Kinds: FaultKinds}
	a, b := draw(f, FaultLostAck), draw(f, FaultLostAck)
	if !slices.Equal(a, b) {
		t.Fatal("same seed, different faults")
	}
	hits := 0
	for _, x := range a {
		if x {
			hits++
		}
	}
	if hits < 900 || hits > 1100 {
		t.Fatalf("rate 0.1 gave %d of 10000", hits)
	}
	f.Seed = 43
	if slices.Equal(a, draw(f, FaultLostAck)) {
		t.Fatal("different seeds, same faults")
	}
	ft := faulter{cfg: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultRebalance}}, state: 1}
	if ft.inject(FaultLostAck) || ft.state != 1 {
		t.Fatal("a kind that is off injected or drew")
	}
	var zero faulter
	if zero.inject(FaultRebalance) {
		t.Fatal("the zero value injected")
	}
}

func TestFaultLostAck(t *testing.T) {
	h := newHarness(t, Config{Faults: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultLostAck}}})
	h.open(1)
	if code := h.createTopic(1, "t", 1); code != 0 {
		t.Fatalf("create topic: %d", code)
	}
	h.send(1, wire.KeyProduce, 8, produceReq("t", 0, batch("a")))
	h.run()
	if len(h.net.sent[1]) != 0 {
		t.Fatal("the produce was answered")
	}
	if m := h.node.Metrics(); m.Faults != 1 || m.Dropped != 0 || m.Open != 0 {
		t.Fatalf("faults %d, dropped %d, open %d: want 1, 0, 0", m.Faults, m.Dropped, m.Open)
	}
	// The write committed: a retry writes it again.
	h.open(2)
	_, vals := h.fetch(2, "t", 0, 0)
	if !slices.Equal(vals, []string{"a"}) {
		t.Fatalf("after a lost ack the log holds %q, want the write", vals)
	}
}

func TestFaultNotLeader(t *testing.T) {
	h := newHarness(t, Config{Faults: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultNotLeader}}})
	h.open(1)
	if code := h.createTopic(1, "t", 1); code != 0 {
		t.Fatalf("create topic (not eligible): %d", code)
	}
	var resp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 8, produceReq("t", 0, batch("a")), &resp)
	if p := resp.Topics[0].Partitions[0]; p.ErrorCode != wire.ErrNotLeaderOrFollower {
		t.Fatalf("produce: %+v", p)
	}
	p, vals := h.fetch(1, "t", 0, 0)
	if p.ErrorCode != wire.ErrNotLeaderOrFollower || len(vals) != 0 {
		t.Fatalf("fetch: error %d, %q", p.ErrorCode, vals)
	}
	h.node.faults.cfg.Rate = 0
	if _, vals := h.fetch(1, "t", 0, 0); len(vals) != 0 {
		t.Fatalf("the refused produce was written: %q", vals)
	}
}

func TestFaultRebalance(t *testing.T) {
	h, a, b := stableGroup(t)
	h.node.faults = faulter{cfg: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultRebalance}}, state: 1}
	if code := h.heartbeat(1, 2, a); code != wire.ErrRebalanceInProgress {
		t.Fatalf("heartbeat: %d, want REBALANCE_IN_PROGRESS", code)
	}
	// Only one draw per stable group: during the round, no more faults.
	if code := h.heartbeat(2, 2, b); code != wire.ErrRebalanceInProgress || h.node.faults.count != 1 {
		t.Fatalf("second heartbeat: %d, faults %d", code, h.node.faults.count)
	}
	h.node.faults.cfg.Rate = 0
	corr := h.joinHeld(2, b)
	ra := h.join(1, joinReq(a, "range"))
	rb := h.joined(2, corr)
	if ra.ErrorCode != 0 || rb.ErrorCode != 0 || ra.GenerationID != 3 || rb.GenerationID != 3 {
		t.Fatalf("rejoin: %+v / %+v", ra, rb)
	}
}

// The fetch faults of item 23a: a consumer is told its offset is out of
// range, or gets nothing, or gets it late, though the data is there.
func TestFaultFetches(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.open(2)
	h.createTopic(1, "t", 1)
	h.produce(1, "t", 0, "a")
	h.produce(1, "t", 0, "b")

	h.node.faults = faulter{cfg: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultOutOfRange}}, state: 1}
	var resp wire.FetchResponse
	h.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 0, 1), &resp)
	if p := resp.Topics[0].Partitions[0]; p.ErrorCode != wire.ErrOffsetOutOfRange || p.LogStartOffset != 0 || p.HighWatermark != 2 || len(p.Records) != 0 {
		t.Fatalf("outofrange: %+v", p)
	}

	h.node.faults = faulter{cfg: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultEmptyFetch}}, state: 1}
	h.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 0, 1), &resp)
	if p := resp.Topics[0].Partitions[0]; p.ErrorCode != 0 || p.HighWatermark != 2 || len(p.Records) != 0 {
		t.Fatalf("emptyfetch: %+v", p)
	}
	// Nothing to return anyway: no fault is counted.
	n := h.node.faults.count
	h.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 2, 0, 1), &resp)
	if h.node.faults.count != n {
		t.Fatal("an empty fetch was faulted again")
	}

	// slowfetch: the data is there, and comes at the deadline.
	h.node.faults = faulter{cfg: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultSlowFetch}}, state: 1}
	corr := h.send(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 500, 1))
	h.run()
	if h.recv(1, wire.KeyFetch, 11, corr, &resp) {
		t.Fatal("a slow fetch was answered at once")
	}
	h.produce(2, "t", 0, "c") // new data does not wake it
	if h.recv(1, wire.KeyFetch, 11, corr, &resp) {
		t.Fatal("a slow fetch was woken by data")
	}
	h.advance(500 * time.Millisecond)
	if !h.recv(1, wire.KeyFetch, 11, corr, &resp) || len(resp.Topics[0].Partitions[0].Records) == 0 {
		t.Fatalf("a slow fetch at its deadline: %+v", resp)
	}
}

// FaultExpire: a heartbeat finds the member's session gone.
func TestFaultExpire(t *testing.T) {
	h, a, b := stableGroup(t)
	h.node.faults = faulter{cfg: Faults{Seed: 1, Rate: 1, Kinds: []string{FaultExpire}}, state: 1}
	if code := h.heartbeat(1, 2, a); code != wire.ErrUnknownMemberID {
		t.Fatalf("heartbeat: %d, want UNKNOWN_MEMBER_ID", code)
	}
	h.node.faults.cfg.Rate = 0
	// The other member is told to rejoin: the group is rebalancing.
	if code := h.heartbeat(2, 2, b); code != wire.ErrRebalanceInProgress {
		t.Fatalf("the other member's heartbeat: %d, want REBALANCE_IN_PROGRESS", code)
	}
	if code := h.heartbeat(1, 2, a); code != wire.ErrUnknownMemberID {
		t.Fatalf("the expired member's next heartbeat: %d", code)
	}
}
