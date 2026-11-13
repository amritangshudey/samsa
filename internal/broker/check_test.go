package broker

import (
	"strings"
	"testing"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// checked builds a node's data: one topic, enough batches for several
// segments, and a snapshot. It returns the harness and Check's answer.
func checked(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, Config{SegmentBytes: 16 << 10, MaxBatchBytes: 1000, SnapshotEvery: 50})
	h.open(1)
	if code := h.createTopic(1, "t", 2); code != wire.ErrNone {
		t.Fatalf("create: %d", code)
	}
	for i := range 150 {
		if r := h.produce(1, "t", int32(i%2), strings.Repeat("x", 300)); r.ErrorCode != wire.ErrNone {
			t.Fatalf("produce: %d", r.ErrorCode)
		}
	}
	return h
}

func TestCheckCleanData(t *testing.T) {
	h := checked(t)
	res, err := Check(h.disk, h.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Topics != 1 || res.Snapshot < 0 || res.LastIndex < 150 {
		t.Fatalf("got %+v", res)
	}
}

// Each planted fault is found, and named.
func TestCheckFindsCorruption(t *testing.T) {
	flip := func(name string, at func(size int64) int64) func(*harness) {
		return func(h *harness) {
			f, err := h.disk.Open(name, sys.ReadWrite)
			h.must(err)
			size, err := f.Size()
			h.must(err)
			b := make([]byte, 1)
			off := at(size)
			_, err = f.ReadAt(b, off)
			h.must(err)
			b[0] ^= 0xff
			_, err = f.WriteAt(b, off)
			h.must(err)
			h.must(f.Sync())
		}
	}
	for _, tc := range []struct {
		name, want string
		plant      func(*harness)
	}{
		{"byte flipped in a sealed segment", "checksum", flip("raft/00000000000000000000.log", func(s int64) int64 { return s / 2 })},
		{"corrupt hard state", "hard state", flip("raft/hardstate", func(int64) int64 { return 2 })},
		{"truncated snapshot", "snapshot", func(h *harness) {
			f, err := h.disk.Open("raft/snapshot", sys.ReadWrite)
			h.must(err)
			size, err := f.Size()
			h.must(err)
			h.must(f.Truncate(size / 2))
			h.must(f.Sync())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := checked(t)
			tc.plant(h)
			_, err := Check(h.disk, h.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error about %q", err, tc.want)
			}
		})
	}
}

func TestMetrics(t *testing.T) {
	h := checked(t)
	m := h.node.Metrics()
	if !m.Ready || !m.Leader || !m.Serving || m.Topics != 1 || m.Partitions != 2 || m.Open != 1 ||
		m.Requests[wire.KeyProduce] != 150 || m.BytesIn == 0 || m.BytesOut == 0 || m.Snapshot < 0 {
		t.Fatalf("got %+v", m)
	}
	h.must(h.node.OnStop())
	if m := h.node.Metrics(); m.Ready || m.NotReady != "stopping" {
		t.Fatalf("stopping node: %+v", m)
	}
	for _, a := range wire.APIs {
		if a.Key < 0 || int(a.Key) >= len(m.Requests) {
			t.Errorf("API key %d does not fit Metrics.Requests", a.Key)
		}
	}
}
