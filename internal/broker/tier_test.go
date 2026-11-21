package broker

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// tierHarness is a harness whose node has a simulated object store. ops
// completes the object operations pending now; drain completes them, and
// what they lead to, until none is left, and fails the test if they never
// stop (BUGS.md S16).
func tierHarness(t *testing.T, cfg Config) (h *harness, shared *simsys.SharedObjects, ops, drain func()) {
	t.Helper()
	shared = simsys.NewSharedObjects(simsys.ObjFaults{MaxDelay: 5 * time.Millisecond})
	var pending []func()
	cfg.ObjectStore = simsys.NewNodeObjectStore(
		shared,
		simsys.NewRand(1),
		func(_ time.Duration, fn func()) { pending = append(pending, fn) },
		func(id sys.ObjOpID, data []byte, err error) {
			if err := h.node.OnObjectDone(id, data, err); err != nil {
				t.Errorf("OnObjectDone: %v", err)
			}
		},
	)
	h = newHarness(t, cfg)
	ops = func() {
		q := pending
		pending = nil
		for _, fn := range q {
			fn()
		}
	}
	drain = func() {
		t.Helper()
		for range 1000 {
			if len(pending) == 0 {
				return
			}
			ops()
			h.run()
		}
		t.Fatalf("object operations never stop: %d puts, %+v", shared.Puts, h.node.Metrics())
	}
	return h, shared, ops, drain
}

func TestTieringLifecycleFaultsAndGC(t *testing.T) {
	h, shared, _, drainObj := tierHarness(t, Config{SegmentBytes: 16384, MaxBatchBytes: 8192, SnapshotEvery: 20})
	h.open(1)
	if code := h.createTopic(1, "tier", 1); code != wire.ErrNone {
		t.Fatalf("create topic: %d", code)
	}
	val := strings.Repeat("x", 512)
	for i := range 45 {
		h.produce(1, "tier", 0, val)
		if i%5 == 0 {
			h.advance(time.Second)
			drainObj()
		}
	}
	h.advance(2 * time.Second)
	drainObj()
	h.advance(2 * time.Second)
	drainObj()

	if shared.ObjectCount() == 0 {
		t.Fatalf("expected tiered objects in store, got 0 (logStart=%d, metrics=%+v)", h.node.st.logStart, h.node.Metrics())
	}
	if h.node.Metrics().TieredBatches == 0 {
		t.Fatalf("expected TieredBatches > 0, got %+v", h.node.Metrics())
	}

	// Verify snapshot v13 round-trip preserves remote batchRef fields.
	snap := h.node.st.encode()
	dec, err := decodeState(snap)
	if err != nil {
		t.Fatalf("decodeState v13: %v", err)
	}
	if !bytes.Equal(snap, dec.encode()) {
		t.Fatal("snapshot v13 round-trip mismatch")
	}

	// Clear cache and inject torn reads + lost gets; Fetch must retry and serve intact data.
	h.node.objCache = newObjCache(32 << 20)
	shared.Faults.TornRead = 0.3
	shared.Faults.LostGet = 0.2
	corr := h.send(1, wire.KeyFetch, 11, fetchReq("tier", 0, 0, 0, 1))
	h.run()
	drainObj()
	var fresp wire.FetchResponse
	if !h.recv(1, wire.KeyFetch, 11, corr, &fresp) {
		t.Fatal("no fetch response after remote read")
	}
	pr := fresp.Topics[0].Partitions[0]
	if pr.ErrorCode != wire.ErrNone || len(pr.Records) == 0 {
		t.Fatalf("fetch tiered records: err=%d bytes=%d", pr.ErrorCode, len(pr.Records))
	}
	base, recs, err := wire.DecodeBatch(wire.SplitBatches(pr.Records)[0])
	if err != nil || base != 0 || string(recs[0].Value) != val {
		t.Fatalf("decoded tiered record: err=%v base=%d len=%d", err, base, len(recs))
	}

	// DeleteRecords past all records and trigger GC; objects must be deleted from store.
	shared.Faults.TornRead = 0
	shared.Faults.LostGet = 0
	var dresp wire.DeleteRecordsResponse
	h.call(1, wire.KeyDeleteRecords, 2, &wire.DeleteRecordsRequest{
		Topics: []wire.DeleteRecordsTopic{{Name: "tier", Partitions: []wire.DeleteRecordsPartition{{Index: 0, Offset: -1}}}},
	}, &dresp)
	h.advance(2 * time.Second)
	drainObj()
	h.advance(2 * time.Second)
	drainObj()
	if shared.ObjectCount() != 0 {
		t.Fatalf("expected GC to delete unreferenced objects, still have %d", shared.ObjectCount())
	}
}

// TestTieringManySmallBatches: a segment of many small batches is tiered in
// several objects, each named by a cmdTier that fits a log entry, and then
// trimmed. Before BUGS.md S16 the segment's one cmdTier was too large for
// the log, so it was refused and the segment uploaded again, for ever.
func TestTieringManySmallBatches(t *testing.T) {
	h, shared, _, drain := tierHarness(t, Config{SegmentBytes: 16384, MaxBatchBytes: 2048})
	h.open(1)
	if code := h.createTopic(1, "tier", 1); code != wire.ErrNone {
		t.Fatalf("create topic: %d", code)
	}
	const n = 600 // about 200 batches a segment: a cmdTier for one is 8 KB
	for i := range n {
		h.produce(1, "tier", 0, fmt.Sprint(i))
	}
	for range 5 {
		h.advance(time.Second)
		drain()
	}
	if m := h.node.Metrics(); m.TieredBatches < n/2 || h.node.st.logStart == 0 {
		t.Fatalf("not tiered: %+v, log start %d", m, h.node.st.logStart)
	}
	if objects := int64(shared.ObjectCount()); objects != shared.Puts {
		t.Fatalf("%d objects from %d uploads: some uploads were for nothing", objects, shared.Puts)
	}

	h.node.objCache = newObjCache(32 << 20) // read from the objects
	var got []string
	for len(got) < n {
		corr := h.send(1, wire.KeyFetch, 11, fetchReq("tier", 0, int64(len(got)), 0, 1))
		h.run()
		drain()
		var resp wire.FetchResponse
		if !h.recv(1, wire.KeyFetch, 11, corr, &resp) {
			t.Fatalf("no fetch response at offset %d", len(got))
		}
		pr := resp.Topics[0].Partitions[0]
		if pr.ErrorCode != wire.ErrNone || len(pr.Records) == 0 {
			t.Fatalf("fetch at offset %d: error %d, %d bytes", len(got), pr.ErrorCode, len(pr.Records))
		}
		first, values := h.values(pr)
		if first > int64(len(got)) {
			t.Fatalf("fetch at offset %d starts at %d", len(got), first)
		}
		got = append(got, values[int64(len(got))-first:]...)
	}
	for i, v := range got[:n] {
		if v != fmt.Sprint(i) {
			t.Fatalf("offset %d holds %q, want %d", i, v, i)
		}
	}
}

func TestTierEntryBytes(t *testing.T) {
	e := tierEntry{topic: "some-topic"}
	if got, want := len(encodeTier("k", []tierEntry{e, e}))-len(encodeTier("k", nil)), 2*tierEntryBytes(e.topic); got != want {
		t.Fatalf("two entries encode to %d bytes, tierEntryBytes says %d", got, want)
	}
}

// TestTieringAgainWhenServingAgain: a leader that stops serving while its
// cmdTier waits for commit is never told how it ended. When it serves
// again it must tier again. Before BUGS.md S17 its tiering flag stayed
// set, and it never tiered or trimmed again.
func TestTieringAgainWhenServingAgain(t *testing.T) {
	h, _, ops, drain := tierHarness(t, Config{SegmentBytes: 16384, MaxBatchBytes: 8192})
	h.open(1)
	if code := h.createTopic(1, "tier", 1); code != wire.ErrNone {
		t.Fatalf("create topic: %d", code)
	}
	val := strings.Repeat("x", 512)
	for range 45 {
		h.produce(1, "tier", 0, val)
	}
	h.advance(time.Second) // retain: a round's upload starts
	ops()                  // it completes, and the round proposes its cmdTier
	if !h.node.tiering || len(h.node.waiters) == 0 {
		t.Fatalf("setup: tiering %v, %d waiters", h.node.tiering, len(h.node.waiters))
	}
	h.node.serving = false // what lead does when the node stops leading:
	h.node.stopServing()   // the cmdTier's waiter is dropped
	for range 5 {
		h.advance(time.Second) // lead serves again, in the same term, as after a handover that failed
		drain()
	}
	if end := h.node.HeadSegmentEnd(); end >= 0 {
		t.Fatalf("serving again, the leader's log still starts with a sealed segment, up to index %d (tiering %v)", end, h.node.tiering)
	}
}
