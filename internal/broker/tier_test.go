package broker

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

func TestTieringLifecycleFaultsAndGC(t *testing.T) {
	shared := simsys.NewSharedObjects(simsys.ObjFaults{MaxDelay: 5 * time.Millisecond})
	var h *harness
	var pending []func()
	obj := simsys.NewNodeObjectStore(
		shared,
		simsys.NewRand(1),
		func(_ time.Duration, fn func()) { pending = append(pending, fn) },
		func(id sys.ObjOpID, data []byte, err error) {
			if err := h.node.OnObjectDone(id, data, err); err != nil {
				t.Errorf("OnObjectDone: %v", err)
			}
		},
	)
	drainObj := func() {
		for len(pending) > 0 {
			q := pending
			pending = nil
			for _, fn := range q {
				fn()
			}
			h.run()
		}
	}

	h = newHarness(t, Config{
		SegmentBytes:  16384,
		MaxBatchBytes: 8192,
		SnapshotEvery: 20,
		ObjectStore:   obj,
	})
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
