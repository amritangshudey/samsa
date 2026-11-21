package broker

import (
	"bytes"
	"errors"
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

// fetchAt sends one fetch of topic's partition 0 at offset, with partMax
// bytes for the partition, and completes the object reads it needs.
func fetchAt(t *testing.T, h *harness, drain func(), topic string, offset int64, partMax int32) wire.FetchPartitionResponse {
	t.Helper()
	req := fetchReq(topic, 0, offset, 0, 1)
	req.Topics[0].Partitions[0].MaxBytes = partMax
	corr := h.send(1, wire.KeyFetch, 11, req)
	h.run()
	drain()
	var resp wire.FetchResponse
	if !h.recv(1, wire.KeyFetch, 11, corr, &resp) {
		t.Fatalf("no fetch response at offset %d", offset)
	}
	return resp.Topics[0].Partitions[0]
}

// readTopic fetches every record of topic's partition 0 from offset 0, as
// "offset:key=value" (see fold). A compacted partition has gaps: each fetch
// goes on after the last batch read.
func readTopic(t *testing.T, h *harness, drain func(), topic string) []string {
	t.Helper()
	var out []string
	for off, next := int64(0), h.node.st.topics[topic].parts[0].next; off < next; {
		pr := fetchAt(t, h, drain, topic, off, 1<<20)
		if pr.ErrorCode != wire.ErrNone || len(pr.Records) == 0 {
			t.Fatalf("fetch of %s at offset %d: error %d, %d bytes", topic, off, pr.ErrorCode, len(pr.Records))
		}
		from := off
		for rest := pr.Records; len(rest) > 0; {
			info, code := wire.ParseCompacted(rest, len(rest))
			if code != wire.ErrNone {
				t.Fatalf("fetched batch is invalid: error %d", code)
			}
			batch := rest[:info.Size]
			base, _, err := wire.DecodeBatch(batch)
			recs, err2 := wire.SplitBatch(batch)
			if err != nil || err2 != nil {
				t.Fatal(errors.Join(err, err2))
			}
			for _, r := range recs {
				if o := base + int64(r.OffsetDelta); o >= from {
					v := string(r.Value)
					if r.Value == nil {
						v = "<tombstone>"
					}
					out = append(out, fmt.Sprintf("%d:%s=%s", o, r.Key, v))
				}
			}
			off = base + int64(wire.BatchLastOffsetDelta(batch)) + 1
			rest = rest[info.Size:]
		}
	}
	return out
}

// TestTieringLeavesCompactedTopicsLocal: compaction rewrites a batch in
// place, which a batch in an object cannot be. Before BUGS.md S18 the
// tierer took a compacted topic's batches too, and compaction then
// rewrote tiered ones: each kept its object for reads and took the log
// index of the rewrite, which the trim left behind, and the next snapshot
// did not decode. A compacted topic's batches now stay in the log, copied
// forward as without object storage, while the other topics are tiered.
func TestTieringLeavesCompactedTopicsLocal(t *testing.T) {
	h, _, _, drain := tierHarness(t, Config{SegmentBytes: 4096, MaxBatchBytes: 2048, SnapshotEvery: 20})
	h.open(1)
	for _, name := range []string{"c", "d"} {
		if code := h.createTopic(1, name, 1); code != wire.ErrNone {
			t.Fatalf("create topic %s: %d", name, code)
		}
	}
	h.alterTopic("c", setConfig("cleanup.policy", "compact"))
	want, pad, sent, other := map[string]string{}, strings.Repeat("v", 60), 0, 0
	for round := range 15 { // a compaction sweep every 30 s
		for i := range 30 {
			// Two keys written again later and one that is not: compaction
			// keeps a third of the batch.
			k, j, u := fmt.Sprintf("k%d", i%7), fmt.Sprintf("j%d", i%5), fmt.Sprintf("u%d-%d", round, i)
			v := fmt.Sprintf("%s%d-%d", pad, round, i)
			if code := h.produceKV("c", k, v, j, v, u, v); code != wire.ErrNone {
				t.Fatalf("produce: %d", code)
			}
			want[k], want[j], want[u] = v, v, v
			sent += 3
			h.produce(1, "d", 0, v)
			other++
		}
		readTopic(t, h, drain, "c") // what is in objects comes into the cache, where compaction reads
		for range 4 {
			h.advance(retainEvery)
			drain()
		}
	}
	for range 5 {
		h.advance(retainEvery)
		drain()
	}
	if _, err := decodeState(h.node.st.encode()); err != nil {
		t.Fatalf("the snapshot of the state does not decode: %v", err)
	}
	for _, b := range h.node.st.topics["c"].parts[0].batches {
		if b.objKey != "" {
			t.Fatalf("the batch at %d of the compacted topic is in object %s (log index %d)", b.base, b.objKey, b.logIndex)
		}
	}
	if m := h.node.Metrics(); m.TieredBatches == 0 || h.node.st.logStart == 0 {
		t.Fatalf("the other topic was not tiered, or the log not trimmed: %+v, log start %d", m, h.node.st.logStart)
	}
	h.node.objCache = newObjCache(32 << 20) // read from the objects
	records := readTopic(t, h, drain, "c")
	if got := fold(records); fmt.Sprint(got) != fmt.Sprint(want) || len(records) >= sent {
		t.Fatalf("compacted topic: %d of %d records left, final state\n got %v\nwant %v", len(records), sent, got, want)
	}
	if d := readTopic(t, h, drain, "d"); len(d) != other {
		t.Fatalf("the other topic: %d records, want %d", len(d), other)
	}
	hash := h.node.st.hash()
	h.start()
	if h.node.st.hash() != hash {
		t.Fatal("state after restart differs")
	}
}

// TestTieredFetchInACompactionGap: compaction can remove whole batches, and
// a fetch at an offset they held starts with the next batch. Before BUGS.md
// S19 the prefetch counted the batch before the gap, which the read skips,
// and stopped short of the one it starts with: the fetch was answered
// OFFSET_NOT_AVAILABLE, the same at every retry.
func TestTieredFetchInACompactionGap(t *testing.T) {
	h, _, _, drain := tierHarness(t, Config{SegmentBytes: 4096, MaxBatchBytes: 2048})
	h.open(1)
	if code := h.createTopic(1, "g", 1); code != wire.ErrNone {
		t.Fatalf("create topic: %d", code)
	}
	h.alterTopic("g", setConfig("cleanup.policy", "compact"))
	h.produceKV("g", "z", "0")                               // offset 0
	h.produceKV("g", "a", "1")                               // offset 1, a batch compaction removes
	h.produceKV("g", "b", strings.Repeat("b", 500))          // offset 2
	h.produceKV("g", "a", "2", "a", "3", "a", "4", "a", "5") // offsets 3 to 6
	for range 4 {
		h.advance(retainEvery)
		drain()
	}
	p := h.node.st.topics["g"].parts[0]
	if len(p.batches) != 3 || p.batches[1].base != 2 {
		t.Fatalf("setup: compaction left batches %+v", p.batches)
	}
	// A compacted topic stays in the log: it stops being one, and is tiered.
	h.alterTopic("g", setConfig("cleanup.policy", "delete"))
	for range 60 {
		h.produce(1, "g", 0, strings.Repeat("f", 100))
	}
	for range 5 {
		h.advance(retainEvery)
		drain()
	}
	if p.batches[0].objKey == "" || p.batches[1].objKey == "" {
		t.Fatalf("setup: not tiered: %+v", p.batches[:2])
	}
	h.node.objCache = newObjCache(32 << 20)
	pr := fetchAt(t, h, drain, "g", 1, 1) // in the gap; the first batch is sent whatever its size
	first, values := h.values(pr)
	if pr.ErrorCode != wire.ErrNone || first != 2 || len(values) != 1 {
		t.Fatalf("fetch at 1: error %d, first offset %d, %d records; want the batch at 2", pr.ErrorCode, first, len(values))
	}
}

// TestCompactionLeavesTieredBatches: a topic tiered before it became
// compacted keeps its batches in objects, which compaction cannot rewrite.
// Before BUGS.md S18 it rewrote them as if they were in the log, and the
// next snapshot did not decode.
func TestCompactionLeavesTieredBatches(t *testing.T) {
	h, _, _, drain := tierHarness(t, Config{SegmentBytes: 4096, MaxBatchBytes: 2048, SnapshotEvery: 20})
	h.open(1)
	if code := h.createTopic(1, "t", 1); code != wire.ErrNone {
		t.Fatalf("create topic: %d", code)
	}
	pad := strings.Repeat("v", 60)
	produce := func(round int) {
		for i := range 30 {
			v := fmt.Sprintf("%s%d-%d", pad, round, i)
			if code := h.produceKV("t", fmt.Sprintf("k%d", i%7), v, fmt.Sprintf("j%d", i%5), v, fmt.Sprintf("u%d-%d", round, i), v); code != wire.ErrNone {
				t.Fatalf("produce: %d", code)
			}
		}
		for range 4 {
			h.advance(retainEvery)
			drain()
		}
	}
	for round := range 3 {
		produce(round)
	}
	tiered := 0
	for _, b := range h.node.st.topics["t"].parts[0].batches {
		if b.objKey != "" {
			tiered++
		}
	}
	if tiered == 0 {
		t.Fatal("setup: nothing tiered")
	}
	h.alterTopic("t", setConfig("cleanup.policy", "compact"))
	for round := 3; round < 15; round++ { // a compaction sweep every 30 s
		readTopic(t, h, drain, "t") // what is in objects comes into the cache, where compaction reads
		produce(round)
	}
	if m := h.node.Metrics(); m.CompactedRecords == 0 {
		t.Fatalf("setup: nothing compacted: %+v", m)
	}
	if _, err := decodeState(h.node.st.encode()); err != nil {
		t.Fatalf("the snapshot of the state does not decode: %v", err)
	}
	for _, b := range h.node.st.topics["t"].parts[0].batches {
		if b.objKey != "" && b.logIndex != 0 {
			t.Fatalf("the batch at %d is in object %s and at log index %d", b.base, b.objKey, b.logIndex)
		}
	}
}
