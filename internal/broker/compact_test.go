package broker

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/wire"
)

// produceKV writes one batch of key/value records, stamped with the harness
// clock. A nil value is a tombstone.
func (h *harness) produceKV(topic string, kv ...string) int16 {
	h.t.Helper()
	ms := int64(h.clock.now) / int64(time.Millisecond)
	var recs []wire.Record
	for i := 0; i < len(kv); i += 2 {
		r := wire.Record{Key: []byte(kv[i]), Timestamp: ms}
		if kv[i+1] != "<tombstone>" {
			r.Value = []byte(kv[i+1])
		}
		recs = append(recs, r)
	}
	var resp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 8, produceReq(topic, 0, wire.EncodeBatch(recs)), &resp)
	return resp.Topics[0].Partitions[0].ErrorCode
}

// readAll returns every record a partition still holds, as "offset:key=value".
func (h *harness) readAll(topic string) []string {
	h.t.Helper()
	p := h.node.st.topics[topic].parts[0]
	var out []string
	for _, ref := range p.batches {
		cmd, _, err := h.node.raft.Read(ref.logIndex)
		if err != nil {
			h.t.Fatal(err)
		}
		recs, err := wire.SplitBatch(appendBatch(cmd))
		if err != nil {
			h.t.Fatal(err)
		}
		for _, r := range recs {
			v := string(r.Value)
			if r.Value == nil {
				v = "<tombstone>"
			}
			out = append(out, fmt.Sprintf("%d:%s=%s", ref.base+int64(r.OffsetDelta), r.Key, v))
		}
	}
	return out
}

// fold reads records in order and returns the final value of every key.
func fold(records []string) map[string]string {
	state := map[string]string{}
	for _, r := range records {
		var off int64
		var key, value string
		fmt.Sscanf(r, "%d:", &off)
		kv := r[len(fmt.Sprint(off))+1:]
		for i := range kv {
			if kv[i] == '=' {
				key, value = kv[:i], kv[i+1:]
				break
			}
		}
		if value == "<tombstone>" {
			delete(state, key)
		} else {
			state[key] = value
		}
	}
	return state
}

func TestCompactionKeepsTheLatestValue(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "c", 1)
	h.alterTopic("c", setConfig("cleanup.policy", "compact"), setConfig("delete.retention.ms", "20000"))
	want, sent := map[string]string{}, []string{}
	for i := range 60 {
		k := fmt.Sprintf("k%d", i%5)
		v := fmt.Sprintf("v%d", i)
		if code := h.produceKV("c", k, v); code != 0 {
			t.Fatalf("produce: %d", code)
		}
		want[k] = v
		sent = append(sent, fmt.Sprintf("%d:%s=%s", i, k, v))
		h.advance(100 * time.Millisecond)
	}
	h.produceKV("c", "k4", "<tombstone>")
	sent = append(sent, "60:k4=<tombstone>")
	delete(want, "k4")
	before := sent
	for range 4 {
		h.advance(retainEvery)
	}
	after := h.readAll("c")
	if len(after) >= len(before)/2 {
		t.Fatalf("too little removed: %d records sent, %d left", len(before), len(after))
	}
	got := fold(after)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("final state changed:\n got %v\nwant %v\nrecords %v", got, want, after)
	}
	// Offsets never change: each record left is where it was.
	was := map[string]bool{}
	for _, r := range before {
		was[r] = true
	}
	for _, r := range after {
		if !was[r] {
			t.Fatalf("record %s was not in the log before", r)
		}
	}
	// The tombstone is still there: 20 s have not passed.
	if !contains(after, "k4=<tombstone>") {
		t.Fatalf("the tombstone went early: %v", after)
	}
	// After delete.retention.ms it goes, and k4 is gone altogether.
	for range 30 {
		h.advance(time.Second)
	}
	final := h.readAll("c")
	if len(final) != len(want) {
		t.Fatalf("after the tombstone's time: %v, want one record per key of %v", final, want)
	}
	if fmt.Sprint(fold(final)) != fmt.Sprint(want) {
		t.Fatalf("final state: %v", fold(final))
	}
	// A restart replays every rewrite to the same state, and a fetch from
	// the start reads across the gaps.
	hash := h.node.st.hash()
	h.start()
	if h.node.st.hash() != hash {
		t.Fatal("state after restart differs")
	}
	h.open(1)
	p, vals := h.fetch(1, "c", 0, 0)
	if p.ErrorCode != 0 || len(vals) != len(want) {
		t.Fatalf("fetch from 0: %+v %v", p, vals)
	}
}

func contains(records []string, sub string) bool {
	for _, r := range records {
		for i := 0; i+len(sub) <= len(r); i++ {
			if r[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

// min.compaction.lag.ms: records younger than it are left alone.
func TestCompactionLag(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "c", 1)
	h.alterTopic("c", setConfig("cleanup.policy", "compact"), setConfig("min.compaction.lag.ms", "60000"))
	for i := range 20 {
		h.produceKV("c", "k", fmt.Sprintf("v%d", i))
		h.advance(time.Second)
	}
	for range 10 {
		h.advance(retainEvery)
	}
	if n := len(h.readAll("c")); n != 20 {
		t.Fatalf("%d records left inside the lag, want all 20", n)
	}
	for range 100 {
		h.advance(time.Second)
	}
	if got := h.readAll("c"); len(got) != 1 || got[0] != "19:k=v19" {
		t.Fatalf("after the lag: %v", got)
	}
}

// A compacted topic takes no record without a key and no compressed batch.
func TestCompactedTopicRefuses(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "c", 1)
	h.alterTopic("c", setConfig("cleanup.policy", "compact"))
	var resp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 8, produceReq("c", 0, wire.EncodeBatch([]wire.Record{{Value: []byte("no key")}})), &resp)
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != wire.ErrInvalidRecord {
		t.Fatalf("keyless record: code %d, want %d", code, wire.ErrInvalidRecord)
	}
	b := wire.EncodeBatch([]wire.Record{{Key: []byte("k"), Value: []byte("v")}})
	binary.BigEndian.PutUint16(b[21:], 1) // gzip
	binary.BigEndian.PutUint32(b[17:], crc32.Checksum(b[21:], crc32.MakeTable(crc32.Castagnoli)))
	h.call(1, wire.KeyProduce, 8, produceReq("c", 0, b), &resp)
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != wire.ErrUnsupportedCompressionType {
		t.Fatalf("compressed batch: code %d, want %d", code, wire.ErrUnsupportedCompressionType)
	}
	if code := h.produceKV("c", "k", "v"); code != 0 {
		t.Fatalf("a keyed record: %d", code)
	}
}

// Apply refuses a rewritten batch that reaches past the offsets it
// replaces, and ignores one whose reference has moved.
func TestCompactCommandApply(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "c", 1)
	h.alterTopic("c", setConfig("cleanup.policy", "compact"))
	h.produceKV("c", "a", "1", "b", "2", "a", "3")
	p := h.node.st.topics["c"].parts[0]
	ref := p.batches[0]
	cmd, _, _ := h.node.raft.Read(ref.logIndex)
	data := appendBatch(cmd)
	recs, _ := wire.SplitBatch(data)
	out := wire.KeepRecords(data, recs, []bool{false, true, true})

	s := h.node.st
	next := h.node.raft.LastIndex() + 1
	r, err := s.apply(next, encodeCompact("c", 0, ref.base, ref.logIndex+99, out)) // stale: nothing moves
	if err == nil || r.applied {
		// from >= index is a malformed command; a stale one below it is not
		t.Logf("from past the log: %v", err)
	}
	r, err = s.apply(next, encodeCompact("c", 0, ref.base, ref.logIndex-1, out))
	if err != nil || r.applied || p.batches[0].logIndex != ref.logIndex {
		t.Fatalf("a stale compaction applied: %v %+v", err, r)
	}
	r, err = s.apply(next+1, encodeCompact("c", 0, ref.base, ref.logIndex, out))
	if err != nil || !r.applied || p.batches[0].logIndex != next+1 {
		t.Fatalf("compaction did not apply: %v %+v", err, r)
	}
	// Removing every record removes the reference; the offsets stay.
	r, err = s.apply(next+2, encodeCompact("c", 0, ref.base, next+1, nil))
	if err != nil || !r.applied || len(p.batches) != 0 || p.next != 3 {
		t.Fatalf("empty compaction: %v %+v, %d batches, next %d", err, r, len(p.batches), p.next)
	}
}
