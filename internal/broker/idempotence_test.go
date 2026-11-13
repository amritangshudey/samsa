package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

const topSeq = 1<<31 - 1 // the last sequence number before 0 again

// batchAs is batch stamped as sent by a producer.
func batchAs(id int64, epoch int16, seq int32, values ...string) []byte {
	b := batch(values...)
	wire.SetBatchProducer(b, id, epoch, seq)
	return b
}

func (h *harness) produceAs(c sys.ConnID, topic string, part int32, id int64, epoch int16, seq int32, values ...string) wire.ProducePartitionResponse {
	h.t.Helper()
	var resp wire.ProduceResponse
	h.call(c, wire.KeyProduce, 8, produceReq(topic, part, batchAs(id, epoch, seq, values...)), &resp)
	return resp.Topics[0].Partitions[0]
}

// One case per rule of docs/design/a2-idempotent-producer.md, section 2.
// Each step is one batch from producer 7; want is its base offset, or the
// error code negated. The partition must end up holding exactly log.
func TestSequenceRules(t *testing.T) {
	const ooo, oldEpoch = -int64(wire.ErrOutOfOrderSequenceNumber), -int64(wire.ErrInvalidProducerEpoch)
	type step struct {
		epoch  int16
		seq    int32
		values []string
		want   int64
	}
	v := func(values ...string) []string { return values }
	tests := []struct {
		name  string
		first []string // appended first without a producer id, if any
		steps []step
		log   []string
	}{
		{"in sequence", nil, []step{{0, 0, v("a", "b"), 0}, {0, 2, v("c"), 2}, {0, 3, v("d"), 3}}, v("a", "b", "c", "d")},
		{"a retry is answered with the first copy's offset", nil,
			[]step{{0, 0, v("a", "b"), 0}, {0, 0, v("a", "b"), 0}, {0, 2, v("c"), 2}, {0, 2, v("c"), 2}, {0, 0, v("a", "b"), 0}}, v("a", "b", "c")},
		{"five batches are remembered, the sixth back is not", nil,
			[]step{{0, 0, v("a"), 0}, {0, 1, v("b"), 1}, {0, 2, v("c"), 2}, {0, 3, v("d"), 3}, {0, 4, v("e"), 4}, {0, 5, v("f"), 5},
				{0, 1, v("b"), 1}, {0, 0, v("a"), ooo}}, v("a", "b", "c", "d", "e", "f")},
		{"a retry must match first and last sequence", nil, []step{{0, 0, v("a", "b"), 0}, {0, 0, v("a"), ooo}, {0, 1, v("b"), ooo}}, v("a", "b")},
		{"a gap", nil, []step{{0, 0, v("a", "b"), 0}, {0, 5, v("c"), ooo}, {0, 2, v("c"), 2}}, v("a", "b", "c")},
		{"an older epoch", nil, []step{{2, 0, v("a"), 0}, {1, 0, v("x"), oldEpoch}, {1, 1, v("x"), oldEpoch}, {2, 1, v("b"), 1}}, v("a", "b")},
		{"a newer epoch starts at 0", nil, []step{{0, 0, v("a"), 0}, {1, 1, v("x"), ooo}, {1, 0, v("b"), 1}, {1, 1, v("c"), 2}}, v("a", "b", "c")},
		{"a newer epoch forgets the old one's batches", nil, []step{{0, 0, v("a"), 0}, {1, 0, v("b"), 1}, {0, 0, v("a"), oldEpoch}}, v("a", "b")},
		{"an unknown producer on a partition never written must start at 0", nil, []step{{0, 3, v("x"), ooo}, {0, 0, v("a"), 0}}, v("a")},
		{"an unknown producer on a written partition must start at 0 too", v("p"), []step{{4, 30, v("x"), ooo}, {4, 0, v("a"), 1}, {4, 1, v("b"), 2}}, v("p", "a", "b")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, Config{})
			h.open(1)
			h.createTopic(1, "t", 1)
			if tt.first != nil {
				h.produce(1, "t", 0, tt.first...)
			}
			for i, s := range tt.steps {
				got := h.produceAs(1, "t", 0, 7, s.epoch, s.seq, s.values...)
				want := wire.ProducePartitionResponse{BaseOffset: s.want}
				if s.want < 0 {
					want = wire.ProducePartitionResponse{ErrorCode: int16(-s.want), BaseOffset: -1}
				}
				if got != want {
					t.Fatalf("step %d: %+v, want %+v", i, got, want)
				}
			}
			if _, got := h.fetch(1, "t", 0, 0); !reflect.DeepEqual(got, tt.log) {
				t.Fatalf("the partition holds %v, want %v", got, tt.log)
			}
		})
	}
}

// Sequences are per partition and per producer, and batches without a
// producer id pass between them untouched.
func TestSequencesAreSeparate(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 2)
	for i, s := range []struct {
		part int32
		id   int64
		seq  int32
		want int64
	}{{0, 7, 0, 0}, {1, 7, 0, 0}, {0, 8, 0, 1}, {0, 7, 1, 2}, {1, 7, 1, 1}, {0, 8, 0, 1}, {1, 7, 0, 0}} {
		if got := h.produceAs(1, "t", s.part, s.id, 0, s.seq, fmt.Sprint("v", i)); got.ErrorCode != 0 || got.BaseOffset != s.want {
			t.Fatalf("step %d: %+v, want offset %d", i, got, s.want)
		}
	}
	if got := h.produce(1, "t", 0, "plain"); got.BaseOffset != 3 {
		t.Fatalf("a batch without a producer id: %+v", got)
	}
	if got := h.produceAs(1, "t", 0, 7, 0, 2, "after"); got.ErrorCode != 0 || got.BaseOffset != 4 {
		t.Fatalf("the sequence after a plain batch: %+v", got)
	}
}

// Several batches of one partition in one request each carry a sequence;
// retrying the whole request appends nothing.
func TestSequencesInOneRequest(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	records := append(batchAs(7, 0, 0, "a", "b"), batchAs(7, 0, 2, "c")...)
	for range 2 {
		var resp wire.ProduceResponse
		h.call(1, wire.KeyProduce, 8, produceReq("t", 0, records), &resp)
		if got := resp.Topics[0].Partitions[0]; got.ErrorCode != 0 || got.BaseOffset != 0 {
			t.Fatal(got)
		}
	}
	if _, got := h.fetch(1, "t", 0, 0); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("the partition holds %v", got)
	}
}

// A producer id needs a sequence; the leader refuses before proposing.
func TestProducerWithoutSequence(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	entries := h.node.raft.LastIndex()
	if got := h.produceAs(1, "t", 0, 7, 0, -1, "a"); got.ErrorCode != wire.ErrInvalidRecord {
		t.Fatal(got)
	}
	if h.node.raft.LastIndex() != entries {
		t.Fatal("a refused batch was proposed")
	}
}

// The window that recognises a retry must survive a restart, with and
// without a snapshot: the retry may arrive at the next process.
func TestSequenceStateSurvivesRestart(t *testing.T) {
	for _, every := range []int64{-1, 1} {
		h := newHarness(t, Config{SnapshotEvery: every})
		h.open(1)
		h.createTopic(1, "t", 1)
		h.produceAs(1, "t", 0, 7, 3, 0, "a", "b")
		h.produceAs(1, "t", 0, 7, 3, 2, "c")
		hash := h.node.st.hash()
		h.start()
		if h.node.st.hash() != hash {
			t.Fatalf("snapshot every %d: state after restart differs from before", every)
		}
		h.open(1)
		for _, s := range []struct {
			epoch int16
			seq   int32
			vals  []string
			want  wire.ProducePartitionResponse
		}{
			{3, 2, []string{"c"}, wire.ProducePartitionResponse{BaseOffset: 2}},
			{3, 0, []string{"a", "b"}, wire.ProducePartitionResponse{BaseOffset: 0}},
			{2, 0, []string{"x"}, wire.ProducePartitionResponse{ErrorCode: wire.ErrInvalidProducerEpoch, BaseOffset: -1}},
			{3, 3, []string{"d"}, wire.ProducePartitionResponse{BaseOffset: 3}},
		} {
			if got := h.produceAs(1, "t", 0, 7, s.epoch, s.seq, s.vals...); got != s.want {
				t.Fatalf("snapshot every %d, after restart: %+v, want %+v", every, got, s.want)
			}
		}
	}
}

// The deliberate bug: with the check off, a retry is appended again.
func TestNoSequenceCheckAppendsRetries(t *testing.T) {
	h := newHarness(t, Config{UnsafeNoSequenceCheck: true})
	h.open(1)
	h.createTopic(1, "t", 1)
	h.produceAs(1, "t", 0, 7, 0, 0, "a")
	if got := h.produceAs(1, "t", 0, 7, 0, 0, "a"); got.BaseOffset != 1 {
		t.Fatalf("the mutation did not append the retry: %+v", got)
	}
}

func (h *harness) initProducerID(c sys.ConnID, version int16, transactional bool) wire.InitProducerIDResponse {
	h.t.Helper()
	var resp wire.InitProducerIDResponse
	h.call(c, wire.KeyInitProducerID, version, &wire.InitProducerIDRequest{Transactional: transactional}, &resp)
	return resp
}

// No producer id is ever handed out twice: not by one node, not after a
// restart, not by two nodes; and a node answers whether or not it leads.
func TestProducerIDs(t *testing.T) {
	seen := map[int64]string{}
	take := func(who string, resp wire.InitProducerIDResponse) {
		t.Helper()
		if resp.ErrorCode != 0 || resp.ProducerID < 0 || resp.ProducerEpoch != 0 {
			t.Fatalf("%s: %+v", who, resp)
		}
		if other, dup := seen[resp.ProducerID]; dup {
			t.Fatalf("%s was given id %d, which %s has", who, resp.ProducerID, other)
		}
		seen[resp.ProducerID] = who
	}
	h := newHarness(t, Config{})
	h.open(1)
	for v := int16(0); v <= 4; v++ {
		take(fmt.Sprint("first life v", v), h.initProducerID(1, v, false))
	}
	if got := h.initProducerID(1, 4, true); got.ErrorCode != wire.ErrInvalidRequest || got.ProducerID != -1 {
		t.Fatalf("a transactional id: %+v", got)
	}
	for life := range 3 {
		h.start()
		h.open(1)
		take(fmt.Sprint("after restart ", life), h.initProducerID(1, 4, false))
	}
	// A block used up: the next id comes from a new block, durably.
	h.node.pids.used = 1<<pidIndexBits - 1
	take("last of a block", h.initProducerID(1, 4, false))
	take("first of the next block", h.initProducerID(1, 4, false))
	block := h.node.pids.block
	h.start()
	if h.node.pids.block != block+1 {
		t.Fatalf("after a block ran out and a restart: block %d, want %d", h.node.pids.block, block+1)
	}

	clear(seen)      // another cluster, with a node 1 of its own
	tr := newTrio(t) // nobody leads: no election has run
	for id, n := range tr.h {
		if n.node.Serving() {
			t.Fatal("a node of a fresh trio is serving")
		}
		n.open(1)
		take(fmt.Sprint("node ", id), n.initProducerID(1, 4, false))
	}
}

// A node id that does not fit in a producer id stops the start.
func TestNodeIDTooLargeForProducerIDs(t *testing.T) {
	n := New(Config{NodeID: maxNodeID + 1, Host: "h", Port: 1}, &fakeClock{timers: map[sys.TimerID]fakeTimer{}},
		&fakeNet{sent: map[sys.ConnID][]byte{}}, simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{}), simsys.NewRand(2))
	if err := n.OnStart(); err == nil {
		t.Fatal("a node id of 65536 was accepted")
	}
}

// The wrap from 2^31-1 to 0, on a partition whose producer is already near
// it: a producer cannot get there through the broker in a test's time.
func TestSequenceWrap(t *testing.T) {
	info := func(seq, count int32) wire.BatchInfo {
		return wire.BatchInfo{Count: count, ProducerID: 7, BaseSequence: seq}
	}
	for _, tt := range []struct {
		name  string
		start sentBatch // the producer's last batch so far, ending at offset 9
		steps []wire.BatchInfo
		want  []int64 // base offset, the duplicate's offset, or the code negated
	}{
		{"0 follows the last sequence number", sentBatch{topSeq - 1, topSeq, 8},
			[]wire.BatchInfo{info(0, 1), info(topSeq-1, 2), info(1, 1), info(5, 1)}, []int64{10, 8, 11, -int64(wire.ErrOutOfOrderSequenceNumber)}},
		{"a batch may span the wrap", sentBatch{topSeq - 2, topSeq - 2, 9},
			[]wire.BatchInfo{info(topSeq-1, 3), info(topSeq-1, 3), info(1, 1)}, []int64{10, 10, 13}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &partition{next: 10, producers: map[int64]*producer{7: {batches: []sentBatch{tt.start}}}}
			for i, b := range tt.steps {
				dup, code := p.admit(b)
				got := dup
				switch {
				case code != wire.ErrNone:
					got = -int64(code)
				case dup < 0:
					got = p.next
					p.remember(b, p.next, 0)
					p.next += int64(b.Count)
				}
				if got != tt.want[i] {
					t.Fatalf("step %d (sequence %d): got %d, want %d", i, b.BaseSequence, got, tt.want[i])
				}
			}
		})
	}
}

// Expiry (design section 7): a producer silent for ProducerExpiry is
// forgotten on every partition, one that keeps writing is not, and the
// forgotten one comes back by starting a new epoch at sequence 0.
func TestProducerExpiry(t *testing.T) {
	const expiry = time.Hour
	h := newHarness(t, Config{ProducerExpiry: expiry})
	h.open(1)
	h.createTopic(1, "t", 2)
	h.produceAs(1, "t", 0, 7, 0, 0, "a")
	h.produceAs(1, "t", 1, 7, 0, 0, "b")
	known := func(part int32, id int64) bool { return h.node.st.topics["t"].parts[part].producers[id] != nil }
	// Producer 8 writes every eighth of the expiry, which also gives the
	// leader the events it samples on.
	seq := int32(0)
	for step := time.Duration(0); step <= expiry*3/2; step += expiry / 8 {
		if got := h.produceAs(1, "t", 0, 8, 0, seq, "x"); got.ErrorCode != 0 {
			t.Fatalf("at %v the active producer was refused: %+v", step, got)
		}
		seq++
		if step < expiry && (!known(0, 7) || !known(1, 7)) {
			t.Fatalf("producer 7 was forgotten after %v, before the expiry of %v", step, expiry)
		}
		h.advance(expiry / 8)
	}
	if known(0, 7) || known(1, 7) {
		t.Fatal("producer 7 is still remembered after one and a half expiry times")
	}
	if !known(0, 8) {
		t.Fatal("the active producer was forgotten")
	}
	if got := h.produceAs(1, "t", 0, 7, 0, 1, "c"); got.ErrorCode != wire.ErrOutOfOrderSequenceNumber {
		t.Fatalf("an expired producer continuing its sequence: %+v, want OUT_OF_ORDER_SEQUENCE_NUMBER", got)
	}
	if got := h.produceAs(1, "t", 0, 7, 1, 0, "c"); got.ErrorCode != 0 {
		t.Fatalf("an expired producer starting a new epoch at 0: %+v", got)
	}
}

// Negative expiry never forgets; the deliberate bug forgets at once.
func TestProducerExpirySwitches(t *testing.T) {
	for _, tt := range []struct {
		cfg    Config
		forgot bool
	}{{Config{ProducerExpiry: -1}, false}, {Config{ProducerExpiry: time.Hour, UnsafeExpireEarly: true}, true}} {
		h := newHarness(t, tt.cfg)
		h.open(1)
		h.createTopic(1, "t", 1)
		h.produceAs(1, "t", 0, 7, 0, 0, "a")
		for i := range 3 {
			h.advance(time.Hour / 4)
			h.produceAs(1, "t", 0, 8, 0, int32(i), "x")
		}
		if forgot := h.node.st.topics["t"].parts[0].producers[7] == nil; forgot != tt.forgot {
			t.Errorf("%+v: producer forgotten: %v, want %v", tt.cfg, forgot, tt.forgot)
		}
	}
}

// A version 3 snapshot has no producer index; decoding takes the newest
// batch's, which is never earlier than the real one.
func TestSnapshotV3ProducerIndex(t *testing.T) {
	const id = 0x0102030405060708
	s := newState()
	for i, cmd := range [][]byte{
		encodeCreateTopic("t", 1),
		encodeAppend("t", 0, batchAs(id, 0x0a0b, 0, "a")),
		encodeAppend("t", 0, batch("b")),
	} {
		if _, err := s.apply(int64(i), cmd); err != nil {
			t.Fatal(err)
		}
	}
	b := s.encode()
	b = b[:len(b)-8] // version 5's ACL list and version 6's credentials, empty here
	field := binary.BigEndian.AppendUint16(binary.BigEndian.AppendUint64(nil, id), 0x0a0b)
	at := bytes.Index(b, field)
	if at < 0 || binary.BigEndian.Uint64(b[at+len(field):]) != 1 {
		t.Fatalf("producer entry not found in the encoding")
	}
	v3 := append(append([]byte{3}, b[1:at+len(field)]...), b[at+len(field)+8:]...)
	got, err := decodeState(v3)
	if err != nil {
		t.Fatal(err)
	}
	if ps := got.topics["t"].parts[0].producers[id]; ps == nil || ps.index != 2 {
		t.Fatalf("decoded producer %+v, want index 2 (the newest batch)", ps)
	}
}
