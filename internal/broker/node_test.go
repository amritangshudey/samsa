package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// fakeClock is a manual clock: time moves only when the test moves it.
type fakeClock struct {
	now    sys.Instant
	seq    int
	timers map[sys.TimerID]fakeTimer
}

type fakeTimer struct {
	at  sys.Instant
	seq int
}

func (c *fakeClock) Now() sys.Instant { return c.now }
func (c *fakeClock) SetTimer(id sys.TimerID, after time.Duration) {
	c.seq++
	c.timers[id] = fakeTimer{c.now + sys.Instant(after), c.seq}
}
func (c *fakeClock) CancelTimer(id sys.TimerID) { delete(c.timers, id) }

// fakeNet records what the node sends and which connections it closes.
type fakeNet struct {
	sent   map[sys.ConnID][]byte
	closed []sys.ConnID
	peers  []peerMsg // what the node sent to other nodes, in order
}

type peerMsg struct {
	to  sys.NodeID
	msg []byte
}

func (n *fakeNet) Send(c sys.ConnID, p []byte) { n.sent[c] = append(n.sent[c], p...) }
func (n *fakeNet) Close(c sys.ConnID)          { n.closed = append(n.closed, c) }
func (n *fakeNet) SendPeer(to sys.NodeID, p []byte) {
	n.peers = append(n.peers, peerMsg{to, append([]byte(nil), p...)})
}

// harness drives one node with no goroutines and no sockets.
type harness struct {
	t     *testing.T
	cfg   Config
	disk  *simsys.Disk
	clock *fakeClock
	net   *fakeNet
	node  *Node
	rand  *simsys.Rand // survives restarts, like real randomness
	corr  int32
}

func newHarness(t *testing.T, cfg Config) *harness {
	h := &harness{t: t, cfg: cfg, disk: simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{}), rand: simsys.NewRand(2)}
	h.start()
	return h
}

// start (re)creates the node on the same disk, as after a process restart.
func (h *harness) start() {
	h.t.Helper()
	h.clock = &fakeClock{now: h.clockNow(), timers: map[sys.TimerID]fakeTimer{}}
	h.net = &fakeNet{sent: map[sys.ConnID][]byte{}}
	if h.cfg.NodeID == 0 {
		h.cfg.NodeID, h.cfg.Host, h.cfg.Port = 1, "localhost", 9092
	}
	h.node = New(h.cfg, h.clock, h.net, h.disk, h.rand)
	if err := h.node.OnStart(); err != nil {
		h.t.Fatalf("OnStart: %v", err)
	}
}

func (h *harness) clockNow() sys.Instant {
	if h.clock != nil {
		return h.clock.now
	}
	return 1_700_000_000_000_000_000
}

func (h *harness) must(err error) {
	h.t.Helper()
	if err != nil {
		h.t.Fatalf("node failed: %v", err)
	}
}

func (h *harness) open(c sys.ConnID) { h.must(h.node.OnConnOpen(c, "127.0.0.1")) }

// run delivers everything that is due: closes the node asked for, then
// timers in deadline order, until nothing is left.
func (h *harness) run() {
	h.t.Helper()
	for {
		if len(h.net.closed) > 0 {
			c := h.net.closed[0]
			h.net.closed = h.net.closed[1:]
			h.must(h.node.OnConnClose(c))
			continue
		}
		var due []sys.TimerID
		for id, tm := range h.clock.timers {
			if tm.at <= h.clock.now {
				due = append(due, id)
			}
		}
		if len(due) == 0 {
			return
		}
		sort.Slice(due, func(i, j int) bool {
			a, b := h.clock.timers[due[i]], h.clock.timers[due[j]]
			return a.at < b.at || (a.at == b.at && a.seq < b.seq)
		})
		delete(h.clock.timers, due[0])
		h.must(h.node.OnTimer(due[0]))
	}
}

func (h *harness) advance(d time.Duration) {
	h.t.Helper()
	h.clock.now += sys.Instant(d)
	h.run()
}

// frame encodes one request and returns its bytes and correlation id.
func (h *harness) frame(key, version int16, req wire.Message) ([]byte, int32) {
	h.corr++
	w := wire.Writer{B: make([]byte, 4)}
	(&wire.RequestHeader{Key: key, Version: version, CorrelationID: h.corr, ClientID: "test"}).Encode(&w)
	req.Encode(&w, version)
	binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
	return w.B, h.corr
}

// send delivers a request without running timers.
func (h *harness) send(c sys.ConnID, key, version int16, req wire.Message) int32 {
	h.t.Helper()
	b, corr := h.frame(key, version, req)
	h.must(h.node.OnConnData(c, b))
	return corr
}

// recv takes the next response on c, checks its correlation id, and decodes
// it into resp. It reports false if no complete response is waiting.
func (h *harness) recv(c sys.ConnID, key, version int16, corr int32, resp wire.Message) bool {
	h.t.Helper()
	b := h.net.sent[c]
	if len(b) < 4 {
		return false
	}
	size := int(binary.BigEndian.Uint32(b))
	if len(b) < 4+size {
		h.t.Fatalf("partial response frame: %d of %d bytes", len(b)-4, size)
	}
	h.net.sent[c] = b[4+size:]
	r := wire.NewReader(b[4 : 4+size])
	if got := wire.DecodeResponseHeader(r, key, version); got != corr {
		h.t.Fatalf("response correlation id %d, want %d", got, corr)
	}
	resp.Decode(r, version)
	if r.Err() != nil || r.Len() != 0 {
		h.t.Fatalf("decode response: %v, %d bytes left", r.Err(), r.Len())
	}
	return true
}

// call sends a request, lets the node commit, and returns the response.
func (h *harness) call(c sys.ConnID, key, version int16, req, resp wire.Message) {
	h.t.Helper()
	corr := h.send(c, key, version, req)
	h.run()
	if !h.recv(c, key, version, corr, resp) {
		h.t.Fatalf("no response to request key %d", key)
	}
}

func (h *harness) createTopic(c sys.ConnID, name string, parts int32) int16 {
	h.t.Helper()
	var resp wire.CreateTopicsResponse
	h.call(c, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: name, NumPartitions: parts, ReplicationFactor: 1}}}, &resp)
	return resp.Topics[0].ErrorCode
}

func batch(values ...string) []byte {
	var recs []wire.Record
	for i, v := range values {
		recs = append(recs, wire.Record{Key: []byte("k"), Value: []byte(v), Timestamp: int64(1000 + i)})
	}
	return wire.EncodeBatch(recs)
}

func produceReq(topic string, part int32, records []byte) *wire.ProduceRequest {
	return &wire.ProduceRequest{Acks: -1, TimeoutMs: 1000, Topics: []wire.ProduceTopic{{Name: topic, Partitions: []wire.ProducePartition{{Index: part, Records: records}}}}}
}

func (h *harness) produce(c sys.ConnID, topic string, part int32, values ...string) wire.ProducePartitionResponse {
	h.t.Helper()
	var resp wire.ProduceResponse
	h.call(c, wire.KeyProduce, 8, produceReq(topic, part, batch(values...)), &resp)
	return resp.Topics[0].Partitions[0]
}

func fetchReq(topic string, part int32, offset int64, maxWaitMs, minBytes int32) *wire.FetchRequest {
	return &wire.FetchRequest{MaxWaitMs: maxWaitMs, MinBytes: minBytes, MaxBytes: 1 << 20,
		Topics: []wire.FetchTopic{{Name: topic, Partitions: []wire.FetchPartition{{Index: part, Offset: offset, MaxBytes: 1 << 20}}}}}
}

// values decodes every record in a fetch partition response and checks that
// offsets are dense, returning the values and the first offset.
func (h *harness) values(p wire.FetchPartitionResponse) (first int64, values []string) {
	h.t.Helper()
	first = -1
	next := int64(-1)
	for rest := p.Records; len(rest) > 0; {
		info, code := wire.ParseBatch(rest, 1<<20)
		if code != wire.ErrNone {
			h.t.Fatalf("fetched batch is invalid: error %d", code)
		}
		base, recs, err := wire.DecodeBatch(rest[:info.Size])
		if err != nil {
			h.t.Fatal(err)
		}
		if first < 0 {
			first = base
		} else if base != next {
			h.t.Fatalf("offset gap: batch at %d, expected %d", base, next)
		}
		next = base + int64(len(recs))
		for _, r := range recs {
			values = append(values, string(r.Value))
		}
		rest = rest[info.Size:]
	}
	return first, values
}

func (h *harness) fetch(c sys.ConnID, topic string, part int32, offset int64) (wire.FetchPartitionResponse, []string) {
	h.t.Helper()
	var resp wire.FetchResponse
	h.call(c, wire.KeyFetch, 11, fetchReq(topic, part, offset, 0, 0), &resp)
	p := resp.Topics[0].Partitions[0]
	_, vals := h.values(p)
	return p, vals
}

func TestAPIVersions(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	for v := int16(0); v <= 4; v++ {
		var resp wire.APIVersionsResponse
		h.call(1, wire.KeyAPIVersions, v, &wire.APIVersionsRequest{ClientSoftwareName: "t", ClientSoftwareVersion: "1"}, &resp)
		if resp.ErrorCode != 0 || len(resp.APIs) != len(wire.APIs) {
			t.Fatalf("v%d: %+v", v, resp)
		}
		if resp.APIs[0] != (wire.APIRange{Key: wire.KeyProduce, Min: 0, Max: 8}) {
			t.Errorf("v%d: produce advertised as %+v", v, resp.APIs[0])
		}
	}
	// A version we do not speak is answered in version 0 with the list, so
	// the client can retry lower.
	b, corr := h.frame(wire.KeyAPIVersions, 9, &wire.APIVersionsRequest{})
	h.must(h.node.OnConnData(1, b))
	var resp wire.APIVersionsResponse
	if !h.recv(1, wire.KeyAPIVersions, 0, corr, &resp) || resp.ErrorCode != wire.ErrUnsupportedVersion || len(resp.APIs) == 0 {
		t.Fatalf("unsupported version: %+v", resp)
	}
}

func TestCreateTopicsAndMetadata(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	tests := []struct {
		name string
		req  wire.CreateTopic
		want int16
	}{
		{"ok", wire.CreateTopic{Name: "orders", NumPartitions: 3, ReplicationFactor: 1}, wire.ErrNone},
		{"defaults", wire.CreateTopic{Name: "defaults", NumPartitions: -1, ReplicationFactor: -1}, wire.ErrNone},
		{"exists", wire.CreateTopic{Name: "orders", NumPartitions: 1, ReplicationFactor: 1}, wire.ErrTopicAlreadyExists},
		{"bad name", wire.CreateTopic{Name: "no spaces", NumPartitions: 1, ReplicationFactor: 1}, wire.ErrInvalidTopic},
		{"dot", wire.CreateTopic{Name: "..", NumPartitions: 1, ReplicationFactor: 1}, wire.ErrInvalidTopic},
		{"zero partitions", wire.CreateTopic{Name: "z", NumPartitions: 0, ReplicationFactor: 1}, wire.ErrInvalidPartitions},
		{"too many partitions", wire.CreateTopic{Name: "z", NumPartitions: 5000, ReplicationFactor: 1}, wire.ErrInvalidPartitions},
		{"replication 3", wire.CreateTopic{Name: "z", NumPartitions: 1, ReplicationFactor: 3}, wire.ErrInvalidReplicationFactor},
	}
	for _, tt := range tests {
		var resp wire.CreateTopicsResponse
		h.call(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{tt.req}}, &resp)
		if got := resp.Topics[0].ErrorCode; got != tt.want {
			t.Errorf("%s: error %d, want %d", tt.name, got, tt.want)
		}
	}
	// validateOnly creates nothing; the same name twice in one request and
	// two requests racing for one name each resolve to one winner.
	var resp wire.CreateTopicsResponse
	h.call(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{ValidateOnly: true, Topics: []wire.CreateTopic{{Name: "dry", NumPartitions: 1, ReplicationFactor: 1}}}, &resp)
	h.call(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{
		{Name: "twice", NumPartitions: 1, ReplicationFactor: 1}, {Name: "twice", NumPartitions: 1, ReplicationFactor: 1}}}, &resp)
	if resp.Topics[0].ErrorCode != 0 || resp.Topics[1].ErrorCode != wire.ErrInvalidRequest {
		t.Errorf("duplicate in one request: %+v", resp.Topics)
	}
	h.open(2)
	race := &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "race", NumPartitions: 1, ReplicationFactor: 1}}}
	c1, c2 := h.send(1, wire.KeyCreateTopics, 4, race), h.send(2, wire.KeyCreateTopics, 4, race)
	h.run()
	var r1, r2 wire.CreateTopicsResponse
	h.recv(1, wire.KeyCreateTopics, 4, c1, &r1)
	h.recv(2, wire.KeyCreateTopics, 4, c2, &r2)
	if r1.Topics[0].ErrorCode != 0 || r2.Topics[0].ErrorCode != wire.ErrTopicAlreadyExists {
		t.Errorf("racing creates: %d and %d", r1.Topics[0].ErrorCode, r2.Topics[0].ErrorCode)
	}

	for v := int16(1); v <= 8; v++ {
		var md wire.MetadataResponse
		h.call(1, wire.KeyMetadata, v, &wire.MetadataRequest{AllTopics: true}, &md)
		var names []string
		for _, tp := range md.Topics {
			names = append(names, tp.Name)
		}
		if want := []string{"defaults", "orders", "race", "twice"}; !reflect.DeepEqual(names, want) {
			t.Fatalf("v%d topics = %v, want %v", v, names, want)
		}
		if len(md.Topics[1].Partitions) != 3 || md.Brokers[0].Port != 9092 || md.ControllerID != 1 {
			t.Fatalf("v%d metadata = %+v", v, md)
		}
		h.call(1, wire.KeyMetadata, v, &wire.MetadataRequest{Topics: []string{"orders", "nope"}}, &md)
		if md.Topics[0].ErrorCode != 0 || md.Topics[1].ErrorCode != wire.ErrUnknownTopicOrPartition {
			t.Fatalf("v%d named topics = %+v", v, md.Topics)
		}
		h.call(1, wire.KeyMetadata, v, &wire.MetadataRequest{}, &md)
		if len(md.Topics) != 0 {
			t.Fatalf("v%d: empty topic list returned %d topics", v, len(md.Topics))
		}
	}
}

func TestProduceIsAcknowledgedOnlyAfterSync(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)

	corr := h.send(1, wire.KeyProduce, 8, produceReq("t", 0, batch("a", "b")))
	var resp wire.ProduceResponse
	if h.recv(1, wire.KeyProduce, 8, corr, &resp) {
		t.Fatal("produce acknowledged before the flush")
	}
	if _, vals := h.fetchNoRun(1, "t", 0, 0); len(vals) != 0 {
		t.Fatalf("uncommitted records visible to a fetch: %v", vals)
	}
	h.run()
	if !h.recv(1, wire.KeyProduce, 8, corr, &resp) || resp.Topics[0].Partitions[0].BaseOffset != 0 {
		t.Fatalf("after flush: %+v", resp)
	}
	if got := h.produce(1, "t", 0, "c"); got.BaseOffset != 2 || got.ErrorCode != 0 {
		t.Fatalf("second produce: %+v", got)
	}
	p, vals := h.fetch(1, "t", 0, 0)
	if p.HighWatermark != 3 || !reflect.DeepEqual(vals, []string{"a", "b", "c"}) {
		t.Fatalf("fetch from 0: hw %d, %v", p.HighWatermark, vals)
	}
	// An offset inside a batch returns that whole batch; the client skips.
	var fr wire.FetchResponse
	h.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 1, 0, 0), &fr)
	if first, vals := h.values(fr.Topics[0].Partitions[0]); first != 0 || len(vals) != 3 {
		t.Fatalf("fetch from 1: first offset %d, %v", first, vals)
	}
}

// fetchNoRun fetches without running timers, so nothing gets committed.
func (h *harness) fetchNoRun(c sys.ConnID, topic string, part int32, offset int64) (wire.FetchPartitionResponse, []string) {
	h.t.Helper()
	h.open(99)
	defer func() { h.must(h.node.OnConnClose(99)) }()
	corr := h.send(99, wire.KeyFetch, 11, fetchReq(topic, part, offset, 0, 0))
	var resp wire.FetchResponse
	if !h.recv(99, wire.KeyFetch, 11, corr, &resp) {
		h.t.Fatal("no fetch response")
	}
	_, vals := h.values(resp.Topics[0].Partitions[0])
	return resp.Topics[0].Partitions[0], vals
}

func TestProduceRejects(t *testing.T) {
	h := newHarness(t, Config{MaxBatchBytes: 200})
	h.open(1)
	h.createTopic(1, "t", 2)
	good := batch("x")
	corrupt := bytes.Clone(good)
	corrupt[len(corrupt)-1] ^= 1
	old := bytes.Clone(good)
	old[16] = 1
	tests := []struct {
		name string
		req  *wire.ProduceRequest
		want int16
	}{
		{"unknown topic", produceReq("nope", 0, good), wire.ErrUnknownTopicOrPartition},
		{"unknown partition", produceReq("t", 2, good), wire.ErrUnknownTopicOrPartition},
		{"negative partition", produceReq("t", -1, good), wire.ErrUnknownTopicOrPartition},
		{"corrupt batch", produceReq("t", 0, corrupt), wire.ErrCorruptMessage},
		{"old format", produceReq("t", 0, old), wire.ErrUnsupportedForMessageFormat},
		{"too large", produceReq("t", 0, batch(string(make([]byte, 300)))), wire.ErrMessageTooLarge},
		{"empty", produceReq("t", 0, nil), wire.ErrInvalidRequest},
		{"good then corrupt", produceReq("t", 0, append(bytes.Clone(good), corrupt...)), wire.ErrCorruptMessage},
		{"bad acks", &wire.ProduceRequest{Acks: 2, Topics: produceReq("t", 0, good).Topics}, wire.ErrInvalidRequiredAcks},
	}
	for _, tt := range tests {
		var resp wire.ProduceResponse
		h.call(1, wire.KeyProduce, 8, tt.req, &resp)
		if got := resp.Topics[0].Partitions[0]; got.ErrorCode != tt.want || got.BaseOffset != -1 {
			t.Errorf("%s: %+v, want error %d", tt.name, got, tt.want)
		}
	}
	if p, _ := h.fetch(1, "t", 0, 0); p.HighWatermark != 0 {
		t.Errorf("a rejected produce wrote records: high watermark %d", p.HighWatermark)
	}

	// One request, two partitions, one bad: each gets its own verdict, and
	// two batches in one partition get consecutive offsets.
	var resp wire.ProduceResponse
	h.call(1, wire.KeyProduce, 8, &wire.ProduceRequest{Acks: 1, Topics: []wire.ProduceTopic{{Name: "t", Partitions: []wire.ProducePartition{
		{Index: 0, Records: append(batch("a", "b"), batch("c")...)}, {Index: 1, Records: corrupt}}}}}, &resp)
	if ps := resp.Topics[0].Partitions; ps[0].ErrorCode != 0 || ps[0].BaseOffset != 0 || ps[1].ErrorCode != wire.ErrCorruptMessage {
		t.Errorf("mixed request: %+v", ps)
	}
	if _, vals := h.fetch(1, "t", 0, 0); !reflect.DeepEqual(vals, []string{"a", "b", "c"}) {
		t.Errorf("two batches in one request: %v", vals)
	}

	// acks=0: no response, but the write happens and the connection goes on.
	req := produceReq("t", 1, good)
	req.Acks = 0
	h.send(1, wire.KeyProduce, 8, req)
	h.run()
	if len(h.net.sent[1]) != 0 {
		t.Error("acks=0 got a response")
	}
	if _, vals := h.fetch(1, "t", 1, 0); !reflect.DeepEqual(vals, []string{"x"}) {
		t.Errorf("acks=0 write: %v", vals)
	}
}

func TestFetchErrorsAndLimits(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	for i := range 5 {
		h.produce(1, "t", 0, fmt.Sprintf("value-%d", i))
	}
	one := len(batch("value-0"))
	tests := []struct {
		name      string
		offset    int64
		maxBytes  int32
		wantErr   int16
		wantCount int
	}{
		{"all", 0, 1 << 20, 0, 5},
		{"from the middle", 3, 1 << 20, 0, 2},
		{"at the end", 5, 1 << 20, 0, 0},
		{"past the end", 6, 1 << 20, wire.ErrOffsetOutOfRange, 0},
		{"negative", -1, 1 << 20, wire.ErrOffsetOutOfRange, 0},
		{"limit of two batches", 0, int32(2 * one), 0, 2},
		{"limit below one batch still returns one", 0, 1, 0, 1},
	}
	for _, tt := range tests {
		req := fetchReq("t", 0, tt.offset, 0, 0)
		req.Topics[0].Partitions[0].MaxBytes = tt.maxBytes
		var resp wire.FetchResponse
		h.call(1, wire.KeyFetch, 11, req, &resp)
		p := resp.Topics[0].Partitions[0]
		if _, vals := h.values(p); p.ErrorCode != tt.wantErr || len(vals) != tt.wantCount || p.HighWatermark != 5 {
			t.Errorf("%s: error %d, %d records, hw %d", tt.name, p.ErrorCode, len(vals), p.HighWatermark)
		}
	}
	var resp wire.FetchResponse
	h.call(1, wire.KeyFetch, 4, fetchReq("nope", 0, 0, 500, 1), &resp)
	if resp.Topics[0].Partitions[0].ErrorCode != wire.ErrUnknownTopicOrPartition {
		t.Errorf("unknown topic must answer at once with an error: %+v", resp)
	}
}

func TestFetchLongPoll(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.open(2)
	h.createTopic(1, "t", 1)
	var resp wire.FetchResponse

	// Data arrives while waiting: answered when it commits, not before.
	corr := h.send(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 500, 1))
	h.run()
	if h.recv(1, wire.KeyFetch, 11, corr, &resp) {
		t.Fatal("fetch with nothing to return answered immediately")
	}
	h.produce(2, "t", 0, "late")
	if !h.recv(1, wire.KeyFetch, 11, corr, &resp) {
		t.Fatal("parked fetch not woken by a produce")
	}
	if _, vals := h.values(resp.Topics[0].Partitions[0]); !reflect.DeepEqual(vals, []string{"late"}) {
		t.Fatalf("woken fetch returned %v", vals)
	}

	// Nothing arrives: answered empty at the deadline, and only once.
	corr = h.send(1, wire.KeyFetch, 11, fetchReq("t", 0, 1, 500, 1))
	h.advance(499 * time.Millisecond)
	if h.recv(1, wire.KeyFetch, 11, corr, &resp) {
		t.Fatal("answered before the deadline")
	}
	h.advance(time.Millisecond)
	if !h.recv(1, wire.KeyFetch, 11, corr, &resp) || len(resp.Topics[0].Partitions[0].Records) != 0 {
		t.Fatalf("at the deadline: %+v", resp)
	}
	h.advance(time.Second)
	if len(h.net.sent[1]) != 0 || len(h.node.parked) != 0 || len(h.clock.timers) != 0 {
		t.Fatal("fetch answered twice or left state behind")
	}

	// The connection closes while waiting: nothing is sent, nothing leaks.
	h.send(1, wire.KeyFetch, 11, fetchReq("t", 0, 1, 500, 1))
	h.must(h.node.OnConnClose(1))
	h.produce(2, "t", 0, "nobody is listening")
	h.advance(time.Second)
	if len(h.net.sent[1]) != 0 || len(h.node.parked) != 0 || len(h.clock.timers) != 0 {
		t.Fatal("parked fetch outlived its connection")
	}

	// A request behind a parked fetch on the same connection waits its turn.
	h.open(3)
	fc := h.send(3, wire.KeyFetch, 11, fetchReq("t", 0, 2, 500, 1))
	mc := h.send(3, wire.KeyMetadata, 8, &wire.MetadataRequest{})
	h.run()
	if len(h.net.sent[3]) != 0 {
		t.Fatal("a later request overtook a parked fetch")
	}
	h.advance(500 * time.Millisecond)
	var md wire.MetadataResponse
	if !h.recv(3, wire.KeyFetch, 11, fc, &resp) || !h.recv(3, wire.KeyMetadata, 8, mc, &md) {
		t.Fatal("responses missing or out of order after the fetch deadline")
	}
}

func TestListOffsets(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	h.produce(1, "t", 0, "a", "b") // timestamps 1000, 1001
	h.produce(1, "t", 0, "c")      // timestamp 1000
	tests := []struct {
		ts         int64
		wantOffset int64
		wantErr    int16
	}{
		{wire.TimestampEarliest, 0, 0},
		{wire.TimestampLatest, 3, 0},
		{500, 0, 0},
		{1001, 0, 0},
		{5000, -1, 0},
		{-3, -1, wire.ErrInvalidRequest},
	}
	for _, tt := range tests {
		for v := int16(1); v <= 5; v++ {
			var resp wire.ListOffsetsResponse
			h.call(1, wire.KeyListOffsets, v, &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: "t", Partitions: []wire.ListOffsetsPartition{{Index: 0, Timestamp: tt.ts}}}}}, &resp)
			if p := resp.Topics[0].Partitions[0]; p.Offset != tt.wantOffset || p.ErrorCode != tt.wantErr {
				t.Errorf("v%d timestamp %d: %+v", v, tt.ts, p)
			}
		}
	}
}

// Frames can arrive glued together or cut anywhere; responses must come
// back complete and in request order either way.
func TestFramingAndPipelining(t *testing.T) {
	for _, chunk := range []int{1, 3, 7, 1 << 20} {
		h := newHarness(t, Config{})
		h.open(1)
		h.createTopic(1, "t", 1)
		var stream []byte
		var corrs []int32
		for i := range 5 {
			b, corr := h.frame(wire.KeyProduce, 8, produceReq("t", 0, batch(fmt.Sprint(i))))
			stream = append(stream, b...)
			corrs = append(corrs, corr)
		}
		for len(stream) > 0 {
			n := min(chunk, len(stream))
			h.must(h.node.OnConnData(1, stream[:n]))
			stream = stream[n:]
			h.run()
		}
		for i, corr := range corrs {
			var resp wire.ProduceResponse
			if !h.recv(1, wire.KeyProduce, 8, corr, &resp) || resp.Topics[0].Partitions[0].BaseOffset != int64(i) {
				t.Fatalf("chunk %d: response %d = %+v", chunk, i, resp)
			}
		}
		if _, vals := h.fetch(1, "t", 0, 0); !reflect.DeepEqual(vals, []string{"0", "1", "2", "3", "4"}) {
			t.Fatalf("chunk %d: order on disk %v", chunk, vals)
		}
	}
}

func TestBadInputClosesConnection(t *testing.T) {
	frame := func(body ...byte) []byte {
		return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
	}
	tests := map[string][]byte{
		"negative size":       {0xff, 0xff, 0xff, 0xff},
		"size over the limit": {0x7f, 0xff, 0xff, 0xff},
		"empty frame":         frame(),
		"truncated header":    frame(0, 3, 0),
		"unknown api":         frame(0, 77, 0, 0, 0, 0, 0, 1, 0, 0),
		"unsupported version": frame(0, 0, 0, 99, 0, 0, 0, 1, 0, 0),
		"produce v2":          frame(0, 0, 0, 2, 0, 0, 0, 1, 0, 0),
		"truncated body":      frame(0, 0, 0, 8, 0, 0, 0, 1, 0, 0, 0xff, 0xff, 0, 1),
		"huge array":          frame(0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0x7f, 0xff, 0xff, 0xff),
	}
	for name, in := range tests {
		h := newHarness(t, Config{MaxRequestBytes: 1 << 20})
		h.open(1)
		h.must(h.node.OnConnData(1, in))
		if len(h.net.closed) != 1 || len(h.net.sent[1]) != 0 {
			t.Errorf("%s: closed %v, sent %d bytes; want the connection closed and nothing sent", name, h.net.closed, len(h.net.sent[1]))
		}
		h.must(h.node.OnConnData(1, in)) // data after the close is ignored
		h.run()
		if len(h.node.conns) != 0 {
			t.Errorf("%s: connection state not released", name)
		}
	}
}

func TestRestartKeepsCommittedData(t *testing.T) {
	for _, every := range []int64{-1, 1, 7} { // without snapshots, after every entry, every few
		t.Run(fmt.Sprint("snapshot every ", every), func(t *testing.T) { restartKeepsCommittedData(t, every) })
	}
}

func restartKeepsCommittedData(t *testing.T, every int64) {
	h := newHarness(t, Config{SegmentBytes: 2048, MaxBatchBytes: 1000, SnapshotEvery: every})
	h.open(1)
	h.createTopic(1, "a", 2)
	h.createTopic(1, "b", 1)
	want := map[string][]string{}
	for i := range 60 {
		topic, part := []string{"a", "a", "b"}[i%3], int32([]int{0, 1, 0}[i%3])
		v := fmt.Sprintf("v%d", i)
		if got := h.produce(1, topic, part, v); got.ErrorCode != 0 {
			t.Fatal(got)
		}
		key := fmt.Sprint(topic, part)
		want[key] = append(want[key], v)
	}
	hash := h.node.st.hash()
	for range 2 { // twice: replay must be repeatable
		h.start()
		if i := h.node.raft.SnapshotIndex(); (i >= 0) != (every > 0) {
			t.Fatalf("snapshot index %d with SnapshotEvery %d", i, every)
		}
		if h.node.st.hash() != hash {
			t.Fatal("state after restart differs from before")
		}
		h.open(1)
		for i, key := range []string{"a0", "a1", "b0"} {
			topic, part := []string{"a", "a", "b"}[i], int32([]int{0, 1, 0}[i])
			if _, vals := h.fetch(1, topic, part, 0); !reflect.DeepEqual(vals, want[key]) {
				t.Fatalf("%s after restart: %v, want %v", key, vals, want[key])
			}
		}
		if got := h.createTopic(1, "a", 1); got != wire.ErrTopicAlreadyExists {
			t.Fatalf("topic forgotten after restart: %d", got)
		}
	}
	if got := h.produce(1, "b", 0, "after"); got.BaseOffset != 20 {
		t.Fatalf("offsets did not continue after restart: %+v", got)
	}
}

func TestSyncPolicies(t *testing.T) {
	// Under SyncAlways an acknowledged write survives power loss, and one
	// that was never acknowledged may vanish but can never be half-there.
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	h.produce(1, "t", 0, "acked")
	h.send(1, wire.KeyProduce, 8, produceReq("t", 0, batch("never acked")))
	h.disk.Restart()
	h.start()
	h.open(1)
	if _, vals := h.fetch(1, "t", 0, 0); len(vals) < 1 || vals[0] != "acked" {
		t.Fatalf("acknowledged write lost: %v", vals)
	}

	// SyncNever acknowledges without any timer and without a sync.
	h = newHarness(t, Config{Sync: SyncNever})
	h.open(1)
	corr := h.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 1}}})
	var ct wire.CreateTopicsResponse
	if !h.recv(1, wire.KeyCreateTopics, 4, corr, &ct) || len(h.clock.timers) != 0 {
		t.Fatal("SyncNever did not answer immediately")
	}
	if h.node.raft.Durable() != 0 {
		t.Fatal("SyncNever synced")
	}

	// SyncInterval acknowledges at once and syncs when its timer fires.
	h = newHarness(t, Config{Sync: SyncInterval, SyncInterval: 100 * time.Millisecond})
	h.open(1)
	h.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 1}}})
	if len(h.net.sent[1]) == 0 || h.node.raft.Durable() != 0 {
		t.Fatal("SyncInterval must answer before syncing")
	}
	h.advance(100 * time.Millisecond)
	if h.node.raft.Durable() != 2 { // the leader's no-op and the command
		t.Fatal("SyncInterval timer did not sync")
	}
	h.advance(100 * time.Millisecond)
	if len(h.clock.timers) != 1 {
		t.Fatal("SyncInterval timer not re-armed")
	}
}

func TestSyncFailureStopsNode(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 1)
	h.disk.SetSyncErrorRate(1)
	h.send(1, wire.KeyProduce, 8, produceReq("t", 0, batch("x")))
	err := h.node.OnTimer(timerFlush)
	if !errors.Is(err, simsys.ErrInjected) {
		t.Fatalf("handler returned %v, want the sync failure", err)
	}
	if len(h.net.sent[1]) != 0 {
		t.Fatal("a write was acknowledged although its sync failed")
	}
	if err := h.node.OnConnOpen(5, "127.0.0.1"); !errors.Is(err, simsys.ErrInjected) {
		t.Fatalf("node kept working after a failed sync: %v", err)
	}
}

func TestUnknownCommandInLogStopsStart(t *testing.T) {
	h := newHarness(t, Config{})
	h.node.propose([]byte{200, 1, 2, 3}, func(result) {})
	if err := h.node.OnTimer(timerFlush); !errors.Is(err, errBadCommand) {
		t.Fatalf("got %v, want errBadCommand", err)
	}
	n := New(h.cfg, h.clock, h.net, h.disk, h.rand)
	if err := n.OnStart(); !errors.Is(err, errBadCommand) {
		t.Fatalf("restart over an unknown command: got %v, want errBadCommand", err)
	}
}

// FuzzNodeInput throws arbitrary bytes at a connection, in arbitrary
// pieces. The node must not panic or fail, whatever arrives.
func FuzzNodeInput(f *testing.F) {
	h := &harness{}
	for _, m := range []struct {
		key, v int16
		req    wire.Message
	}{
		{wire.KeyAPIVersions, 3, &wire.APIVersionsRequest{ClientSoftwareName: "f"}},
		{wire.KeyMetadata, 8, &wire.MetadataRequest{AllTopics: true}},
		{wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 1}}}},
		{wire.KeyProduce, 8, produceReq("t", 0, batch("x"))},
		{wire.KeyFetch, 11, fetchReq("t", 0, 0, 10, 1)},
		{wire.KeyListOffsets, 5, &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: "t", Partitions: []wire.ListOffsetsPartition{{Timestamp: -1}}}}}},
	} {
		b, _ := h.frame(m.key, m.v, m.req)
		f.Add(b, uint8(5))
	}
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		h := newHarness(t, Config{MaxRequestBytes: 1 << 16})
		h.open(1)
		h.createTopic(1, "t", 1)
		step := int(chunk)%64 + 1
		for len(data) > 0 {
			n := min(step, len(data))
			h.must(h.node.OnConnData(1, data[:n]))
			data = data[n:]
			h.run()
		}
		h.advance(time.Hour)
	})
}

// Retention (docs/design/a1-snapshots.md, section 3): the leader trims old
// segments; fetches below the new start answer OFFSET_OUT_OF_RANGE, the
// earliest offset moves up, everything above is unchanged, segments are
// deleted, and a restart comes back to the same state.
func TestRetentionTrimsTheLog(t *testing.T) {
	h := newHarness(t, Config{SegmentBytes: 2048, MaxBatchBytes: 1000, SnapshotEvery: -1, RetentionBytes: 4096})
	h.open(1)
	h.createTopic(1, "t", 1)
	for i := range 200 {
		if got := h.produce(1, "t", 0, fmt.Sprintf("value-%03d", i)); got.ErrorCode != 0 {
			t.Fatal(got)
		}
	}
	start := h.node.st.topics["t"].parts[0].start
	if start == 0 || h.node.raft.LogStart() == 0 || h.node.raft.SnapshotIndex() < 0 {
		t.Fatalf("nothing trimmed: partition start %d, log start %d, snapshot %d", start, h.node.raft.LogStart(), h.node.raft.SnapshotIndex())
	}
	check := func() {
		t.Helper()
		if p, _ := h.fetch(1, "t", 0, start-1); p.ErrorCode != wire.ErrOffsetOutOfRange || p.LogStartOffset != start {
			t.Fatalf("fetch below the start: %+v", p)
		}
		p, vals := h.fetch(1, "t", 0, start)
		if p.ErrorCode != 0 || p.LogStartOffset != start || len(vals) == 0 || vals[0] != fmt.Sprintf("value-%03d", start) {
			t.Fatalf("fetch at the start %d: %+v %v", start, p, vals)
		}
		var resp wire.ListOffsetsResponse
		h.call(1, wire.KeyListOffsets, 1, &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: "t", Partitions: []wire.ListOffsetsPartition{{Index: 0, Timestamp: wire.TimestampEarliest}}}}}, &resp)
		if got := resp.Topics[0].Partitions[0].Offset; got != start {
			t.Fatalf("earliest offset %d, want %d", got, start)
		}
	}
	check()
	hash := h.node.st.hash()
	h.start()
	if h.node.st.hash() != hash {
		t.Fatal("state after restart differs from before")
	}
	h.open(1)
	check()
}
