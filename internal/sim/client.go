package sim

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// The topics clients use. Each has a fixed partition count, so that two
// clients racing to create one agree on what it looks like. Nobody ever
// creates ghostTopic.
var catalogue = []struct {
	name  string
	parts int32
}{{"t0", 1}, {"t1", 2}, {"t2", 3}}

const ghostTopic = "ghost"

// inCatalogue reports whether tp could ever exist. A write to anything else
// has to be rejected.
func inCatalogue(tp TP) bool {
	for _, t := range catalogue {
		if t.name == tp.Topic {
			return tp.Part >= 0 && tp.Part < t.parts
		}
	}
	return false
}

// maxBatchBytes is the broker's batch limit in the simulator; clients
// sometimes exceed it on purpose.
const maxBatchBytes = 2048

// answerBound is I7's limit: how long a connection with requests
// outstanding may go without a response. It exceeds the longest fetch wait
// a client asks for plus any network delay.
const answerBound = 10 * time.Second

// call is a request whose response has not arrived.
type call struct {
	key, version int16 // version the response is encoded in
	corr         int32
	resp         wire.Message
	done         func(ok bool) // ok is false if the connection died first
}

// client is one simulated Kafka client: a connection, the requests in
// flight on it, and a behaviour that issues requests.
type client struct {
	w        *world
	rng      *simsys.Rand
	step     func(*client) // issue one request
	c        *conn
	in       []byte
	pending  []*call
	corr     int32
	progress uint64
	cursors  map[TP]int64 // consumers: next offset to read
	mem      *membership  // group members: their place in the group
	pin      *simNode     // if set, the only node this client connects to
	fetchMin int16        // lowest Fetch version to use
}

// tick is the client's heartbeat: connect, misbehave, or issue a request.
func (cl *client) tick() {
	w := cl.w
	if w.quiet {
		return
	}
	switch {
	case cl.c == nil:
		cl.c = w.connect(cl)
	case w.opts.disconnects && cl.rng.Chance(0.01):
		w.stats.Disconnects++
		cl.hangUp()
	case len(cl.pending) < 3: // up to three requests pipelined
		cl.step(cl)
	}
	w.after(time.Duration(cl.rng.Intn(int(20*time.Millisecond))), cl.tick)
}

// version picks a random supported version of an API.
func (cl *client) version(key int16) int16 {
	a, _ := wire.LookupAPI(key)
	return a.Min + int16(cl.rng.Intn(int(a.Max-a.Min)+1))
}

// request sends one request. resp receives the decoded response before
// done runs; a nil resp means no response is expected.
func (cl *client) request(key, version int16, body, resp wire.Message, done func(ok bool)) {
	cl.corr++
	h := wire.RequestHeader{Key: key, Version: version, CorrelationID: cl.corr, ClientID: "sim"}
	w := wire.Writer{B: make([]byte, 4)}
	h.Encode(&w)
	body.Encode(&w, version)
	binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
	cl.w.stats.Requests++
	if resp != nil {
		respVersion := version
		if !wire.Supported(key, version) {
			respVersion = 0 // how the broker answers an ApiVersions it cannot read
		}
		cl.pending = append(cl.pending, &call{key: key, version: respVersion, corr: cl.corr, resp: resp, done: done})
		cl.watch()
	}
	cl.w.send(cl.c, w.B)
}

// watch checks I7: by answerBound from now, the connection must have made
// progress or be gone.
func (cl *client) watch() {
	c, progress := cl.c, cl.progress
	cl.w.after(answerBound, func() {
		if cl.c == c && cl.progress == progress && len(cl.pending) > 0 {
			cl.w.fail("I7: request (key %d) unanswered for %v on a live connection", cl.pending[0].key, answerBound)
		}
	})
}

// onData receives bytes from the node.
func (cl *client) onData(c *conn, p []byte) {
	if c != cl.c {
		return
	}
	cl.in = append(cl.in, p...)
	for cl.c == c && len(cl.in) >= 4 {
		size := int(int32(binary.BigEndian.Uint32(cl.in)))
		if size < 4 || size > 64<<20 {
			cl.w.fail("response frame of %d bytes", size)
			return
		}
		if len(cl.in) < 4+size {
			return
		}
		frame := cl.in[4 : 4+size]
		cl.in = cl.in[4+size:]
		if len(cl.pending) == 0 {
			cl.w.fail("response without a request")
			return
		}
		q := cl.pending[0]
		cl.pending = cl.pending[1:]
		r := wire.NewReader(frame)
		if corr := wire.DecodeResponseHeader(r, q.key, q.version); corr != q.corr {
			cl.w.fail("response out of order: correlation id %d, expected %d", corr, q.corr)
			return
		}
		q.resp.Decode(r, q.version)
		if r.Err() != nil || r.Len() != 0 {
			cl.w.fail("malformed response to key %d version %d: %v, %d bytes left over", q.key, q.version, r.Err(), r.Len())
			return
		}
		cl.progress++
		if refused(q.resp) {
			// The node is not the leader. Whether the request did
			// anything is treated as unknown, which claims less than the
			// truth (it did nothing); then look for the leader.
			cl.w.stats.NotLeader++
			q.done(false)
			cl.hangUp()
			return
		}
		q.done(true)
		if cl.c == c && len(cl.pending) > 0 {
			cl.watch()
		}
	}
}

// onClose is called when the connection dies under the client.
func (cl *client) onClose(c *conn) {
	if c == cl.c {
		cl.lost()
	}
}

// hangUp closes the connection from the client's side.
func (cl *client) hangUp() {
	cl.w.closeByClient(cl.c)
	cl.lost()
}

func (cl *client) lost() {
	pending := cl.pending
	cl.c, cl.in, cl.pending = nil, nil, nil
	cl.progress++
	for _, q := range pending {
		q.done(false)
	}
}

// refused reports whether resp is the answer of a node that is not serving
// as leader: one of the three not-leader codes, wherever the API puts it.
func refused(resp wire.Message) bool {
	is := func(code int16) bool {
		return code == wire.ErrNotLeaderOrFollower || code == wire.ErrNotCoordinator || code == wire.ErrNotController
	}
	switch m := resp.(type) {
	case *wire.CreateTopicsResponse:
		return len(m.Topics) > 0 && is(m.Topics[0].ErrorCode)
	case *wire.ProduceResponse:
		return len(m.Topics) > 0 && len(m.Topics[0].Partitions) > 0 && is(m.Topics[0].Partitions[0].ErrorCode)
	case *wire.FetchResponse:
		return len(m.Topics) > 0 && len(m.Topics[0].Partitions) > 0 && is(m.Topics[0].Partitions[0].ErrorCode)
	case *wire.ListOffsetsResponse:
		return len(m.Topics) > 0 && len(m.Topics[0].Partitions) > 0 && is(m.Topics[0].Partitions[0].ErrorCode)
	case *wire.OffsetCommitResponse:
		return len(m.Topics) > 0 && len(m.Topics[0].Partitions) > 0 && is(m.Topics[0].Partitions[0].ErrorCode)
	case *wire.OffsetFetchResponse:
		return is(m.ErrorCode) || (len(m.Topics) > 0 && len(m.Topics[0].Partitions) > 0 && is(m.Topics[0].Partitions[0].ErrorCode))
	case *wire.JoinGroupResponse:
		return is(m.ErrorCode)
	case *wire.SyncGroupResponse:
		return is(m.ErrorCode)
	case *wire.HeartbeatResponse:
		return is(m.ErrorCode)
	case *wire.LeaveGroupResponse:
		return is(m.ErrorCode)
	}
	return false
}

// check reports a model violation, if any.
func (cl *client) check(err error) {
	if err != nil {
		cl.w.fail("%v", err)
	}
}

// --- behaviours ------------------------------------------------------------

// randomTP picks a partition: usually a real one, sometimes one that cannot
// exist.
func (cl *client) randomTP() TP {
	if cl.rng.Chance(0.05) {
		return TP{ghostTopic, 0}
	}
	t := catalogue[cl.rng.Intn(len(catalogue))]
	return TP{t.name, int32(cl.rng.Intn(int(t.parts) + 1))} // one past the end sometimes
}

// distinctTPs picks up to n different partitions.
func (cl *client) distinctTPs(n int) []TP {
	var out []TP
	for range n {
		tp := cl.randomTP()
		dup := false
		for _, o := range out {
			dup = dup || o == tp
		}
		if !dup {
			out = append(out, tp)
		}
	}
	return out
}

// misc issues a request that changes nothing: ApiVersions or Metadata.
func (cl *client) misc() {
	m := cl.w.model
	if cl.rng.Chance(0.5) {
		v := int16(cl.rng.Intn(7)) // 5 and 6 are beyond what the broker speaks
		resp := &wire.APIVersionsResponse{}
		cl.request(wire.KeyAPIVersions, v, &wire.APIVersionsRequest{ClientSoftwareName: "sim", ClientSoftwareVersion: "1"}, resp, func(ok bool) {
			want := wire.ErrNone
			if !wire.Supported(wire.KeyAPIVersions, v) {
				want = wire.ErrUnsupportedVersion
			}
			if ok && (resp.ErrorCode != want || len(resp.APIs) != len(wire.APIs)) {
				cl.w.fail("ApiVersions v%d: error %d with %d APIs", v, resp.ErrorCode, len(resp.APIs))
			}
		})
		return
	}
	tp := cl.randomTP()
	known, epoch, asked := m.Known(TP{tp.Topic, 0}), m.Epoch(), cl.c.inc.node.id
	req, resp := &wire.MetadataRequest{Topics: []string{tp.Topic}, AllTopics: cl.rng.Chance(0.3)}, &wire.MetadataResponse{}
	cl.request(wire.KeyMetadata, cl.version(wire.KeyMetadata), req, resp, func(ok bool) {
		if !ok || m.stale(epoch) {
			return
		}
		found := false
		for _, t := range resp.Topics {
			if t.Name != tp.Topic || t.ErrorCode != wire.ErrNone {
				continue
			}
			found = true
			if !m.possible(TP{t.Name, int32(len(t.Partitions)) - 1}) || m.possible(TP{t.Name, int32(len(t.Partitions))}) {
				cl.w.fail("I4: metadata reports topic %q with %d partitions", t.Name, len(t.Partitions))
			}
		}
		// Any node answers Metadata, from what it has applied. Only the
		// leader, which names itself controller, is sure to have applied
		// everything that was acknowledged; a follower may be behind.
		if known && !found && resp.ControllerID == asked {
			cl.w.fail("I1: metadata from the leader does not list topic %q, which was created", tp.Topic)
		}
	})
}

// createTopic asks for one of the catalogue's topics, or for an invalid one.
func (cl *client) createTopic() {
	m := cl.w.model
	t := catalogue[cl.rng.Intn(len(catalogue))]
	name, parts, v := t.name, t.parts, cl.version(wire.KeyCreateTopics)
	valid := true
	switch {
	case cl.rng.Chance(0.1):
		name, valid = "not a valid name!", false
	case parts == 1 && v >= 4 && cl.rng.Chance(0.5):
		parts = -1 // "broker default", which is 1
	}
	if valid {
		m.TopicAttempted(name, t.parts)
	}
	epoch := m.Epoch()
	req := &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: name, NumPartitions: parts, ReplicationFactor: int16(len(cl.w.nodes))}}, TimeoutMs: 1000}
	resp := &wire.CreateTopicsResponse{}
	cl.request(wire.KeyCreateTopics, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Topics) != 1 || resp.Topics[0].Name != name {
			cl.w.fail("CreateTopics response does not match the request")
			return
		}
		switch code := resp.Topics[0].ErrorCode; {
		case !valid && code != wire.ErrInvalidTopic:
			cl.w.fail("invalid topic name answered with code %d", code)
		case !valid:
		case code == wire.ErrNone || code == wire.ErrTopicAlreadyExists:
			m.TopicCreated(name, epoch)
		default:
			cl.w.fail("creating topic %q failed with code %d", name, code)
		}
	})
}

// sentPart is what a producer expects of one partition of a request.
type sentPart struct {
	tp       TP
	first    uint64 // id of its first record
	n        int    // records, over all its batches
	mustFail bool   // the broker has to reject it
	mustOK   bool   // the broker has to accept it
}

// produce sends records, some of them deliberately unacceptable.
func (cl *client) produce() {
	w, m := cl.w, cl.w.model
	switch {
	case cl.rng.Chance(0.1):
		cl.createTopic()
		return
	case cl.rng.Chance(0.03):
		cl.misc()
		return
	}
	req := &wire.ProduceRequest{Acks: []int16{-1, -1, 1, 1, 1, 0, 2}[cl.rng.Intn(7)], TimeoutMs: 1000}
	badAcks := req.Acks == 2
	var parts []sentPart
	for _, tp := range cl.distinctTPs(1 + cl.rng.Intn(3)) {
		sp := sentPart{tp: tp, mustFail: badAcks || !inCatalogue(tp)}
		var records []byte
		for range 1 + cl.rng.Intn(3) {
			n := 1 + cl.rng.Intn(5)
			first := m.Send(tp, n)
			if sp.n == 0 {
				sp.first = first
			}
			sp.n += n
			recs := make([]wire.Record, n)
			for i := range recs {
				recs[i] = wire.Record{Value: Value(first + uint64(i)), Timestamp: int64(w.now / 1e6)}
			}
			switch b := cl.rng.Intn(40); b {
			case 0: // too large for the broker
				recs[0].Key = make([]byte, maxBatchBytes)
				sp.mustFail = true
			case 1: // damaged in transit: anywhere from the magic byte on
				batch := wire.EncodeBatch(recs)
				batch[16+cl.rng.Intn(len(batch)-16)] ^= 1 << cl.rng.Intn(8)
				records = append(records, batch...)
				sp.mustFail = true
				continue
			}
			records = append(records, wire.EncodeBatch(recs)...)
		}
		sp.mustOK = !sp.mustFail && m.Known(tp)
		parts = append(parts, sp)
		// One topic entry per partition: the encoding allows it, and it
		// keeps the response easy to match up.
		req.Topics = append(req.Topics, wire.ProduceTopic{Name: tp.Topic, Partitions: []wire.ProducePartition{{Index: tp.Part, Records: records}}})
	}
	if req.Acks == 0 {
		for _, sp := range parts {
			m.Unknown(sp.first, sp.n)
		}
		cl.request(wire.KeyProduce, cl.version(wire.KeyProduce), req, nil, nil)
		return
	}
	epoch := m.Epoch()
	resp := &wire.ProduceResponse{}
	cl.request(wire.KeyProduce, cl.version(wire.KeyProduce), req, resp, func(ok bool) {
		if ok && len(resp.Topics) != len(parts) {
			w.fail("produce response has %d topics for %d sent", len(resp.Topics), len(parts))
			return
		}
		for i, sp := range parts {
			if !ok {
				m.Unknown(sp.first, sp.n)
				continue
			}
			t := resp.Topics[i]
			if t.Name != sp.tp.Topic || len(t.Partitions) != 1 || t.Partitions[0].Index != sp.tp.Part {
				w.fail("produce response does not match the request at entry %d", i)
				return
			}
			switch p := t.Partitions[0]; {
			case p.ErrorCode == wire.ErrNone && sp.mustFail:
				w.fail("I5: the broker accepted a write to %v that it had to reject", sp.tp)
			case p.ErrorCode == wire.ErrNone:
				cl.check(m.Acked(sp.first, sp.n, p.BaseOffset, epoch))
				if !m.stale(epoch) {
					w.stats.RecordsAcked += sp.n
				}
			case sp.mustOK && !m.stale(epoch):
				w.fail("the broker rejected a valid write to %v with code %d", sp.tp, p.ErrorCode)
			default:
				cl.check(m.Rejected(sp.first, sp.n))
			}
		}
	})
}

// consume fetches, with every option chosen at random.
func (cl *client) consume() {
	m := cl.w.model
	switch {
	case cl.rng.Chance(0.03):
		cl.misc()
		return
	case cl.rng.Chance(0.1):
		cl.listOffsets()
		return
	}
	req := &wire.FetchRequest{
		MaxWaitMs: []int32{0, 0, 5, 50, 500}[cl.rng.Intn(5)],
		MinBytes:  []int32{0, 1, 1, 1000}[cl.rng.Intn(4)],
		MaxBytes:  []int32{1, 300, 1 << 20}[cl.rng.Intn(3)],
	}
	var asked []Fetch
	for _, tp := range cl.distinctTPs(1 + cl.rng.Intn(3)) {
		f := Fetch{TP: tp, Offset: cl.cursors[tp], Floor: m.Floor(tp), Epoch: m.Epoch(), Known: m.Known(tp), Start: m.Start(tp)}
		switch cl.rng.Intn(10) {
		case 0:
			f.Offset = int64(cl.rng.Intn(int(f.Floor) + 3)) // anywhere, perhaps just past the end
		case 1:
			f.Offset = []int64{-1, f.Floor + 1000}[cl.rng.Intn(2)]
		}
		asked = append(asked, f)
		req.Topics = append(req.Topics, wire.FetchTopic{Name: tp.Topic, Partitions: []wire.FetchPartition{
			{Index: tp.Part, Offset: f.Offset, MaxBytes: []int32{1, 300, 1 << 20}[cl.rng.Intn(3)]},
		}})
	}
	cl.fetch(req, asked, func(f Fetch, next int64) {
		switch {
		case next >= 0:
			cl.cursors[f.TP] = next
		case f.Err == wire.ErrOffsetOutOfRange || m.stale(f.Epoch):
			cl.cursors[f.TP] = max(f.LogStart, 0) // the log may have been cut: start over
		}
	})
}

// fetch sends req, whose partitions are described by asked, checks the
// response against the model, and reports each partition's next offset.
func (cl *client) fetch(req *wire.FetchRequest, asked []Fetch, each func(f Fetch, next int64)) {
	w := cl.w
	resp := &wire.FetchResponse{}
	version := max(cl.version(wire.KeyFetch), cl.fetchMin)
	cl.request(wire.KeyFetch, version, req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Topics) != len(asked) {
			w.fail("fetch response has %d topics for %d asked", len(resp.Topics), len(asked))
			return
		}
		bytesSoFar := 0
		for i, f := range asked {
			t := resp.Topics[i]
			if t.Name != f.TP.Topic || len(t.Partitions) != 1 || t.Partitions[0].Index != f.TP.Part {
				w.fail("fetch response does not match the request at entry %d", i)
				return
			}
			p := t.Partitions[0]
			f.Err, f.HW, f.Records, f.MustProgress, f.LogStart = p.ErrorCode, p.HighWatermark, p.Records, bytesSoFar == 0, -1
			if version >= 5 {
				f.LogStart = p.LogStartOffset
			}
			bytesSoFar += len(p.Records)
			next, err := w.model.Fetched(f)
			if err != nil {
				w.fail("%v", err)
				return
			}
			w.stats.FetchesChecked++
			if next >= 0 {
				w.stats.RecordsRead += int(next - max(f.Offset, 0))
			}
			each(f, next)
		}
	})
}

// listOffsets asks for the end of one partition.
func (cl *client) listOffsets() {
	m := cl.w.model
	tp := cl.randomTP()
	floor, epoch, known := m.Floor(tp), m.Epoch(), m.Known(tp)
	req := &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: tp.Topic, Partitions: []wire.ListOffsetsPartition{
		{Index: tp.Part, Timestamp: wire.TimestampLatest},
	}}}}
	resp := &wire.ListOffsetsResponse{}
	cl.request(wire.KeyListOffsets, cl.version(wire.KeyListOffsets), req, resp, func(ok bool) {
		if !ok || m.stale(epoch) {
			return
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			cl.w.fail("ListOffsets response does not match the request")
			return
		}
		switch p := resp.Topics[0].Partitions[0]; {
		case p.ErrorCode == wire.ErrUnknownTopicOrPartition && !known:
		case p.ErrorCode != wire.ErrNone:
			cl.w.fail("ListOffsets of %v failed with code %d", tp, p.ErrorCode)
		case !m.possible(tp):
			cl.w.fail("I4: ListOffsets of %v succeeded, but nobody created it", tp)
		case p.Offset < floor:
			cl.w.fail("I1/I3: %v log end %d is below %d, which was acknowledged or seen before the request", tp, p.Offset, floor)
		}
	})
}

// vandalise sends bytes that are not a request. The node may do anything
// to this connection, and nothing to any other.
func (cl *client) vandalise() {
	junk := make([]byte, 4+cl.rng.Intn(64))
	for i := range junk {
		junk[i] = byte(cl.rng.Uint64())
	}
	switch cl.rng.Intn(4) {
	case 0: // a frame of the right length holding noise
		binary.BigEndian.PutUint32(junk, uint32(len(junk)-4))
	case 1: // a plausible header, then noise
		binary.BigEndian.PutUint32(junk, uint32(len(junk)-4))
		if len(junk) >= 8 {
			a := wire.APIs[cl.rng.Intn(len(wire.APIs))]
			binary.BigEndian.PutUint16(junk[4:], uint16(a.Key))
			binary.BigEndian.PutUint16(junk[6:], uint16(a.Max))
		}
	case 2: // a frame that claims to be enormous, or negative
		binary.BigEndian.PutUint32(junk, []uint32{0x7fffffff, 0xffffffff, 1 << 30}[cl.rng.Intn(3)])
	}
	cl.w.send(cl.c, junk)
	if cl.rng.Chance(0.3) {
		cl.hangUp()
	}
}

func (tp TP) String() string { return fmt.Sprintf("%s/%d", tp.Topic, tp.Part) }
