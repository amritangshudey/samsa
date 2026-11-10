package broker

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// metadata describes the cluster (this one node) and the requested topics.
func (n *Node) metadata(req *wire.MetadataRequest) *wire.MetadataResponse {
	resp := &wire.MetadataResponse{
		Brokers:      []wire.Broker{{NodeID: n.cfg.NodeID, Host: n.cfg.Host, Port: n.cfg.Port}},
		ClusterID:    n.cfg.ClusterID,
		ControllerID: n.cfg.NodeID,
	}
	names := req.Topics
	if req.AllTopics {
		names = n.st.names
	}
	self := []int32{n.cfg.NodeID}
	for _, name := range names {
		t := n.st.topics[name]
		if t == nil {
			resp.Topics = append(resp.Topics, wire.TopicMetadata{ErrorCode: wire.ErrUnknownTopicOrPartition, Name: name})
			continue
		}
		tm := wire.TopicMetadata{Name: name}
		for i := range t.parts {
			tm.Partitions = append(tm.Partitions, wire.PartitionMetadata{
				Index: int32(i), Leader: n.cfg.NodeID, Replicas: self, ISR: self,
			})
		}
		resp.Topics = append(resp.Topics, tm)
	}
	return resp
}

// validTopicName applies Kafka's rules for topic names: 1 to 249 characters
// from [a-zA-Z0-9._-], and not "." or "..".
func validTopicName(name string) bool {
	if name == "" || len(name) > 249 || name == "." || name == ".." {
		return false
	}
	for _, ch := range []byte(name) {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '.', ch == '_', ch == '-':
		default:
			return false
		}
	}
	return true
}

func (n *Node) createTopics(c *conn, h wire.RequestHeader, req *wire.CreateTopicsRequest) {
	resp := &wire.CreateTopicsResponse{Topics: make([]wire.CreateTopicResult, len(req.Topics))}
	// pending starts at 1 for this loop itself, so that a proposal completing
	// at once cannot send the response while later topics are still unseen.
	pending := 1
	done := func() {
		if pending--; pending == 0 {
			n.respond(c, h, resp)
		}
	}
	seen := map[string]bool{}
	for i, t := range req.Topics {
		res := &resp.Topics[i]
		res.Name = t.Name
		parts := t.NumPartitions
		if parts == -1 {
			parts = 1 // "broker default"
		}
		switch {
		case !validTopicName(t.Name):
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidTopic, "topic names are 1-249 characters of [a-zA-Z0-9._-]"
		case seen[t.Name]:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "topic appears twice in the request"
		case t.HasAssignments:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "manual replica assignment is not supported"
		case parts < 1 || parts > n.cfg.MaxPartitions:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidPartitions, "partition count out of range"
		case t.ReplicationFactor != -1 && t.ReplicationFactor != 1:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidReplicationFactor, "this is a single node: replication factor must be 1"
		case n.st.topics[t.Name] != nil:
			res.ErrorCode, res.ErrorMessage = wire.ErrTopicAlreadyExists, "topic already exists"
		}
		seen[t.Name] = true
		if res.ErrorCode != wire.ErrNone || req.ValidateOnly {
			continue
		}
		pending++
		n.propose(encodeCreateTopic(t.Name, parts), func(r result) {
			res.ErrorCode = r.errCode
			if r.errCode == wire.ErrTopicAlreadyExists {
				res.ErrorMessage = "topic already exists"
			}
			done()
		})
	}
	done()
}

func (n *Node) produce(c *conn, h wire.RequestHeader, req *wire.ProduceRequest) {
	resp := &wire.ProduceResponse{Topics: make([]wire.ProduceTopicResponse, len(req.Topics))}
	// acks 0 means the client wants no response at all. -1 and 1 are both
	// answered after commit: offsets are assigned at apply, so there is
	// nothing earlier to acknowledge (ADR 0005).
	// pending starts at 1 for this loop itself; see createTopics.
	pending := 1
	done := func() {
		if pending--; pending > 0 {
			return
		}
		if req.Acks == 0 {
			n.finish(c)
		} else {
			n.respond(c, h, resp)
		}
	}
	for i, t := range req.Topics {
		resp.Topics[i] = wire.ProduceTopicResponse{Name: t.Name, Partitions: make([]wire.ProducePartitionResponse, len(t.Partitions))}
		for j, p := range t.Partitions {
			res := &resp.Topics[i].Partitions[j]
			res.Index, res.BaseOffset = p.Index, -1

			// Validate every batch of the partition before proposing any,
			// so a partition's records are accepted or rejected together.
			var sizes []int
			switch {
			case req.Acks < -1 || req.Acks > 1:
				res.ErrorCode = wire.ErrInvalidRequiredAcks
			case n.st.partition(t.Name, p.Index) == nil:
				res.ErrorCode = wire.ErrUnknownTopicOrPartition
			case len(p.Records) == 0:
				res.ErrorCode = wire.ErrInvalidRequest
			}
			for rest := p.Records; res.ErrorCode == wire.ErrNone && len(rest) > 0; {
				info, code := wire.ParseBatch(rest, n.cfg.MaxBatchBytes)
				if code != wire.ErrNone {
					// info describes nothing on error; in particular its
					// size must not be used to step forward.
					res.ErrorCode = code
					break
				}
				sizes = append(sizes, info.Size)
				rest = rest[info.Size:]
			}
			if res.ErrorCode != wire.ErrNone {
				continue
			}
			rest := p.Records
			for k, size := range sizes {
				pending++
				first := k == 0
				n.propose(encodeAppend(t.Name, p.Index, rest[:size]), func(r result) {
					if r.errCode != wire.ErrNone {
						res.ErrorCode = r.errCode
					} else if first {
						res.BaseOffset = r.baseOffset
					}
					done()
				})
				rest = rest[size:]
			}
		}
	}
	done()
}

func (n *Node) listOffsets(req *wire.ListOffsetsRequest) *wire.ListOffsetsResponse {
	resp := &wire.ListOffsetsResponse{}
	for _, t := range req.Topics {
		tr := wire.ListOffsetsTopicResponse{Name: t.Name}
		for _, q := range t.Partitions {
			pr := wire.ListOffsetsPartitionResponse{Index: q.Index, Timestamp: -1, Offset: -1}
			switch p := n.st.partition(t.Name, q.Index); {
			case p == nil:
				pr.ErrorCode = wire.ErrUnknownTopicOrPartition
			case q.Timestamp == wire.TimestampLatest:
				pr.Offset = p.next
			case q.Timestamp == wire.TimestampEarliest:
				pr.Offset = 0
			case q.Timestamp < 0:
				pr.ErrorCode = wire.ErrInvalidRequest // other special values are not supported
			default:
				// shortcut: answers with the first batch that can contain the
				// timestamp, not the exact record (the broker does not decode
				// records). A consumer may see a few earlier records. Upgrade
				// if exact seek-by-time matters.
				for _, b := range p.batches {
					if b.maxTimestamp >= q.Timestamp {
						pr.Offset, pr.Timestamp = b.base, b.maxTimestamp
						break
					}
				}
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

// parkedFetch is a fetch waiting for data or for its deadline. It is owned
// by Node.parked and removed exactly once, by whichever comes first:
// enough data, the timer, or the connection closing.
type parkedFetch struct {
	c     *conn
	h     wire.RequestHeader
	req   *wire.FetchRequest
	timer sys.TimerID
}

func (n *Node) fetch(c *conn, h wire.RequestHeader, req *wire.FetchRequest) {
	resp, size, failed := n.buildFetch(req)
	if failed || size >= int(req.MinBytes) || req.MaxWaitMs <= 0 || n.fatal != nil {
		n.respond(c, h, resp)
		return
	}
	f := &parkedFetch{c: c, h: h, req: req, timer: n.nextTimer}
	n.nextTimer++
	n.parked = append(n.parked, f)
	n.clock.SetTimer(f.timer, time.Duration(req.MaxWaitMs)*time.Millisecond)
}

// completeFetch answers a parked fetch with whatever is available now.
func (n *Node) completeFetch(f *parkedFetch) {
	for i, g := range n.parked {
		if g == f {
			n.parked = append(n.parked[:i], n.parked[i+1:]...)
			break
		}
	}
	n.clock.CancelTimer(f.timer)
	resp, _, _ := n.buildFetch(f.req)
	n.respond(f.c, f.h, resp)
}

// wakeFetches answers the parked fetches that newly applied data satisfies.
func (n *Node) wakeFetches() {
	for _, f := range append([]*parkedFetch(nil), n.parked...) { // completeFetch edits n.parked
		if f.c.closed || !n.hasNewData(f.req) {
			continue
		}
		if f.req.MinBytes > 1 {
			if _, size, failed := n.buildFetch(f.req); !failed && size < int(f.req.MinBytes) {
				continue
			}
		}
		n.completeFetch(f)
	}
}

// hasNewData reports whether any partition of req has records at or past
// the offset asked for, without reading them.
func (n *Node) hasNewData(req *wire.FetchRequest) bool {
	for _, t := range req.Topics {
		for _, q := range t.Partitions {
			if p := n.st.partition(t.Name, q.Index); p != nil && p.next > q.Offset {
				return true
			}
		}
	}
	return false
}

// buildFetch reads what req asks for from applied state. It returns the
// response, the number of record bytes in it, and whether any partition
// reported an error (in which case the response should not be delayed).
func (n *Node) buildFetch(req *wire.FetchRequest) (resp *wire.FetchResponse, size int, failed bool) {
	resp = &wire.FetchResponse{}
	budget := n.cfg.MaxFetchBytes
	if req.MaxBytes > 0 && int(req.MaxBytes) < budget {
		budget = int(req.MaxBytes)
	}
	for _, t := range req.Topics {
		tr := wire.FetchTopicResponse{Name: t.Name}
		for _, q := range t.Partitions {
			pr := wire.FetchPartitionResponse{Index: q.Index, HighWatermark: -1}
			p := n.st.partition(t.Name, q.Index)
			switch {
			case p == nil:
				pr.ErrorCode = wire.ErrUnknownTopicOrPartition
			case q.Offset < 0 || q.Offset > p.next:
				pr.ErrorCode, pr.HighWatermark = wire.ErrOffsetOutOfRange, p.next
			default:
				pr.HighWatermark = p.next
				// The response's first batch is sent whatever its size, so
				// a consumer can always make progress.
				pr.Records = n.readBatches(p, q.Offset, min(int(q.MaxBytes), budget-size), size == 0)
				size += len(pr.Records)
			}
			failed = failed || pr.ErrorCode != wire.ErrNone
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, size, failed
}

// readBatches returns whole record batches of p, starting with the one that
// contains offset, up to limit bytes; if force is set the first batch is
// returned even if it is larger. Each batch gets its assigned base offset.
func (n *Node) readBatches(p *partition, offset int64, limit int, force bool) []byte {
	// The last batch whose base is <= offset. Offsets are dense, so it
	// contains offset whenever offset < p.next.
	i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].base > offset }) - 1
	if offset >= p.next || i < 0 {
		return nil
	}
	var out []byte
	for ; i < len(p.batches); i++ {
		ref := p.batches[i]
		got, err := n.cmds.Read(ref.logIndex, 0)
		if err != nil || len(got) == 0 {
			// A read failure of committed data: the log is corrupt or
			// poisoned. Never guess; stop the node.
			if err == nil {
				err = errors.New("command log returned nothing")
			}
			if n.fatal == nil {
				n.fatal = fmt.Errorf("read committed batch at log index %d: %w", ref.logIndex, err)
			}
			return out
		}
		batch := appendBatch(got[0].Payload)
		if len(out)+len(batch) > limit && (len(out) > 0 || !force) {
			break
		}
		start := len(out)
		out = append(out, batch...)
		wire.SetBatchBaseOffset(out[start:], ref.base)
	}
	return out
}
