package broker

import (
	"slices"
	"sort"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// metadata describes the cluster and the requested topics. Every node
// answers it, from its own applied state, which on a follower may be a
// little behind. Every node holds every partition, and the leader of the
// cluster leads them all. A topic the client may not describe is refused
// by name, and left out of an all-topics answer: its existence is not
// disclosed (KafkaApis.handleTopicMetadataRequest).
func (n *Node) metadata(c *conn, req *wire.MetadataRequest) *wire.MetadataResponse {
	leader, known := n.leader()
	resp := &wire.MetadataResponse{ClusterID: n.cfg.ClusterID, ControllerID: leader.ID}
	var all []int32
	for _, p := range n.members() {
		resp.Brokers = append(resp.Brokers, wire.Broker{NodeID: p.ID, Host: p.Host, Port: p.Port, Rack: p.Rack})
		all = append(all, p.ID)
	}
	addTopic := func(t *topic) {
		tm := wire.TopicMetadata{Name: t.name, TopicID: t.id}
		for i := range t.parts {
			pm := wire.PartitionMetadata{Index: int32(i), Leader: leader.ID, Replicas: all, ISR: all}
			if !known {
				pm.ErrorCode = wire.ErrLeaderNotAvailable
			}
			tm.Partitions = append(tm.Partitions, pm)
		}
		resp.Topics = append(resp.Topics, tm)
	}
	if req.AllTopics {
		for _, name := range n.st.names {
			if n.may(c, opDescribe, resTopic, name) {
				addTopic(n.st.topics[name])
			}
		}
		return resp
	}
	count := max(len(req.Topics), len(req.TopicIDs))
	for i := range count {
		var name string
		if i < len(req.Topics) {
			name = req.Topics[i]
		}
		var id wire.UUID
		if i < len(req.TopicIDs) {
			id = req.TopicIDs[i]
		}
		if id != (wire.UUID{}) {
			realName := n.st.byID[id]
			switch {
			case realName == "":
				resp.Topics = append(resp.Topics, wire.TopicMetadata{ErrorCode: wire.ErrUnknownTopicID, Name: name, TopicID: id})
			case !n.may(c, opDescribe, resTopic, realName):
				resp.Topics = append(resp.Topics, wire.TopicMetadata{ErrorCode: wire.ErrTopicAuthorizationFailed, Name: name, TopicID: id})
			default:
				addTopic(n.st.topics[realName])
			}
			continue
		}
		if !n.may(c, opDescribe, resTopic, name) {
			resp.Topics = append(resp.Topics, wire.TopicMetadata{ErrorCode: wire.ErrTopicAuthorizationFailed, Name: name})
			continue
		}
		t := n.st.topics[name]
		if t == nil {
			resp.Topics = append(resp.Topics, wire.TopicMetadata{ErrorCode: wire.ErrUnknownTopicOrPartition, Name: name})
			continue
		}
		addTopic(t)
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
	rf := int16(len(n.raft.Members()))
	seen := map[string]bool{}
	for i, t := range req.Topics {
		res := &resp.Topics[i]
		res.Name, res.NumPartitions, res.ReplicationFactor = t.Name, -1, -1
		parts := t.NumPartitions
		if parts == -1 {
			parts = 1 // "broker default"
		}
		// The configurations Samsa keeps; others are accepted and ignored,
		// as before C2. A null value asks for the default.
		configs, configErr := map[string]string{}, ""
		for _, kv := range t.Configs {
			if kv.Null || !slices.Contains(topicConfigKeys, kv.Name) {
				continue
			}
			if msg := checkTopicConfig(kv.Name, kv.Value); msg != "" && configErr == "" {
				configErr = msg
			}
			configs[kv.Name] = kv.Value
		}
		switch {
		case !validTopicName(t.Name):
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidTopic, "topic names are 1-249 characters of [a-zA-Z0-9._-]"
		case seen[t.Name]:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "topic appears twice in the request"
		case !n.may(c, opCreate, resCluster, clusterName) && !n.may(c, opCreate, resTopic, t.Name):
			res.ErrorCode, res.ErrorMessage = wire.ErrTopicAuthorizationFailed, "Authorization failed."
		case t.HasAssignments:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidRequest, "manual replica assignment is not supported"
		case parts < 1 || parts > n.cfg.MaxPartitions:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidPartitions, "partition count out of range"
		case t.ReplicationFactor != -1 && t.ReplicationFactor != rf:
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidReplicationFactor, "every node holds every partition: the replication factor is the cluster size"
		case n.st.topics[t.Name] != nil:
			res.ErrorCode, res.ErrorMessage = wire.ErrTopicAlreadyExists, "topic already exists"
		case configErr != "":
			res.ErrorCode, res.ErrorMessage = wire.ErrInvalidConfig, configErr
		case len(configs) > 0 && n.levelRefused(configsLevel(configs)) != "":
			res.ErrorCode, res.ErrorMessage = wire.ErrUnsupportedVersion, n.levelRefused(configsLevel(configs))
		case h.Version >= 7 && n.levelRefused(4) != "":
			res.ErrorCode, res.ErrorMessage = wire.ErrUnsupportedVersion, n.levelRefused(4)
		}
		seen[t.Name] = true
		if res.ErrorCode != wire.ErrNone {
			continue
		}
		if req.ValidateOnly {
			res.NumPartitions, res.ReplicationFactor = parts, rf
			res.Configs = n.createdTopicConfigs(configs)
			continue
		}
		var id wire.UUID
		if n.st.level >= 4 {
			id = n.newUUID()
		}
		pending++
		n.propose(encodeCreateTopic(t.Name, parts, configs, id), func(r result) {
			res.ErrorCode = r.errCode
			switch r.errCode {
			case wire.ErrTopicAlreadyExists:
				res.ErrorMessage = "topic already exists"
			case wire.ErrNone:
				res.TopicID = r.topicID
				res.NumPartitions, res.ReplicationFactor = parts, rf
				res.Configs = n.createdTopicConfigs(configs)
			}
			done()
		})
	}
	done()
}

// wroteAny reports whether a produce response acknowledges any write.
func wroteAny(resp *wire.ProduceResponse) bool {
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode == wire.ErrNone {
				return true
			}
		}
	}
	return false
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
		switch {
		case req.Acks == 0:
			n.finish(c)
		case wroteAny(resp) && n.faults.inject(FaultLostAck):
			n.log.Info("fault injected", "kind", FaultLostAck, "fault", n.faults.count)
			if !c.closed { // not drop: Dropped counts bad input, not faults
				c.closed = true
				n.net.Close(c.id)
			}
		default:
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
			case !n.may(c, opWrite, resTopic, t.Name):
				res.ErrorCode = wire.ErrTopicAuthorizationFailed
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
				if tp := n.st.topics[t.Name]; tp != nil && compacts(tp) {
					if code := checkCompactable(rest[:info.Size]); code != wire.ErrNone {
						res.ErrorCode = code
						break
					}
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

func (n *Node) listOffsets(c *conn, req *wire.ListOffsetsRequest) *wire.ListOffsetsResponse {
	resp := &wire.ListOffsetsResponse{}
	for _, t := range req.Topics {
		tr := wire.ListOffsetsTopicResponse{Name: t.Name}
		for _, q := range t.Partitions {
			pr := wire.ListOffsetsPartitionResponse{Index: q.Index, Timestamp: -1, Offset: -1}
			switch p := n.st.partition(t.Name, q.Index); {
			case !n.may(c, opDescribe, resTopic, t.Name):
				pr.ErrorCode = wire.ErrTopicAuthorizationFailed
			case p == nil:
				pr.ErrorCode = wire.ErrUnknownTopicOrPartition
			case q.Timestamp == wire.TimestampLatest:
				if req.IsolationLevel == 1 && !n.st.readUncommitted {
					pr.Offset = p.lso()
				} else {
					pr.Offset = p.next
				}
			case q.Timestamp == wire.TimestampEarliest:
				pr.Offset = p.start
			case q.Timestamp < 0:
				pr.ErrorCode = wire.ErrInvalidRequest // other special values are not supported
			default:
				// shortcut: answers with the first batch that can contain the
				// timestamp, not the exact record (the broker does not decode
				// records). A consumer may see a few earlier records. Upgrade
				// if exact seek-by-time matters.
				for _, b := range p.batches {
					if b.maxTimestamp >= q.Timestamp {
						pr.Offset, pr.Timestamp = max(b.base, p.start), b.maxTimestamp
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
	slow  bool // FaultSlowFetch: not woken by data, only by its deadline
}

func (n *Node) fetch(c *conn, h wire.RequestHeader, req *wire.FetchRequest) {
	resp, size, failed := n.buildFetch(c, h.Version, req)
	if !n.serving {
		n.stats.followerFetchBytes += int64(size)
		n.log.Debug("fetch answered by a follower", "bytes", size, "failed", failed, "rack", req.RackID)
	} else if req.RackID != "" && h.Version >= 11 && !failed && n.preferReplica(req.RackID, resp) {
		n.respond(c, h, resp) // read from that node next: nothing to wait for here
		return
	}
	// Faults that change what a consumer sees of its fetches (faults.go).
	// Each enabled kind draws once per fetch.
	// A kind draws only for a request it can act on.
	outOfRange := !failed && n.faults.inject(FaultOutOfRange)
	empty := size > 0 && n.faults.inject(FaultEmptyFetch)
	slow := !failed && size > 0 && req.MaxWaitMs > 0 && n.fatal == nil && n.faults.inject(FaultSlowFetch)
	switch {
	case outOfRange:
		n.log.Info("fault injected", "kind", FaultOutOfRange, "fault", n.faults.count)
		for _, t := range resp.Topics {
			for i := range t.Partitions {
				if p := &t.Partitions[i]; p.ErrorCode == wire.ErrNone {
					p.ErrorCode, p.Records = wire.ErrOffsetOutOfRange, nil
				}
			}
		}
		n.respond(c, h, resp)
		return
	case empty:
		n.log.Info("fault injected", "kind", FaultEmptyFetch, "fault", n.faults.count)
		for _, t := range resp.Topics {
			for i := range t.Partitions {
				t.Partitions[i].Records = nil
			}
		}
		n.respond(c, h, resp)
		return
	case slow:
		n.log.Info("fault injected", "kind", FaultSlowFetch, "fault", n.faults.count)
		f := &parkedFetch{c: c, h: h, req: req, timer: n.nextTimer, slow: true}
		n.nextTimer++
		n.parked = append(n.parked, f)
		n.clock.SetTimer(f.timer, time.Duration(req.MaxWaitMs)*time.Millisecond)
		return
	}
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
	if !n.canRead() && !n.canServeFetch() { // the lease ran out while it waited
		n.respond(f.c, f.h, refusal(f.req))
		return
	}
	resp, _, _ := n.buildFetch(f.c, f.h.Version, f.req)
	n.respond(f.c, f.h, resp)
}

// wakeFetches answers the parked fetches that newly applied data satisfies.
func (n *Node) wakeFetches() {
	for _, f := range append([]*parkedFetch(nil), n.parked...) { // completeFetch edits n.parked
		if f.c.closed || f.slow || !n.hasNewData(f.h.Version, f.req) {
			continue
		}
		if f.req.MinBytes > 1 {
			if _, size, failed := n.buildFetch(f.c, f.h.Version, f.req); !failed && size < int(f.req.MinBytes) {
				continue
			}
		}
		n.completeFetch(f)
	}
}

// hasNewData reports whether any partition of req has records at or past
// the offset asked for, without reading them.
func (n *Node) hasNewData(v int16, req *wire.FetchRequest) bool {
	for _, t := range req.Topics {
		name := t.Name
		if v >= 13 {
			name = n.st.byID[t.TopicID]
		}
		for _, q := range t.Partitions {
			if p := n.st.partition(name, q.Index); p != nil {
				limit := p.next
				if req.IsolationLevel == 1 && !n.st.readUncommitted {
					limit = p.lso()
				}
				if limit > q.Offset {
					return true
				}
			}
		}
	}
	return false
}

// buildFetch reads what req asks for from applied state. It returns the
// response, the number of record bytes in it, and whether any partition
// reported an error (in which case the response should not be delayed).
func (n *Node) buildFetch(c *conn, v int16, req *wire.FetchRequest) (resp *wire.FetchResponse, size int, failed bool) {
	resp = &wire.FetchResponse{}
	budget := n.cfg.MaxFetchBytes
	if req.MaxBytes > 0 && int(req.MaxBytes) < budget {
		budget = int(req.MaxBytes)
	}
	for _, t := range req.Topics {
		name := t.Name
		if v >= 13 {
			name = n.st.byID[t.TopicID]
		}
		tr := wire.FetchTopicResponse{Name: name, TopicID: t.TopicID}
		if tp := n.st.topics[name]; tp != nil {
			tr.TopicID = tp.id
		}
		for _, q := range t.Partitions {
			pr := wire.FetchPartitionResponse{Index: q.Index, HighWatermark: -1}
			p := n.st.partition(name, q.Index)
			switch {
			case v >= 13 && name == "":
				pr.ErrorCode = wire.ErrUnknownTopicID
			case !n.may(c, opRead, resTopic, name):
				pr.ErrorCode = wire.ErrTopicAuthorizationFailed
			case p == nil:
				pr.ErrorCode = wire.ErrUnknownTopicOrPartition
			case q.Offset > p.next && !n.serving && !n.cfg.UnsafeFollowerOutOfRange:
				// A follower that has not applied that far yet: the consumer
				// is not wrong, and must not reset (KIP-392).
				pr.ErrorCode, pr.HighWatermark, pr.LogStartOffset = wire.ErrOffsetNotAvailable, p.next, p.start
			case q.Offset < p.start || q.Offset > p.next:
				pr.ErrorCode, pr.HighWatermark, pr.LogStartOffset = wire.ErrOffsetOutOfRange, p.next, p.start
			default:
				pr.HighWatermark, pr.LogStartOffset = p.next, p.start
				maxOff := p.next
				if req.IsolationLevel == 1 && !n.st.readUncommitted {
					maxOff = p.lso()
				}
				pr.LastStableOffset, pr.HasLastStableOffset = maxOff, true
				if req.IsolationLevel == 1 && !n.st.readUncommitted {
					pr.AbortedTransactions = p.abortedInRange(q.Offset, maxOff)
				}
				// The response's first batch is sent whatever its size, so
				// a consumer can always make progress.
				pr.Records = n.readBatches(p, q.Offset, maxOff, min(int(q.MaxBytes), budget-size), size == 0)
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
func (n *Node) readBatches(p *partition, offset, maxOffset int64, limit int, force bool) []byte {
	// The last batch whose base is <= offset. Offsets are dense, so it
	// contains offset whenever offset < p.next, except that a compacted
	// partition has gaps: then offset may lie after the batch's last record,
	// and the read starts at the next batch.
	i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].base > offset }) - 1
	if offset >= maxOffset {
		return nil
	}
	if i < 0 {
		i = 0 // below the first batch: a gap, or the start of a compacted partition
		if len(p.batches) == 0 {
			return nil
		}
	}
	var out []byte
	for ; i < len(p.batches); i++ {
		ref := p.batches[i]
		if ref.base >= maxOffset {
			break
		}
		batch, ok := n.readBatch(ref)
		if !ok {
			return out
		}
		if len(out) == 0 && ref.base+int64(wire.BatchLastOffsetDelta(batch)) < offset {
			continue // the whole batch lies before offset: offset is in a gap
		}
		if len(out)+len(batch) > limit && (len(out) > 0 || !force) {
			break
		}
		start := len(out)
		out = append(out, batch...)
		wire.SetBatchBaseOffset(out[start:], ref.base)
	}
	return out
}
