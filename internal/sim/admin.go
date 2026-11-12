package sim

import "github.com/amritangshudey/samsa/internal/wire"

// The admin client of docs/design/b4-operations.md, section 6: it deletes
// records, grows topics, and deletes doomedTopic, which only it ever
// creates. No create may be applied after the delete, or the topic would
// rightly come back. So the delete is sent only after a create was
// answered, and after every connection that carried an unanswered create
// has been closed at the node: a closed connection dispatches nothing more,
// and a create appended before the delete is applied before it. From the
// delete's acknowledgement on, nothing may find the topic (D1). Deleting a topic and
// creating it again under its name is left to the broker and integration
// tests: the model would need to tell the two topics apart.

const doomedTopic = "doomed"

type adminPhase uint8

const (
	adminCreate  adminPhase = iota // create doomedTopic
	adminProduce                   // write a few batches to it
	adminDelete                    // delete it, until acknowledged
	adminGone                      // read it: it must not be there
)

// adminState is the admin client's progress through doomedTopic's life.
type adminState struct {
	phase   adminPhase
	busy    bool // a request about doomedTopic is in flight
	batches int  // acknowledged so far
	// unsettled is a connection that carried a create and died before
	// its answer; nothing more happens until the node has closed it.
	unsettled *conn
}

func (cl *client) admin() {
	if cl.adm == nil {
		cl.adm = &adminState{}
	}
	switch r := cl.rng.Intn(10); {
	case r < 2:
		cl.deleteRecords()
	case r < 3:
		cl.createPartitions()
	default:
		cl.doomed()
	}
}

// deleteRecords deletes the records of a partition below an offset that
// was acknowledged or read, or below the end (-1).
func (cl *client) deleteRecords() {
	w, m := cl.w, cl.w.model
	tp := cl.randomTP()
	floor, start, epoch, known := m.Floor(tp), m.Start(tp), m.Epoch(), m.Known(tp)
	offset := int64(-1)
	if cl.rng.Chance(0.7) {
		offset = int64(cl.rng.Intn(int(floor) + 1))
	}
	req := &wire.DeleteRecordsRequest{Topics: []wire.DeleteRecordsTopic{{Name: tp.Topic, Partitions: []wire.DeleteRecordsPartition{{Index: tp.Part, Offset: offset}}}}, TimeoutMs: 1000}
	resp := &wire.DeleteRecordsResponse{}
	cl.request(wire.KeyDeleteRecords, cl.version(wire.KeyDeleteRecords), req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].Index != tp.Part {
			w.fail("DeleteRecords response does not match the request")
			return
		}
		switch p := resp.Topics[0].Partitions[0]; p.ErrorCode {
		case wire.ErrNone:
			if !m.possible(tp) {
				w.fail("I4: DeleteRecords of %v succeeded, but nobody created it", tp)
				return
			}
			cl.check(m.RecordsDeleted(tp, offset, p.LowWatermark, floor, start, epoch))
			w.stats.RecordDeletes++
		case wire.ErrUnknownTopicOrPartition:
			if known && !m.stale(epoch) {
				w.fail("I1: DeleteRecords: %v was created but the leader does not know it", tp)
			}
		case wire.ErrOffsetOutOfRange:
			if offset >= 0 && offset <= floor && !m.stale(epoch) {
				w.fail("I1: DeleteRecords of %v at %d is out of range, but the log end was already %d", tp, offset, floor)
			}
		case wire.ErrNotLeaderOrFollower:
		default:
			w.fail("DeleteRecords of %v failed with code %d", tp, p.ErrorCode)
		}
	})
}

// createPartitions asks a topic someone tried to create for a partition
// count up to its catalogue maximum: growth for t2, a refusal otherwise.
func (cl *client) createPartitions() {
	w, m := cl.w, cl.w.model
	t := catalogue[cl.rng.Intn(len(catalogue))]
	if _, most := m.PartitionRange(t.name); most == 0 {
		return // never attempted: a create racing this one would not know of the growth
	}
	count := 1 + int32(cl.rng.Intn(int(t.max)))
	m.PartitionsAttempted(t.name, count)
	epoch, known := m.Epoch(), m.Known(TP{t.name, 0})
	req := &wire.CreatePartitionsRequest{Topics: []wire.CreatePartitionsTopic{{Name: t.name, Count: count}}, TimeoutMs: 1000}
	resp := &wire.CreatePartitionsResponse{}
	cl.request(wire.KeyCreatePartitions, cl.version(wire.KeyCreatePartitions), req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Results) != 1 || resp.Results[0].Name != t.name {
			w.fail("CreatePartitions response does not match the request")
			return
		}
		switch code := resp.Results[0].ErrorCode; code {
		case wire.ErrNone:
			w.stats.PartitionsAdded++
			m.PartitionsCreated(t.name, count, epoch)
		case wire.ErrInvalidPartitions: // it has count or more already
			m.PartitionsCreated(t.name, count, epoch)
		case wire.ErrUnknownTopicOrPartition:
			if known && !m.stale(epoch) {
				w.fail("I1: CreatePartitions: %s was created but the leader does not know it", t.name)
			}
		case wire.ErrNotController:
		default:
			w.fail("CreatePartitions of %s to %d failed with code %d", t.name, count, code)
		}
	})
}

// doomed takes doomedTopic one step through its life.
func (cl *client) doomed() {
	w, m, a := cl.w, cl.w.model, cl.adm
	if u := a.unsettled; u != nil && !u.nodeClosed && !u.inc.dead {
		return
	}
	a.unsettled = nil
	if a.busy {
		return
	}
	epoch := m.Epoch()
	switch a.phase {
	case adminCreate:
		m.TopicAttempted(doomedTopic, 1)
		req := &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: doomedTopic, NumPartitions: 1, ReplicationFactor: int16(len(w.nodes))}}, TimeoutMs: 1000}
		resp := &wire.CreateTopicsResponse{}
		a.busy = true
		c := cl.c
		cl.request(wire.KeyCreateTopics, 4, req, resp, func(ok bool) {
			a.busy = false
			switch {
			case !ok:
				a.unsettled = c // try again once its bytes can no longer arrive
			case len(resp.Topics) != 1:
				w.fail("CreateTopics response does not match the request")
			case resp.Topics[0].ErrorCode == wire.ErrNone || resp.Topics[0].ErrorCode == wire.ErrTopicAlreadyExists:
				m.TopicCreated(doomedTopic, epoch)
				a.phase = adminProduce
			case resp.Topics[0].ErrorCode != wire.ErrNotController:
				w.fail("creating %s failed with code %d", doomedTopic, resp.Topics[0].ErrorCode)
			}
		})
	case adminProduce:
		tp, n := TP{doomedTopic, 0}, 1+cl.rng.Intn(3)
		first := m.Send(tp, n)
		recs := make([]wire.Record, n)
		for i := range recs {
			recs[i] = wire.Record{Value: Value(first + uint64(i)), Timestamp: int64(w.now / 1e6)}
		}
		req := &wire.ProduceRequest{Acks: -1, TimeoutMs: 1000, Topics: []wire.ProduceTopic{{Name: tp.Topic, Partitions: []wire.ProducePartition{{Index: 0, Records: wire.EncodeBatch(recs)}}}}}
		resp := &wire.ProduceResponse{}
		a.busy = true
		cl.request(wire.KeyProduce, cl.version(wire.KeyProduce), req, resp, func(ok bool) {
			a.busy = false
			switch {
			case !ok:
				m.Unknown(first, n)
			case len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1:
				w.fail("produce response does not match the request")
			case resp.Topics[0].Partitions[0].ErrorCode == wire.ErrNone:
				cl.check(m.Acked(first, n, resp.Topics[0].Partitions[0].BaseOffset, epoch))
				if a.batches++; a.batches == 2 {
					a.phase = adminDelete
				}
			default:
				cl.check(m.Rejected(first, n))
			}
		})
	case adminDelete:
		m.TopicDeleteAttempted(doomedTopic)
		req := &wire.DeleteTopicsRequest{Topics: []wire.DeleteTopic{{Name: doomedTopic}}, TimeoutMs: 1000}
		resp := &wire.DeleteTopicsResponse{}
		a.busy = true
		cl.request(wire.KeyDeleteTopics, cl.version(wire.KeyDeleteTopics), req, resp, func(ok bool) {
			a.busy = false
			switch {
			case !ok:
			case len(resp.Topics) != 1 || resp.Topics[0].Name != doomedTopic:
				w.fail("DeleteTopics response does not match the request")
			case resp.Topics[0].ErrorCode == wire.ErrNone || resp.Topics[0].ErrorCode == wire.ErrUnknownTopicOrPartition:
				// Unknown: an earlier attempt whose answer was lost did it.
				m.TopicDeleted(doomedTopic, epoch)
				w.stats.TopicsDeleted++
				a.phase = adminGone
			case resp.Topics[0].ErrorCode != wire.ErrNotController:
				w.fail("deleting %s failed with code %d", doomedTopic, resp.Topics[0].ErrorCode)
			}
		})
	case adminGone:
		tp := TP{doomedTopic, 0}
		f := Fetch{TP: tp, Offset: 0, Floor: m.Floor(tp), Epoch: epoch, Known: m.Known(tp), Start: m.Start(tp), Deleted: m.Deleted(doomedTopic)}
		req := &wire.FetchRequest{MaxBytes: 1 << 20, Topics: []wire.FetchTopic{{Name: tp.Topic, Partitions: []wire.FetchPartition{{Index: 0, MaxBytes: 1 << 20}}}}}
		a.busy = true
		cl.fetch(req, []Fetch{f}, func(Fetch, int64) { a.busy = false })
	}
}
