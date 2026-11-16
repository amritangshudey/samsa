package sim

import (
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

type sgPendingAck struct {
	batch    wire.ShareAckBatch
	delivery int16
}

// sgMembership is a simulated client's view of its place in a KIP-932 share group.
type sgMembership struct {
	group       string
	id          string
	epoch       int32
	sessEpoch   int32
	joined      bool
	assigned    []wire.TopicPartitionIDs
	pendingAcks map[TP][]sgPendingAck
}

// shareModel tracks KIP-932 share-group delivery invariants (S8: every
// record is acknowledged once or delivered again).
type shareModel struct {
	acked      map[TP]map[int64]bool  // terminal: Accept, Reject, or archived after max deliveries
	released   map[TP]map[int64]bool  // released and awaiting redelivery
	deliveries map[TP]map[int64]int16 // latest delivery count seen
}

func newShareModel() shareModel {
	return shareModel{
		acked:      map[TP]map[int64]bool{},
		released:   map[TP]map[int64]bool{},
		deliveries: map[TP]map[int64]int16{},
	}
}

func (s *shareModel) forget() {
	clear(s.acked)
	clear(s.released)
	clear(s.deliveries)
}

// ShareAcquired records that a share consumer acquired offset off of tp.
func (m *Model) ShareAcquired(tp TP, off int64, delivery int16, epoch int) error {
	if m.stale(epoch) {
		return nil
	}
	if delivery < 1 || delivery > 20 {
		return fmt.Errorf("S8: %v offset %d acquired with delivery count %d outside [1, 20]", tp, off, delivery)
	}
	if m.share.acked[tp][off] {
		return fmt.Errorf("S8: %v offset %d was already acknowledged/rejected and was delivered again", tp, off)
	}
	if m.share.deliveries[tp] == nil {
		m.share.deliveries[tp] = map[int64]int16{}
	}
	m.share.deliveries[tp][off] = delivery
	delete(m.share.released[tp], off)
	return nil
}

// ShareAcked records that a share consumer acknowledged offset off of tp.
func (m *Model) ShareAcked(tp TP, off int64, ackType int8, delivery int16, epoch int) {
	if m.stale(epoch) {
		return
	}
	switch ackType {
	case wire.AckAccept, wire.AckReject:
		if m.share.acked[tp] == nil {
			m.share.acked[tp] = map[int64]bool{}
		}
		m.share.acked[tp][off] = true
		delete(m.share.released[tp], off)
	case wire.AckRelease:
		if m.share.acked[tp][off] || m.share.deliveries[tp][off] > delivery {
			return
		}
		if delivery >= 20 {
			if m.share.acked[tp] == nil {
				m.share.acked[tp] = map[int64]bool{}
			}
			m.share.acked[tp][off] = true
			delete(m.share.released[tp], off)
			return
		}
		if m.share.released[tp] == nil {
			m.share.released[tp] = map[int64]bool{}
		}
		m.share.released[tp][off] = true
	}
}

// CheckShareFinal verifies that no released record was dropped without redelivery.
func (m *Model) CheckShareFinal() error {
	for tp, offs := range m.share.released {
		if t := m.topics[tp.Topic]; t != nil && t.deleting {
			continue
		}
		start := m.part(tp).start
		for off := range offs {
			if off >= start {
				return fmt.Errorf("S8: %v offset %d was released in share group and never redelivered (start=%d)", tp, off, start)
			}
		}
	}
	return nil
}

func (cl *client) sgMember() {
	if cl.sg == nil {
		cl.sg = &sgMembership{
			group:       "sg0",
			id:          fmt.Sprintf("sgm-%d", cl.lane),
			pendingAcks: map[TP][]sgPendingAck{},
		}
	}
	if len(cl.pending) > 0 {
		return
	}
	g := cl.sg
	if !g.joined {
		cl.sgHeartbeat(true)
		return
	}
	switch r := cl.rng.Intn(100); {
	case r < 3:
		cl.sgLeave()
	case r < 10:
		cl.sgDescribe()
	case r < 35 && len(g.pendingAcks) > 0:
		cl.sgAcknowledge()
	case r < 50:
		cl.sgHeartbeat(false)
	default:
		cl.sgFetch()
	}
}

func (cl *client) sgHeartbeat(join bool) {
	w, g := cl.w, cl.sg
	req := &wire.ShareGroupHeartbeatRequest{
		GroupID:     g.group,
		MemberID:    g.id,
		MemberEpoch: g.epoch,
	}
	if join {
		req.MemberEpoch = wire.EpochJoin
		req.SubscribedTopics = []string{catalogue[0].name}
		g.assigned = nil
		g.sessEpoch = 0
		clear(g.pendingAcks)
	}
	resp := &wire.ShareGroupHeartbeatResponse{}
	cl.request(wire.KeyShareGroupHeartbeat, 1, req, resp, func(ok bool) {
		if !ok {
			return
		}
		switch resp.ErrorCode {
		case wire.ErrNone:
			g.joined = true
			g.epoch = resp.MemberEpoch
			if resp.HasAssignment {
				g.assigned = cloneTopicPartitions(resp.Assignment)
			}
			w.stats.SGHeartbeats++
		case wire.ErrCoordinatorNotAvailable, wire.ErrCoordinatorLoadInProgress:
		case wire.ErrUnknownMemberID, wire.ErrFencedMemberEpoch:
			g.joined, g.epoch, g.sessEpoch = false, 0, 0
		default:
			w.fail("ShareGroupHeartbeat failed with code %d (%s)", resp.ErrorCode, resp.ErrorMessage)
		}
	})
}

func (cl *client) sgLeave() {
	g := cl.sg
	req := &wire.ShareGroupHeartbeatRequest{
		GroupID:     g.group,
		MemberID:    g.id,
		MemberEpoch: wire.EpochLeave,
	}
	g.joined, g.epoch, g.sessEpoch = false, 0, 0
	clear(g.pendingAcks)
	resp := &wire.ShareGroupHeartbeatResponse{}
	cl.request(wire.KeyShareGroupHeartbeat, 1, req, resp, func(ok bool) {
		if ok && resp.ErrorCode != wire.ErrNone && resp.ErrorCode != wire.ErrUnknownMemberID && resp.ErrorCode != wire.ErrCoordinatorNotAvailable {
			cl.w.fail("ShareGroupHeartbeat leave failed with code %d", resp.ErrorCode)
		}
	})
}

func (cl *client) sgFetch() {
	w, m, g := cl.w, cl.w.model, cl.sg
	tid := m.TopicID(catalogue[0].name)
	if tid == (wire.UUID{}) || len(g.assigned) == 0 {
		cl.sgHeartbeat(false)
		return
	}
	v := cl.version(wire.KeyShareFetch)
	req := &wire.ShareFetchRequest{
		GroupID:           g.group,
		MemberID:          g.id,
		ShareSessionEpoch: g.sessEpoch,
		MaxWaitMs:         20,
		MinBytes:          1,
		MaxBytes:          1 << 20,
		MaxRecords:        50,
		BatchSize:         50,
	}
	tp := TP{catalogue[0].name, 0}
	part := wire.ShareFetchPartition{Index: 0}
	var sentAcks []sgPendingAck
	if batches := g.pendingAcks[tp]; len(batches) > 0 {
		for _, pa := range batches {
			part.AckBatches = append(part.AckBatches, pa.batch)
		}
		sentAcks = batches
		delete(g.pendingAcks, tp)
	}
	req.Topics = []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{part}}}
	epoch := m.Epoch()
	resp := &wire.ShareFetchResponse{}
	cl.request(wire.KeyShareFetch, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		switch resp.ErrorCode {
		case wire.ErrNone:
			g.sessEpoch++
		case wire.ErrShareSessionNotFound, wire.ErrInvalidShareSessionEpoch:
			g.sessEpoch = 0
			return
		case wire.ErrCoordinatorNotAvailable:
			return
		default:
			w.fail("ShareFetch failed with code %d (%s)", resp.ErrorCode, resp.ErrorMessage)
			return
		}
		ackErr := wire.ErrNone
		for _, t := range resp.Responses {
			for _, p := range t.Partitions {
				if p.AckErrorCode != wire.ErrNone {
					ackErr = p.AckErrorCode
				}
			}
		}
		if ackErr == wire.ErrNone {
			for _, pa := range sentAcks {
				for off := pa.batch.FirstOffset; off <= pa.batch.LastOffset; off++ {
					m.ShareAcked(tp, off, pa.batch.AcknowledgeTypes[0], pa.delivery, epoch)
				}
			}
		}
		for _, t := range resp.Responses {
			for _, p := range t.Partitions {
				if p.ErrorCode != wire.ErrNone && p.ErrorCode != wire.ErrUnknownTopicID && p.ErrorCode != wire.ErrUnknownTopicOrPartition {
					w.fail("ShareFetch partition failed with code %d", p.ErrorCode)
					return
				}
				for _, r := range p.Acquired {
					w.stats.ShareAcquired += int(r.LastOffset - r.FirstOffset + 1)
					for off := r.FirstOffset; off <= r.LastOffset; off++ {
						cl.check(m.ShareAcquired(tp, off, r.DeliveryCount, epoch))
						ack := wire.AckAccept
						if r.DeliveryCount == 1 && cl.rng.Chance(0.25) {
							ack = wire.AckRelease
						}
						g.pendingAcks[tp] = append(g.pendingAcks[tp], sgPendingAck{
							batch: wire.ShareAckBatch{
								FirstOffset:      off,
								LastOffset:       off,
								AcknowledgeTypes: []int8{ack},
							},
							delivery: r.DeliveryCount,
						})
					}
				}
			}
		}
	})
}

func (cl *client) sgAcknowledge() {
	w, m, g := cl.w, cl.w.model, cl.sg
	tid := m.TopicID(catalogue[0].name)
	if tid == (wire.UUID{}) {
		return
	}
	tp := TP{catalogue[0].name, 0}
	pending := g.pendingAcks[tp]
	delete(g.pendingAcks, tp)
	if len(pending) == 0 {
		return
	}
	batches := make([]wire.ShareAckBatch, len(pending))
	for i, pa := range pending {
		batches[i] = pa.batch
	}
	v := cl.version(wire.KeyShareAcknowledge)
	req := &wire.ShareAcknowledgeRequest{
		GroupID:           g.group,
		MemberID:          g.id,
		ShareSessionEpoch: g.sessEpoch,
		Topics:            []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{{Index: 0, AckBatches: batches}}}},
	}
	epoch := m.Epoch()
	resp := &wire.ShareAcknowledgeResponse{}
	cl.request(wire.KeyShareAcknowledge, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		switch resp.ErrorCode {
		case wire.ErrNone:
			g.sessEpoch++
			if len(resp.Responses) > 0 && len(resp.Responses[0].Partitions) > 0 && resp.Responses[0].Partitions[0].ErrorCode == wire.ErrNone {
				w.stats.ShareAcked += len(batches)
				for _, pa := range pending {
					for off := pa.batch.FirstOffset; off <= pa.batch.LastOffset; off++ {
						m.ShareAcked(tp, off, pa.batch.AcknowledgeTypes[0], pa.delivery, epoch)
					}
				}
			}
		case wire.ErrShareSessionNotFound, wire.ErrInvalidShareSessionEpoch:
			g.sessEpoch = 0
		case wire.ErrCoordinatorNotAvailable:
		default:
			w.fail("ShareAcknowledge failed with code %d (%s)", resp.ErrorCode, resp.ErrorMessage)
		}
	})
}

func (cl *client) sgDescribe() {
	req := &wire.ShareGroupDescribeRequest{Groups: []string{cl.sg.group}}
	resp := &wire.ShareGroupDescribeResponse{}
	cl.request(wire.KeyShareGroupDescribe, 1, req, resp, func(ok bool) {
		if ok && len(resp.Groups) == 1 {
			if code := resp.Groups[0].ErrorCode; code != wire.ErrNone && code != wire.ErrGroupIDNotFound {
				cl.w.fail("ShareGroupDescribe failed with code %d", code)
			}
		}
	})
}

// drainShareGroup fetches and accepts any remaining available or released
// records in sg0 during quiesce, so CheckShareFinal can verify every
// released record is redelivered.
func (w *world) drainShareGroup(leader *simNode) {
	tid := w.model.TopicID(catalogue[0].name)
	if tid == (wire.UUID{}) || w.opts.sgMembers == 0 {
		return
	}
	// Let any in-flight acquisition locks from stopped clients expire before draining.
	w.runUntil(w.now+sys.Instant(550*time.Millisecond), func() bool { return false })
	v := &client{w: w, rng: w.rng.Fork(), pin: leader}
	v.c = w.connect(v)
	defer v.hangUp()
	done := false
	sess := int32(0)
	tp := TP{catalogue[0].name, 0}
	var step func(acks []wire.ShareAckBatch)
	step = func(acks []wire.ShareAckBatch) {
		req := &wire.ShareFetchRequest{
			GroupID: "sg0", MemberID: "final-drain", ShareSessionEpoch: sess,
			MaxBytes: 1 << 20, MaxRecords: 100, BatchSize: 100,
			Topics: []wire.ShareFetchTopic{{TopicID: tid, Partitions: []wire.ShareFetchPartition{{Index: 0, AckBatches: acks}}}},
		}
		resp := &wire.ShareFetchResponse{}
		epoch := w.model.Epoch()
		v.request(wire.KeyShareFetch, 2, req, resp, func(ok bool) {
			if !ok || resp.ErrorCode != wire.ErrNone {
				done = true
				return
			}
			sess++
			for _, b := range acks {
				for off := b.FirstOffset; off <= b.LastOffset; off++ {
					w.model.ShareAcked(tp, off, wire.AckAccept, 1, epoch)
				}
			}
			var nextAcks []wire.ShareAckBatch
			for _, t := range resp.Responses {
				for _, p := range t.Partitions {
					for _, r := range p.Acquired {
						for off := r.FirstOffset; off <= r.LastOffset; off++ {
							v.check(w.model.ShareAcquired(tp, off, r.DeliveryCount, epoch))
							nextAcks = append(nextAcks, wire.ShareAckBatch{FirstOffset: off, LastOffset: off, AcknowledgeTypes: []int8{wire.AckAccept}})
						}
					}
				}
			}
			if len(nextAcks) == 0 {
				done = true
				return
			}
			step(nextAcks)
		})
	}
	step(nil)
	w.runUntil(w.now+sys.Instant(2*answerBound), func() bool { return done || w.err != nil })
}

var _ = time.Millisecond
