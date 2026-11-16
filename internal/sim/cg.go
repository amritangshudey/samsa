package sim

import (
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// cgMembership is a simulated client's view of its place in a KIP-848
// consumer-protocol group.
type cgMembership struct {
	group       string
	id          string
	epoch       int32
	rebalanceMs int32
	assignor    string
	joined      bool
	owned       []wire.TopicPartitionIDs // what the client currently holds
	target      []wire.TopicPartitionIDs // what the last heartbeat told it to keep
	quietUntil  sys.Instant
}

func (cl *client) cgMember() {
	w, g := cl.w, cl.cg
	if len(cl.pending) > 0 || w.now < g.quietUntil {
		return
	}
	if !g.joined {
		cl.cgHeartbeat(true)
		return
	}
	switch r := cl.rng.Intn(100); {
	case r < 3:
		cl.cgLeave()
	case r < 6:
		// Go quiet briefly, sometimes long enough to expire.
		g.quietUntil = w.now + sys.Instant(cl.rng.Intn(400))*sys.Instant(time.Millisecond)
	case r < 12:
		// Zombie commit with an old member epoch.
		if g.epoch > 1 {
			cl.cgCommitOffset(g.epoch-1, false)
		} else {
			cl.cgHeartbeat(false)
		}
	case r < 30:
		cl.cgCommitOffset(g.epoch, true)
	case r < 42:
		cl.cgFetchOffset()
	case r < 46:
		cl.cgDescribe()
	case r < 49:
		cl.describeTopicPartitions()
	default:
		cl.cgHeartbeat(false)
	}
}

func (cl *client) cgHeartbeat(join bool) {
	w, g := cl.w, cl.cg
	v := cl.version(wire.KeyConsumerGroupHeartbeat)
	req := &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            g.group,
		MemberID:           g.id,
		MemberEpoch:        g.epoch,
		RebalanceTimeoutMs: -1,
	}
	if v == 0 && join {
		req.MemberID = ""
	}
	if join {
		req.MemberEpoch = wire.EpochJoin
		req.RebalanceTimeoutMs = g.rebalanceMs
		req.SubscribedTopics = []string{catalogue[0].name, catalogue[1].name}
		req.ServerAssignor = g.assignor
		g.owned = nil
		g.target = nil
	} else if !equalTopicPartitions(g.owned, g.target) {
		// Sometimes delay revocation by one heartbeat to exercise the
		// two-phase window where the old owner still holds partitions.
		if cl.rng.Chance(0.5) {
			g.owned = cloneTopicPartitions(g.target)
		}
		req.Owned = g.owned
	}
	resp := &wire.ConsumerGroupHeartbeatResponse{}
	cl.request(wire.KeyConsumerGroupHeartbeat, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		switch resp.ErrorCode {
		case wire.ErrNone:
			g.joined = true
			if resp.MemberID != "" {
				g.id = resp.MemberID
			}
			g.epoch = resp.MemberEpoch
			if resp.HasAssignment {
				g.target = cloneTopicPartitions(resp.Assignment)
				if join {
					g.owned = cloneTopicPartitions(resp.Assignment)
				}
			}
			w.stats.CGHeartbeats++
		case wire.ErrCoordinatorNotAvailable, wire.ErrCoordinatorLoadInProgress:
			// Format level < 5 or coordinator loading; retry later.
		case wire.ErrUnknownMemberID, wire.ErrFencedMemberEpoch:
			g.joined, g.epoch, g.owned, g.target = false, 0, nil, nil
		default:
			w.fail("ConsumerGroupHeartbeat failed with code %d (%s)", resp.ErrorCode, resp.ErrorMessage)
		}
	})
}

func (cl *client) cgLeave() {
	g := cl.cg
	v := cl.version(wire.KeyConsumerGroupHeartbeat)
	req := &wire.ConsumerGroupHeartbeatRequest{
		GroupID:            g.group,
		MemberID:           g.id,
		MemberEpoch:        wire.EpochLeave,
		RebalanceTimeoutMs: -1,
	}
	g.joined, g.epoch, g.owned, g.target = false, 0, nil, nil
	resp := &wire.ConsumerGroupHeartbeatResponse{}
	cl.request(wire.KeyConsumerGroupHeartbeat, v, req, resp, func(ok bool) {
		if ok && resp.ErrorCode != wire.ErrNone && resp.ErrorCode != wire.ErrUnknownMemberID && resp.ErrorCode != wire.ErrCoordinatorNotAvailable {
			cl.w.fail("ConsumerGroupHeartbeat leave failed with code %d", resp.ErrorCode)
		}
	})
}

func (cl *client) cgCommitOffset(epoch int32, own bool) {
	w, m, g := cl.w, cl.w.model, cl.cg
	tp := cl.randomTP()
	offset, modEpoch, known := m.CommitSend(g.group, tp), m.Epoch(), m.Known(tp)
	req := &wire.OffsetCommitRequest{
		GroupID: g.group, GenerationID: epoch, MemberID: g.id,
		Topics: []wire.OffsetCommitTopic{{Name: tp.Topic, Partitions: []wire.OffsetCommitPartition{{Index: tp.Part, Offset: offset, LeaderEpoch: -1}}}},
	}
	v := int16(8)
	if cl.rng.Chance(0.5) {
		v = 9
	}
	resp := &wire.OffsetCommitResponse{}
	cl.request(wire.KeyOffsetCommit, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			w.fail("OffsetCommit response does not match the request")
			return
		}
		code := resp.Topics[0].Partitions[0].ErrorCode
		// Use gen=-1 for CommitDone since consumer group epochs are independent of classic generation floors.
		cl.check(m.CommitDone(offset, code, -1, 0, modEpoch))
		switch {
		case code == wire.ErrNone && !inCatalogue(tp):
			w.fail("I5: the broker accepted an offset for %v, which cannot exist", tp)
		case code == wire.ErrNone:
			w.stats.OffsetCommits++
		case code == wire.ErrUnknownTopicOrPartition && (!known || m.stale(modEpoch)):
		case code == wire.ErrUnknownMemberID || code == wire.ErrStaleMemberEpoch:
			if !own {
				w.stats.ZombiesRefused++
			} else if code == wire.ErrUnknownMemberID {
				g.joined, g.epoch, g.owned, g.target = false, 0, nil, nil
			}
		default:
			w.fail("OffsetCommit (KIP-848) for %v failed with code %d", tp, code)
		}
	})
}

func (cl *client) cgFetchOffset() {
	cl.fetchOffsets(cl.cg.group, []TP{cl.randomTP()}, cl.rng.Chance(0.2), nil)
}

func (cl *client) cgDescribe() {
	req := &wire.ConsumerGroupDescribeRequest{Groups: []string{cl.cg.group}}
	resp := &wire.ConsumerGroupDescribeResponse{}
	cl.request(wire.KeyConsumerGroupDescribe, cl.version(wire.KeyConsumerGroupDescribe), req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Groups) != 1 {
			cl.w.fail("ConsumerGroupDescribe returned %d groups", len(resp.Groups))
			return
		}
		if code := resp.Groups[0].ErrorCode; code != wire.ErrNone && code != wire.ErrGroupIDNotFound {
			cl.w.fail("ConsumerGroupDescribe failed with code %d", code)
		}
	})
}

func (cl *client) describeTopicPartitions() {
	req := &wire.DescribeTopicPartitionsRequest{
		Topics:                 []string{catalogue[0].name, catalogue[1].name, ghostTopic},
		ResponsePartitionLimit: 2,
	}
	resp := &wire.DescribeTopicPartitionsResponse{}
	cl.request(wire.KeyDescribeTopicPartitions, 0, req, resp, func(ok bool) {
		if ok && len(resp.Topics) == 0 {
			cl.w.fail("DescribeTopicPartitions returned no topics")
		}
	})
}

func cloneTopicPartitions(src []wire.TopicPartitionIDs) []wire.TopicPartitionIDs {
	out := make([]wire.TopicPartitionIDs, len(src))
	for i, t := range src {
		out[i] = wire.TopicPartitionIDs{TopicID: t.TopicID, Partitions: append([]int32(nil), t.Partitions...)}
	}
	return out
}

func equalTopicPartitions(a, b []wire.TopicPartitionIDs) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].TopicID != b[i].TopicID || len(a[i].Partitions) != len(b[i].Partitions) {
			return false
		}
		for j := range a[i].Partitions {
			if a[i].Partitions[j] != b[i].Partitions[j] {
				return false
			}
		}
	}
	return true
}
