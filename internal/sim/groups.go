package sim

import (
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// --- the checker -----------------------------------------------------------

// groupSlots is how many things a group divides among its members. The
// broker never looks inside an assignment, so the simulated members assign
// abstract slots, one byte each, instead of partitions.
const groupSlots = 6

var groupNames = []string{"g0", "g1"}

type genKey struct {
	group string
	gen   int32
}

type genInfo struct{ leader, protocol string }

type offsetKey struct {
	group string
	tp    TP
}

// commitRec is one OffsetCommit a client sent. Every commit carries an
// offset nobody else uses, so a fetched offset names its commit.
type commitRec struct {
	key      offsetKey
	sent     uint64 // model clock when sent
	acked    uint64 // model clock when acknowledged; 0 if it never was
	rejected bool
}

// groupModel is the part of the Model that judges consumer groups
// (invariants G1 to G4 and G6; G5 is the watchdog in client.watch).
type groupModel struct {
	clock   uint64 // orders sends and acknowledgements
	maxGen  map[string]int32
	gens    map[genKey]genInfo
	sent    map[genKey]map[string]string // what each leader assigned, by member
	owner   map[genKey]map[byte]string   // which member received each slot
	commits []commitRec                  // indexed by committed offset
	// latest is, per partition of a group, the latest send time among
	// acknowledged commits: whatever is fetched from now on must not have
	// been acknowledged before it.
	latest map[offsetKey]uint64
}

func newGroupModel() groupModel {
	return groupModel{
		maxGen: map[string]int32{}, gens: map[genKey]genInfo{}, sent: map[genKey]map[string]string{},
		owner: map[genKey]map[byte]string{}, latest: map[offsetKey]uint64{}, commits: make([]commitRec, 1),
	}
}

// forget drops everything a permitted power loss may have rolled back.
func (g *groupModel) forget() {
	clear(g.maxGen)
	clear(g.gens)
	clear(g.sent)
	clear(g.owner)
	clear(g.latest)
}

// GenFloor returns the highest generation of group anyone has been told.
func (m *Model) GenFloor(group string) int32 { return m.groups.maxGen[group] }

// Joined reports a successful JoinGroup response (G2). floor and epoch are
// GenFloor and Epoch when the request was sent.
func (m *Model) Joined(group string, gen int32, leader, protocol string, floor int32, epoch int) error {
	if m.stale(epoch) {
		return nil
	}
	if gen < floor {
		return fmt.Errorf("G2: group %s answered a join with generation %d after generation %d had been seen", group, gen, floor)
	}
	k, info := genKey{group, gen}, genInfo{leader, protocol}
	if old, ok := m.groups.gens[k]; ok && old != info {
		return fmt.Errorf("G2: members of group %s generation %d were told different things: %+v and %+v", group, gen, old, info)
	}
	m.groups.gens[k] = info
	m.groups.maxGen[group] = max(m.groups.maxGen[group], gen)
	return nil
}

// AssignmentSent records what the leader of a generation assigned.
func (m *Model) AssignmentSent(group string, gen int32, assignments []wire.MemberAssignment) {
	to := map[string]string{}
	for _, a := range assignments {
		to[a.MemberID] = string(a.Assignment)
	}
	m.groups.sent[genKey{group, gen}] = to
}

// AssignmentReceived reports a successful SyncGroup response (G1).
func (m *Model) AssignmentReceived(group string, gen int32, member string, assignment []byte, epoch int) error {
	if m.stale(epoch) {
		return nil
	}
	k := genKey{group, gen}
	if sent, ok := m.groups.sent[k]; ok && sent[member] != string(assignment) {
		return fmt.Errorf("G1: member %s of group %s generation %d received assignment %v, the leader sent %v", member, group, gen, assignment, []byte(sent[member]))
	}
	if m.groups.owner[k] == nil {
		m.groups.owner[k] = map[byte]string{}
	}
	for _, slot := range assignment {
		if other := m.groups.owner[k][slot]; other != "" && other != member {
			return fmt.Errorf("G1: slot %d of group %s generation %d was given to both %s and %s", slot, group, gen, other, member)
		}
		m.groups.owner[k][slot] = member
	}
	return nil
}

// CommitSend records that an OffsetCommit is being sent and returns the
// offset it must carry.
func (m *Model) CommitSend(group string, tp TP) int64 {
	m.groups.clock++
	m.groups.commits = append(m.groups.commits, commitRec{key: offsetKey{group, tp}, sent: m.groups.clock})
	return int64(len(m.groups.commits) - 1)
}

// CommitDone reports the response to the commit of offset (G4). gen is the
// generation the commit carried, or -1; genFloor and epoch are GenFloor
// and Epoch when it was sent.
func (m *Model) CommitDone(offset int64, code int16, gen, genFloor int32, epoch int) error {
	c := &m.groups.commits[offset]
	switch {
	case code != wire.ErrNone:
		c.rejected = true
	case m.stale(epoch): // acknowledged, and then perhaps lost
	case gen >= 0 && gen < genFloor:
		return fmt.Errorf("G4: group %s accepted a commit from generation %d after generation %d had been seen", c.key.group, gen, genFloor)
	default:
		m.groups.clock++
		c.acked = m.groups.clock
		m.groups.latest[c.key] = max(m.groups.latest[c.key], c.sent)
	}
	return nil
}

// CommitFloor returns what an OffsetFetch sent now is held to.
func (m *Model) CommitFloor(group string, tp TP) uint64 { return m.groups.latest[offsetKey{group, tp}] }

// OffsetFetched reports the committed offset an OffsetFetch returned, -1
// for none (G3, and G6 when a crash came in between). floor and epoch are
// CommitFloor and Epoch when the request was sent.
func (m *Model) OffsetFetched(group string, tp TP, offset int64, floor uint64, epoch int) error {
	if m.stale(epoch) {
		return nil
	}
	if offset < 0 {
		if floor != 0 {
			return fmt.Errorf("G3: group %s has no committed offset for %v, but a commit was acknowledged", group, tp)
		}
		return nil
	}
	if offset >= int64(len(m.groups.commits)) || m.groups.commits[offset].key != (offsetKey{group, tp}) {
		return fmt.Errorf("G3: group %s returned offset %d for %v, which nobody committed there", group, offset, tp)
	}
	switch c := m.groups.commits[offset]; {
	case c.rejected:
		return fmt.Errorf("G3: group %s returned offset %d for %v, a commit the broker refused", group, offset, tp)
	case c.acked != 0 && c.acked < floor:
		// The commit sent at floor was sent after this one was
		// acknowledged, so it is later in the log, and was acknowledged
		// before the fetch was sent.
		return fmt.Errorf("G3: group %s returned offset %d for %v, which a later acknowledged commit had replaced", group, offset, tp)
	}
	return nil
}

// --- the member behaviour --------------------------------------------------

type memberState uint8

const (
	memberOut    memberState = iota // must join
	memberJoined                    // has a generation, must sync
	memberStable                    // has its assignment
)

// membership is a client's view of its place in a group.
type membership struct {
	group      string
	id         string
	gen        int32
	state      memberState
	session    int32 // timeouts it asks for, ms
	rebalance  int32
	assign     []wire.MemberAssignment // what it sends in SyncGroup, as leader
	quietUntil sys.Instant
}

// rejoin handles the errors that send a member back to JoinGroup, and
// reports whether code was one of them.
func (g *membership) rejoin(code int16) bool {
	switch code {
	case wire.ErrUnknownMemberID:
		g.id = ""
		fallthrough
	case wire.ErrIllegalGeneration, wire.ErrRebalanceInProgress:
		g.state = memberOut
		return true
	}
	return false
}

// groupMember behaves as a consumer in a group: it joins, syncs,
// heartbeats, commits and reads offsets, and now and then leaves, falls
// silent, or commits as a zombie from a generation that is over.
func (cl *client) groupMember() {
	g := cl.mem
	if len(cl.pending) > 0 || cl.w.now < g.quietUntil {
		return // one request at a time: joins and syncs are held by the broker
	}
	switch g.state {
	case memberOut:
		cl.joinGroup()
	case memberJoined:
		cl.syncGroup()
	default:
		switch r := cl.rng.Intn(100); {
		case r < 3:
			cl.leaveGroup()
		case r < 6: // miss heartbeats, perhaps for long enough to be expired
			g.quietUntil = cl.w.now + sys.Instant(cl.rng.Intn(2*int(g.session)))*sys.Instant(time.Millisecond)
		case r < 12:
			cl.commitOffset(g.gen-1-int32(cl.rng.Intn(2)), g.id, false) // zombie
		case r < 16:
			cl.commitOffset(-1, "", false) // as a consumer outside any group
		case r < 45:
			cl.commitOffset(g.gen, g.id, true)
		case r < 65:
			cl.fetchOffset()
		default:
			cl.heartbeat()
		}
	}
}

func (cl *client) joinGroup() {
	w, m, g := cl.w, cl.w.model, cl.mem
	v := cl.version(wire.KeyJoinGroup)
	floor, epoch, sentID := m.GenFloor(g.group), m.Epoch(), g.id
	req := &wire.JoinGroupRequest{GroupID: g.group, SessionTimeoutMs: g.session, RebalanceTimeoutMs: g.rebalance, MemberID: g.id,
		ProtocolType: "consumer", Protocols: []wire.GroupProtocol{{Name: "slots", Metadata: []byte("meta")}}}
	resp := &wire.JoinGroupResponse{}
	cl.request(wire.KeyJoinGroup, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		switch resp.ErrorCode {
		case wire.ErrMemberIDRequired:
			if v < 4 || sentID != "" || resp.MemberID == "" {
				w.fail("JoinGroup v%d with member id %q answered MEMBER_ID_REQUIRED with id %q", v, sentID, resp.MemberID)
			}
			g.id = resp.MemberID
		case wire.ErrUnknownMemberID:
			if sentID == "" {
				w.fail("JoinGroup without a member id answered UNKNOWN_MEMBER_ID")
			}
			g.id = ""
		case wire.ErrNone:
			leader := resp.Leader == resp.MemberID
			if resp.MemberID == "" || (sentID != "" && resp.MemberID != sentID) || leader != (len(resp.Members) > 0) || resp.ProtocolName != "slots" {
				w.fail("JoinGroup by %q answered with %+v", sentID, resp)
				return
			}
			cl.check(m.Joined(g.group, resp.GenerationID, resp.Leader, resp.ProtocolName, floor, epoch))
			g.id, g.gen, g.state, g.assign = resp.MemberID, resp.GenerationID, memberJoined, nil
			w.stats.GroupJoins++
			if leader {
				g.assign = make([]wire.MemberAssignment, len(resp.Members))
				for i, member := range resp.Members {
					g.assign[i].MemberID = member.MemberID
					if string(member.Metadata) != "meta" {
						w.fail("JoinGroup: member metadata %q came back changed", member.Metadata)
					}
				}
				for slot := range byte(groupSlots) {
					a := &g.assign[int(slot)%len(g.assign)]
					a.Assignment = append(a.Assignment, slot)
				}
				// After a power loss that may roll generations back,
				// this generation number may be formed again by others.
				if !m.stale(epoch) {
					m.AssignmentSent(g.group, g.gen, g.assign)
				}
			}
		default:
			w.fail("JoinGroup failed with code %d", resp.ErrorCode)
		}
	})
}

func (cl *client) syncGroup() {
	w, m, g := cl.w, cl.w.model, cl.mem
	gen, id, epoch := g.gen, g.id, m.Epoch()
	req := &wire.SyncGroupRequest{GroupID: g.group, GenerationID: gen, MemberID: id, Assignments: g.assign}
	resp := &wire.SyncGroupResponse{}
	cl.request(wire.KeySyncGroup, cl.version(wire.KeySyncGroup), req, resp, func(ok bool) {
		switch {
		case !ok || g.rejoin(resp.ErrorCode):
		case resp.ErrorCode != wire.ErrNone:
			w.fail("SyncGroup failed with code %d", resp.ErrorCode)
		default:
			cl.check(m.AssignmentReceived(g.group, gen, id, resp.Assignment, epoch))
			g.state = memberStable
			w.stats.GroupSyncs++
		}
	})
}

func (cl *client) heartbeat() {
	g := cl.mem
	req, resp := &wire.HeartbeatRequest{GroupID: g.group, GenerationID: g.gen, MemberID: g.id}, &wire.HeartbeatResponse{}
	cl.request(wire.KeyHeartbeat, cl.version(wire.KeyHeartbeat), req, resp, func(ok bool) {
		if ok && resp.ErrorCode != wire.ErrNone && !g.rejoin(resp.ErrorCode) {
			cl.w.fail("Heartbeat failed with code %d", resp.ErrorCode)
		}
	})
}

func (cl *client) leaveGroup() {
	g := cl.mem
	req, resp := &wire.LeaveGroupRequest{GroupID: g.group, Members: []string{g.id}}, &wire.LeaveGroupResponse{}
	g.id, g.state = "", memberOut
	cl.request(wire.KeyLeaveGroup, cl.version(wire.KeyLeaveGroup), req, resp, func(ok bool) {
		if ok && resp.ErrorCode != wire.ErrNone && resp.ErrorCode != wire.ErrUnknownMemberID {
			cl.w.fail("LeaveGroup failed with code %d", resp.ErrorCode)
		}
	})
}

// commitOffset commits a fresh offset for a random partition. own says
// whether the commit speaks for the member's current generation, in which
// case a refusal sends it back to JoinGroup.
func (cl *client) commitOffset(gen int32, id string, own bool) {
	w, m, g := cl.w, cl.w.model, cl.mem
	tp := cl.randomTP()
	offset, floor, epoch, known := m.CommitSend(g.group, tp), m.GenFloor(g.group), m.Epoch(), m.Known(tp)
	req := &wire.OffsetCommitRequest{GroupID: g.group, GenerationID: gen, MemberID: id, Topics: []wire.OffsetCommitTopic{
		{Name: tp.Topic, Partitions: []wire.OffsetCommitPartition{{Index: tp.Part, Offset: offset, LeaderEpoch: -1}}}}}
	resp := &wire.OffsetCommitResponse{}
	cl.request(wire.KeyOffsetCommit, cl.version(wire.KeyOffsetCommit), req, resp, func(ok bool) {
		if !ok {
			return
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].Index != tp.Part {
			w.fail("OffsetCommit response does not match the request")
			return
		}
		code := resp.Topics[0].Partitions[0].ErrorCode
		cl.check(m.CommitDone(offset, code, gen, floor, epoch))
		switch {
		case code == wire.ErrNone && !inCatalogue(tp):
			w.fail("I5: the broker accepted an offset for %v, which cannot exist", tp)
		case code == wire.ErrNone:
			w.stats.OffsetCommits++
		case code == wire.ErrUnknownTopicOrPartition && (!known || m.stale(epoch)):
		case code == wire.ErrUnknownMemberID || code == wire.ErrIllegalGeneration || code == wire.ErrRebalanceInProgress:
			if gen >= 0 && !own {
				w.stats.ZombiesRefused++
			}
			if own {
				g.rejoin(code)
			}
		default:
			w.fail("OffsetCommit for %v failed with code %d", tp, code)
		}
	})
}

func (cl *client) fetchOffset() {
	cl.fetchOffsets(cl.mem.group, []TP{cl.randomTP()}, cl.rng.Chance(0.2), nil)
}

// fetchOffsets asks for a group's committed offsets of tps (or, with all
// set, for every offset it has) and checks each against the model.
func (cl *client) fetchOffsets(group string, tps []TP, all bool, then func()) {
	w, m := cl.w, cl.w.model
	v := cl.version(wire.KeyOffsetFetch)
	req := &wire.OffsetFetchRequest{GroupID: group, AllTopics: all && v >= 2}
	floors := make([]uint64, len(tps))
	for i, tp := range tps {
		floors[i] = m.CommitFloor(group, tp)
		if !req.AllTopics {
			req.Topics = append(req.Topics, wire.OffsetFetchTopic{Name: tp.Topic, Partitions: []int32{tp.Part}})
		}
	}
	epoch := m.Epoch()
	resp := &wire.OffsetFetchResponse{}
	cl.request(wire.KeyOffsetFetch, v, req, resp, func(ok bool) {
		if !ok {
			return
		}
		if resp.ErrorCode != wire.ErrNone || (!req.AllTopics && len(resp.Topics) != len(tps)) {
			w.fail("OffsetFetch: error %d, %d topics for %d asked", resp.ErrorCode, len(resp.Topics), len(tps))
			return
		}
		for i, tp := range tps {
			offset := int64(-1) // asking for everything omits what has no commit
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if t.Name == tp.Topic && p.Index == tp.Part {
						offset = p.Offset
						if p.ErrorCode != wire.ErrNone {
							w.fail("OffsetFetch of %v failed with code %d", tp, p.ErrorCode)
						}
					}
				}
			}
			cl.check(m.OffsetFetched(group, tp, offset, floors[i], epoch))
			w.stats.OffsetFetches++
		}
		if then != nil {
			then()
		}
	})
}
