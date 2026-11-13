package broker

import (
	"fmt"
	"slices"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// maxOffsetMetadata bounds the metadata string of one committed offset
// (Kafka's offset.metadata.max.bytes default).
const maxOffsetMetadata = 4096

// softGroup is what this node knows about a group beyond its applied
// state: the rebalance in progress, requests being held, and timers.
//
// None of it survives a restart, and safety needs none of it (roadmap rule
// R5): fencing is decided in apply. After a restart the group is at its
// last applied generation, OnStart gives every member a fresh session
// timer, and members whose requests were held find their connections
// closed and retry.
type softGroup struct {
	name  string
	round *round // non-nil while JoinGroups are being collected
	// proposing is set while a GroupGeneration is in the log and not yet
	// applied. Joins and leaves that arrive meanwhile wait in late.
	proposing bool
	late      []func()
	// issued holds member ids handed out with MEMBER_ID_REQUIRED and not
	// yet used, with the timer that forgets each.
	issued map[string]sys.TimerID
	// gone holds applied members that left or expired since the generation
	// was applied. The next generation is formed without them.
	gone      map[string]bool
	sessions  map[string]sys.TimerID // member id to its session timer
	syncs     []*heldSync
	syncing   bool        // a GroupAssignment is in the log, not yet applied
	syncTimer sys.TimerID // deadline for the leader's SyncGroup; 0 if unarmed
}

// joiner is a member that has sent JoinGroup in the current round. Its
// request is held until the round's generation is applied.
type joiner struct {
	id               string
	protocolType     string
	sessionTimeout   int32
	rebalanceTimeout int32
	protocols        []wire.GroupProtocol
	c                *conn
	h                wire.RequestHeader
}

// round is one collection of JoinGroups: the PreparingRebalance state.
type round struct {
	joiners []*joiner // in arrival order
	timer   sys.TimerID
}

func (r *round) joiner(id string) *joiner {
	if r != nil {
		for _, j := range r.joiners {
			if j.id == id {
				return j
			}
		}
	}
	return nil
}

func (r *round) remove(j *joiner) {
	if r != nil {
		r.joiners = slices.DeleteFunc(r.joiners, func(x *joiner) bool { return x == j })
	}
}

// heldSync is a SyncGroup waiting for the leader's assignment.
type heldSync struct {
	c      *conn
	h      wire.RequestHeader
	member string
}

// after runs fn once, no sooner than d from now, unless cancelled.
func (n *Node) after(d time.Duration, fn func()) sys.TimerID {
	id := n.nextTimer
	n.nextTimer++
	n.timers[id] = fn
	n.clock.SetTimer(id, d)
	return id
}

func (n *Node) cancel(id sys.TimerID) {
	if id != 0 {
		delete(n.timers, id)
		n.clock.CancelTimer(id)
	}
}

func ms(v int32) time.Duration { return time.Duration(v) * time.Millisecond }

// soft returns the node's soft state for a group, creating it.
func (n *Node) soft(name string) *softGroup {
	sg := n.groups[name]
	if sg == nil {
		sg = &softGroup{name: name, issued: map[string]sys.TimerID{}, gone: map[string]bool{}, sessions: map[string]sys.TimerID{}}
		n.groups[name] = sg
	}
	return sg
}

// startGroups arms the timers that applied group state calls for. It runs each
// time the node starts serving as leader.
func (n *Node) startGroups() {
	for _, name := range sortedKeys(n.st.groups) {
		g := n.st.groups[name]
		if len(g.members) == 0 {
			continue
		}
		sg := n.soft(name)
		for _, m := range g.members {
			n.touch(sg, m)
		}
		if !g.assigned {
			n.armSyncDeadline(sg, g)
		}
	}
}

// touch restarts a member's session timer: the member has shown it is alive.
func (n *Node) touch(sg *softGroup, m *member) {
	n.cancel(sg.sessions[m.id])
	sg.sessions[m.id] = n.after(ms(m.sessionTimeout), func() {
		delete(sg.sessions, m.id)
		cur := n.st.group(sg.name).member(m.id)
		switch {
		case cur == nil:
		case sg.proposing || sg.round.joiner(m.id) != nil || slices.ContainsFunc(sg.syncs, func(s *heldSync) bool { return s.member == m.id }):
			// A member whose request the broker is holding cannot
			// heartbeat on that connection, and is not dead.
			n.touch(sg, cur)
		default:
			n.log.Info("member session expired", "group", sg.name, "member", m.id)
			n.removeMember(sg, m.id)
		}
	})
}

// active reports whether id is a member of the applied generation that has
// not left or expired.
func (sg *softGroup) active(g *group, id string) bool {
	return g.member(id) != nil && !sg.gone[id]
}

// removeMember takes a member out of the group, whether it is in the
// applied generation, only in the current round, or both, and starts or
// re-examines the round that forms the next generation.
func (n *Node) removeMember(sg *softGroup, id string) {
	if sg.proposing { // membership is about to change: decide once it has
		sg.late = append(sg.late, func() { n.removeMember(sg, id) })
		return
	}
	g := n.st.group(sg.name)
	if j := sg.round.joiner(id); j != nil {
		sg.round.remove(j)
		n.respond(j.c, j.h, &wire.JoinGroupResponse{ErrorCode: wire.ErrUnknownMemberID, GenerationID: -1, MemberID: id})
	}
	if sg.active(g, id) {
		sg.gone[id] = true
		n.cancel(sg.sessions[id])
		delete(sg.sessions, id)
		if sg.round == nil {
			n.startRound(sg, nil)
		}
	}
	n.maybeComplete(sg)
}

// startRound moves the group to PreparingRebalance. first, if not nil, is
// the joiner that caused it.
func (n *Node) startRound(sg *softGroup, first *joiner) {
	g := n.st.group(sg.name)
	r := &round{}
	var wait int32
	if first != nil {
		r.joiners = []*joiner{first}
		wait = first.rebalanceTimeout
	}
	for _, m := range g.members {
		if !sg.gone[m.id] {
			wait = max(wait, m.rebalanceTimeout)
		}
	}
	sg.round = r
	r.timer = n.after(ms(wait), func() {
		// The last join and this timer can be due in the same instant:
		// whichever runs second must find the round already completed.
		if sg.round == r {
			n.completeRound(sg)
		}
	})
	n.cancel(sg.syncTimer)
	sg.syncTimer = 0
	n.answerSyncs(sg, wire.ErrRebalanceInProgress)
}

// maybeComplete completes the round if every member still expected from
// the applied generation has rejoined.
func (n *Node) maybeComplete(sg *softGroup) {
	if sg.round == nil {
		return
	}
	for _, m := range n.st.group(sg.name).members {
		if !sg.gone[m.id] && sg.round.joiner(m.id) == nil {
			return
		}
	}
	n.completeRound(sg)
}

// completeRound ends PreparingRebalance: the joiners become the next
// generation, proposed as one command. Their requests are answered when it
// has been applied.
func (n *Node) completeRound(sg *softGroup) {
	r := sg.round
	sg.round = nil
	n.cancel(r.timer)
	old := n.st.group(sg.name)
	if len(r.joiners) == 0 && len(old.members) == 0 {
		return // every joiner went away again; nothing changes
	}
	next := &group{generation: old.generation + 1}
	if len(r.joiners) > 0 {
		next.protocolType = r.joiners[0].protocolType
		next.protocol = chooseProtocol(r.joiners)
		next.leader = r.joiners[0].id
		if r.joiner(old.leader) != nil {
			next.leader = old.leader
		}
	}
	for _, j := range r.joiners {
		next.members = append(next.members, &member{id: j.id, sessionTimeout: j.sessionTimeout, rebalanceTimeout: j.rebalanceTimeout, protocols: j.protocols})
	}
	oldMembers := old.members // apply replaces the group's contents in place
	sg.proposing = true
	n.propose(encodeGroupGeneration(sg.name, next), func(res result) {
		sg.proposing = false
		g := n.st.group(sg.name)
		if res.errCode == wire.ErrNone {
			for _, m := range oldMembers {
				n.cancel(sg.sessions[m.id])
				delete(sg.sessions, m.id)
			}
			clear(sg.gone)
			for _, m := range g.members {
				n.touch(sg, m)
			}
			if len(g.members) > 0 {
				n.armSyncDeadline(sg, g)
			}
		}
		for _, j := range r.joiners {
			resp := &wire.JoinGroupResponse{ErrorCode: res.errCode, GenerationID: -1, MemberID: j.id}
			if res.errCode == wire.ErrNone {
				resp = joinResponse(g, j.id)
			}
			n.respond(j.c, j.h, resp)
		}
		late := sg.late
		sg.late = nil
		for _, f := range late {
			f()
		}
	})
}

// chooseProtocol picks the assignment strategy for a generation: among the
// protocols every joiner supports, each joiner votes for the first in its
// own list; most votes wins, ties go to the smaller name. JoinGroup admits
// a member only if a common protocol remains, so one always exists.
func chooseProtocol(joiners []*joiner) string {
	common := func(name string) bool {
		for _, j := range joiners {
			if !slices.ContainsFunc(j.protocols, func(p wire.GroupProtocol) bool { return p.Name == name }) {
				return false
			}
		}
		return true
	}
	votes := map[string]int{}
	for _, j := range joiners {
		for _, p := range j.protocols {
			if common(p.Name) {
				votes[p.Name]++
				break
			}
		}
	}
	best := ""
	for _, name := range sortedKeys(votes) {
		if best == "" || votes[name] > votes[best] {
			best = name
		}
	}
	return best
}

// joinResponse is the successful answer to a member of g's generation.
// Only the leader is told who the members are.
func joinResponse(g *group, id string) *wire.JoinGroupResponse {
	resp := &wire.JoinGroupResponse{GenerationID: g.generation, ProtocolName: g.protocol, Leader: g.leader, MemberID: id}
	if id == g.leader {
		for _, m := range g.members {
			gm := wire.GroupMember{MemberID: m.id}
			for _, p := range m.protocols {
				if p.Name == g.protocol {
					gm.Metadata = p.Metadata
				}
			}
			resp.Members = append(resp.Members, gm)
		}
	}
	return resp
}

// armSyncDeadline bounds CompletingRebalance: if no assignment has been
// applied when it fires, the group rebalances again, so a leader that
// never sends SyncGroup cannot stall the others.
func (n *Node) armSyncDeadline(sg *softGroup, g *group) {
	var wait int32
	for _, m := range g.members {
		wait = max(wait, m.rebalanceTimeout)
	}
	n.cancel(sg.syncTimer)
	sg.syncTimer = n.after(ms(wait), func() {
		sg.syncTimer = 0
		if sg.round == nil && !sg.proposing && !n.st.group(sg.name).assigned {
			n.log.Info("no assignment in time, rebalancing", "group", sg.name)
			n.startRound(sg, nil)
			n.maybeComplete(sg)
		}
	})
}

// answerSyncs answers every held SyncGroup: with the member's assignment
// if code is ErrNone, otherwise with code.
func (n *Node) answerSyncs(sg *softGroup, code int16) {
	g := n.st.group(sg.name)
	held := sg.syncs
	sg.syncs = nil
	for _, s := range held {
		resp := &wire.SyncGroupResponse{ErrorCode: code}
		if m := g.member(s.member); code == wire.ErrNone && m != nil {
			resp.Assignment = m.assignment
			n.touch(sg, m)
		}
		n.respond(s.c, s.h, resp)
	}
}

func (n *Node) newMemberID(clientID string) string {
	// 128 random bits: unique for the life of the cluster, restarts
	// included, which a counter would not be.
	return fmt.Sprintf("%s-%016x%016x", clientID, n.rand.Uint64(), n.rand.Uint64())
}

func sameProtocols(a, b []wire.GroupProtocol) bool {
	return slices.EqualFunc(a, b, func(x, y wire.GroupProtocol) bool {
		return x.Name == y.Name && string(x.Metadata) == string(y.Metadata)
	})
}

// compatible reports whether a member with the given protocol type and
// protocols could share a generation with everyone else now in the group:
// same type, and at least one protocol that all of them support.
func (sg *softGroup) compatible(g *group, id, protocolType string, protocols []wire.GroupProtocol) bool {
	var names []string
	for _, p := range protocols {
		names = append(names, p.Name)
	}
	keep := func(theirs []wire.GroupProtocol) {
		names = slices.DeleteFunc(names, func(name string) bool {
			return !slices.ContainsFunc(theirs, func(p wire.GroupProtocol) bool { return p.Name == name })
		})
	}
	for _, m := range g.members {
		if m.id != id && !sg.gone[m.id] && sg.round.joiner(m.id) == nil {
			if protocolType != g.protocolType {
				return false
			}
			keep(m.protocols)
		}
	}
	if sg.round != nil {
		for _, j := range sg.round.joiners {
			if j.id != id {
				if protocolType != j.protocolType {
					return false
				}
				keep(j.protocols)
			}
		}
	}
	return len(names) > 0
}

// size counts everyone who holds or may claim a place in the group.
func (sg *softGroup) size(g *group) int {
	size := len(sg.issued)
	for _, m := range g.members {
		if !sg.gone[m.id] {
			size++
		}
	}
	if sg.round != nil {
		for _, j := range sg.round.joiners {
			if !sg.active(g, j.id) {
				size++
			}
		}
	}
	return size
}

func (n *Node) findCoordinator(c *conn, req *wire.FindCoordinatorRequest) *wire.FindCoordinatorResponse {
	if req.KeyType != 0 { // only groups have a coordinator here
		return &wire.FindCoordinatorResponse{ErrorCode: wire.ErrCoordinatorNotAvailable, NodeID: -1}
	}
	if !n.may(c, opDescribe, resGroup, req.Key) {
		return &wire.FindCoordinatorResponse{ErrorCode: wire.ErrGroupAuthorizationFailed, NodeID: -1}
	}
	// The leader coordinates every group. Any node can say who that is.
	p, ok := n.leader()
	if !ok {
		return &wire.FindCoordinatorResponse{ErrorCode: wire.ErrCoordinatorNotAvailable, NodeID: -1}
	}
	return &wire.FindCoordinatorResponse{NodeID: p.ID, Host: p.Host, Port: p.Port}
}

func (n *Node) joinGroup(c *conn, h wire.RequestHeader, req *wire.JoinGroupRequest) {
	fail := func(code int16) {
		n.respond(c, h, &wire.JoinGroupResponse{ErrorCode: code, GenerationID: -1, MemberID: req.MemberID})
	}
	if !n.may(c, opRead, resGroup, req.GroupID) {
		fail(wire.ErrGroupAuthorizationFailed)
		return
	}
	if req.GroupID == "" {
		fail(wire.ErrInvalidGroupID)
		return
	}
	sg := n.soft(req.GroupID)
	if sg.proposing {
		// Who is a member is about to change. Decide once it has.
		sg.late = append(sg.late, func() {
			if !c.closed {
				n.joinGroup(c, h, req)
			}
		})
		return
	}
	g := n.st.group(req.GroupID)
	session := ms(req.SessionTimeoutMs)
	switch {
	case session < n.cfg.MinSessionTimeout || session > n.cfg.MaxSessionTimeout:
		fail(wire.ErrInvalidSessionTimeout)
		return
	case req.ProtocolType == "" || len(req.Protocols) == 0 || !sg.compatible(g, req.MemberID, req.ProtocolType, req.Protocols):
		fail(wire.ErrInconsistentGroupProtocol)
		return
	}

	id := req.MemberID
	_, issued := sg.issued[id]
	switch {
	case id == "":
		if sg.size(g) >= n.cfg.MaxGroupSize {
			fail(wire.ErrGroupMaxSizeReached)
			return
		}
		id = n.newMemberID(h.ClientID)
		if h.Version >= 4 {
			// The client must come back with the id before it counts as
			// a member, so that a lost response cannot leave a member
			// nobody knows about holding up every rebalance (KIP-394).
			sg.issued[id] = n.after(session, func() { delete(sg.issued, id) })
			n.respond(c, h, &wire.JoinGroupResponse{ErrorCode: wire.ErrMemberIDRequired, GenerationID: -1, MemberID: id})
			return
		}
	case issued:
		n.cancel(sg.issued[id])
		delete(sg.issued, id)
	case sg.round.joiner(id) != nil:
		// Joining again on another connection: the old request is dead.
		old := sg.round.joiner(id)
		sg.round.remove(old)
		n.drop(old.c, "member joined again on another connection")
	case !sg.active(g, id):
		fail(wire.ErrUnknownMemberID)
		return
	case sg.round == nil && id != g.leader && sameProtocols(g.member(id).protocols, req.Protocols):
		// Nothing changed (a retry, usually): no rebalance. A leader
		// that rejoins does force one: it may have new metadata to act on.
		n.touch(sg, g.member(id))
		n.respond(c, h, joinResponse(g, id))
		return
	}

	j := &joiner{id: id, protocolType: req.ProtocolType, sessionTimeout: req.SessionTimeoutMs, rebalanceTimeout: req.RebalanceTimeoutMs, c: c, h: h}
	if j.rebalanceTimeout < 0 { // version 0 has no such field
		j.rebalanceTimeout = j.sessionTimeout
	}
	for _, p := range req.Protocols { // copied: the request aliases the connection buffer
		j.protocols = append(j.protocols, wire.GroupProtocol{Name: p.Name, Metadata: slices.Clone(p.Metadata)})
	}
	c.onClose = func() {
		sg.round.remove(j)
		n.maybeComplete(sg)
	}
	if sg.round == nil {
		n.startRound(sg, j)
	} else {
		sg.round.joiners = append(sg.round.joiners, j)
	}
	n.maybeComplete(sg)
}

func (n *Node) syncGroup(c *conn, h wire.RequestHeader, req *wire.SyncGroupRequest) {
	if !n.may(c, opRead, resGroup, req.GroupID) {
		n.respond(c, h, &wire.SyncGroupResponse{ErrorCode: wire.ErrGroupAuthorizationFailed})
		return
	}
	sg, g := n.groups[req.GroupID], n.st.group(req.GroupID)
	m := g.member(req.MemberID)
	code := wire.ErrNone
	switch {
	case sg == nil || m == nil || sg.gone[m.id]:
		code = wire.ErrUnknownMemberID
	case req.GenerationID != g.generation:
		code = wire.ErrIllegalGeneration
	case sg.round != nil || sg.proposing:
		code = wire.ErrRebalanceInProgress
	case g.assigned:
		n.touch(sg, m)
		n.respond(c, h, &wire.SyncGroupResponse{Assignment: m.assignment})
		return
	}
	if code != wire.ErrNone {
		n.respond(c, h, &wire.SyncGroupResponse{ErrorCode: code})
		return
	}
	s := &heldSync{c: c, h: h, member: m.id}
	sg.syncs = append(sg.syncs, s)
	c.onClose = func() {
		sg.syncs = slices.DeleteFunc(sg.syncs, func(x *heldSync) bool { return x == s })
	}
	if m.id == g.leader && !sg.syncing {
		sg.syncing = true
		n.propose(encodeGroupAssignment(req.GroupID, g.generation, req.Assignments), func(res result) {
			sg.syncing = false
			if res.errCode == wire.ErrNone {
				n.cancel(sg.syncTimer)
				sg.syncTimer = 0
			}
			n.answerSyncs(sg, res.errCode)
		})
	}
}

func (n *Node) heartbeat(c *conn, req *wire.HeartbeatRequest) *wire.HeartbeatResponse {
	if !n.may(c, opRead, resGroup, req.GroupID) {
		return &wire.HeartbeatResponse{ErrorCode: wire.ErrGroupAuthorizationFailed}
	}
	sg, g := n.groups[req.GroupID], n.st.group(req.GroupID)
	m := g.member(req.MemberID)
	switch {
	case sg == nil || m == nil || sg.gone[m.id]:
		return &wire.HeartbeatResponse{ErrorCode: wire.ErrUnknownMemberID}
	case req.GenerationID != g.generation:
		return &wire.HeartbeatResponse{ErrorCode: wire.ErrIllegalGeneration}
	}
	n.touch(sg, m)
	if sg.round == nil && !sg.proposing && n.faults.inject(FaultRebalance) {
		n.log.Info("fault injected", "kind", FaultRebalance, "group", req.GroupID, "fault", n.faults.count)
		n.startRound(sg, nil)
	}
	if sg.round != nil || sg.proposing {
		return &wire.HeartbeatResponse{ErrorCode: wire.ErrRebalanceInProgress}
	}
	return &wire.HeartbeatResponse{}
}

func (n *Node) leaveGroup(c *conn, h wire.RequestHeader, req *wire.LeaveGroupRequest) {
	if !n.may(c, opRead, resGroup, req.GroupID) {
		n.respond(c, h, &wire.LeaveGroupResponse{ErrorCode: wire.ErrGroupAuthorizationFailed})
		return
	}
	sg := n.groups[req.GroupID]
	resp := &wire.LeaveGroupResponse{}
	for _, id := range req.Members {
		code := wire.ErrUnknownMemberID
		if sg != nil {
			_, issued := sg.issued[id]
			switch {
			case issued:
				n.cancel(sg.issued[id])
				delete(sg.issued, id)
				code = wire.ErrNone
			case sg.active(n.st.group(sg.name), id) || sg.round.joiner(id) != nil:
				n.removeMember(sg, id)
				code = wire.ErrNone
			}
		}
		resp.Members = append(resp.Members, wire.MemberError{MemberID: id, ErrorCode: code})
	}
	if h.Version <= 2 && len(resp.Members) > 0 {
		resp.ErrorCode = resp.Members[0].ErrorCode // these versions name one member
	}
	n.respond(c, h, resp)
}

func (n *Node) offsetCommit(c *conn, h wire.RequestHeader, req *wire.OffsetCommitRequest) {
	resp := &wire.OffsetCommitResponse{}
	var entries []offsetEntry
	var accepted []*int16 // error slots of the partitions that are proposed
	groupOK := n.may(c, opRead, resGroup, req.GroupID)
	for _, t := range req.Topics {
		topicOK := n.may(c, opRead, resTopic, t.Name)
		tr := wire.OffsetCommitTopicResponse{Name: t.Name, Partitions: make([]wire.PartitionError, len(t.Partitions))}
		for i, p := range t.Partitions {
			pr := &tr.Partitions[i]
			pr.Index = p.Index
			switch {
			case !groupOK:
				pr.ErrorCode = wire.ErrGroupAuthorizationFailed
			case req.GroupID == "":
				pr.ErrorCode = wire.ErrInvalidGroupID
			case !topicOK:
				pr.ErrorCode = wire.ErrTopicAuthorizationFailed
			case len(p.Metadata) > maxOffsetMetadata:
				pr.ErrorCode = wire.ErrOffsetMetadataTooLarge
			case n.st.partition(t.Name, p.Index) == nil:
				pr.ErrorCode = wire.ErrUnknownTopicOrPartition
			default:
				entries = append(entries, offsetEntry{t.Name, p.Index, committedOffset{offset: p.Offset, leaderEpoch: p.LeaderEpoch, metadata: p.Metadata}})
				accepted = append(accepted, &pr.ErrorCode)
			}
		}
		resp.Topics = append(resp.Topics, tr)
	}
	if len(entries) == 0 {
		n.respond(c, h, resp)
		return
	}
	now := int64(n.clock.Now()) / int64(time.Millisecond)
	// Whether the member and generation are still right is decided in
	// apply, at the commit's place in the log, not here.
	n.propose(encodeOffsetCommit(req.GroupID, req.GenerationID, req.MemberID, now, entries), func(res result) {
		for _, code := range accepted {
			*code = res.errCode
		}
		if sg := n.groups[req.GroupID]; res.errCode == wire.ErrNone && sg != nil && sg.active(n.st.group(req.GroupID), req.MemberID) {
			n.touch(sg, n.st.group(req.GroupID).member(req.MemberID)) // a commit shows the member is alive
		}
		n.respond(c, h, resp)
	})
}

// offsetFetch needs DESCRIBE on the group, and on each topic: offsets of
// topics the client may not describe are refused by name and left out of
// an all-topics answer (KafkaApis.handleOffsetFetchRequest).
func (n *Node) offsetFetch(c *conn, req *wire.OffsetFetchRequest) *wire.OffsetFetchResponse {
	offsets := n.st.group(req.GroupID).offsets
	resp := &wire.OffsetFetchResponse{}
	groupOK := n.may(c, opDescribe, resGroup, req.GroupID)
	if !groupOK {
		resp.ErrorCode = wire.ErrGroupAuthorizationFailed // v2+; v1 sees it per partition
	}
	topics := req.Topics
	if req.AllTopics {
		topics = nil
		for _, name := range sortedKeys(offsets) {
			if groupOK && n.may(c, opDescribe, resTopic, name) {
				topics = append(topics, wire.OffsetFetchTopic{Name: name, Partitions: sortedKeys(offsets[name])})
			}
		}
	}
	for _, t := range topics {
		tr := wire.OffsetFetchTopicResponse{Name: t.Name}
		topicOK := n.may(c, opDescribe, resTopic, t.Name)
		for _, index := range t.Partitions {
			// No commit is not an error: the consumer falls back to its
			// reset policy.
			pr := wire.OffsetFetchPartition{Index: index, Offset: -1, LeaderEpoch: -1}
			if !groupOK {
				pr.ErrorCode = wire.ErrGroupAuthorizationFailed
			} else if !topicOK {
				pr.ErrorCode = wire.ErrTopicAuthorizationFailed
			} else if o, ok := offsets[t.Name][index]; ok {
				pr.Offset, pr.LeaderEpoch, pr.Metadata = o.offset, o.leaderEpoch, o.metadata
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}
