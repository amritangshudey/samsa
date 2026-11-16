package broker

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/amritangshudey/samsa/internal/wire"
)

// The consumer group protocol of KIP-848 (docs/design/c18-kip848.md, ADR
// 0033). The group coordinator, not a client, decides who owns which
// partition. What it decides is applied state: a command is proposed only
// when a heartbeat changes something, so a heartbeat that changes nothing
// costs no log entry.
//
// The rule everything rests on: a partition is in at most one member's
// owned set. A partition moves from one member to another only after the
// first has reported that it no longer holds it, or has left the group,
// or has been removed for missing its session or rebalance timeout.

// tpKey is a partition, by the id of its topic.
type tpKey struct {
	id   wire.UUID
	part int32
}

func cmpTP(a, b tpKey) int {
	if c := bytes.Compare(a.id[:], b.id[:]); c != 0 {
		return c
	}
	return cmp.Compare(a.part, b.part)
}

// tps is a sorted list of partitions without repeats.
type tps []tpKey

func (s tps) has(p tpKey) bool {
	_, ok := slices.BinarySearchFunc(s, p, cmpTP)
	return ok
}

func sortTPs(s tps) tps {
	slices.SortFunc(s, cmpTP)
	return slices.CompactFunc(s, func(a, b tpKey) bool { return a == b })
}

// minus returns the partitions of s that are not in t.
func (s tps) minus(t tps) tps {
	var out tps
	for _, p := range s {
		if !t.has(p) {
			out = append(out, p)
		}
	}
	return out
}

// within returns the partitions of s that are also in t.
func (s tps) within(t tps) tps {
	var out tps
	for _, p := range s {
		if t.has(p) {
			out = append(out, p)
		}
	}
	return out
}

// cmember is one member of a consumer-protocol group.
type cmember struct {
	id string
	// epoch is the member epoch; prevEpoch the one before it, which a
	// member that missed the answer to its last heartbeat may still send.
	epoch, prevEpoch int32
	rack             string
	clientID, host   string
	rebalanceMs      int32
	assignor         string   // the server assignor it asked for, if any
	subscribed       []string // sorted topic names
	owned            tps      // what it holds, as far as the coordinator knows
	target           tps      // what the assignor wants it to hold
}

// topicMeta is what a group's assignment was computed from, for one topic.
type topicMeta struct {
	id    wire.UUID
	parts int32
}

// cgroup is the applied state of a group of the consumer protocol.
type cgroup struct {
	epoch       int32 // the group epoch: changes whenever the target does
	targetEpoch int32 // the group epoch the members' targets were computed at
	assignor    string
	meta        []topicMeta // the topics the targets were computed from, sorted by id
	members     []*cmember  // sorted by id
}

const (
	assignorUniform = "uniform"
	assignorRange   = "range"
)

func validAssignor(name string) bool { return name == assignorUniform || name == assignorRange }

func (cg *cgroup) find(id string) (int, bool) {
	return sort.Find(len(cg.members), func(i int) int { return strings.Compare(id, cg.members[i].id) })
}

func (cg *cgroup) member(id string) *cmember {
	if i, ok := cg.find(id); ok {
		return cg.members[i]
	}
	return nil
}

func (cg *cgroup) insert(m *cmember) {
	i, _ := cg.find(m.id)
	cg.members = slices.Insert(cg.members, i, m)
}

func (cg *cgroup) remove(id string) {
	if i, ok := cg.find(id); ok {
		cg.members = slices.Delete(cg.members, i, i+1)
	}
}

// heldByOther reports whether a member other than self holds p.
func (cg *cgroup) heldByOther(p tpKey, self *cmember) bool {
	for _, m := range cg.members {
		if m != self && m.owned.has(p) {
			return true
		}
	}
	return false
}

// revoking reports whether m holds partitions that its target no longer
// gives it: it must report that it released them before it gets more.
func (m *cmember) revoking() bool { return len(m.owned.minus(m.target)) > 0 }

// visible is what the member may use right now: what it holds, less what
// it has been asked to give up.
func (m *cmember) visible() tps {
	if m.revoking() {
		return m.owned.within(m.target)
	}
	return m.owned
}

// reconcile returns what m's owned set and epoch become at its next
// heartbeat. A member that holds something it must give up stays where
// it is until it reports that it has; otherwise it takes the partitions
// of its target that nobody holds. early is the deliberate bug that skips
// both rules.
func (cg *cgroup) reconcile(m *cmember, early bool) (tps, int32) {
	if m.revoking() && !early {
		return m.owned, m.epoch
	}
	owned := slices.Clone(m.owned)
	for _, p := range m.target {
		if !m.owned.has(p) && (early || !cg.heldByOther(p, m)) {
			owned = append(owned, p)
		}
	}
	return sortTPs(owned), cg.targetEpoch
}

// stateName is the group's state as ConsumerGroupDescribe reports it.
func (cg *cgroup) stateName() string {
	if len(cg.members) == 0 {
		return "Empty"
	}
	for _, m := range cg.members {
		if m.epoch != cg.targetEpoch || m.revoking() || !slices.Equal(m.owned, m.target) {
			return "Reconciling"
		}
	}
	return "Stable"
}

// --- topics and assignors -----------------------------------------------------

// topicInfo is a topic an assignor may assign.
type topicInfo struct {
	name  string
	id    wire.UUID
	parts int32
}

// subscribedTopics returns the existing topics some member subscribes
// to, sorted by name.
func (s *state) subscribedTopics(cg *cgroup) []topicInfo {
	seen := map[string]bool{}
	var names []string
	for _, m := range cg.members {
		for _, name := range m.subscribed {
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	var out []topicInfo
	for _, name := range names {
		if t := s.topics[name]; t != nil && t.id != (wire.UUID{}) {
			out = append(out, topicInfo{name, t.id, int32(len(t.parts))})
		}
	}
	return out
}

func metaOf(topics []topicInfo) []topicMeta {
	out := make([]topicMeta, len(topics))
	for i, t := range topics {
		out[i] = topicMeta{t.id, t.parts}
	}
	slices.SortFunc(out, func(a, b topicMeta) int { return bytes.Compare(a.id[:], b.id[:]) })
	return out
}

// retarget computes every member's target for the group's current epoch.
func (s *state) retarget(cg *cgroup) {
	topics := s.subscribedTopics(cg)
	cg.meta = metaOf(topics)
	switch cg.assignor {
	case assignorRange:
		rangeAssign(cg.members, topics)
	default:
		uniformAssign(cg.members, topics)
	}
	cg.targetEpoch = cg.epoch
}

func subscribes(m *cmember, topic string) bool {
	_, ok := slices.BinarySearch(m.subscribed, topic)
	return ok
}

// rangeAssign gives, per topic, each subscribed member a contiguous run
// of partitions: the first parts%n members one more than the others.
func rangeAssign(members []*cmember, topics []topicInfo) {
	for _, m := range members {
		m.target = nil
	}
	for _, t := range topics {
		var subs []*cmember
		for _, m := range members {
			if subscribes(m, t.name) {
				subs = append(subs, m)
			}
		}
		if len(subs) == 0 {
			continue
		}
		q, r := int(t.parts)/len(subs), int(t.parts)%len(subs)
		next := int32(0)
		for i, m := range subs {
			count := int32(q)
			if i < r {
				count++
			}
			for k := range count {
				m.target = append(m.target, tpKey{t.id, next + k})
			}
			next += count
		}
	}
	for _, m := range members {
		m.target = sortTPs(m.target)
	}
}

// uniformAssign balances the number of partitions per member, as far as
// subscriptions allow, and moves as little as it can: a member keeps what
// its previous target gave it, unless that leaves it with more than one
// partition over another member that could take it.
func uniformAssign(members []*cmember, topics []topicInfo) {
	type slot struct {
		p     tpKey
		topic string
	}
	var all []slot // in topic name order, then partition
	for _, t := range topics {
		for part := range t.parts {
			all = append(all, slot{tpKey{t.id, part}, t.name})
		}
	}
	owner := make(map[tpKey]int, len(all))
	counts := make([]int, len(members))
	valid := make(map[tpKey]bool, len(all))
	for _, sl := range all {
		valid[sl.p] = true
	}
	topicOf := make(map[wire.UUID]string, len(topics))
	for _, t := range topics {
		topicOf[t.id] = t.name
	}
	for i, m := range members {
		for _, p := range m.target { // sticky
			if _, taken := owner[p]; !taken && valid[p] && subscribes(m, topicOf[p.id]) {
				owner[p] = i
				counts[i]++
			}
		}
	}
	least := func(topic string, below int) int { // the member with the fewest, among those that subscribe
		best := -1
		for j, m := range members {
			if subscribes(m, topic) && counts[j] < below && (best < 0 || counts[j] < counts[best]) {
				best = j
			}
		}
		return best
	}
	for _, sl := range all {
		if _, taken := owner[sl.p]; !taken {
			if j := least(sl.topic, int(^uint(0)>>1)); j >= 0 {
				owner[sl.p] = j
				counts[j]++
			}
		}
	}
	for moved := true; moved; { // each move lowers the sum of squares, so this ends
		moved = false
		for _, sl := range all {
			i, ok := owner[sl.p]
			if !ok {
				continue
			}
			// A member that would have no more than the owner's count
			// less one: moving then strictly improves the balance.
			if j := least(sl.topic, counts[i]-1); j >= 0 {
				owner[sl.p] = j
				counts[i]--
				counts[j]++
				moved = true
			}
		}
	}
	for _, m := range members {
		m.target = nil
	}
	for _, sl := range all {
		if i, ok := owner[sl.p]; ok {
			members[i].target = append(members[i].target, sl.p)
		}
	}
	for _, m := range members {
		m.target = sortTPs(m.target)
	}
}

// --- the command -----------------------------------------------------------

// cgBeat is a heartbeat that may change a group, as a command carries it.
type cgBeat struct {
	group, member    string
	epoch            int32
	rebalanceMs      int32
	rack             string
	clientID, host   string
	assignor         string
	hasSub, hasOwned bool
	sub              []string
	owned            tps
}

func encodeCGBeat(b cgBeat) []byte {
	w := wire.Writer{B: []byte{cmdCGBeat}}
	w.String(false, b.group)
	w.String(false, b.member)
	w.Int32(b.epoch)
	w.Int32(b.rebalanceMs)
	w.String(false, b.rack)
	w.String(false, b.clientID)
	w.String(false, b.host)
	w.String(false, b.assignor)
	var flags int8
	if b.hasSub {
		flags |= 1
	}
	if b.hasOwned {
		flags |= 2
	}
	w.Int8(flags)
	if b.hasSub {
		w.ArrayLen(false, len(b.sub))
		for _, name := range b.sub {
			w.String(false, name)
		}
	}
	if b.hasOwned {
		w.ArrayLen(false, len(b.owned))
		for _, p := range b.owned {
			w.UUID(p.id)
			w.Int32(p.part)
		}
	}
	return w.B
}

func decodeCGBeat(r *wire.Reader) (cgBeat, bool) {
	b := cgBeat{group: r.String(false), member: r.String(false), epoch: r.Int32(), rebalanceMs: r.Int32(),
		rack: r.String(false), clientID: r.String(false), host: r.String(false), assignor: r.String(false)}
	flags := r.Int8()
	b.hasSub, b.hasOwned = flags&1 != 0, flags&2 != 0
	if b.hasSub {
		b.sub = wire.Array(r, false, func() string { return r.String(false) })
	}
	if b.hasOwned {
		b.owned = wire.Array(r, false, func() tpKey { return tpKey{r.UUID(), r.Int32()} })
	}
	return b, r.Err() == nil && r.Len() == 0 && flags>>2 == 0
}

// encodeCGRemove removes a member that missed a timeout. It acts only if
// the member is still at epoch: one that heartbeated meanwhile stays.
func encodeCGRemove(group, member string, epoch int32) []byte {
	w := wire.Writer{B: []byte{cmdCGRemove}}
	w.String(false, group)
	w.String(false, member)
	w.Int32(epoch)
	return w.B
}

// normalSub sorts and de-duplicates a subscription.
func normalSub(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return slices.Compact(out)
}

// validTP reports whether p is a partition that exists.
func (s *state) validTP(p tpKey) bool {
	t := s.topics[s.byID[p.id]]
	return t != nil && p.part >= 0 && int(p.part) < len(t.parts)
}

// chooseAssignor picks the group's assignor: the one the member with the
// smallest id that named one asked for; uniform if none did.
func (cg *cgroup) chooseAssignor() string {
	for _, m := range cg.members {
		if m.assignor != "" {
			return m.assignor
		}
	}
	return assignorUniform
}

// response is the heartbeat answer for m. The assignment is the partitions
// the member may use now.
func (m *cmember) response(withAssignment bool) *wire.ConsumerGroupHeartbeatResponse {
	resp := &wire.ConsumerGroupHeartbeatResponse{MemberID: m.id, MemberEpoch: m.epoch}
	if withAssignment {
		resp.HasAssignment, resp.Assignment = true, groupByTopic(m.visible())
	}
	return resp
}

// groupByTopic turns a sorted partition list into the wire's shape.
func groupByTopic(s tps) []wire.TopicPartitionIDs {
	var out []wire.TopicPartitionIDs
	for _, p := range s {
		if n := len(out); n > 0 && out[n-1].TopicID == p.id {
			out[n-1].Partitions = append(out[n-1].Partitions, p.part)
		} else {
			out = append(out, wire.TopicPartitionIDs{TopicID: p.id, Partitions: []int32{p.part}})
		}
	}
	return out
}

// cgNeedsApply reports whether a heartbeat changes anything, which takes a
// command. It reads applied state only, and is the same test that apply
// makes, so that what is not proposed is exactly what would have changed
// nothing.
func (s *state) cgNeedsApply(b cgBeat) bool {
	g := s.groups[b.group]
	if g == nil || g.cg == nil {
		return b.epoch == 0
	}
	cg := g.cg
	m := cg.member(b.member)
	switch {
	case b.epoch == 0 || b.epoch == wire.EpochLeave:
		return b.epoch == 0 || m != nil
	case m == nil || b.epoch != m.epoch:
		return false // an error, or a repeat of the last answer
	case b.hasSub && !slices.Equal(m.subscribed, normalSub(b.sub)),
		b.assignor != "" && b.assignor != m.assignor,
		b.rack != "" && b.rack != m.rack,
		b.rebalanceMs > 0 && b.rebalanceMs != m.rebalanceMs,
		b.hasOwned && !slices.Equal(m.owned.within(b.owned), m.owned),
		!slices.Equal(cg.meta, metaOf(s.subscribedTopics(cg))):
		return true
	}
	owned, epoch := cg.reconcile(m, s.assignEarly)
	return epoch != m.epoch || !slices.Equal(owned, m.owned)
}

// cgImmediate answers a heartbeat that changes nothing from applied state.
func (s *state) cgImmediate(b cgBeat) (*wire.ConsumerGroupHeartbeatResponse, int16, string) {
	g := s.groups[b.group]
	var m *cmember
	if g != nil && g.cg != nil {
		m = g.cg.member(b.member)
	}
	switch {
	case m == nil && b.epoch == wire.EpochLeave:
		return nil, wire.ErrUnknownMemberID, "the member is not in the group"
	case m == nil:
		return nil, wire.ErrUnknownMemberID, "the member is not in the group"
	case b.epoch == m.epoch:
		return m.response(m.revoking()), wire.ErrNone, ""
	case b.epoch == m.prevEpoch:
		// The answer to its last heartbeat was lost: give it again, whole.
		return m.response(true), wire.ErrNone, ""
	}
	return nil, wire.ErrFencedMemberEpoch, fmt.Sprintf("member epoch %d is not the current %d", b.epoch, m.epoch)
}

// applyCGBeat applies a heartbeat. The result carries the response.
func (s *state) applyCGBeat(index int64, b cgBeat) (result, error) {
	g := s.groups[b.group]
	if g != nil && (len(g.members) > 0 || (g.sg != nil && len(g.sg.members) > 0)) {
		return result{errCode: wire.ErrInconsistentGroupProtocol, errMsg: "the group uses another protocol"}, nil
	}
	if g == nil || g.cg == nil {
		if b.epoch != 0 {
			return result{errCode: wire.ErrUnknownMemberID, errMsg: "the member is not in the group"}, nil
		}
		g = s.forWrite(b.group)
		g.cg = &cgroup{assignor: assignorUniform}
	}
	cg := g.cg
	m := cg.member(b.member)
	changed, joined := false, false
	switch {
	case b.epoch == wire.EpochLeave:
		if m == nil {
			return result{errCode: wire.ErrUnknownMemberID, errMsg: "the member is not in the group"}, nil
		}
		cg.remove(m.id)
		cg.assignor = cg.chooseAssignor()
		cg.epoch++
		s.retarget(cg)
		return result{cg: &wire.ConsumerGroupHeartbeatResponse{MemberID: m.id, MemberEpoch: -1}}, nil
	case b.epoch == 0:
		if m == nil {
			m = &cmember{id: b.member}
			cg.insert(m)
		} else {
			m.owned, m.epoch, m.prevEpoch = nil, 0, 0 // it dropped everything before rejoining
		}
		changed, joined = true, true
	case m == nil:
		return result{errCode: wire.ErrUnknownMemberID, errMsg: "the member is not in the group"}, nil
	case b.epoch != m.epoch:
		resp, code, msg := s.cgImmediate(b)
		return result{cg: resp, errCode: code, errMsg: msg}, nil
	}
	if b.hasSub {
		if names := normalSub(b.sub); !slices.Equal(m.subscribed, names) {
			m.subscribed, changed = names, true
		}
	}
	if b.assignor != "" && b.assignor != m.assignor {
		m.assignor, changed = b.assignor, true
	}
	if b.rack != "" {
		m.rack = b.rack
	}
	if b.rebalanceMs > 0 {
		m.rebalanceMs = b.rebalanceMs
	}
	if b.clientID != "" {
		m.clientID = b.clientID
	}
	if b.host != "" {
		m.host = b.host
	}
	if b.hasOwned && !joined {
		m.owned = m.owned.within(b.owned) // it cannot gain a partition by claiming it
	}
	for _, o := range cg.members { // a deleted topic's partitions are nobody's
		if len(o.owned) > 0 {
			o.owned = slices.DeleteFunc(slices.Clone(o.owned), func(p tpKey) bool { return !s.validTP(p) })
		}
	}
	if a := cg.chooseAssignor(); a != cg.assignor {
		cg.assignor, changed = a, true
	}
	if !slices.Equal(cg.meta, metaOf(s.subscribedTopics(cg))) {
		changed = true
	}
	if changed {
		cg.epoch++
		s.retarget(cg)
	}
	owned, epoch := cg.reconcile(m, s.assignEarly)
	grew := !slices.Equal(owned, m.owned) || epoch != m.epoch
	m.owned = owned
	if epoch != m.epoch {
		m.prevEpoch, m.epoch = m.epoch, epoch
	}
	return result{cg: m.response(joined || grew || m.revoking())}, nil
}

// applyCGRemove removes a member that missed its session or rebalance
// timeout, if it has not moved on since the timer was set.
func (s *state) applyCGRemove(group, member string, epoch int32) result {
	g := s.groups[group]
	if g == nil || g.cg == nil {
		return result{errCode: wire.ErrUnknownMemberID}
	}
	m := g.cg.member(member)
	if m == nil || m.epoch != epoch {
		return result{errCode: wire.ErrUnknownMemberID}
	}
	g.cg.remove(member)
	g.cg.assignor = g.cg.chooseAssignor()
	g.cg.epoch++
	s.retarget(g.cg)
	return result{}
}

// checkCGCommit decides an offset commit to a group of the consumer
// protocol. The member epoch must be the member's current one: this is
// what fences a member that has been removed or has moved on.
func (cg *cgroup) checkCommit(epoch int32, memberID string, noFencing bool) int16 {
	if epoch == -1 && memberID == "" { // a consumer that assigns partitions itself
		return wire.ErrUnknownMemberID // not while the group has members
	}
	switch m := cg.member(memberID); {
	case m == nil:
		return wire.ErrUnknownMemberID
	case epoch != m.epoch && !noFencing:
		return wire.ErrStaleMemberEpoch
	}
	return wire.ErrNone
}

// checkConsumerGroups verifies, on applied state, what the protocol
// promises: no partition is held by two members, no partition is the
// target of two members, and no member is ahead of its group.
func (s *state) checkConsumerGroups() error {
	for _, name := range sortedKeys(s.groups) {
		cg := s.groups[name].cg
		if cg == nil {
			continue
		}
		heldBy, targetOf := map[tpKey]string{}, map[tpKey]string{}
		for _, m := range cg.members {
			if m.epoch > cg.epoch {
				return fmt.Errorf("C1: member %s of group %s is at epoch %d, ahead of its group's %d", m.id, name, m.epoch, cg.epoch)
			}
			for _, p := range m.owned {
				if other, ok := heldBy[p]; ok {
					return fmt.Errorf("C1: partition %x/%d of group %s is held by both %s and %s", p.id[:4], p.part, name, other, m.id)
				}
				heldBy[p] = m.id
			}
			for _, p := range m.target {
				if other, ok := targetOf[p]; ok {
					return fmt.Errorf("C2: partition %x/%d of group %s is the target of both %s and %s", p.id[:4], p.part, name, other, m.id)
				}
				targetOf[p] = m.id
			}
		}
	}
	return nil
}

// ConsumerGroupCheck verifies the invariants of the consumer protocol on
// what this node has applied. The simulator calls it after every event.
func (n *Node) ConsumerGroupCheck() error { return n.st.checkConsumerGroups() }

// --- the handlers ----------------------------------------------------------

// consumerHeartbeat serves ConsumerGroupHeartbeat.
func (n *Node) consumerHeartbeat(c *conn, h wire.RequestHeader, req *wire.ConsumerGroupHeartbeatRequest) {
	fail := func(code int16, msg string) {
		n.respond(c, h, &wire.ConsumerGroupHeartbeatResponse{ErrorCode: code, ErrorMessage: msg, MemberID: req.MemberID})
	}
	b := cgBeat{group: req.GroupID, member: req.MemberID, epoch: req.MemberEpoch, rebalanceMs: req.RebalanceTimeoutMs, rack: req.RackID,
		clientID: h.ClientID, host: "/" + c.host, assignor: req.ServerAssignor, hasSub: req.SubscribedTopics != nil, sub: req.SubscribedTopics}
	if req.Owned != nil {
		b.hasOwned = true
		for _, t := range req.Owned {
			for _, part := range t.Partitions {
				b.owned = append(b.owned, tpKey{t.TopicID, part})
			}
		}
		b.owned = sortTPs(b.owned)
	}
	switch {
	case !n.may(c, opRead, resGroup, req.GroupID):
		fail(wire.ErrGroupAuthorizationFailed, "")
		return
	case req.GroupID == "":
		fail(wire.ErrInvalidRequest, "GroupId can't be empty")
		return
	case req.MemberID == "" && (h.Version != 0 || req.MemberEpoch != 0):
		fail(wire.ErrInvalidRequest, "MemberId can't be empty")
		return
	case req.InstanceID != "" || req.MemberEpoch == wire.EpochRejoin:
		fail(wire.ErrInvalidRequest, "static membership is not supported")
		return
	case req.SubscribedRegex != "":
		fail(wire.ErrInvalidRequest, "subscribing by regular expression is not supported")
		return
	case req.MemberEpoch < wire.EpochRejoin:
		fail(wire.ErrInvalidRequest, "MemberEpoch is invalid")
		return
	case req.ServerAssignor != "" && !validAssignor(req.ServerAssignor):
		fail(wire.ErrUnsupportedAssignor, "server assignor "+req.ServerAssignor+" is not one of uniform and range")
		return
	case req.MemberEpoch == 0 && (req.SubscribedTopics == nil || req.RebalanceTimeoutMs <= 0):
		fail(wire.ErrInvalidRequest, "SubscribedTopicNames and RebalanceTimeoutMs must be set when joining")
		return
	case n.levelRefused(5) != "":
		fail(wire.ErrCoordinatorNotAvailable, n.levelRefused(5)) // retriable: a later heartbeat finds the cluster upgraded
		return
	}
	for _, name := range req.SubscribedTopics {
		if !n.may(c, opDescribe, resTopic, name) {
			fail(wire.ErrTopicAuthorizationFailed, "not allowed to describe topic "+name)
			return
		}
	}
	if b.epoch == 0 && b.member == "" { // version 0: the coordinator names the member
		b.member = n.newMemberID(h.ClientID)
	}
	g := n.st.group(req.GroupID)
	if len(g.members) > 0 || (g.sg != nil && len(g.sg.members) > 0) {
		fail(wire.ErrInconsistentGroupProtocol, "the group uses another protocol")
		return
	}
	sg := n.soft(req.GroupID)
	if b.epoch == 0 && (g.cg == nil || g.cg.member(b.member) == nil) && g.cg != nil && len(g.cg.members) >= n.cfg.MaxGroupSize {
		fail(wire.ErrGroupMaxSizeReached, "")
		return
	}
	if g.cg != nil {
		if m := g.cg.member(b.member); m != nil && b.epoch != wire.EpochLeave {
			n.cgTouch(sg, m) // it is alive, whatever else this heartbeat turns out to be
		}
	}
	answer := func(resp *wire.ConsumerGroupHeartbeatResponse, code int16, msg string) {
		if code != wire.ErrNone {
			n.respond(c, h, &wire.ConsumerGroupHeartbeatResponse{ErrorCode: code, ErrorMessage: msg, MemberID: b.member})
			return
		}
		resp.HeartbeatIntervalMs = int32(n.cfg.ConsumerHeartbeatInterval.Milliseconds())
		n.respond(c, h, resp)
	}
	if !n.st.cgNeedsApply(b) {
		answer(n.st.cgImmediate(b))
		return
	}
	n.propose(encodeCGBeat(b), func(res result) {
		n.cgSync(sg)
		if cur := n.st.group(req.GroupID).cg; cur != nil && res.errCode == wire.ErrNone && res.cg != nil && res.cg.MemberEpoch >= 0 {
			if m := cur.member(b.member); m != nil {
				n.cgTouch(sg, m)
			}
		}
		answer(res.cg, res.errCode, res.errMsg)
	})
}

// cgTouch restarts a member's session timer.
func (n *Node) cgTouch(sg *softGroup, m *cmember) {
	n.cancel(sg.sessions[m.id])
	id := m.id
	sg.sessions[id] = n.after(n.cfg.ConsumerSessionTimeout, func() {
		delete(sg.sessions, id)
		if cg := n.st.group(sg.name).cg; cg != nil {
			if cur := cg.member(id); cur != nil {
				n.log.Info("consumer session expired", "group", sg.name, "member", id)
				n.cgRemove(sg, cur)
			}
		}
	})
}

// cgRemove proposes to remove a member that missed a timeout.
func (n *Node) cgRemove(sg *softGroup, m *cmember) {
	n.propose(encodeCGRemove(sg.name, m.id, m.epoch), func(result) { n.cgSync(sg) })
}

// cgSync arms the timers a group's applied state calls for, and drops the
// others: a session timer for every member, and a rebalance timer for each
// member that has been asked to give up partitions.
func (n *Node) cgSync(sg *softGroup) {
	cg := n.st.group(sg.name).cg
	for _, id := range sortedKeys(sg.sessions) {
		if cg == nil || cg.member(id) == nil {
			n.cancel(sg.sessions[id])
			delete(sg.sessions, id)
		}
	}
	for _, id := range sortedKeys(sg.revoke) {
		if cg == nil || cg.member(id) == nil || !cg.member(id).revoking() {
			n.cancel(sg.revoke[id])
			delete(sg.revoke, id)
		}
	}
	if cg == nil {
		return
	}
	for _, m := range cg.members {
		if _, ok := sg.revoke[m.id]; ok || !m.revoking() {
			continue
		}
		id := m.id
		sg.revoke[id] = n.after(ms(m.rebalanceMs), func() {
			delete(sg.revoke, id)
			if cur := n.st.group(sg.name).cg; cur != nil {
				if mm := cur.member(id); mm != nil && mm.revoking() {
					n.log.Info("member did not release partitions in time", "group", sg.name, "member", id)
					n.cgRemove(sg, mm)
				}
			}
		})
	}
}

// startConsumerGroups arms the timers of a group of the consumer protocol
// when this node starts serving.
func (n *Node) startConsumerGroups(name string, cg *cgroup) {
	sg := n.soft(name)
	for _, m := range cg.members {
		n.cgTouch(sg, m)
	}
	n.cgSync(sg)
}

// consumerGroupDescribe serves ConsumerGroupDescribe.
func (n *Node) consumerGroupDescribe(c *conn, req *wire.ConsumerGroupDescribeRequest) *wire.ConsumerGroupDescribeResponse {
	resp := &wire.ConsumerGroupDescribeResponse{}
	named := func(s tps) []wire.NamedTopicPartitions {
		var out []wire.NamedTopicPartitions
		for _, t := range groupByTopic(s) {
			if name := n.st.byID[t.TopicID]; name != "" {
				out = append(out, wire.NamedTopicPartitions{TopicID: t.TopicID, TopicName: name, Partitions: t.Partitions})
			}
		}
		return out
	}
	for _, name := range req.Groups {
		dg := wire.ConsumerGroupDescribeGroup{GroupID: name}
		g := n.st.groups[name]
		switch {
		case !n.may(c, opDescribe, resGroup, name):
			dg.ErrorCode = wire.ErrGroupAuthorizationFailed
		case g == nil || g.cg == nil:
			dg.ErrorCode, dg.ErrorMessage = wire.ErrGroupIDNotFound, "group "+name+" is not a group of the consumer protocol"
		default:
			cg := g.cg
			dg.State, dg.GroupEpoch, dg.AssignmentEpoch, dg.AssignorName = cg.stateName(), cg.epoch, cg.targetEpoch, cg.assignor
			for _, m := range cg.members {
				dg.Members = append(dg.Members, wire.ConsumerGroupDescribeMember{MemberID: m.id, RackID: m.rack, MemberEpoch: m.epoch,
					ClientID: m.clientID, ClientHost: m.host, SubscribedTopics: m.subscribed,
					Assignment: named(m.visible()), Target: named(m.target), MemberType: wire.MemberConsumer})
			}
		}
		resp.Groups = append(resp.Groups, dg)
	}
	return resp
}

// describeTopicPartitions serves DescribeTopicPartitions: the same
// metadata as Metadata, a page at a time. Every node answers, from what
// it has applied.
func (n *Node) describeTopicPartitions(c *conn, req *wire.DescribeTopicPartitionsRequest) *wire.DescribeTopicPartitionsResponse {
	const most = 2000 // the broker's own bound on one response
	limit := int(req.ResponsePartitionLimit)
	if limit <= 0 || limit > most {
		limit = most
	}
	names := normalSub(req.Topics) // by name, as Kafka does
	resp := &wire.DescribeTopicPartitionsResponse{}
	leader, known := n.leader()
	var all []int32
	for _, p := range n.members() {
		all = append(all, p.ID)
	}
	start := 0
	if req.Cursor != nil {
		if i, ok := slices.BinarySearch(names, req.Cursor.TopicName); ok {
			start = i
		} else {
			start = sort.SearchStrings(names, req.Cursor.TopicName)
		}
	}
	used := 0
	for i := start; i < len(names); i++ {
		name := names[i]
		t := n.st.topics[name]
		switch {
		case !n.may(c, opDescribe, resTopic, name):
			resp.Topics = append(resp.Topics, wire.DescribedTopic{ErrorCode: wire.ErrTopicAuthorizationFailed, Name: name})
			continue
		case t == nil:
			resp.Topics = append(resp.Topics, wire.DescribedTopic{ErrorCode: wire.ErrUnknownTopicOrPartition, Name: name})
			continue
		}
		dt := wire.DescribedTopic{Name: name, TopicID: t.id}
		first := 0
		if req.Cursor != nil && i == start && req.Cursor.TopicName == name {
			first = int(max(req.Cursor.PartitionIndex, 0))
		}
		for part := first; part < len(t.parts); part++ {
			if used == limit {
				resp.NextCursor = &wire.TopicCursor{TopicName: name, PartitionIndex: int32(part)}
				break
			}
			pm := wire.DescribedPartition{Index: int32(part), Leader: leader.ID, Replicas: all, ISR: all}
			if !known {
				pm.ErrorCode = wire.ErrLeaderNotAvailable
			}
			dt.Partitions = append(dt.Partitions, pm)
			used++
		}
		resp.Topics = append(resp.Topics, dt)
		if resp.NextCursor != nil {
			break
		}
	}
	return resp
}
