package broker

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Share groups (KIP-932, docs/design/c22-share-groups.md, ADR 0034).
// All membership and per-record acquisition lock state is replicated in the
// Raft log (format level 6, snapshot version 11).

const (
	assignorSimple        = "simple"
	maxShareDeliveryCount = 5
)

type shareRecState uint8

const (
	shareAvailable shareRecState = iota
	shareAcquired
	shareAcked
	shareArchived
)

type shareLock struct {
	state     shareRecState
	delivery  int16
	owner     string
	lockUntil int64 // ms since epoch
}

type sharePart struct {
	spso  int64 // share partition start offset (all < spso are terminal)
	speo  int64 // next offset never yet acquired
	locks map[int64]*shareLock
}

type sgMember struct {
	id               string
	epoch, prevEpoch int32
	rack             string
	clientID, host   string
	subscribed       []string
	assigned         tps
}

type shareGroup struct {
	epoch   int32
	meta    []topicMeta
	members []*sgMember // sorted by id
	parts   map[tpKey]*sharePart
}

func newShareGroup() *shareGroup {
	return &shareGroup{parts: map[tpKey]*sharePart{}}
}

func (sg *shareGroup) find(id string) (int, bool) {
	return sort.Find(len(sg.members), func(i int) int { return strings.Compare(id, sg.members[i].id) })
}

func (sg *shareGroup) member(id string) *sgMember {
	if i, ok := sg.find(id); ok {
		return sg.members[i]
	}
	return nil
}

func (sg *shareGroup) insert(m *sgMember) {
	i, _ := sg.find(m.id)
	sg.members = slices.Insert(sg.members, i, m)
}

func (sg *shareGroup) remove(id string) {
	if i, ok := sg.find(id); ok {
		sg.members = slices.Delete(sg.members, i, i+1)
	}
}

func (sg *shareGroup) stateName() string {
	if len(sg.members) == 0 {
		return "Empty"
	}
	return "Stable"
}

func (sg *shareGroup) part(tp tpKey, floor int64) *sharePart {
	sp := sg.parts[tp]
	if sp == nil {
		sp = &sharePart{spso: floor, speo: floor, locks: map[int64]*shareLock{}}
		sg.parts[tp] = sp
	}
	return sp
}

func (sp *sharePart) advance(floor int64) {
	if floor > sp.spso {
		for _, off := range sortedKeys(sp.locks) {
			if off < floor {
				delete(sp.locks, off)
			} else {
				break
			}
		}
		sp.spso = floor
	}
	for {
		l := sp.locks[sp.spso]
		if l == nil || (l.state != shareAcked && l.state != shareArchived) {
			break
		}
		delete(sp.locks, sp.spso)
		sp.spso++
	}
	if sp.speo < sp.spso {
		sp.speo = sp.spso
	}
}

func (sg *shareGroup) releaseMemberLocks(memberID string, dropLock bool, s *state) {
	for _, tp := range sortedTPKeys(sg.parts) {
		sp := sg.parts[tp]
		for _, off := range sortedKeys(sp.locks) {
			l := sp.locks[off]
			if l.state == shareAcquired && l.owner == memberID {
				releaseLock(l, dropLock)
			}
		}
		var floor int64
		if t := s.topics[s.byID[tp.id]]; t != nil && int(tp.part) < len(t.parts) {
			floor = t.parts[tp.part].start
		}
		sp.advance(floor)
	}
}

func releaseLock(l *shareLock, dropLock bool) {
	if dropLock {
		l.state = shareAcked
		l.owner = ""
		l.lockUntil = 0
		return
	}
	if l.delivery >= maxShareDeliveryCount {
		l.state = shareArchived
	} else {
		l.state = shareAvailable
	}
	l.owner = ""
	l.lockUntil = 0
}

func sortedTPKeys[V any](m map[tpKey]V) []tpKey {
	keys := make([]tpKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, cmpTP)
	return keys
}

func (s *state) sgSubscribedTopics(sg *shareGroup) []topicInfo {
	seen := map[string]bool{}
	var names []string
	for _, m := range sg.members {
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

func (s *state) sgReassign(sg *shareGroup) {
	topics := s.sgSubscribedTopics(sg)
	sg.meta = metaOf(topics)
	for _, m := range sg.members {
		var assigned tps
		for _, t := range topics {
			if slices.Contains(m.subscribed, t.name) {
				for p := range t.parts {
					assigned = append(assigned, tpKey{t.id, p})
				}
			}
		}
		m.assigned = sortTPs(assigned)
	}
}

// --- Commands: sgBeat, sgRemove, shareAcquire, shareAck, shareExpire -------

type sgBeat struct {
	group, member  string
	epoch          int32
	rack           string
	clientID, host string
	hasSub         bool
	sub            []string
}

func encodeSGBeat(b sgBeat) []byte {
	w := wire.Writer{B: []byte{cmdSGBeat}}
	w.String(false, b.group)
	w.String(false, b.member)
	w.Int32(b.epoch)
	w.String(false, b.rack)
	w.String(false, b.clientID)
	w.String(false, b.host)
	w.Bool(b.hasSub)
	if b.hasSub {
		w.ArrayLen(false, len(b.sub))
		for _, s := range b.sub {
			w.String(false, s)
		}
	}
	return w.B
}

func decodeSGBeat(r *wire.Reader) (sgBeat, bool) {
	b := sgBeat{
		group: r.String(false), member: r.String(false), epoch: r.Int32(),
		rack: r.String(false), clientID: r.String(false), host: r.String(false),
		hasSub: r.Bool(),
	}
	if b.hasSub {
		b.sub = wire.Array(r, false, func() string { return r.String(false) })
	}
	return b, r.Err() == nil && r.Len() == 0
}

func encodeSGRemove(group, member string, epoch int32) []byte {
	w := wire.Writer{B: []byte{cmdSGRemove}}
	w.String(false, group)
	w.String(false, member)
	w.Int32(epoch)
	return w.B
}

type shareAcquireReq struct {
	group, member string
	nowMs         int64
	lockTimeoutMs int64
	maxRecords    int32
	tps           []tpKey
}

func encodeShareAcquire(req shareAcquireReq) []byte {
	w := wire.Writer{B: []byte{cmdShareAcquire}}
	w.String(false, req.group)
	w.String(false, req.member)
	w.Int64(req.nowMs)
	w.Int64(req.lockTimeoutMs)
	w.Int32(req.maxRecords)
	w.ArrayLen(false, len(req.tps))
	for _, tp := range req.tps {
		w.UUID(tp.id)
		w.Int32(tp.part)
	}
	return w.B
}

func decodeShareAcquire(r *wire.Reader) (shareAcquireReq, bool) {
	req := shareAcquireReq{
		group: r.String(false), member: r.String(false),
		nowMs: r.Int64(), lockTimeoutMs: r.Int64(), maxRecords: r.Int32(),
	}
	req.tps = wire.Array(r, false, func() tpKey { return tpKey{r.UUID(), r.Int32()} })
	return req, r.Err() == nil && r.Len() == 0
}

type shareAckTopic struct {
	tp      tpKey
	batches []wire.ShareAckBatch
}

type shareAckCmd struct {
	group, member string
	nowMs         int64
	lockTimeoutMs int64
	topics        []shareAckTopic
}

func encodeShareAck(cmd shareAckCmd) []byte {
	w := wire.Writer{B: []byte{cmdShareAck}}
	w.String(false, cmd.group)
	w.String(false, cmd.member)
	w.Int64(cmd.nowMs)
	w.Int64(cmd.lockTimeoutMs)
	w.ArrayLen(false, len(cmd.topics))
	for _, t := range cmd.topics {
		w.UUID(t.tp.id)
		w.Int32(t.tp.part)
		w.ArrayLen(false, len(t.batches))
		for _, b := range t.batches {
			w.Int64(b.FirstOffset)
			w.Int64(b.LastOffset)
			w.ArrayLen(false, len(b.AcknowledgeTypes))
			for _, ty := range b.AcknowledgeTypes {
				w.Int8(ty)
			}
		}
	}
	return w.B
}

func decodeShareAck(r *wire.Reader) (shareAckCmd, bool) {
	cmd := shareAckCmd{
		group: r.String(false), member: r.String(false),
		nowMs: r.Int64(), lockTimeoutMs: r.Int64(),
	}
	cmd.topics = wire.Array(r, false, func() shareAckTopic {
		t := shareAckTopic{tp: tpKey{r.UUID(), r.Int32()}}
		t.batches = wire.Array(r, false, func() wire.ShareAckBatch {
			return wire.ShareAckBatch{
				FirstOffset:      r.Int64(),
				LastOffset:       r.Int64(),
				AcknowledgeTypes: wire.Array(r, false, r.Int8),
			}
		})
		return t
	})
	return cmd, r.Err() == nil && r.Len() == 0
}

func encodeShareExpire(nowMs int64) []byte {
	w := wire.Writer{B: []byte{cmdShareExpire}}
	w.Int64(nowMs)
	return w.B
}

// --- Apply -----------------------------------------------------------------

func (s *state) sgNeedsApply(b sgBeat) bool {
	g := s.groups[b.group]
	if g == nil || g.sg == nil {
		return b.epoch == 0
	}
	sg := g.sg
	m := sg.member(b.member)
	switch {
	case b.epoch == 0 || b.epoch == wire.EpochLeave:
		return b.epoch == 0 || m != nil
	case m == nil || b.epoch != m.epoch:
		return false
	case b.hasSub && !slices.Equal(m.subscribed, normalSub(b.sub)),
		b.rack != "" && b.rack != m.rack,
		!slices.Equal(sg.meta, metaOf(s.sgSubscribedTopics(sg))),
		m.epoch != sg.epoch:
		return true
	}
	return false
}

func (s *state) sgImmediate(b sgBeat) (*wire.ShareGroupHeartbeatResponse, int16, string) {
	g := s.groups[b.group]
	var m *sgMember
	if g != nil && g.sg != nil {
		m = g.sg.member(b.member)
	}
	switch {
	case m == nil:
		return nil, wire.ErrUnknownMemberID, "the member is not in the share group"
	case b.epoch == m.epoch:
		return &wire.ShareGroupHeartbeatResponse{MemberID: m.id, MemberEpoch: m.epoch}, wire.ErrNone, ""
	case b.epoch == m.prevEpoch:
		return &wire.ShareGroupHeartbeatResponse{MemberID: m.id, MemberEpoch: m.epoch, HasAssignment: true, Assignment: groupByTopic(m.assigned)}, wire.ErrNone, ""
	}
	return nil, wire.ErrFencedMemberEpoch, fmt.Sprintf("member epoch %d is not the current %d", b.epoch, m.epoch)
}

func (s *state) applySGBeat(b sgBeat) result {
	g := s.groups[b.group]
	if g != nil && (len(g.members) > 0 || g.cg != nil) {
		return result{errCode: wire.ErrInconsistentGroupProtocol, errMsg: "the group uses another protocol"}
	}
	if g == nil || g.sg == nil {
		if b.epoch != 0 {
			return result{errCode: wire.ErrUnknownMemberID, errMsg: "the member is not in the share group"}
		}
		g = s.forWrite(b.group)
		g.sg = newShareGroup()
	}
	sg := g.sg
	m := sg.member(b.member)
	changed, joined := false, false
	switch {
	case b.epoch == wire.EpochLeave:
		if m == nil {
			return result{errCode: wire.ErrUnknownMemberID, errMsg: "the member is not in the share group"}
		}
		sg.releaseMemberLocks(m.id, s.dropShareLock, s)
		sg.remove(m.id)
		sg.epoch++
		s.sgReassign(sg)
		return result{sgBeat: &wire.ShareGroupHeartbeatResponse{MemberID: m.id, MemberEpoch: -1}}
	case b.epoch == 0:
		if m == nil {
			m = &sgMember{id: b.member}
			sg.insert(m)
		} else {
			sg.releaseMemberLocks(m.id, s.dropShareLock, s)
			m.epoch, m.prevEpoch = 0, 0
		}
		changed, joined = true, true
	case m == nil:
		return result{errCode: wire.ErrUnknownMemberID, errMsg: "the member is not in the share group"}
	case b.epoch != m.epoch:
		resp, code, msg := s.sgImmediate(b)
		return result{sgBeat: resp, errCode: code, errMsg: msg}
	}
	if b.hasSub {
		if names := normalSub(b.sub); !slices.Equal(m.subscribed, names) {
			m.subscribed, changed = names, true
		}
	}
	if b.rack != "" {
		m.rack = b.rack
	}
	if b.clientID != "" {
		m.clientID = b.clientID
	}
	if b.host != "" {
		m.host = b.host
	}
	if !slices.Equal(sg.meta, metaOf(s.sgSubscribedTopics(sg))) {
		changed = true
	}
	if changed {
		sg.epoch++
		s.sgReassign(sg)
	}
	grew := joined || changed || m.epoch != sg.epoch
	if m.epoch != sg.epoch {
		m.prevEpoch, m.epoch = m.epoch, sg.epoch
	}
	resp := &wire.ShareGroupHeartbeatResponse{MemberID: m.id, MemberEpoch: m.epoch}
	if grew {
		resp.HasAssignment, resp.Assignment = true, groupByTopic(m.assigned)
	}
	return result{sgBeat: resp}
}

func (s *state) applySGRemove(group, member string, epoch int32) result {
	g := s.groups[group]
	if g == nil || g.sg == nil {
		return result{errCode: wire.ErrUnknownMemberID}
	}
	m := g.sg.member(member)
	if m == nil || m.epoch != epoch {
		return result{errCode: wire.ErrUnknownMemberID}
	}
	g.sg.releaseMemberLocks(member, s.dropShareLock, s)
	g.sg.remove(member)
	g.sg.epoch++
	s.sgReassign(g.sg)
	return result{}
}

func (s *state) applyShareAcquire(req shareAcquireReq) result {
	g := s.groups[req.group]
	if g == nil || g.sg == nil {
		g = s.forWrite(req.group)
		g.sg = newShareGroup()
	}
	sg := g.sg
	budget := int(req.maxRecords)
	if budget <= 0 {
		budget = 500
	}
	acquired := make(map[tpKey][]wire.AcquiredRange, len(req.tps))
	for _, tp := range req.tps {
		if budget <= 0 {
			break
		}
		name := s.byID[tp.id]
		p := s.partition(name, tp.part)
		if p == nil {
			continue
		}
		sp := sg.part(tp, p.start)
		sp.advance(p.start)
		// Expire any stale locks along the way deterministically.
		for _, off := range sortedKeys(sp.locks) {
			if l := sp.locks[off]; l.state == shareAcquired && l.lockUntil <= req.nowMs {
				releaseLock(l, s.dropShareLock)
			}
		}
		sp.advance(p.start)

		var ranges []wire.AcquiredRange
		for off := max(sp.spso, p.start); off < p.next && budget > 0; off++ {
			l := sp.locks[off]
			if l != nil && l.state != shareAvailable {
				continue
			}
			if l == nil {
				l = &shareLock{}
				sp.locks[off] = l
			}
			l.state = shareAcquired
			l.delivery++
			l.owner = req.member
			l.lockUntil = req.nowMs + req.lockTimeoutMs
			if off >= sp.speo {
				sp.speo = off + 1
			}
			budget--
			if n := len(ranges); n > 0 && ranges[n-1].LastOffset+1 == off && ranges[n-1].DeliveryCount == l.delivery {
				ranges[n-1].LastOffset = off
			} else {
				ranges = append(ranges, wire.AcquiredRange{FirstOffset: off, LastOffset: off, DeliveryCount: l.delivery})
			}
		}
		if len(ranges) > 0 {
			acquired[tp] = ranges
		}
	}
	return result{shareAcquired: acquired}
}

func (s *state) applyShareAck(cmd shareAckCmd) result {
	g := s.groups[cmd.group]
	if g == nil || g.sg == nil {
		return result{errCode: wire.ErrGroupIDNotFound}
	}
	sg := g.sg
	errs := make(map[tpKey]int16, len(cmd.topics))
	for _, t := range cmd.topics {
		name := s.byID[t.tp.id]
		p := s.partition(name, t.tp.part)
		if p == nil {
			errs[t.tp] = wire.ErrUnknownTopicOrPartition
			continue
		}
		sp := sg.part(t.tp, p.start)
		// Validate every batch in the partition before applying any.
		code := wire.ErrNone
		for _, b := range t.batches {
			if b.LastOffset < b.FirstOffset || len(b.AcknowledgeTypes) == 0 || (len(b.AcknowledgeTypes) > 1 && int64(len(b.AcknowledgeTypes)) != b.LastOffset-b.FirstOffset+1) {
				code = wire.ErrInvalidRequest
				break
			}
			for off := b.FirstOffset; off <= b.LastOffset; off++ {
				ty := b.AcknowledgeTypes[0]
				if len(b.AcknowledgeTypes) > 1 {
					ty = b.AcknowledgeTypes[off-b.FirstOffset]
				}
				if ty == wire.AckGap {
					continue
				}
				l := sp.locks[off]
				if l == nil || l.state != shareAcquired || l.owner != cmd.member {
					code = wire.ErrInvalidRecordState
					break
				}
			}
			if code != wire.ErrNone {
				break
			}
		}
		if code != wire.ErrNone {
			errs[t.tp] = code
			continue
		}
		for _, b := range t.batches {
			for off := b.FirstOffset; off <= b.LastOffset; off++ {
				ty := b.AcknowledgeTypes[0]
				if len(b.AcknowledgeTypes) > 1 {
					ty = b.AcknowledgeTypes[off-b.FirstOffset]
				}
				l := sp.locks[off]
				if ty == wire.AckGap {
					if off >= sp.spso {
						if l == nil {
							l = &shareLock{}
							sp.locks[off] = l
						}
						l.state, l.owner, l.lockUntil = shareAcked, "", 0
					}
					continue
				}
				switch ty {
				case wire.AckAccept:
					l.state, l.owner, l.lockUntil = shareAcked, "", 0
				case wire.AckRelease:
					releaseLock(l, s.dropShareLock)
				case wire.AckReject:
					l.state, l.owner, l.lockUntil = shareArchived, "", 0
				case wire.AckRenew:
					l.lockUntil = cmd.nowMs + cmd.lockTimeoutMs
				}
			}
		}
		sp.advance(p.start)
	}
	return result{shareAckErrs: errs}
}

func (s *state) applyShareExpire(nowMs int64) result {
	released := false
	for _, gname := range sortedKeys(s.groups) {
		sg := s.groups[gname].sg
		if sg == nil {
			continue
		}
		for _, tp := range sortedTPKeys(sg.parts) {
			sp := sg.parts[tp]
			for _, off := range sortedKeys(sp.locks) {
				l := sp.locks[off]
				if l.state == shareAcquired && l.lockUntil <= nowMs {
					releaseLock(l, s.dropShareLock)
					released = true
				}
			}
			var floor int64
			if t := s.topics[s.byID[tp.id]]; t != nil && int(tp.part) < len(t.parts) {
				floor = t.parts[tp.part].start
			}
			sp.advance(floor)
		}
	}
	return result{applied: released}
}

// --- Handlers --------------------------------------------------------------

type shareSessionKey struct{ group, member string }

type shareSession struct {
	epoch int32
	parts []tpKey
}

type parkedShareFetch struct {
	c       *conn
	h       wire.RequestHeader
	req     *wire.ShareFetchRequest
	tps     []tpKey
	ackErrs map[tpKey]int16
	timer   sys.TimerID
}

func (n *Node) nowMs() int64 { return int64(n.clock.Now()) / int64(time.Millisecond) }

func (n *Node) shareGroupHeartbeat(c *conn, h wire.RequestHeader, req *wire.ShareGroupHeartbeatRequest) {
	fail := func(code int16, msg string) {
		n.respond(c, h, &wire.ShareGroupHeartbeatResponse{ErrorCode: code, ErrorMessage: msg, MemberID: req.MemberID})
	}
	switch {
	case !n.may(c, opRead, resGroup, req.GroupID):
		fail(wire.ErrGroupAuthorizationFailed, "")
		return
	case req.GroupID == "" || req.MemberID == "":
		fail(wire.ErrInvalidRequest, "GroupId and MemberId are required")
		return
	case req.MemberEpoch < wire.EpochLeave:
		fail(wire.ErrInvalidRequest, "MemberEpoch is invalid")
		return
	case n.levelRefused(6) != "":
		fail(wire.ErrCoordinatorNotAvailable, n.levelRefused(6))
		return
	}
	for _, name := range req.SubscribedTopics {
		if !n.may(c, opDescribe, resTopic, name) {
			fail(wire.ErrTopicAuthorizationFailed, "not allowed to describe topic "+name)
			return
		}
	}
	g := n.st.group(req.GroupID)
	if len(g.members) > 0 || g.cg != nil {
		fail(wire.ErrInconsistentGroupProtocol, "the group uses another protocol")
		return
	}
	b := sgBeat{
		group: req.GroupID, member: req.MemberID, epoch: req.MemberEpoch,
		rack: req.RackID, clientID: h.ClientID, host: "/" + c.host,
		hasSub: req.SubscribedTopics != nil, sub: req.SubscribedTopics,
	}
	sgSoft := n.soft(req.GroupID)
	if g.sg != nil {
		if m := g.sg.member(b.member); m != nil && b.epoch != wire.EpochLeave {
			n.sgTouch(sgSoft, m)
		}
	}
	answer := func(resp *wire.ShareGroupHeartbeatResponse, code int16, msg string) {
		if code != wire.ErrNone {
			n.respond(c, h, &wire.ShareGroupHeartbeatResponse{ErrorCode: code, ErrorMessage: msg, MemberID: b.member})
			return
		}
		resp.HeartbeatIntervalMs = int32(n.cfg.ConsumerHeartbeatInterval.Milliseconds())
		n.respond(c, h, resp)
	}
	if !n.st.sgNeedsApply(b) {
		answer(n.st.sgImmediate(b))
		return
	}
	n.propose(encodeSGBeat(b), func(res result) {
		n.sgSync(sgSoft)
		if cur := n.st.group(req.GroupID).sg; cur != nil && res.errCode == wire.ErrNone && res.sgBeat != nil && res.sgBeat.MemberEpoch >= 0 {
			if m := cur.member(b.member); m != nil {
				n.sgTouch(sgSoft, m)
			}
		}
		n.wakeShareFetches()
		n.armShareLockTimer()
		answer(res.sgBeat, res.errCode, res.errMsg)
	})
}

func (n *Node) sgTouch(sg *softGroup, m *sgMember) {
	n.cancel(sg.sessions[m.id])
	id := m.id
	sg.sessions[id] = n.after(n.cfg.ConsumerSessionTimeout, func() {
		delete(sg.sessions, id)
		if curGroup := n.st.group(sg.name).sg; curGroup != nil {
			if cur := curGroup.member(id); cur != nil {
				n.log.Info("share member session expired", "group", sg.name, "member", id)
				n.propose(encodeSGRemove(sg.name, cur.id, cur.epoch), func(result) {
					n.sgSync(sg)
					n.wakeShareFetches()
					n.armShareLockTimer()
				})
			}
		}
	})
}

func (n *Node) sgSync(sg *softGroup) {
	sgroup := n.st.group(sg.name).sg
	for _, id := range sortedKeys(sg.sessions) {
		if sgroup == nil || sgroup.member(id) == nil {
			n.cancel(sg.sessions[id])
			delete(sg.sessions, id)
		}
	}
}

func (n *Node) shareGroupDescribe(c *conn, req *wire.ShareGroupDescribeRequest) *wire.ShareGroupDescribeResponse {
	resp := &wire.ShareGroupDescribeResponse{}
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
		dg := wire.ShareGroupDescribeGroup{GroupID: name}
		g := n.st.groups[name]
		switch {
		case !n.may(c, opDescribe, resGroup, name):
			dg.ErrorCode = wire.ErrGroupAuthorizationFailed
		case g == nil || g.sg == nil:
			dg.ErrorCode, dg.ErrorMessage = wire.ErrGroupIDNotFound, "group "+name+" is not a share group"
		default:
			sg := g.sg
			dg.State, dg.GroupEpoch, dg.AssignmentEpoch, dg.AssignorName = sg.stateName(), sg.epoch, sg.epoch, assignorSimple
			for _, m := range sg.members {
				dg.Members = append(dg.Members, wire.ShareGroupDescribeMember{
					MemberID: m.id, RackID: m.rack, MemberEpoch: m.epoch,
					ClientID: m.clientID, ClientHost: m.host,
					SubscribedTopics: m.subscribed, Assignment: named(m.assigned),
				})
			}
		}
		resp.Groups = append(resp.Groups, dg)
	}
	return resp
}

func (n *Node) validateShareSession(groupID, memberID string, epoch int32, update bool, add []tpKey, forget []tpKey) (tps []tpKey, code int16, msg string) {
	key := shareSessionKey{groupID, memberID}
	sess := n.shareSessions[key]
	switch {
	case epoch == 0:
		sess = &shareSession{epoch: 1}
		n.shareSessions[key] = sess
	case epoch == -1:
		if sess == nil {
			return nil, wire.ErrShareSessionNotFound, "share session not found"
		}
		tps = slices.Clone(sess.parts)
		if update {
			delete(n.shareSessions, key)
		}
		return tps, wire.ErrNone, ""
	case epoch < -1:
		return nil, wire.ErrInvalidShareSessionEpoch, "invalid share session epoch"
	case sess == nil:
		return nil, wire.ErrShareSessionNotFound, "share session not found"
	case epoch != sess.epoch:
		return nil, wire.ErrInvalidShareSessionEpoch, fmt.Sprintf("expected share session epoch %d, got %d", sess.epoch, epoch)
	default:
		if update {
			sess.epoch++
		}
	}
	if update {
		for _, tp := range add {
			if !slices.Contains(sess.parts, tp) {
				sess.parts = append(sess.parts, tp)
			}
		}
		for _, tp := range forget {
			sess.parts = slices.DeleteFunc(sess.parts, func(x tpKey) bool { return x == tp })
		}
		slices.SortFunc(sess.parts, cmpTP)
	}
	return slices.Clone(sess.parts), wire.ErrNone, ""
}

func (n *Node) shareFetch(c *conn, h wire.RequestHeader, req *wire.ShareFetchRequest) {
	if !n.may(c, opRead, resGroup, req.GroupID) {
		n.respond(c, h, &wire.ShareFetchResponse{ErrorCode: wire.ErrGroupAuthorizationFailed})
		return
	}
	if n.levelRefused(6) != "" {
		n.respond(c, h, &wire.ShareFetchResponse{ErrorCode: wire.ErrCoordinatorNotAvailable, ErrorMessage: n.levelRefused(6)})
		return
	}
	var add, forget []tpKey
	var ackTopics []shareAckTopic
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			tp := tpKey{t.TopicID, p.Index}
			add = append(add, tp)
			if len(p.AckBatches) > 0 {
				ackTopics = append(ackTopics, shareAckTopic{tp: tp, batches: p.AckBatches})
			}
		}
	}
	for _, f := range req.ForgottenTopics {
		for _, idx := range f.Partitions {
			forget = append(forget, tpKey{f.TopicID, idx})
		}
	}
	sessionTPs, code, msg := n.validateShareSession(req.GroupID, req.MemberID, req.ShareSessionEpoch, true, add, forget)
	if code != wire.ErrNone {
		n.respond(c, h, &wire.ShareFetchResponse{ErrorCode: code, ErrorMessage: msg})
		return
	}
	runFetch := func(ackErrs map[tpKey]int16) {
		if req.ShareSessionEpoch == -1 || (req.MaxRecords == 0 && req.MaxBytes == 0) {
			n.respond(c, h, n.buildShareFetchResp(c, req, sessionTPs, nil, ackErrs))
			return
		}
		if n.hasAvailableShareRecords(c, req.GroupID, sessionTPs) {
			n.acquireAndRespondShareFetch(c, h, req, sessionTPs, ackErrs)
			return
		}
		if req.MaxWaitMs <= 0 || n.fatal != nil {
			n.respond(c, h, n.buildShareFetchResp(c, req, sessionTPs, nil, ackErrs))
			return
		}
		pf := &parkedShareFetch{c: c, h: h, req: req, tps: sessionTPs, ackErrs: ackErrs, timer: n.nextTimer}
		n.nextTimer++
		n.parkedShare = append(n.parkedShare, pf)
		n.clock.SetTimer(pf.timer, time.Duration(req.MaxWaitMs)*time.Millisecond)
	}
	if len(ackTopics) > 0 {
		cmd := shareAckCmd{
			group: req.GroupID, member: req.MemberID,
			nowMs: n.nowMs(), lockTimeoutMs: n.cfg.ShareAcquisitionLockTimeout.Milliseconds(),
			topics: ackTopics,
		}
		n.propose(encodeShareAck(cmd), func(res result) {
			n.wakeShareFetches()
			n.armShareLockTimer()
			runFetch(res.shareAckErrs)
		})
		return
	}
	runFetch(nil)
}

func (n *Node) hasAvailableShareRecords(c *conn, groupID string, tps []tpKey) bool {
	nowMs := n.nowMs()
	var sg *shareGroup
	if g := n.st.groups[groupID]; g != nil {
		sg = g.sg
	}
	for _, tp := range tps {
		name := n.st.byID[tp.id]
		if name == "" || !n.may(c, opRead, resTopic, name) {
			return true // answer error immediately
		}
		p := n.st.partition(name, tp.part)
		if p == nil {
			return true // answer error immediately
		}
		if sg == nil {
			if p.next > p.start {
				return true
			}
			continue
		}
		sp := sg.parts[tp]
		spso := p.start
		if sp != nil {
			spso = max(sp.spso, p.start)
		}
		for off := spso; off < p.next; off++ {
			if sp == nil {
				return true
			}
			l := sp.locks[off]
			if l == nil || l.state == shareAvailable || (l.state == shareAcquired && l.lockUntil <= nowMs) {
				return true
			}
		}
	}
	return false
}

func (n *Node) acquireAndRespondShareFetch(c *conn, h wire.RequestHeader, req *wire.ShareFetchRequest, sessionTPs []tpKey, ackErrs map[tpKey]int16) {
	var validTPs []tpKey
	for _, tp := range sessionTPs {
		name := n.st.byID[tp.id]
		if name != "" && n.may(c, opRead, resTopic, name) && n.st.partition(name, tp.part) != nil {
			validTPs = append(validTPs, tp)
		}
	}
	if len(validTPs) == 0 {
		n.respond(c, h, n.buildShareFetchResp(c, req, sessionTPs, nil, ackErrs))
		return
	}
	acq := shareAcquireReq{
		group: req.GroupID, member: req.MemberID,
		nowMs: n.nowMs(), lockTimeoutMs: n.cfg.ShareAcquisitionLockTimeout.Milliseconds(),
		maxRecords: req.MaxRecords, tps: validTPs,
	}
	n.propose(encodeShareAcquire(acq), func(res result) {
		n.armShareLockTimer()
		n.respond(c, h, n.buildShareFetchResp(c, req, sessionTPs, res.shareAcquired, ackErrs))
	})
}

func (n *Node) buildShareFetchResp(c *conn, req *wire.ShareFetchRequest, sessionTPs []tpKey, acquired map[tpKey][]wire.AcquiredRange, ackErrs map[tpKey]int16) *wire.ShareFetchResponse {
	resp := &wire.ShareFetchResponse{AcquisitionLockTimeoutMs: int32(n.cfg.ShareAcquisitionLockTimeout.Milliseconds())}
	budget := n.cfg.MaxFetchBytes
	if req.MaxBytes > 0 && int(req.MaxBytes) < budget {
		budget = int(req.MaxBytes)
	}
	byTopic := map[wire.UUID][]int32{}
	var topicOrder []wire.UUID
	for _, tp := range sessionTPs {
		if _, ok := byTopic[tp.id]; !ok {
			topicOrder = append(topicOrder, tp.id)
		}
		byTopic[tp.id] = append(byTopic[tp.id], tp.part)
	}
	for _, t := range req.Topics {
		if _, ok := byTopic[t.TopicID]; !ok {
			topicOrder = append(topicOrder, t.TopicID)
			for _, p := range t.Partitions {
				byTopic[t.TopicID] = append(byTopic[t.TopicID], p.Index)
			}
		}
	}
	used := 0
	for _, tid := range topicOrder {
		name := n.st.byID[tid]
		tr := wire.ShareFetchTopicResponse{TopicID: tid}
		for _, partIdx := range byTopic[tid] {
			tp := tpKey{tid, partIdx}
			pr := wire.ShareFetchPartitionResponse{
				Index: partIdx, LeaderID: n.cfg.NodeID, LeaderEpoch: int32(n.raft.Term()),
				AckErrorCode: ackErrs[tp],
			}
			p := n.st.partition(name, partIdx)
			switch {
			case name == "":
				pr.ErrorCode = wire.ErrUnknownTopicID
			case !n.may(c, opRead, resTopic, name):
				pr.ErrorCode = wire.ErrTopicAuthorizationFailed
			case p == nil:
				pr.ErrorCode = wire.ErrUnknownTopicOrPartition
			default:
				ranges := acquired[tp]
				if len(ranges) > 0 {
					firstOff := ranges[0].FirstOffset
					lastOff := ranges[len(ranges)-1].LastOffset
					pr.Records = n.readBatchesSpan(p, firstOff, lastOff, budget-used, used == 0)
					used += len(pr.Records)
					pr.Acquired = ranges
				}
			}
			if pr.ErrorCode != 0 || pr.AckErrorCode != 0 || len(pr.Acquired) > 0 {
				tr.Partitions = append(tr.Partitions, pr)
			}
		}
		if len(tr.Partitions) > 0 {
			resp.Responses = append(resp.Responses, tr)
		}
	}
	return resp
}

// readBatchesSpan returns batches covering offsets [firstOff, lastOff] up to limit.
func (n *Node) readBatchesSpan(p *partition, firstOff, lastOff int64, limit int, force bool) []byte {
	i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].base > firstOff }) - 1
	if i < 0 {
		i = 0
	}
	var out []byte
	for ; i < len(p.batches); i++ {
		ref := p.batches[i]
		if ref.base > lastOff {
			break
		}
		cmd, _, err := n.raft.Read(ref.logIndex)
		if err != nil {
			if n.fatal == nil {
				n.fatal = fmt.Errorf("read committed batch at log index %d: %w", ref.logIndex, err)
			}
			return out
		}
		batch := appendBatch(cmd)
		if len(out)+len(batch) > limit && (len(out) > 0 || !force) {
			break
		}
		start := len(out)
		out = append(out, batch...)
		wire.SetBatchBaseOffset(out[start:], ref.base)
	}
	return out
}

func (n *Node) shareAcknowledge(c *conn, h wire.RequestHeader, req *wire.ShareAcknowledgeRequest) {
	if !n.may(c, opRead, resGroup, req.GroupID) {
		n.respond(c, h, &wire.ShareAcknowledgeResponse{ErrorCode: wire.ErrGroupAuthorizationFailed})
		return
	}
	if n.levelRefused(6) != "" {
		n.respond(c, h, &wire.ShareAcknowledgeResponse{ErrorCode: wire.ErrCoordinatorNotAvailable, ErrorMessage: n.levelRefused(6)})
		return
	}
	_, code, msg := n.validateShareSession(req.GroupID, req.MemberID, req.ShareSessionEpoch, true, nil, nil)
	if code != wire.ErrNone {
		n.respond(c, h, &wire.ShareAcknowledgeResponse{ErrorCode: code, ErrorMessage: msg})
		return
	}
	var topics []shareAckTopic
	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			topics = append(topics, shareAckTopic{tp: tpKey{t.TopicID, p.Index}, batches: p.AckBatches})
		}
	}
	finish := func(ackErrs map[tpKey]int16) {
		resp := &wire.ShareAcknowledgeResponse{AcquisitionLockTimeoutMs: int32(n.cfg.ShareAcquisitionLockTimeout.Milliseconds())}
		for _, t := range req.Topics {
			tr := wire.ShareAckTopicResponse{TopicID: t.TopicID}
			name := n.st.byID[t.TopicID]
			for _, p := range t.Partitions {
				tp := tpKey{t.TopicID, p.Index}
				pr := wire.ShareAckPartitionResponse{
					Index: p.Index, LeaderID: n.cfg.NodeID, LeaderEpoch: int32(n.raft.Term()),
					ErrorCode: ackErrs[tp],
				}
				if name == "" {
					pr.ErrorCode = wire.ErrUnknownTopicID
				} else if !n.may(c, opRead, resTopic, name) {
					pr.ErrorCode = wire.ErrTopicAuthorizationFailed
				}
				tr.Partitions = append(tr.Partitions, pr)
			}
			resp.Responses = append(resp.Responses, tr)
		}
		n.respond(c, h, resp)
	}
	if len(topics) == 0 {
		finish(nil)
		return
	}
	cmd := shareAckCmd{
		group: req.GroupID, member: req.MemberID,
		nowMs: n.nowMs(), lockTimeoutMs: n.cfg.ShareAcquisitionLockTimeout.Milliseconds(),
		topics: topics,
	}
	n.propose(encodeShareAck(cmd), func(res result) {
		n.wakeShareFetches()
		n.armShareLockTimer()
		finish(res.shareAckErrs)
	})
}

func (n *Node) completeShareFetch(pf *parkedShareFetch) {
	for i, g := range n.parkedShare {
		if g == pf {
			n.parkedShare = append(n.parkedShare[:i], n.parkedShare[i+1:]...)
			break
		}
	}
	n.clock.CancelTimer(pf.timer)
	if !n.canRead() {
		n.respond(pf.c, pf.h, refusal(pf.req))
		return
	}
	if n.hasAvailableShareRecords(pf.c, pf.req.GroupID, pf.tps) {
		n.acquireAndRespondShareFetch(pf.c, pf.h, pf.req, pf.tps, pf.ackErrs)
		return
	}
	n.respond(pf.c, pf.h, n.buildShareFetchResp(pf.c, pf.req, pf.tps, nil, pf.ackErrs))
}

func (n *Node) wakeShareFetches() {
	for _, pf := range append([]*parkedShareFetch(nil), n.parkedShare...) {
		if pf.c.closed || !n.hasAvailableShareRecords(pf.c, pf.req.GroupID, pf.tps) {
			continue
		}
		n.completeShareFetch(pf)
	}
}

func (n *Node) armShareLockTimer() {
	if !n.serving || n.fatal != nil {
		return
	}
	var earliest int64
	for _, gname := range sortedKeys(n.st.groups) {
		sg := n.st.groups[gname].sg
		if sg == nil {
			continue
		}
		for _, tp := range sortedTPKeys(sg.parts) {
			for _, off := range sortedKeys(sg.parts[tp].locks) {
				if l := sg.parts[tp].locks[off]; l.state == shareAcquired && (earliest == 0 || l.lockUntil < earliest) {
					earliest = l.lockUntil
				}
			}
		}
	}
	n.cancel(n.shareLockTimer)
	n.shareLockTimer = 0
	if earliest == 0 {
		return
	}
	delay := max(time.Duration(earliest-n.nowMs())*time.Millisecond, 0)
	n.shareLockTimer = n.after(delay, func() {
		n.shareLockTimer = 0
		if !n.serving || n.fatal != nil {
			return
		}
		n.propose(encodeShareExpire(n.nowMs()), func(res result) {
			if res.applied {
				n.wakeShareFetches()
			}
			n.armShareLockTimer()
		})
	})
}

func (n *Node) startShareGroups(name string, sg *shareGroup) {
	sgSoft := n.soft(name)
	for _, m := range sg.members {
		n.sgTouch(sgSoft, m)
	}
	n.armShareLockTimer()
}

// ShareGroupCheck verifies share group state invariants:
// every acquired record has a valid owner or is bounded by delivery count.
func (n *Node) ShareGroupCheck() error {
	for _, name := range sortedKeys(n.st.groups) {
		sg := n.st.groups[name].sg
		if sg == nil {
			continue
		}
		for _, tp := range sortedTPKeys(sg.parts) {
			sp := sg.parts[tp]
			for _, off := range sortedKeys(sp.locks) {
				if off < sp.spso {
					return fmt.Errorf("SG: lock at offset %d < spso %d in group %s", off, sp.spso, name)
				}
				if l := sp.locks[off]; l.state == shareAcquired && l.owner == "" {
					return fmt.Errorf("SG: acquired offset %d in group %s has empty owner", off, name)
				}
			}
		}
	}
	return nil
}

// Unused helper guard for cmp/bytes imports if needed.
var _ = cmp.Compare[int]
var _ = bytes.Compare
