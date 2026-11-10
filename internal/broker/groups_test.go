package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Every group test uses group "g", topic "t" partition 0, a 10 s session
// timeout and a 30 s rebalance timeout.

func joinReq(id string, protocols ...string) *wire.JoinGroupRequest {
	req := &wire.JoinGroupRequest{GroupID: "g", SessionTimeoutMs: 10_000, RebalanceTimeoutMs: 30_000, MemberID: id, ProtocolType: "consumer"}
	for _, p := range protocols {
		req.Protocols = append(req.Protocols, wire.GroupProtocol{Name: p, Metadata: []byte("sub-" + p)})
	}
	return req
}

// joinHeld sends a JoinGroup that is expected to be held.
func (h *harness) joinHeld(c sys.ConnID, id string) int32 {
	h.t.Helper()
	corr := h.send(c, wire.KeyJoinGroup, 5, joinReq(id, "range"))
	h.run()
	if len(h.net.sent[c]) != 0 {
		h.t.Fatalf("JoinGroup of %q on conn %d was answered, want held", id, c)
	}
	return corr
}

func (h *harness) joined(c sys.ConnID, corr int32) wire.JoinGroupResponse {
	h.t.Helper()
	var resp wire.JoinGroupResponse
	h.run()
	if !h.recv(c, wire.KeyJoinGroup, 5, corr, &resp) {
		h.t.Fatalf("no JoinGroup response on conn %d", c)
	}
	return resp
}

func (h *harness) join(c sys.ConnID, req *wire.JoinGroupRequest) wire.JoinGroupResponse {
	h.t.Helper()
	return h.joined(c, h.send(c, wire.KeyJoinGroup, 5, req))
}

// memberID obtains a member id the way a current client does.
func (h *harness) memberID(c sys.ConnID) string {
	h.t.Helper()
	resp := h.join(c, joinReq("", "range"))
	if resp.ErrorCode != wire.ErrMemberIDRequired || resp.MemberID == "" {
		h.t.Fatalf("first join: %+v, want MEMBER_ID_REQUIRED with an id", resp)
	}
	return resp.MemberID
}

func (h *harness) syncGroup(c sys.ConnID, generation int32, id string, assign ...wire.MemberAssignment) wire.SyncGroupResponse {
	h.t.Helper()
	var resp wire.SyncGroupResponse
	h.call(c, wire.KeySyncGroup, 3, &wire.SyncGroupRequest{GroupID: "g", GenerationID: generation, MemberID: id, Assignments: assign}, &resp)
	return resp
}

func (h *harness) heartbeat(c sys.ConnID, generation int32, id string) int16 {
	h.t.Helper()
	var resp wire.HeartbeatResponse
	h.call(c, wire.KeyHeartbeat, 3, &wire.HeartbeatRequest{GroupID: "g", GenerationID: generation, MemberID: id}, &resp)
	return resp.ErrorCode
}

func (h *harness) commit(c sys.ConnID, generation int32, id string, offset int64) int16 {
	h.t.Helper()
	var resp wire.OffsetCommitResponse
	h.call(c, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "g", GenerationID: generation, MemberID: id,
		Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: offset, LeaderEpoch: -1, Metadata: "m"}}}}}, &resp)
	return resp.Topics[0].Partitions[0].ErrorCode
}

func (h *harness) committed(c sys.ConnID) int64 {
	h.t.Helper()
	var resp wire.OffsetFetchResponse
	h.call(c, wire.KeyOffsetFetch, 5, &wire.OffsetFetchRequest{GroupID: "g", Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}, &resp)
	return resp.Topics[0].Partitions[0].Offset
}

func (h *harness) leave(c sys.ConnID, id string) int16 {
	h.t.Helper()
	var resp wire.LeaveGroupResponse
	h.call(c, wire.KeyLeaveGroup, 3, &wire.LeaveGroupRequest{GroupID: "g", Members: []string{id}}, &resp)
	return resp.Members[0].ErrorCode
}

// stableGroup forms a group of two members, A (the leader, conn 1) and B
// (conn 2), at generation 2 with assignments "a" and "b".
func stableGroup(t *testing.T) (h *harness, a, b string) {
	t.Helper()
	h = newHarness(t, Config{})
	h.open(1)
	h.open(2)
	if code := h.createTopic(1, "t", 1); code != 0 {
		t.Fatalf("create topic: %d", code)
	}
	a = h.memberID(1)
	if resp := h.join(1, joinReq(a, "range")); resp.ErrorCode != 0 || resp.GenerationID != 1 || resp.Leader != a || len(resp.Members) != 1 {
		t.Fatalf("first generation: %+v", resp)
	}
	b = h.memberID(2)
	corrB := h.joinHeld(2, b)
	if code := h.heartbeat(1, 1, a); code != wire.ErrRebalanceInProgress {
		t.Fatalf("heartbeat during a rebalance: %d", code)
	}
	ra := h.join(1, joinReq(a, "range"))
	rb := h.joined(2, corrB)
	if ra.GenerationID != 2 || rb.GenerationID != 2 || ra.Leader != a || rb.Leader != a || ra.ProtocolName != "range" {
		t.Fatalf("second generation: %+v / %+v", ra, rb)
	}
	if len(ra.Members) != 2 || string(ra.Members[0].Metadata) != "sub-range" || len(rb.Members) != 0 {
		t.Fatalf("only the leader gets the member list: %+v / %+v", ra, rb)
	}
	// The follower's SyncGroup waits for the leader's.
	corr := h.send(2, wire.KeySyncGroup, 3, &wire.SyncGroupRequest{GroupID: "g", GenerationID: 2, MemberID: b})
	h.run()
	if len(h.net.sent[2]) != 0 {
		t.Fatal("follower's SyncGroup answered before the leader's arrived")
	}
	if resp := h.syncGroup(1, 2, a, wire.MemberAssignment{MemberID: a, Assignment: []byte("a")}, wire.MemberAssignment{MemberID: b, Assignment: []byte("b")}); string(resp.Assignment) != "a" {
		t.Fatalf("leader's assignment: %+v", resp)
	}
	var sb wire.SyncGroupResponse
	if !h.recv(2, wire.KeySyncGroup, 3, corr, &sb) || string(sb.Assignment) != "b" {
		t.Fatalf("follower's assignment: %+v", sb)
	}
	return h, a, b
}

func TestGroupLifecycle(t *testing.T) {
	h, a, b := stableGroup(t)
	if h.heartbeat(1, 2, a) != 0 || h.heartbeat(2, 2, b) != 0 {
		t.Fatal("heartbeat in a stable group failed")
	}
	// A retry of JoinGroup by a follower with nothing changed is answered
	// at once and does not disturb the group.
	if resp := h.join(2, joinReq(b, "range")); resp.ErrorCode != 0 || resp.GenerationID != 2 {
		t.Fatalf("unchanged rejoin: %+v", resp)
	}

	// Offsets: accepted from a member of the current generation only.
	for _, tc := range []struct {
		name       string
		generation int32
		member     string
		want       int16
	}{
		{"member", 2, b, wire.ErrNone},
		{"zombie from an older generation", 1, b, wire.ErrIllegalGeneration},
		{"unknown member", 2, "nobody", wire.ErrUnknownMemberID},
		{"no membership, but the group has members", -1, "", wire.ErrUnknownMemberID},
	} {
		if got := h.commit(2, tc.generation, tc.member, 100+int64(tc.generation)); got != tc.want {
			t.Errorf("commit by %s: error %d, want %d", tc.name, got, tc.want)
		}
	}
	if got := h.committed(1); got != 102 {
		t.Fatalf("committed offset %d, want 102", got)
	}

	// A restart leaves the group stable: same generation, no rebalance,
	// assignments and offsets intact.
	h.start()
	h.open(1)
	h.open(2)
	if h.heartbeat(1, 2, a) != 0 || h.heartbeat(2, 2, b) != 0 {
		t.Fatal("heartbeat after restart failed")
	}
	if resp := h.syncGroup(2, 2, b); string(resp.Assignment) != "b" {
		t.Fatalf("assignment after restart: %+v", resp)
	}
	if got := h.committed(1); got != 102 {
		t.Fatalf("committed offset after restart %d, want 102", got)
	}

	// B leaves: A is told to rejoin and becomes the only member.
	if code := h.leave(2, b); code != 0 {
		t.Fatalf("leave: %d", code)
	}
	if code := h.leave(2, b); code != wire.ErrUnknownMemberID {
		t.Fatalf("second leave: %d", code)
	}
	if code := h.heartbeat(1, 2, a); code != wire.ErrRebalanceInProgress {
		t.Fatalf("heartbeat after a member left: %d", code)
	}
	if resp := h.join(1, joinReq(a, "range")); resp.GenerationID != 3 || len(resp.Members) != 1 {
		t.Fatalf("generation after leave: %+v", resp)
	}
	if code := h.heartbeat(2, 3, b); code != wire.ErrUnknownMemberID {
		t.Fatalf("heartbeat by a member that left: %d", code)
	}

	// A stops heartbeating: its session expires and the group is empty.
	h.syncGroup(1, 3, a, wire.MemberAssignment{MemberID: a, Assignment: []byte("a")})
	h.advance(9 * time.Second)
	if code := h.heartbeat(1, 3, a); code != 0 {
		t.Fatalf("heartbeat within the session timeout: %d", code)
	}
	h.advance(11 * time.Second)
	if code := h.heartbeat(1, 3, a); code != wire.ErrUnknownMemberID {
		t.Fatalf("heartbeat after session expiry: %d", code)
	}
	if code := h.commit(1, -1, "", 7); code != 0 {
		t.Fatalf("commit without membership on an empty group: %d", code)
	}
	if got := h.committed(1); got != 7 {
		t.Fatalf("committed offset %d, want 7", got)
	}
}

func TestGroupRebalanceTimeoutDropsSilentMember(t *testing.T) {
	h, a, b := stableGroup(t)
	h.open(3)
	c := h.memberID(3)
	corr := h.joinHeld(3, c)
	corrB := h.send(2, wire.KeyJoinGroup, 5, joinReq(b, "range"))
	// A stays alive by heartbeating but never rejoins.
	for range 3 {
		h.advance(9 * time.Second)
		if code := h.heartbeat(1, 2, a); code != wire.ErrRebalanceInProgress {
			t.Fatalf("heartbeat during the round: %d", code)
		}
	}
	h.advance(2 * time.Second)
	if len(h.net.sent[3]) != 0 {
		t.Fatal("round completed before the rebalance timeout")
	}
	h.advance(2 * time.Second)
	rc, rb := h.joined(3, corr), h.joined(2, corrB)
	if rc.GenerationID != 3 || rb.GenerationID != 3 || rb.Leader != c || len(rc.Members) != 2 {
		t.Fatalf("generation without the silent leader: %+v / %+v", rc, rb)
	}
	if code := h.heartbeat(1, 2, a); code != wire.ErrUnknownMemberID {
		t.Fatalf("heartbeat by the dropped member: %d", code)
	}
	if code := h.commit(1, 2, a, 5); code != wire.ErrUnknownMemberID {
		t.Fatalf("commit by the dropped member: %d", code)
	}
}

func TestGroupLeaderThatNeverSyncs(t *testing.T) {
	h, a, b := stableGroup(t)
	// Force generation 3; this time the leader does not send SyncGroup.
	corrA := h.send(1, wire.KeyJoinGroup, 5, joinReq(a, "range"))
	h.joined(2, h.send(2, wire.KeyJoinGroup, 5, joinReq(b, "range")))
	h.joined(1, corrA)
	if code := h.commit(2, 3, b, 9); code != wire.ErrRebalanceInProgress {
		t.Fatalf("commit before the assignment: %d", code)
	}
	corr := h.send(2, wire.KeySyncGroup, 3, &wire.SyncGroupRequest{GroupID: "g", GenerationID: 3, MemberID: b})
	h.advance(31 * time.Second)
	var resp wire.SyncGroupResponse
	if !h.recv(2, wire.KeySyncGroup, 3, corr, &resp) || resp.ErrorCode != wire.ErrRebalanceInProgress {
		t.Fatalf("held SyncGroup after the deadline: %+v", resp)
	}
}

func TestGroupHeldJoinWithdrawnOnClose(t *testing.T) {
	h, a, _ := stableGroup(t)
	h.open(3)
	c := h.memberID(3)
	h.joinHeld(3, c)
	h.must(h.node.OnConnClose(3))
	h.must(h.node.OnConnClose(2)) // B is silent from now on
	h.joinHeld(1, a)
	h.advance(31 * time.Second)
	var resp wire.JoinGroupResponse
	if !h.recv(1, wire.KeyJoinGroup, 5, h.corr, &resp) || resp.GenerationID != 3 || len(resp.Members) != 1 {
		t.Fatalf("generation after two members vanished: %+v", resp)
	}
}

func TestJoinGroupRejects(t *testing.T) {
	h, _, _ := stableGroup(t)
	h.open(3)
	mod := func(f func(*wire.JoinGroupRequest)) *wire.JoinGroupRequest {
		req := joinReq("", "range")
		f(req)
		return req
	}
	for _, tc := range []struct {
		name string
		req  *wire.JoinGroupRequest
		want int16
	}{
		{"empty group id", mod(func(r *wire.JoinGroupRequest) { r.GroupID = "" }), wire.ErrInvalidGroupID},
		{"session timeout too short", mod(func(r *wire.JoinGroupRequest) { r.SessionTimeoutMs = 10 }), wire.ErrInvalidSessionTimeout},
		{"session timeout too long", mod(func(r *wire.JoinGroupRequest) { r.SessionTimeoutMs = 1 << 30 }), wire.ErrInvalidSessionTimeout},
		{"no protocols", mod(func(r *wire.JoinGroupRequest) { r.Protocols = nil }), wire.ErrInconsistentGroupProtocol},
		{"another protocol type", mod(func(r *wire.JoinGroupRequest) { r.ProtocolType = "connect" }), wire.ErrInconsistentGroupProtocol},
		{"no protocol in common", joinReq("", "sticky"), wire.ErrInconsistentGroupProtocol},
		{"unknown member id", joinReq("nobody", "range"), wire.ErrUnknownMemberID},
	} {
		if got := h.join(3, tc.req).ErrorCode; got != tc.want {
			t.Errorf("%s: error %d, want %d", tc.name, got, tc.want)
		}
	}

	small := newHarness(t, Config{MaxGroupSize: 1})
	small.open(1)
	small.memberID(1)
	if got := small.join(1, joinReq("", "range")).ErrorCode; got != wire.ErrGroupMaxSizeReached {
		t.Errorf("full group: error %d", got)
	}
	// An id that is never used stops counting after one session timeout.
	small.advance(11 * time.Second)
	small.memberID(1)
}

func TestProtocolChoice(t *testing.T) {
	j := func(names ...string) *joiner {
		j := &joiner{}
		for _, n := range names {
			j.protocols = append(j.protocols, wire.GroupProtocol{Name: n})
		}
		return j
	}
	for _, tc := range []struct {
		joiners []*joiner
		want    string
	}{
		{[]*joiner{j("range")}, "range"},
		{[]*joiner{j("sticky", "range"), j("range")}, "range"},                         // only common one
		{[]*joiner{j("a", "b"), j("b", "a"), j("b", "a")}, "b"},                        // most first choices
		{[]*joiner{j("b", "a"), j("a", "b")}, "a"},                                     // tie: smaller name
		{[]*joiner{j("x", "b", "a"), j("y", "a", "b"), j("a", "b"), j("b", "a")}, "a"}, // unsupported first choices are skipped
	} {
		if got := chooseProtocol(tc.joiners); got != tc.want {
			t.Errorf("chooseProtocol = %q, want %q", got, tc.want)
		}
	}
}

// The version 0 protocol has no MEMBER_ID_REQUIRED step and no rebalance
// timeout: the first join is the join.
func TestJoinGroupVersion0(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	var resp wire.JoinGroupResponse
	h.call(1, wire.KeyJoinGroup, 0, joinReq("", "range"), &resp)
	if resp.ErrorCode != 0 || resp.GenerationID != 1 || !strings.HasPrefix(resp.MemberID, "test-") || resp.Leader != resp.MemberID {
		t.Fatalf("version 0 join: %+v", resp)
	}
}

func TestOffsetCommitAndFetchEdges(t *testing.T) {
	h := newHarness(t, Config{})
	h.open(1)
	h.createTopic(1, "t", 2)
	var cr wire.OffsetCommitResponse
	h.call(1, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "g", GenerationID: -1, Topics: []wire.OffsetCommitTopic{
		{Name: "t", Partitions: []wire.OffsetCommitPartition{
			{Index: 1, Offset: 5, LeaderEpoch: 3, Metadata: "note"},
			{Index: 0, Offset: 6, Metadata: strings.Repeat("x", maxOffsetMetadata+1)},
			{Index: 9, Offset: 7}}},
		{Name: "missing", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 8}}}}}, &cr)
	got := []int16{cr.Topics[0].Partitions[0].ErrorCode, cr.Topics[0].Partitions[1].ErrorCode, cr.Topics[0].Partitions[2].ErrorCode, cr.Topics[1].Partitions[0].ErrorCode}
	want := []int16{0, wire.ErrOffsetMetadataTooLarge, wire.ErrUnknownTopicOrPartition, wire.ErrUnknownTopicOrPartition}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("commit partition %d: error %d, want %d", i, got[i], want[i])
		}
	}
	// Asking for everything returns exactly what was committed; a
	// partition without a commit reads as -1 with no error.
	var all, one wire.OffsetFetchResponse
	h.call(1, wire.KeyOffsetFetch, 5, &wire.OffsetFetchRequest{GroupID: "g", AllTopics: true}, &all)
	if len(all.Topics) != 1 || len(all.Topics[0].Partitions) != 1 || all.Topics[0].Partitions[0] != (wire.OffsetFetchPartition{Index: 1, Offset: 5, LeaderEpoch: 3, Metadata: "note"}) {
		t.Fatalf("fetch all: %+v", all)
	}
	h.call(1, wire.KeyOffsetFetch, 5, &wire.OffsetFetchRequest{GroupID: "g", Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}, &one)
	if p := one.Topics[0].Partitions[0]; p.Offset != -1 || p.ErrorCode != 0 {
		t.Fatalf("fetch of an uncommitted partition: %+v", p)
	}
	var fc wire.FindCoordinatorResponse
	h.call(1, wire.KeyFindCoord, 2, &wire.FindCoordinatorRequest{Key: "g"}, &fc)
	if fc.ErrorCode != 0 || fc.NodeID != 1 || fc.Port != 9092 {
		t.Fatalf("find coordinator: %+v", fc)
	}
}

// The commands themselves, with no node around them: each is applied or
// refused by its position in the log alone.
func TestGroupCommandsApply(t *testing.T) {
	s := newState()
	gen := func(n int32, ids ...string) []byte {
		g := &group{generation: n, protocolType: "consumer", protocol: "range"}
		for _, id := range ids {
			g.members = append(g.members, &member{id: id})
			g.leader = ids[0]
		}
		return encodeGroupGeneration("g", g)
	}
	assign := func(n int32) []byte {
		return encodeGroupAssignment("g", n, []wire.MemberAssignment{{MemberID: "a", Assignment: []byte("x")}})
	}
	commit := func(n int32, id string) []byte {
		return encodeOffsetCommit("g", n, id, 0, []offsetEntry{{"t", 0, committedOffset{offset: 1}}})
	}
	for i, tc := range []struct {
		name string
		cmd  []byte
		want int16
	}{
		{"commit with no group yet", commit(-1, ""), wire.ErrNone},
		{"generation skipping ahead", gen(2, "a"), wire.ErrIllegalGeneration},
		{"first generation", gen(1, "a"), wire.ErrNone},
		{"the same generation again", gen(1, "b"), wire.ErrIllegalGeneration},
		{"commit before the assignment", commit(1, "a"), wire.ErrRebalanceInProgress},
		{"assignment for another generation", assign(2), wire.ErrIllegalGeneration},
		{"assignment", assign(1), wire.ErrNone},
		{"a second assignment", assign(1), wire.ErrIllegalGeneration},
		{"commit by a member", commit(1, "a"), wire.ErrNone},
		{"next generation", gen(2, "b"), wire.ErrNone},
		{"commit by the fenced member", commit(1, "a"), wire.ErrUnknownMemberID},
		{"empty generation", gen(3), wire.ErrNone},
		{"commit without membership", commit(-1, ""), wire.ErrNone},
	} {
		res, err := s.apply(int64(i), tc.cmd)
		if err != nil || res.errCode != tc.want {
			t.Fatalf("%s: error code %d (%v), want %d", tc.name, res.errCode, err, tc.want)
		}
	}
	// Truncated commands are refused, never half applied.
	for _, cmd := range [][]byte{gen(4, "a"), assign(3), commit(3, "a")} {
		if _, err := s.apply(99, cmd[:len(cmd)-1]); err == nil {
			t.Fatalf("truncated command type %d was applied", cmd[0])
		}
	}
}
