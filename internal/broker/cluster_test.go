package broker

import (
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// trio is three nodes joined by a network the test operates by hand: a
// peer message moves only when the test says so.
type trio struct {
	t *testing.T
	h map[sys.NodeID]*harness
}

func newTrio(t *testing.T) *trio {
	tr := &trio{t: t, h: map[sys.NodeID]*harness{}}
	peers := []Peer{{1, "host1", 9092}, {2, "host2", 9092}, {3, "host3", 9092}}
	for _, p := range peers {
		tr.h[sys.NodeID(p.ID)] = &harness{t: t, cfg: Config{NodeID: p.ID, Host: p.Host, Port: p.Port, Peers: peers},
			disk: simsys.NewDisk(simsys.NewRand(uint64(p.ID)), simsys.DiskConfig{}), rand: simsys.NewRand(uint64(10 + p.ID))}
		tr.h[sys.NodeID(p.ID)].start()
	}
	return tr
}

// deliver moves peer messages, and runs what they make due, until no
// message between the given nodes is left. Messages to other nodes stay
// queued where they are: those nodes are cut off.
func (tr *trio) deliver(among ...sys.NodeID) {
	tr.t.Helper()
	in := func(id sys.NodeID) bool {
		for _, a := range among {
			if a == id {
				return true
			}
		}
		return false
	}
	for moved := true; moved; {
		moved = false
		for _, from := range among {
			h := tr.h[from]
			h.run() // the zero-delay flush that makes entries durable
			var kept []peerMsg
			for _, m := range h.net.peers {
				if !in(m.to) {
					kept = append(kept, m)
					continue
				}
				moved = true
				tr.h[m.to].must(tr.h[m.to].node.OnPeerMessage(from, m.msg))
			}
			h.net.peers = kept
		}
	}
}

// elect makes id stand for election and win with the votes of the nodes
// in among.
func (tr *trio) elect(id sys.NodeID, among ...sys.NodeID) {
	tr.t.Helper()
	tr.passTime()
	tr.h[id].must(tr.h[id].node.OnTimer(timerElection))
	tr.deliver(among...)
	if !tr.h[id].node.Serving() {
		tr.t.Fatalf("node %d did not become a serving leader", id)
	}
}

// passTime moves every node's clock past an election timeout without
// firing any timer: what an election timer that fires implies, and what
// lets the others vote (they ignore candidates while a leader is recent).
func (tr *trio) passTime() {
	for _, h := range tr.h {
		h.clock.now += sys.Instant(time.Second)
	}
}

func (h *harness) metadata(c sys.ConnID) wire.MetadataResponse {
	h.t.Helper()
	var resp wire.MetadataResponse
	h.call(c, wire.KeyMetadata, 7, &wire.MetadataRequest{AllTopics: true}, &resp)
	return resp
}

// Only the leader serves. Every other node names the leader and answers
// each API with the retriable error that sends a client there.
func TestOnlyTheLeaderServes(t *testing.T) {
	tr := newTrio(t)
	for id, h := range tr.h {
		h.open(1)
		if md := h.metadata(1); md.ControllerID != -1 || len(md.Brokers) != 3 {
			t.Fatalf("node %d before any election: controller %d, %d brokers", id, md.ControllerID, len(md.Brokers))
		}
	}
	tr.elect(1, 1, 2, 3)
	lead, follower := tr.h[1], tr.h[2]

	// The leader commits through a majority and the followers apply too.
	var ct wire.CreateTopicsResponse
	corr := lead.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 3}}})
	lead.run()
	if lead.recv(1, wire.KeyCreateTopics, 4, corr, &ct) {
		t.Fatal("the leader answered before any follower had the command")
	}
	tr.deliver(1, 2, 3)
	if !lead.recv(1, wire.KeyCreateTopics, 4, corr, &ct) || ct.Topics[0].ErrorCode != wire.ErrNone {
		t.Fatalf("create topic on the leader: %+v", ct)
	}
	lead.send(1, wire.KeyProduce, 8, produceReq("t", 0, batch("a", "b")))
	tr.deliver(1, 2, 3)
	lead.must(lead.node.OnTimer(timerHeartbeat)) // carries the new commit index
	tr.deliver(1, 2, 3)
	la, lh := lead.node.StateHash()
	for id, h := range tr.h {
		if a, hash := h.node.StateHash(); a != la || hash != lh {
			t.Fatalf("node %d applied %d entries with hash %08x; the leader %d with %08x", id, a, hash, la, lh)
		}
	}

	// A follower knows the topic and who leads, and refuses the rest.
	md := follower.metadata(1)
	if md.ControllerID != 1 || len(md.Topics) != 1 || md.Topics[0].Partitions[0].Leader != 1 || md.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("follower metadata: %+v", md)
	}
	var fc wire.FindCoordinatorResponse
	follower.call(1, wire.KeyFindCoord, 2, &wire.FindCoordinatorRequest{Key: "g"}, &fc)
	if fc.ErrorCode != 0 || fc.NodeID != 1 || fc.Host != "host1" {
		t.Fatalf("follower FindCoordinator: %+v", fc)
	}
	var pr wire.ProduceResponse
	follower.call(1, wire.KeyProduce, 8, produceReq("t", 0, batch("x")), &pr)
	var fr wire.FetchResponse
	follower.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 500, 1), &fr)
	var lo wire.ListOffsetsResponse
	follower.call(1, wire.KeyListOffsets, 5, &wire.ListOffsetsRequest{Topics: []wire.ListOffsetsTopic{{Name: "t", Partitions: []wire.ListOffsetsPartition{{Index: 0, Timestamp: wire.TimestampLatest}}}}}, &lo)
	follower.call(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "u", NumPartitions: 1, ReplicationFactor: -1}}}, &ct)
	var jg wire.JoinGroupResponse
	follower.call(1, wire.KeyJoinGroup, 5, &wire.JoinGroupRequest{GroupID: "g", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 10000, ProtocolType: "consumer", Protocols: []wire.GroupProtocol{{Name: "p"}}}, &jg)
	var sg wire.SyncGroupResponse
	follower.call(1, wire.KeySyncGroup, 3, &wire.SyncGroupRequest{GroupID: "g", MemberID: "m"}, &sg)
	var hb wire.HeartbeatResponse
	follower.call(1, wire.KeyHeartbeat, 3, &wire.HeartbeatRequest{GroupID: "g", MemberID: "m"}, &hb)
	var lg wire.LeaveGroupResponse
	follower.call(1, wire.KeyLeaveGroup, 3, &wire.LeaveGroupRequest{GroupID: "g", Members: []string{"m"}}, &lg)
	var oc wire.OffsetCommitResponse
	follower.call(1, wire.KeyOffsetCommit, 7, &wire.OffsetCommitRequest{GroupID: "g", GenerationID: -1, Topics: []wire.OffsetCommitTopic{{Name: "t", Partitions: []wire.OffsetCommitPartition{{Index: 0, Offset: 1}}}}}, &oc)
	var of wire.OffsetFetchResponse
	follower.call(1, wire.KeyOffsetFetch, 5, &wire.OffsetFetchRequest{GroupID: "g", Topics: []wire.OffsetFetchTopic{{Name: "t", Partitions: []int32{0}}}}, &of)
	for name, got := range map[string][2]int16{
		"Produce":       {pr.Topics[0].Partitions[0].ErrorCode, wire.ErrNotLeaderOrFollower},
		"Fetch":         {fr.Topics[0].Partitions[0].ErrorCode, wire.ErrNotLeaderOrFollower},
		"ListOffsets":   {lo.Topics[0].Partitions[0].ErrorCode, wire.ErrNotLeaderOrFollower},
		"CreateTopics":  {ct.Topics[0].ErrorCode, wire.ErrNotController},
		"JoinGroup":     {jg.ErrorCode, wire.ErrNotCoordinator},
		"SyncGroup":     {sg.ErrorCode, wire.ErrNotCoordinator},
		"Heartbeat":     {hb.ErrorCode, wire.ErrNotCoordinator},
		"LeaveGroup":    {lg.ErrorCode, wire.ErrNotCoordinator},
		"OffsetCommit":  {oc.Topics[0].Partitions[0].ErrorCode, wire.ErrNotCoordinator},
		"OffsetFetch":   {of.ErrorCode, wire.ErrNotCoordinator},
		"OffsetFetch/p": {of.Topics[0].Partitions[0].ErrorCode, wire.ErrNotCoordinator},
	} {
		if got[0] != got[1] {
			t.Errorf("%s on a follower: error code %d, want %d", name, got[0], got[1])
		}
	}
	if a, _ := follower.node.StateHash(); a != la {
		t.Fatalf("a refused request changed the follower: applied %d, was %d", a, la)
	}
}

// Roadmap bugs 10 and 12. A leader that learns it was replaced tells no
// client that a pending write succeeded, leaves no request hanging, and
// refuses what comes next.
func TestDeposedLeaderDropsWhatItHeld(t *testing.T) {
	tr := newTrio(t)
	tr.elect(1, 1, 2, 3)
	old := tr.h[1]
	old.open(1)
	old.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: -1}}})
	tr.deliver(1, 2, 3)
	old.open(2)
	old.open(3)
	join := &wire.JoinGroupRequest{GroupID: "g", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 10000, ProtocolType: "consumer", Protocols: []wire.GroupProtocol{{Name: "p"}}}
	var jg wire.JoinGroupResponse
	old.call(3, wire.KeyJoinGroup, 5, join, &jg) // answered with the member id to use
	join.MemberID = jg.MemberID
	old.net.sent[1] = nil

	// Cut off, the old leader accepts a write it cannot commit, parks a
	// fetch, and holds a JoinGroup whose generation it cannot commit.
	old.send(1, wire.KeyProduce, 8, produceReq("t", 0, batch("lost?")))
	old.send(2, wire.KeyFetch, 11, fetchReq("t", 0, 0, 30000, 1))
	old.send(3, wire.KeyJoinGroup, 5, join)
	old.run()
	if len(old.node.waiters) != 2 || len(old.node.parked) != 1 || len(old.node.groups) != 1 {
		t.Fatalf("setup: %d waiters, %d parked fetches, %d groups", len(old.node.waiters), len(old.node.parked), len(old.node.groups))
	}
	old.net.peers = nil // none of it reaches anyone

	// The others elect node 2; its first message reaches the old leader.
	tr.elect(2, 2, 3)
	tr.deliver(1, 2, 3)
	if old.node.Serving() || len(old.node.waiters) != 0 || len(old.node.parked) != 0 || len(old.node.groups) != 0 || len(old.node.timers) != 0 {
		t.Fatalf("deposed leader still holds state: serving=%v waiters=%d parked=%d groups=%d timers=%d",
			old.node.Serving(), len(old.node.waiters), len(old.node.parked), len(old.node.groups), len(old.node.timers))
	}
	for c := sys.ConnID(1); c <= 3; c++ {
		if len(old.net.sent[c]) != 0 {
			t.Errorf("connection %d was sent an answer by a node that could not know it", c)
		}
	}
	// (h.run has already delivered the closes back to the node.)
	if len(old.node.conns) != 0 {
		t.Fatalf("%d connections with a request in progress were left open", len(old.node.conns))
	}
	old.open(4)
	var pr wire.ProduceResponse
	old.call(4, wire.KeyProduce, 8, produceReq("t", 0, batch("x")), &pr)
	if code := pr.Topics[0].Partitions[0].ErrorCode; code != wire.ErrNotLeaderOrFollower {
		t.Fatalf("deposed leader answered a produce with code %d", code)
	}
	// The write nobody acknowledged is gone from the old leader's log:
	// the new leader's entries replaced it.
	tr.h[2].open(1)
	if _, vals := tr.h[2].fetch(1, "t", 0, 0); len(vals) != 0 {
		t.Fatalf("the new leader serves %v, which was never committed", vals)
	}
}

// Roadmap bug 11. A node that has won an election does not serve until
// its own first entry is committed and applied: before that it cannot
// know how much of its log is committed.
func TestNewLeaderWaitsForItsOwnEntry(t *testing.T) {
	tr := newTrio(t)
	tr.elect(1, 1, 2, 3)
	tr.h[1].open(1)
	tr.h[1].send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: -1}}})
	tr.deliver(1, 2, 3)
	tr.h[1].must(tr.h[1].node.OnTimer(timerHeartbeat)) // tells the followers it is committed
	tr.deliver(1, 2, 3)

	// Node 2 collects the votes, but nothing it sends afterwards arrives.
	n2 := tr.h[2]
	tr.passTime()
	n2.must(n2.node.OnTimer(timerElection))
	for _, m := range n2.net.peers {
		tr.h[m.to].must(tr.h[m.to].node.OnPeerMessage(2, m.msg))
	}
	n2.net.peers = nil
	for _, id := range []sys.NodeID{1, 3} {
		for _, m := range tr.h[id].net.peers {
			if m.to == 2 {
				n2.must(n2.node.OnPeerMessage(id, m.msg))
			}
		}
		tr.h[id].net.peers = nil
	}
	n2.run()
	n2.net.peers = nil
	if n2.node.Serving() {
		t.Fatal("setup: node 2 serves although its first entry reached nobody")
	}
	n2.open(1)
	var fr wire.FetchResponse
	n2.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 0, 1), &fr)
	if code := fr.Topics[0].Partitions[0].ErrorCode; code != wire.ErrNotLeaderOrFollower {
		t.Fatalf("fetch in the window: code %d", code)
	}
	if md := n2.metadata(1); md.ControllerID != -1 || md.Topics[0].Partitions[0].ErrorCode != wire.ErrLeaderNotAvailable {
		t.Fatalf("metadata in the window names a leader: %+v", md)
	}

	// Its heartbeat gets through: the entry commits, and it serves.
	n2.advance(time.Second)
	tr.deliver(1, 2, 3)
	n2.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 0, 1), &fr)
	if code := fr.Topics[0].Partitions[0].ErrorCode; !n2.node.Serving() || code != wire.ErrNone {
		t.Fatalf("after its entry committed: serving=%v, fetch code %d", n2.node.Serving(), code)
	}
}

// A leader that has not heard from a majority for three quarters of an
// election timeout may already have been replaced: it refuses reads, and
// stops naming itself, until a round of heartbeats is answered. Writes
// still go to the log, which decides them.
func TestLeaderReadsNeedTheLease(t *testing.T) {
	tr := newTrio(t)
	tr.elect(1, 1, 2, 3)
	lead := tr.h[1]
	lead.open(1)
	lead.send(1, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: -1}}})
	tr.deliver(1, 2, 3)
	lead.net.sent[1] = nil

	fetchCode := func() int16 {
		var fr wire.FetchResponse
		lead.call(1, wire.KeyFetch, 11, fetchReq("t", 0, 0, 0, 1), &fr)
		return fr.Topics[0].Partitions[0].ErrorCode
	}
	if code := fetchCode(); code != wire.ErrNone {
		t.Fatalf("fetch with a fresh lease: code %d", code)
	}
	lead.clock.now += sys.Instant(230 * time.Millisecond) // past 3/4 of the 300ms default
	if code := fetchCode(); code != wire.ErrNotLeaderOrFollower {
		t.Fatalf("fetch with an expired lease: code %d", code)
	}
	if md := lead.metadata(1); md.ControllerID != -1 {
		t.Fatalf("a leader without its lease names %d as controller", md.ControllerID)
	}
	lead.must(lead.node.OnTimer(timerHeartbeat))
	tr.deliver(1, 2, 3)
	if code := fetchCode(); code != wire.ErrNone {
		t.Fatalf("fetch after the heartbeats were answered: code %d", code)
	}

	// The mutation switch turns the check off.
	tr2 := newTrio(t)
	for _, h := range tr2.h {
		h.node.cfg.UnsafeNoLease = true
	}
	tr2.elect(1, 1, 2, 3)
	tr2.h[1].clock.now += sys.Instant(time.Hour)
	if !tr2.h[1].node.canRead() {
		t.Fatal("UnsafeNoLease did not switch the lease off")
	}
}
