package broker

import (
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// levelRun plays a rolling upgrade's last step: node 3 runs an older build
// that never reports a level, as v0.2.0 does. It returns the cluster's
// level and CreateAcls' answer while node 3 is old, then the same once
// node 3 runs this build.
func levelRun(t *testing.T, early bool) (oldLevel int, oldCode int16, newLevel int, newCode int16) {
	tr := newTrio(t)
	for _, h := range tr.h {
		h.cfg.Authorization = &Authorization{AllowIfNoACL: true}
		h.cfg.UnsafeRaiseLevelEarly = early
		h.start()
		h.open(1)
	}
	old := true
	tr.h[3].net.drop = func(m []byte) bool { return old && len(m) > 1 && m[0] == brokerMsg && m[1] == msgLevel }
	tr.elect(1, 1, 2, 3)
	seconds := func(n int) {
		for range n * 20 { // in heartbeat-sized steps, so that no election timer fires
			for _, id := range []sys.NodeID{1, 2, 3} {
				tr.h[id].advance(50 * time.Millisecond)
			}
			tr.deliver(1, 2, 3)
		}
	}
	lead := tr.h[1]
	defer func() {
		if !lead.node.Serving() {
			t.Fatal("node 1 lost leadership during the test")
		}
	}()
	create := func() int16 {
		corr := lead.send(1, wire.KeyCreateAcls, 3, &wire.CreateAclsRequest{Creations: []wire.ACL{allowAlice(resTopic, "t", opRead)}})
		var resp wire.CreateAclsResponse
		for i := 0; !lead.recv(1, wire.KeyCreateAcls, 3, corr, &resp); i++ {
			if i == 10 {
				t.Fatalf("CreateAcls unanswered (early %v)", early)
			}
			lead.must(lead.node.OnTimer(timerHeartbeat))
			tr.deliver(1, 2, 3)
		}
		return resp.Results[0].ErrorCode
	}

	seconds(5)
	oldLevel, _ = lead.node.Level()
	oldCode = create()
	old = false
	seconds(2)
	newLevel, _ = lead.node.Level()
	newCode = create()
	for id := sys.NodeID(2); id <= 3; id++ {
		if l, _ := tr.h[id].node.Level(); l != newLevel {
			t.Errorf("node %d is at level %d, the leader at %d", id, l, newLevel)
		}
	}
	return oldLevel, oldCode, newLevel, newCode
}

// The leader raises the cluster's format level only once every voter
// supports it; until then a request that would write the newer format is
// refused, and the old node is never sent it.
func TestLevelWaitsForEveryVoter(t *testing.T) {
	oldLevel, oldCode, newLevel, newCode := levelRun(t, false)
	if oldLevel != 0 || oldCode != wire.ErrUnsupportedVersion {
		t.Fatalf("with an old voter: level %d, CreateAcls error %d; want level 0 and UNSUPPORTED_VERSION", oldLevel, oldCode)
	}
	if newLevel != formatLevel || newCode != wire.ErrNone {
		t.Fatalf("after the upgrade: level %d, CreateAcls error %d; want level %d and success", newLevel, newCode, formatLevel)
	}
	if oldLevel, _, _, _ := levelRun(t, true); oldLevel == 0 {
		t.Fatal("UnsafeRaiseLevelEarly was not caught: the level stayed 0 with an old voter")
	}
}

// Snapshots are written in the oldest version that holds the state, and
// the level survives a round trip once raised.
func TestSnapshotVersionFollowsContent(t *testing.T) {
	s := newState()
	if v := s.encode()[0]; v != 4 {
		t.Fatalf("an empty state encodes as version %d, want 4", v)
	}
	if _, err := s.apply(0, encodeCreateACL(allowAlice(resTopic, "t", opRead))); err != nil {
		t.Fatal(err)
	}
	if v := s.encode()[0]; v != 6 {
		t.Fatalf("a state with an ACL encodes as version %d, want 6", v)
	}
	if _, err := s.apply(1, []byte{cmdSetLevel, formatLevel}); err != nil {
		t.Fatal(err)
	}
	b := s.encode()
	got, err := decodeState(b)
	if err != nil || b[0] != snapshotVersion || got.level != formatLevel || got.hash() != s.hash() {
		t.Fatalf("version %d, decoded level %d, err %v", b[0], got.level, err)
	}
	if _, err := s.apply(2, []byte{cmdSetLevel, formatLevel + 1}); err == nil {
		t.Fatal("a level above this build's was applied")
	}
}
