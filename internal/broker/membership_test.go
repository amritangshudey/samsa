package broker

import (
	"slices"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// joinNode adds node id to the trio with no configuration, as a node
// joining a running cluster starts.
func (tr *trio) joinNode(id int32) *harness {
	h := &harness{t: tr.t, cfg: Config{NodeID: id, Host: "host" + string(rune('0'+id)), Port: 9092, Join: true},
		disk: simsys.NewDisk(simsys.NewRand(uint64(id)), simsys.DiskConfig{}), rand: simsys.NewRand(uint64(10 + id))}
	tr.h[sys.NodeID(id)] = h
	h.start()
	return h
}

func (h *harness) voterChange(c sys.ConnID, key int16, version int16, req wire.Message) (corr int32) {
	h.t.Helper()
	return h.send(c, key, version, req)
}

func brokerIDs(md wire.MetadataResponse) []int32 {
	var out []int32
	for _, b := range md.Brokers {
		out = append(out, b.NodeID)
	}
	return out
}

// The whole path through the Kafka APIs: add a joining node, see it in
// Metadata everywhere, refuse the wrong requests, remove the leader.
func TestVoterChanges(t *testing.T) {
	tr := newTrio(t)
	tr.elect(1, 1, 2, 3)
	lead := tr.h[1]
	n4 := tr.joinNode(4)
	all := []sys.NodeID{1, 2, 3, 4}
	lead.open(1)
	add := &wire.AddRaftVoterRequest{TimeoutMs: 10000, VoterID: 4, Listeners: []wire.Listener{{Name: "CLIENT", Host: "host4", Port: 9092}, {Name: "PEER", Host: "host4", Port: 9192}}}
	corr := lead.voterChange(1, wire.KeyAddRaftVoter, 1, add)
	var resp wire.VoterChangeResponse
	for i := 0; i < 20 && !lead.recv(1, wire.KeyAddRaftVoter, 1, corr, &resp); i++ {
		lead.must(lead.node.OnTimer(timerHeartbeat))
		tr.deliver(all...)
		lead.advance(60 * time.Millisecond)
		tr.deliver(all...)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("add node 4: %+v", resp)
	}
	lead.must(lead.node.OnTimer(timerHeartbeat))
	tr.deliver(all...)
	n4.open(1)
	if got := brokerIDs(n4.metadata(1)); !slices.Equal(got, []int32{1, 2, 3, 4}) {
		t.Fatalf("node 4 lists brokers %v", got)
	}
	// Peer addresses travel in the configuration: node 4 started knowing
	// none, and the others learned node 4's from the request or the log.
	for _, h := range []*harness{lead, tr.h[2], tr.h[3], n4} {
		for _, id := range all {
			if want := "host" + string(rune('0'+id)) + ":9192"; id != sys.NodeID(h.cfg.NodeID) && h.net.addrs[id] != want {
				t.Fatalf("node %d sends to node %d at %q, want %q", h.cfg.NodeID, id, h.net.addrs[id], want)
			}
		}
	}
	for _, b := range n4.metadata(1).Brokers {
		if want := "host" + string(rune('0'+b.NodeID)); b.Host != want {
			t.Fatalf("node 4 knows broker %d as %q, want %q (from the log)", b.NodeID, b.Host, want)
		}
	}

	for _, tc := range []struct {
		name string
		h    *harness
		key  int16
		req  wire.Message
		want int16
	}{
		{"duplicate", lead, wire.KeyAddRaftVoter, add, wire.ErrDuplicateVoter},
		{"no listener", lead, wire.KeyAddRaftVoter, &wire.AddRaftVoterRequest{VoterID: 5}, wire.ErrInvalidRequest},
		{"absent", lead, wire.KeyRemoveRaftVoter, &wire.RemoveRaftVoterRequest{VoterID: 9}, wire.ErrVoterNotFound},
		{"other cluster", lead, wire.KeyRemoveRaftVoter, &wire.RemoveRaftVoterRequest{ClusterID: "other", VoterID: 2}, wire.ErrInconsistentClusterID},
		{"follower", tr.h[2], wire.KeyRemoveRaftVoter, &wire.RemoveRaftVoterRequest{VoterID: 3}, wire.ErrNotController},
	} {
		tc.h.open(2)
		var r wire.VoterChangeResponse
		version := int16(0)
		if tc.key == wire.KeyAddRaftVoter {
			version = 1
		}
		tc.h.call(2, tc.key, version, tc.req, &r)
		if r.ErrorCode != tc.want {
			t.Errorf("%s: %+v, want error %d", tc.name, r, tc.want)
		}
		tc.h.node.OnConnClose(2)
	}

	// The leader removes itself: answered with success as it steps down.
	corr = lead.voterChange(1, wire.KeyRemoveRaftVoter, 0, &wire.RemoveRaftVoterRequest{VoterID: 1})
	resp = wire.VoterChangeResponse{}
	for i := 0; i < 20 && !lead.recv(1, wire.KeyRemoveRaftVoter, 0, corr, &resp); i++ {
		lead.must(lead.node.OnTimer(timerHeartbeat))
		tr.deliver(all...)
	}
	if resp.ErrorCode != 0 || lead.node.Serving() {
		t.Fatalf("leader removing itself: %+v, still serving %v", resp, lead.node.Serving())
	}
	tr.elect(2, 2, 3, 4)
	tr.h[2].open(1)
	if got := brokerIDs(tr.h[2].metadata(1)); !slices.Equal(got, []int32{2, 3, 4}) {
		t.Fatalf("after removing node 1, brokers %v", got)
	}
}

// AddRaftVoter and RemoveRaftVoter need ALTER on the cluster.
func TestVoterChangesAllowedAndDenied(t *testing.T) {
	h := aclHarness(t, &Authorization{})
	req := &wire.RemoveRaftVoterRequest{VoterID: 7}
	var resp wire.VoterChangeResponse
	h.call(user, wire.KeyRemoveRaftVoter, 0, req, &resp)
	if resp.ErrorCode != wire.ErrClusterAuthorizationFailed {
		t.Fatalf("alice without ALTER: %+v", resp)
	}
	var add wire.VoterChangeResponse
	h.call(user, wire.KeyAddRaftVoter, 1, &wire.AddRaftVoterRequest{VoterID: 7, Listeners: []wire.Listener{{Name: "C", Host: "h", Port: 1}}}, &add)
	if add.ErrorCode != wire.ErrClusterAuthorizationFailed {
		t.Fatalf("alice adding without ALTER: %+v", add)
	}
	h.createACLs(admin, allowAlice(resCluster, clusterName, opAlter))
	resp = wire.VoterChangeResponse{}
	h.call(user, wire.KeyRemoveRaftVoter, 0, req, &resp)
	if resp.ErrorCode != wire.ErrVoterNotFound {
		t.Fatalf("alice with ALTER: %+v, want VOTER_NOT_FOUND", resp)
	}
}

// Member info round-trips and malformed info is refused (rule 4).
func TestMemberInfo(t *testing.T) {
	ls := []wire.Listener{{Name: "CLIENT", Host: "h1", Port: 9092}, {Name: "PEER", Host: "h1", Port: 7000}}
	got, err := decodeMemberInfo(encodeMemberInfo(ls))
	if err != nil || !slices.Equal(got, ls) {
		t.Fatalf("round trip: %v, %v", got, err)
	}
	b := encodeMemberInfo(ls)
	for i := range b {
		if _, err := decodeMemberInfo(b[:i]); err == nil {
			t.Fatalf("truncated at %d accepted", i)
		}
	}
	if _, err := decodeMemberInfo(append(b, 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
}
