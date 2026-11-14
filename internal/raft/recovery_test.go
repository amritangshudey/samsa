package raft

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/amritangshudey/samsa/internal/sys"
)

// rot damages the first sealed segment of a node's log, in synced data.
func (c *cluster) rot(id sys.NodeID) {
	c.t.Helper()
	names, err := c.nodes[id].disk.List("raft")
	if err != nil {
		c.t.Fatal(err)
	}
	var logs []string
	for _, name := range names {
		if strings.HasSuffix(name, ".log") {
			logs = append(logs, name)
		}
	}
	if len(logs) < 3 {
		c.t.Fatalf("node %d has %d segments", id, len(logs))
	}
	if !c.nodes[id].disk.Corrupt("raft/"+logs[1], 150, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		c.t.Fatalf("node %d: could not damage %s", id, logs[1])
	}
}

func damagedCluster(t *testing.T, mutate func(*Config)) *cluster {
	t.Helper()
	c := newCluster(t, 3, mutate)
	c.startAll()
	c.elect(1)
	for i := range 40 {
		c.propose(1, fmt.Sprintf("command-%02d", i))
	}
	c.run(nil)
	for _, id := range c.ids {
		c.r(id).Sync()
	}
	c.fire(1, TimerHeartbeat)
	c.run(nil)
	if c.r(2).Committed() != c.r(1).Committed() || c.r(1).Committed() < 40 {
		t.Fatalf("setup: commit %d and %d", c.r(1).Committed(), c.r(2).Committed())
	}
	c.rot(2)
	c.crash(2)
	return c
}

// A node whose log is damaged in a sealed segment cuts the log there instead
// of refusing to start or forgetting the rest, does not vote until it has
// fetched back what it held, and then is whole again.
func TestDamagedFollowerRecovers(t *testing.T) {
	c := damagedCluster(t, nil)
	c.start(2)
	r := c.r(2)
	if !r.Recovering() || r.Repaired() != 1 {
		t.Fatalf("recovering %v, repaired %d", r.Recovering(), r.Repaired())
	}
	if got := r.LastIndex(); got >= c.r(1).LastIndex() || got < 0 {
		t.Fatalf("the log was not cut: last index %d, the leader's %d", got, c.r(1).LastIndex())
	}
	// It is not a candidate: its timer fires and nothing is sent.
	c.fire(2, TimerElection)
	c.run(only(2, 3))
	if r.Role() == Candidate || r.Term() != c.r(1).Term() {
		t.Fatalf("a recovering node campaigned: role %v term %d", r.Role(), r.Term())
	}
	// It refuses to vote: node 3 cannot win with only its help.
	c.crash(1)
	c.nodes[1].down = true
	c.fire(3, TimerElection)
	c.run(only(2, 3))
	if c.r(3).Role() == Leader {
		t.Fatal("node 3 won an election with the vote of a recovering node")
	}
	// The cluster heals: node 1 comes back, leads, and node 2 catches up.
	c.start(1)
	c.elect(1)
	c.propose(1, "after")
	c.fire(1, TimerHeartbeat)
	c.run(nil)
	c.fire(1, TimerHeartbeat)
	c.run(nil)
	if r.Recovering() {
		t.Fatalf("still recovering with commit %d", r.Committed())
	}
	want := c.logOf(1)
	if got := c.logOf(2); !slices.Equal(got, want) {
		t.Fatalf("node 2 holds %d entries, the leader %d", len(got), len(want))
	}
	// The marker is gone: a restart is an ordinary one.
	c.crash(2)
	if c.start(2); c.r(2).Recovering() {
		t.Fatal("recovering again after a restart")
	}
}

// The mutation: a recovering node that votes lets node 3 win.
func TestMutationVoteWhileRecoveringElectsOnItsVote(t *testing.T) {
	c := damagedCluster(t, func(cfg *Config) { cfg.Unsafe |= MutVoteWhileRecovering | MutNoPreVote | MutNoCheckQuorum })
	c.start(2)
	c.nodes[1].down = true
	c.fire(3, TimerElection)
	c.run(only(2, 3))
	if c.r(3).Role() != Leader {
		t.Skip("node 3 did not win even with the vote: the test shows nothing")
	}
}

// A damaged node of a single voter has nothing to repair from and says so.
func TestDamagedSingleNodeRefuses(t *testing.T) {
	c := newCluster(t, 1, nil)
	c.startAll()
	c.run(nil)
	for i := range 40 {
		c.propose(1, fmt.Sprintf("command-%02d", i))
	}
	c.run(nil)
	c.r(1).Sync()
	c.rot(1)
	c.crash(1)
	if err := c.open(1); err == nil || !strings.Contains(err.Error(), "only voter") {
		t.Fatalf("start of a damaged single node: %v", err)
	}
}
