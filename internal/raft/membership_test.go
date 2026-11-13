package raft

import (
	"errors"
	"slices"
	"testing"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// join adds a node with an empty disk and no configuration, as a node
// joining a running cluster starts (design, section 6, finding 2).
func (c *cluster) join(id sys.NodeID) *Raft {
	c.t.Helper()
	c.nodes[id] = &tnode{c: c, id: id, joining: true, disk: simsys.NewDisk(c.rng.Fork(), simsys.DiskConfig{})}
	c.ids = append(c.ids, id)
	return c.start(id)
}

func members(idList ...sys.NodeID) []Member {
	var ms []Member
	for _, id := range idList {
		ms = append(ms, Member{ID: id, Info: []byte{byte(id)}})
	}
	return ms
}

func voterIDs(r *Raft) []sys.NodeID { return ids(r.Members()) }

// mustChange makes a change on leader and runs the cluster until it commits.
func (c *cluster) mustChange(leader sys.NodeID, ms []Member) {
	c.t.Helper()
	index, err := c.r(leader).ChangeMembers(ms)
	if err != nil {
		c.t.Fatalf("change to %v: %v", ids(ms), err)
	}
	c.settle(c.nodes[leader])
	c.run(nil)
	c.tick(0, nil) // a heartbeat carries the new commit index
	if c.r(leader).Committed() <= index && c.r(leader).Role() == Leader {
		c.t.Fatalf("change to %v at %d not committed (commit %d)", ids(ms), index, c.r(leader).Committed())
	}
}

// The leader's rules (design, section 3), one refusal at a time.
func TestChangeRules(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.fire(1, TimerElection)
	c.run(votes) // elected, but its no-op has not reached anyone
	if c.r(1).Role() != Leader {
		t.Fatal("node 1 not elected")
	}
	c.join(4)
	if err := c.r(1).AddLearner(4); err != nil {
		t.Fatal(err)
	}
	if _, err := c.r(1).ChangeMembers(members(1, 2, 3, 4)); !errors.Is(err, ErrChangePending) {
		t.Fatalf("before an entry of its term committed: %v, want ErrChangePending (the erratum)", err)
	}
	c.run(nil)
	c.tick(0, nil)
	for _, tc := range []struct {
		ms   []Member
		want error
	}{
		{members(1, 2, 3), ErrBadChange},                             // no change
		{members(1, 2, 3, 4, 5), ErrBadChange},                       // two added
		{members(1), ErrBadChange},                                   // two removed
		{members(1, 2, 3, 5), ErrNotCaughtUp},                        // not a learner
		{[]Member{{ID: 2}, {ID: 1}, {ID: 3}, {ID: 4}}, ErrBadChange}, // unsorted
	} {
		if _, err := c.r(1).ChangeMembers(tc.ms); !errors.Is(err, tc.want) {
			t.Errorf("change to %v: %v, want %v", ids(tc.ms), err, tc.want)
		}
	}
	if _, err := c.r(1).ChangeMembers(members(1, 2, 3, 4)); err != nil {
		t.Fatalf("add caught-up learner 4: %v", err)
	}
	if _, err := c.r(1).ChangeMembers(members(1, 2, 4)); !errors.Is(err, ErrChangePending) {
		t.Fatalf("second change while the first is uncommitted: %v, want ErrChangePending", err)
	}
	if _, err := c.r(2).ChangeMembers(members(1, 2)); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("change on a follower: %v", err)
	}

	m := newCluster(t, 3, func(cfg *Config) { cfg.Unsafe |= MutTwoChanges | MutChangeBeforeCommit })
	m.startAll()
	m.fire(1, TimerElection)
	m.run(votes)
	if _, err := m.r(1).ChangeMembers(members(1, 2)); err != nil {
		t.Fatalf("mutated, before commit: %v", err)
	}
	if _, err := m.r(1).ChangeMembers(members(1)); err != nil {
		t.Fatalf("mutated, two changes at once: %v", err)
	}
}

// A joining node catches up as a learner, becomes a voter, and counts:
// with four voters, a commit needs three, and the new node can be one.
func TestAddVoter(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.propose(1, "a")
	c.run(nil)
	n4 := c.join(4)
	if n4.isVoter(4) || len(n4.Members()) != 0 {
		t.Fatalf("a joining node starts with configuration %v", voterIDs(n4))
	}
	c.fire(4, TimerElection) // never stands while not a voter
	if n4.Role() != Follower || n4.Term() != 0 {
		t.Fatalf("joining node campaigned: role %d term %d", n4.Role(), n4.Term())
	}
	if err := c.r(1).AddLearner(4); err != nil {
		t.Fatal(err)
	}
	c.run(nil)
	c.mustChange(1, members(1, 2, 3, 4))
	for _, id := range c.ids {
		if got := voterIDs(c.r(id)); !slices.Equal(got, []sys.NodeID{1, 2, 3, 4}) {
			t.Fatalf("node %d has voters %v", id, got)
		}
	}
	if string(n4.Members()[3].Info) != "\x04" {
		t.Fatalf("member info not replicated: %q", n4.Members()[3].Info)
	}
	c.crash(2)
	i := c.propose(1, "b")
	c.run(nil)
	if c.r(1).Committed() <= i {
		t.Fatal("three of four voters (1, 3, 4) did not commit")
	}
	c.crash(3)
	j := c.propose(1, "c")
	c.run(nil)
	if c.r(1).Committed() > j {
		t.Fatal("two of four voters committed")
	}
}

// Removing the leader: it leads until the removal commits, then steps down
// and never stands again; the others elect a leader among themselves.
func TestRemoveLeader(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.tick(0, nil)
	c.mustChange(1, members(2, 3))
	if c.r(1).Role() == Leader || c.r(1).isVoter(1) {
		t.Fatalf("removed leader: role %d, voter %v", c.r(1).Role(), c.r(1).isVoter(1))
	}
	c.fire(1, TimerElection)
	if c.r(1).Role() != Follower {
		t.Fatal("a removed node stood for election")
	}
	c.elect(2)
	if got := voterIDs(c.r(2)); !slices.Equal(got, []sys.NodeID{2, 3}) {
		t.Fatalf("new leader has voters %v", got)
	}
	i := c.propose(2, "x")
	c.run(only(2, 3))
	if c.r(2).Committed() <= i {
		t.Fatal("two voters of two did not commit")
	}
}

// A configuration entry that loses is truncated, and the node falls back
// to the configuration before it (thesis 4.1).
func TestConfigTruncated(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.tick(0, nil)
	if _, err := c.r(1).ChangeMembers(members(1, 2)); err != nil {
		t.Fatal(err)
	}
	c.settle(c.nodes[1])
	c.queue = nil // nobody else ever sees it
	if got := voterIDs(c.r(1)); !slices.Equal(got, []sys.NodeID{1, 2}) {
		t.Fatalf("not in force on append: %v", got)
	}
	c.crash(1)
	c.elect(2)
	c.propose(2, "y")
	c.start(1)
	c.run(nil)
	c.tick(0, nil)
	if got := voterIDs(c.r(1)); !slices.Equal(got, []sys.NodeID{1, 2, 3}) {
		t.Fatalf("after truncation node 1 has voters %v, want the old configuration", got)
	}
}

// The configuration survives a restart, from the log and from a snapshot.
func TestConfigRestart(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.tick(0, nil)
	c.mustChange(1, members(1, 2))
	c.crash(2)
	c.start(2)
	if got := voterIDs(c.r(2)); !slices.Equal(got, []sys.NodeID{1, 2}) {
		t.Fatalf("after restart, from the log: %v", got)
	}
	r2 := c.r(2)
	for range 20 {
		c.propose(1, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
		c.run(nil)
	}
	c.tick(0, nil)
	r2.SaveSnapshot(r2.Committed()-1, []byte("state"))
	r2.Compact(r2.Committed())
	if r2.LogStart() == 0 {
		t.Fatal("nothing compacted")
	}
	c.crash(2)
	c.start(2)
	if got := voterIDs(c.r(2)); !slices.Equal(got, []sys.NodeID{1, 2}) {
		t.Fatalf("after restart, from the snapshot: %v", got)
	}
}

// Ongaro's 2015 counterexample 1 (raft-dev, "bug in single-server
// membership changes"), step by step. In {1,2,3,4}, leader 1 appends
// D = +5 and only 5 gets it. Node 2 wins term 2 with {2,3,4} and appends
// E = -1 at once, committing it with {2,3}, a majority of E. Node 1 comes
// back and wins term 3 with {1,4,5}, a majority of D, then overwrites the
// committed E. The erratum's rule refuses E until node 2's no-op commits.
func TestErratumCounterexample(t *testing.T) {
	for _, mutated := range []bool{false, true} {
		c := newSeededCluster(t, 1, 5, func(cfg *Config) {
			if mutated {
				cfg.Unsafe |= MutChangeBeforeCommit
			}
		})
		c.boot = []sys.NodeID{1, 2, 3, 4}
		c.nodes[5].joining = true
		c.startAll()
		c.elect(1)
		c.run(nil)
		c.tick(0, nil)
		if err := c.r(1).AddLearner(5); err != nil {
			t.Fatal(err)
		}
		c.run(nil)
		c.tick(0, nil)
		if _, err := c.r(1).ChangeMembers(members(1, 2, 3, 4, 5)); err != nil {
			t.Fatal(err)
		}
		c.settle(c.nodes[1])
		c.run(only(1, 5)) // D reaches node 5 only: not committed
		c.crash(1)

		c.fire(2, TimerElection)
		c.run(both(votes, only(2, 3, 4)))
		if c.r(2).Role() != Leader {
			t.Fatalf("mutated %v: node 2 not elected in term 2", mutated)
		}
		_, err := c.r(2).ChangeMembers(members(2, 3, 4))
		if !mutated {
			if !errors.Is(err, ErrChangePending) {
				t.Fatalf("E before node 2's no-op committed: %v, want ErrChangePending", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("mutated: E: %v", err)
		}
		c.settle(c.nodes[2])
		c.run(only(2, 3))
		c.tick(0, only(2, 3))
		e := c.r(2).LastIndex()
		if c.r(2).Committed() <= e {
			t.Fatalf("mutated: E at %d not committed by {2,3} (commit %d)", e, c.r(2).Committed())
		}

		c.start(1)
		for range 2 { // the first PreVote only teaches node 1 that term 2 exists
			c.fire(1, TimerElection)
			c.run(both(votes, only(1, 4, 5)))
		}
		if c.r(1).Role() != Leader {
			t.Fatalf("mutated: node 1 not elected with {1,4,5}")
		}
		// Node 1 now replicates D, overwriting E where it was committed.
		for round := 0; round < 20; round++ {
			c.r(1).OnTimer(TimerHeartbeat)
			c.r(1).Flush()
			for len(c.queue) > 0 {
				m := c.queue[0]
				c.queue = c.queue[1:]
				if n := c.nodes[m.to]; !n.down {
					n.r.Step(m.from, m.raw)
					n.r.Flush()
					if err := n.r.Err(); err != nil {
						t.Logf("mutated: caught: %v", err)
						return
					}
				}
			}
		}
		t.Fatal("mutated: node 1 overwrote nothing: the counterexample did not play out")
	}
}

// BUGS.md S9: a node left the only voter by a change must not lead the
// moment it starts. The leader that removed itself counts this node's
// acknowledgements for its lease until the removal commits; the node
// promised to ignore candidates, itself included, for an election timeout.
// A node alone in its bootstrap configuration still leads at once.
func TestAloneByChangeWaits(t *testing.T) {
	c := newCluster(t, 2, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.tick(0, nil)
	if _, err := c.r(1).ChangeMembers(members(2)); err != nil {
		t.Fatal(err)
	}
	c.settle(c.nodes[1])
	c.run(nil) // node 2 has {2}; node 1 still leads until it hears the ack
	c.crash(2)
	if r := c.start(2); r.Role() == Leader {
		t.Fatal("node 2, alone by an uncommitted change, led at once on restart")
	}
	single := newCluster(t, 1, nil)
	if r := single.start(1); r.Role() != Leader {
		t.Fatal("a node alone in its bootstrap configuration did not lead at once")
	}
}
