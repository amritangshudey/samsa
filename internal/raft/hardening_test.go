package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Scenarios of docs/design/b3-raft-hardening.md, section 3. Each runs with
// the feature and without it, and must tell the two apart.

// tick moves the clock by d and fires what would expire by then: a
// leader's heartbeat each time, an election timer only once two election
// timeouts (its longest draw) have passed since it was armed. Messages
// that keep refuses are lost.
func (c *cluster) tick(d time.Duration, keep func(tmsg) bool) {
	c.t.Helper()
	c.now += sys.Instant(d)
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.down {
			continue
		}
		if n.armed[TimerHeartbeat] {
			n.armed[TimerHeartbeat] = false
			n.r.OnTimer(TimerHeartbeat)
			c.settle(n)
		}
		if n.armed[TimerElection] && c.now-n.armedAt[TimerElection] >= sys.Instant(2*300*time.Millisecond) {
			n.armed[TimerElection] = false
			n.r.OnTimer(TimerElection)
			c.settle(n)
		}
	}
	c.run(keep)
}

func cutOff(id sys.NodeID) func(tmsg) bool {
	return func(m tmsg) bool { return m.from != id && m.to != id }
}

// A node cut off for many election timeouts returns without deposing the
// leader: with PreVote its term never moved.
func TestReturningNodeDoesNotDepose(t *testing.T) {
	deposed := func(mutate func(*Config)) bool {
		c := newCluster(t, 3, mutate)
		c.startAll()
		c.elect(1)
		c.run(nil)
		term := c.r(1).Term()
		for range 50 { // 5 s, about ten election timeouts for node 3
			c.tick(100*time.Millisecond, cutOff(3))
		}
		if c.r(1).Role() != Leader || c.r(1).Term() != term {
			t.Fatalf("the leader changed while node 3 was away")
		}
		for range 5 {
			c.tick(100*time.Millisecond, nil)
		}
		return c.r(1).Role() != Leader || c.r(1).Term() != term
	}
	if deposed(nil) {
		t.Error("a returning node deposed a healthy leader")
	}
	if !deposed(func(c *Config) { c.Unsafe |= MutNoPreVote }) {
		t.Error("without PreVote the returning node did not depose the leader: this test proves nothing")
	}
}

// A leader that can still reach a follower, but hears from nobody, steps
// down, so that the follower stops ignoring candidates and the majority
// elects someone else.
func TestCheckQuorumLetsTheMajorityElect(t *testing.T) {
	elected := func(mutate func(*Config)) bool {
		c := newCluster(t, 3, mutate)
		c.startAll()
		c.elect(1)
		c.run(nil)
		term := c.r(1).Term()
		// Node 1 can only send, and only to node 2; 2 and 3 talk freely.
		oneWay := func(m tmsg) bool { return (m.from == 1 && m.to == 2) || (m.from != 1 && m.to != 1) }
		for range 60 {
			c.tick(100*time.Millisecond, oneWay)
		}
		for _, id := range []sys.NodeID{2, 3} {
			if c.r(id).Role() == Leader && c.r(id).Term() > term {
				return true
			}
		}
		return false
	}
	if !elected(nil) {
		t.Error("the majority never elected a leader while the old one could reach node 2")
	}
	if elected(func(c *Config) { c.Unsafe |= MutNoCheckQuorum }) {
		t.Error("without CheckQuorum the majority still elected: this test proves nothing")
	}
}

// A transfer hands leadership to the chosen follower in one election, and
// the old leader serves nothing in between.
func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.propose(1, "a")
	c.run(nil)
	c.tick(10*time.Millisecond, nil)
	if !c.r(1).Lease() {
		t.Fatal("setup: no lease")
	}
	if err := c.r(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	if c.r(1).Lease() || !c.r(1).Transferring() {
		t.Fatal("the leader kept its lease during a transfer")
	}
	if _, err := c.r(1).Propose([]byte("b")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose during a transfer: %v", err)
	}
	c.run(nil)
	if c.r(3).Role() != Leader || c.r(3).Term() != 2 || c.r(1).Role() != Follower {
		t.Fatalf("after the transfer: node 3 role %d term %d, node 1 role %d", c.r(3).Role(), c.r(3).Term(), c.r(1).Role())
	}
	if c.r(2).Term() != 2 {
		t.Fatalf("node 2 is in term %d: a second election happened", c.r(2).Term())
	}
	// Choosing by itself, the leader picks a follower with the whole log.
	c.propose(3, "b")
	c.run(only(3, 1)) // node 2 misses "b"
	if err := c.r(3).TransferLeadership(0); err != nil {
		t.Fatal(err)
	}
	c.run(nil)
	if c.r(1).Role() != Leader {
		t.Fatalf("the transfer went to a follower without the whole log")
	}
}

// A transfer to a dead follower that never caught up is abandoned after an
// election timeout, and the leader serves again: no TimeoutNow went out.
// With MutTransferKeepsLease it never stopped.
func TestLeadershipTransferAbandoned(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.crash(3)
	c.propose(1, "a") // node 3 misses it, so it never gets TimeoutNow
	c.run(nil)
	if err := c.r(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		c.tick(100*time.Millisecond, nil)
	}
	if c.r(1).Role() != Leader || c.r(1).Transferring() || !c.r(1).Lease() {
		t.Fatalf("after an abandoned transfer: role %d, transferring %v, lease %v", c.r(1).Role(), c.r(1).Transferring(), c.r(1).Lease())
	}
	if err := c.r(1).TransferLeadership(9); !errors.Is(err, ErrTransferring) {
		t.Fatalf("transfer to a node outside the cluster: %v", err)
	}

	m := newCluster(t, 3, func(cfg *Config) { cfg.Unsafe |= MutTransferKeepsLease })
	m.startAll()
	m.elect(1)
	m.run(nil)
	m.tick(10*time.Millisecond, nil)
	if err := m.r(1).TransferLeadership(2); err != nil {
		t.Fatal(err)
	}
	if !m.r(1).Lease() {
		t.Fatal("MutTransferKeepsLease gave up the lease: the switch does nothing")
	}
}

// BUGS.md S8: a TimeoutNow that arrives after the transfer was abandoned
// starts an election that ignores leases. The leader that sent it must not
// hold a lease by then, so abandoning after TimeoutNow steps down.
func TestTimeoutNowAfterAbandon(t *testing.T) {
	for _, mutated := range []bool{false, true} {
		c := newCluster(t, 3, func(cfg *Config) {
			if mutated {
				cfg.Unsafe |= MutAbandonKeepsLeading
			}
		})
		c.startAll()
		c.elect(1)
		c.run(nil)
		c.tick(10*time.Millisecond, nil)
		if err := c.r(1).TransferLeadership(2); err != nil {
			t.Fatal(err)
		}
		late := c.queue // the TimeoutNow, held back
		c.queue = nil
		if len(late) != 1 {
			t.Fatalf("%d messages after TransferLeadership, want the TimeoutNow", len(late))
		}
		for range 5 {
			c.tick(100*time.Millisecond, nil)
		}
		c.queue = append(c.queue, late...)
		c.step(len(c.queue) - 1) // the TimeoutNow reaches node 2 at last
		c.run(only(2, 3))        // node 1 has not heard yet: any overlap shows now
		overlap := c.r(1).Lease() && c.r(2).Role() == Leader && c.r(2).Term() > c.r(1).Term()
		if overlap != mutated {
			t.Fatalf("mutated %v: node 1 leads %v with lease %v while node 2 leads %v in term %d",
				mutated, c.r(1).Role() == Leader, c.r(1).Lease(), c.r(2).Role() == Leader, c.r(2).Term())
		}
	}
}
