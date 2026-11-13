package raft

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/storage"
	"github.com/amritangshudey/samsa/internal/sys"
)

// tmsg is a message in flight between two test nodes.
type tmsg struct {
	from, to sys.NodeID
	raw      []byte
}

// tnode is one node of a test cluster: a Raft instance, its disk, and the
// Env that records what it asks for.
type tnode struct {
	c     *cluster
	id    sys.NodeID
	r     *Raft
	disk  *simsys.Disk
	armed map[Timer]bool
	arms  int // times the election timer was armed
	down  bool
	// joining marks a node added after the cluster formed: it starts with
	// no configuration (membership_test.go).
	joining bool
	// armedAt is when each timer was last armed, for tests that move the
	// clock themselves (hardening_test.go).
	armedAt map[Timer]sys.Instant
}

func (n *tnode) Send(to sys.NodeID, msg []byte) {
	n.c.queue = append(n.c.queue, tmsg{n.id, to, slices.Clone(msg)})
}
func (n *tnode) SetTimer(t Timer, _ time.Duration) {
	n.armed[t] = true
	if n.armedAt == nil {
		n.armedAt = map[Timer]sys.Instant{}
	}
	n.armedAt[t] = n.c.now
	if t == TimerElection {
		n.arms++
	}
}
func (n *tnode) CancelTimer(t Timer) { n.armed[t] = false }
func (n *tnode) Rand() uint64        { return n.c.rng.Uint64() }
func (n *tnode) Now() sys.Instant    { return n.c.now }

// cluster is a set of Raft nodes with no network and no clock: the test
// decides which message is delivered and which timer fires.
type cluster struct {
	t      *testing.T
	rng    *simsys.Rand
	ids    []sys.NodeID
	nodes  map[sys.NodeID]*tnode
	queue  []tmsg
	mutate func(*Config)
	// boot, if set, is the bootstrap configuration; otherwise every id.
	boot []sys.NodeID
	now  sys.Instant // moved only by election timers; see passTime
	// manualFlush stops run from flushing nodes, for tests about what
	// happens before entries are durable.
	manualFlush bool
}

func newCluster(t *testing.T, n int, mutate func(*Config)) *cluster {
	return newSeededCluster(t, 1, n, mutate)
}

func newSeededCluster(t *testing.T, seed uint64, n int, mutate func(*Config)) *cluster {
	c := &cluster{t: t, rng: simsys.NewRand(seed), nodes: map[sys.NodeID]*tnode{}, mutate: mutate}
	for i := range n {
		c.ids = append(c.ids, sys.NodeID(i+1))
	}
	for _, id := range c.ids {
		c.nodes[id] = &tnode{c: c, id: id, disk: simsys.NewDisk(c.rng.Fork(), simsys.DiskConfig{})}
	}
	return c
}

// open (re)creates a node's Raft instance on its disk.
func (c *cluster) open(id sys.NodeID) error {
	n := c.nodes[id]
	cfg := Config{ID: id, Peers: c.ids, Dir: "raft", Log: storage.Config{SegmentBytes: 512, MaxPayloadBytes: 128}}
	if c.boot != nil {
		cfg.Peers = c.boot
	}
	if n.joining {
		cfg.Peers = nil
	}
	if c.mutate != nil {
		c.mutate(&cfg)
	}
	n.armed, n.down = map[Timer]bool{}, false
	n.r = New(cfg, n, n.disk)
	return n.r.Start()
}

func (c *cluster) start(id sys.NodeID) *Raft {
	c.t.Helper()
	if err := c.open(id); err != nil {
		c.t.Fatalf("start node %d: %v", id, err)
	}
	return c.r(id)
}

func (c *cluster) startAll() {
	c.t.Helper()
	for _, id := range c.ids {
		c.start(id)
	}
}

// crash cuts a node's power: unsynced data is lost, messages to it vanish.
func (c *cluster) crash(id sys.NodeID) {
	n := c.nodes[id]
	n.down = true
	n.disk.Crash()
	n.disk.Restart()
}

func (c *cluster) r(id sys.NodeID) *Raft { return c.nodes[id].r }

// step delivers the i-th queued message and checks that nobody failed.
func (c *cluster) step(i int) {
	c.t.Helper()
	m := c.queue[i]
	c.queue = slices.Delete(c.queue, i, i+1)
	if n := c.nodes[m.to]; !n.down {
		n.r.Step(m.from, m.raw)
		c.settle(n)
	}
}

func (c *cluster) settle(n *tnode) {
	c.t.Helper()
	if !c.manualFlush && n.r.NeedsFlush() {
		n.r.Flush()
	}
	if err := n.r.Err(); err != nil {
		c.t.Fatalf("node %d stopped: %v", n.id, err)
	}
}

// run delivers queued messages in order until none are left. keep, if not
// nil, says which messages are delivered; the others are lost.
func (c *cluster) run(keep func(m tmsg) bool) {
	c.t.Helper()
	for limit := 10000; len(c.queue) > 0; limit-- {
		if limit == 0 {
			c.t.Fatal("messages never stop")
		}
		if keep != nil && !keep(c.queue[0]) {
			c.queue = c.queue[1:]
			continue
		}
		c.step(0)
	}
}

// fire expires a timer on a node, if it is armed.
func (c *cluster) fire(id sys.NodeID, t Timer) {
	c.t.Helper()
	n := c.nodes[id]
	if n.down || !n.armed[t] {
		c.t.Fatalf("node %d: timer %d is not armed", id, t)
	}
	n.armed[t] = false
	if t == TimerElection {
		c.passTime()
	}
	n.r.OnTimer(t)
	c.settle(n)
}

// passTime moves the clock by two election timeouts, the most an election
// timer can take: an election timer that fires means at least that much
// time has passed for its node. Moving everyone's clock is the weakest
// assumption, since every node then forgets its leader.
func (c *cluster) passTime() { c.now += sys.Instant(2 * 300 * time.Millisecond) }

// elect makes id the leader with the votes of everyone who will give one.
func (c *cluster) elect(id sys.NodeID) {
	c.t.Helper()
	c.fire(id, TimerElection)
	c.run(nil)
	if c.r(id).Role() != Leader {
		c.t.Fatalf("node %d did not become leader (term %d)", id, c.r(id).Term())
	}
}

func (c *cluster) propose(id sys.NodeID, cmd string) int64 {
	c.t.Helper()
	index, err := c.r(id).Propose([]byte(cmd))
	if err != nil {
		c.t.Fatalf("propose on node %d: %v", id, err)
	}
	c.settle(c.nodes[id])
	return index
}

// only keeps messages between the given nodes.
func only(ids ...sys.NodeID) func(tmsg) bool {
	return func(m tmsg) bool { return slices.Contains(ids, m.from) && slices.Contains(ids, m.to) }
}

// votes keeps only the messages of an election.
func votes(m tmsg) bool {
	return slices.Contains([]byte{msgRequestVote, msgVoteResponse, msgPreVote, msgPreVoteResponse, msgTransferVote}, m.raw[1])
}

// classic is Raft as in the paper, without PreVote and CheckQuorum, for
// tests of the vote rules themselves: they count terms that PreVote would
// never start, and keep leaders that CheckQuorum would retire.
func classic(cfg *Config) { cfg.Unsafe |= MutNoPreVote | MutNoCheckQuorum }

// both keeps the messages that both filters keep.
func both(a, b func(tmsg) bool) func(tmsg) bool { return func(m tmsg) bool { return a(m) && b(m) } }

// logOf renders a node's log as "term:command" strings.
func (c *cluster) logOf(id sys.NodeID) []string {
	c.t.Helper()
	var out []string
	for i := int64(0); i <= c.r(id).LastIndex(); i++ {
		cmd, noop, err := c.r(id).Read(i)
		if err != nil {
			c.t.Fatalf("node %d: read %d: %v", id, i, err)
		}
		if noop {
			cmd = []byte("noop")
		}
		out = append(out, fmt.Sprintf("%d:%s", c.r(id).EntryTerm(i), cmd))
	}
	return out
}

func TestSingleNodeElectsItselfAndCommitsAfterFlush(t *testing.T) {
	c := newCluster(t, 1, nil)
	c.manualFlush = true
	r := c.start(1)
	if r.Role() != Leader || r.Term() != 1 {
		t.Fatalf("role %d term %d, want leader of term 1", r.Role(), r.Term())
	}
	index := c.propose(1, "a")
	if r.Committed() != 0 {
		t.Fatalf("committed %d before the entries were synced", r.Committed())
	}
	r.Flush()
	if r.Committed() != index+1 {
		t.Fatalf("committed %d after flush, want %d", r.Committed(), index+1)
	}
	// After a restart the node knows nothing is committed until it has
	// committed an entry of its new term.
	c.crash(1)
	r = c.start(1)
	if r.Term() != 2 || r.Committed() != 0 {
		t.Fatalf("after restart: term %d committed %d", r.Term(), r.Committed())
	}
	r.Flush()
	if got := c.logOf(1); !slices.Equal(got, []string{"1:noop", "1:a", "2:noop"}) || r.Committed() != 3 {
		t.Fatalf("after restart: log %v, committed %d", got, r.Committed())
	}
}

func TestReplicationAndCommit(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.propose(1, "a")
	c.propose(1, "b")
	c.run(nil)
	want := []string{"1:noop", "1:a", "1:b"}
	if got := c.logOf(1); !slices.Equal(got, want) || c.r(1).Committed() != 3 {
		t.Fatalf("leader: log %v, committed %d", got, c.r(1).Committed())
	}
	// Followers learn the commit index from the next message.
	c.fire(1, TimerHeartbeat)
	c.run(nil)
	for _, id := range c.ids {
		if got := c.logOf(id); !slices.Equal(got, want) || c.r(id).Committed() != 3 {
			t.Errorf("node %d: log %v, committed %d", id, got, c.r(id).Committed())
		}
		if leader, known := c.r(id).Leader(); !known || leader != 1 {
			t.Errorf("node %d believes the leader is %d (known %v)", id, leader, known)
		}
	}
	// One node of three may be gone; two of three may not.
	c.crash(3)
	c.propose(1, "c")
	c.run(nil)
	if c.r(1).Committed() != 4 {
		t.Fatalf("with one node down: committed %d, want 4", c.r(1).Committed())
	}
	c.crash(2)
	c.propose(1, "d")
	c.run(nil)
	if c.r(1).Committed() != 4 {
		t.Fatalf("without a majority: committed %d, want 4", c.r(1).Committed())
	}
}

// Bug 1. A vote must be on disk before it is given, or a node that loses
// power votes twice in one term.
func TestVoteSurvivesPowerLoss(t *testing.T) {
	votesTwice := func(mutate func(*Config)) bool {
		c := newCluster(t, 3, func(cfg *Config) {
			classic(cfg)
			if mutate != nil {
				mutate(cfg)
			}
		})
		c.startAll()
		c.fire(1, TimerElection)
		c.run(both(only(1, 2), votes)) // node 2 votes for 1 in term 1; node 3 hears nothing
		if c.r(1).Role() != Leader {
			t.Fatal("setup: node 1 not elected")
		}
		c.crash(2)
		c.start(2)
		c.fire(3, TimerElection) // node 3 stands in the same term 1
		c.run(only(2, 3))
		return c.r(3).Role() == Leader // a second leader in term 1
	}
	if votesTwice(nil) {
		t.Error("a node voted twice in one term after a power loss")
	}
	if !votesTwice(func(c *Config) { c.Unsafe |= MutVoteWithoutPersist }) {
		t.Error("the vote was not lost even without persisting it: this test proves nothing")
	}
}

// Bug 2, the paper's Figure 8. An entry of an earlier term that is on a
// majority is not committed, because a node that never saw it can still
// win an election and overwrite it.
func TestFigure8(t *testing.T) {
	committed := func(mutate func(*Config)) (old bool) {
		c := newCluster(t, 5, func(cfg *Config) {
			cfg.MaxAppendBytes = 1 // one entry per message, so the test can place each
			if mutate != nil {
				mutate(cfg)
			}
		})
		c.startAll()
		// (a) Node 1 leads term 1 and gets entry x to node 2 only.
		c.elect(1)
		c.run(nil)
		x := c.propose(1, "x")
		c.run(only(1, 2))
		// (b) Node 1 dies. Node 5 wins term 2 with nodes 3 and 4 and
		// stores an entry of its own at the same index.
		c.crash(1)
		c.fire(5, TimerElection)
		c.run(both(only(3, 4, 5), votes)) // its entries reach nobody
		if c.r(5).Role() != Leader {
			t.Fatal("setup: node 5 not elected")
		}
		c.crash(5)
		// (c) Node 1 returns, wins term 3, and copies x to node 3: x is
		// now on nodes 1, 2 and 3, a majority.
		c.start(1)
		for range 2 { // term 2 is taken: nodes 3 and 4 voted in it
			c.fire(1, TimerElection)
			c.run(both(only(1, 2, 3, 4), votes))
		}
		if c.r(1).Role() != Leader || c.r(1).Term() != 3 {
			t.Fatalf("setup: node 1 role %d term %d", c.r(1).Role(), c.r(1).Term())
		}
		// Node 3 is caught up exactly to x; node 2 takes the new term's
		// no-op, so the leader knows x is on three nodes.
		for c.r(3).LastIndex() < x {
			c.fire(1, TimerHeartbeat)
			c.run(func(m tmsg) bool {
				return only(1, 3)(m) && (m.from == 3 || c.r(3).LastIndex() < x)
			})
		}
		c.fire(1, TimerHeartbeat)
		c.run(only(1, 2))
		if got := c.logOf(3); got[x] != "1:x" {
			t.Fatalf("setup: node 3 holds %v", got)
		}
		return c.r(1).Committed() > x
	}
	if committed(nil) {
		t.Error("an entry of an earlier term was committed by counting replicas")
	}
	if !committed(func(c *Config) { c.Unsafe = MutCommitAnyTerm }) {
		t.Error("the entry was not committed even with the rule removed: this test proves nothing")
	}
}

// Bug 3. An old or repeated AppendEntries must not shorten the log.
func TestStaleAppendEntriesDoesNotTruncate(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.propose(1, "a")
	var old []tmsg
	for _, m := range c.queue { // keep a copy of the message that carries only "a"
		if m.to == 2 {
			old = append(old, m)
		}
	}
	c.run(nil)
	c.propose(1, "b")
	c.propose(1, "c")
	c.run(nil)
	before := c.logOf(2)
	c.queue = append(c.queue, old...)
	c.run(nil)
	if got := c.logOf(2); !slices.Equal(got, before) || len(got) != 4 {
		t.Fatalf("log changed from %v to %v", before, got)
	}
}

// Bug 4. A follower answers only for entries that are on its disk.
func TestFollowerAcknowledgesOnlyAfterSync(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.manualFlush = true
	index := c.propose(1, "a")
	c.r(1).Flush()
	c.run(nil) // followers have appended, not synced
	if len(c.queue) != 0 || c.r(1).Committed() > index {
		t.Fatalf("before the followers synced: %d answers, committed %d", len(c.queue), c.r(1).Committed())
	}
	c.r(2).Flush()
	if len(c.queue) != 1 {
		t.Fatalf("after node 2 synced: %d answers, want 1", len(c.queue))
	}
	c.run(nil)
	if c.r(1).Committed() != index+1 {
		t.Fatalf("committed %d, want %d", c.r(1).Committed(), index+1)
	}
}

// Bug 5. Responses can be late, repeated or reordered: what a follower is
// known to hold never moves backwards, and a stale refusal is ignored.
func TestResponsesOutOfOrder(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	p := c.r(1).peers[2]
	for _, m := range []message{
		{typ: msgAppendResponse, ok: true, echo: -1, index: 0},
		{typ: msgAppendResponse, ok: false, echo: 5, index: -1}, // not the request in flight
		{typ: msgAppendResponse, ok: true, echo: -1, index: 99}, // more than the leader has
		{typ: msgAppendResponse, ok: true, echo: -1, index: -1}, // older than what is known
	} {
		m.term = c.r(1).Term()
		c.r(1).Step(2, m.encode())
		if p.match != 0 || p.next != 1 {
			t.Fatalf("after %+v: match %d next %d, want 0 and 1", m, p.match, p.next)
		}
	}
}

// Bug 6. Only a live leader or a granted vote postpones an election.
func TestElectionTimerIsNotResetByNoise(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	c.run(nil)
	c.fire(2, TimerElection) // term 2: node 2 stands, wins with node 3
	c.run(only(2, 3))
	n := c.nodes[3]
	arms := n.arms
	// Node 1 still believes it leads term 1; its messages are stale.
	c.fire(1, TimerHeartbeat)
	c.run(func(m tmsg) bool { return m.from == 1 && m.to == 3 })
	// And a vote request that must be refused: the candidate's log is behind.
	c.r(3).Step(1, (&message{typ: msgRequestVote, term: 2, lastIndex: -1}).encode())
	if n.arms != arms {
		t.Fatalf("the election timer was re-armed %d times by messages that deserve no trust", n.arms-arms)
	}
}

// Bugs 7 and 8. A higher term clears the vote, durably; and the vote goes
// to the log with the later last term, not the longer one.
func TestTermAndVoteRules(t *testing.T) {
	c := newCluster(t, 3, classic)
	c.startAll()
	c.elect(1) // term 1
	c.propose(1, "a")
	c.propose(1, "b")
	c.run(only(1, 2)) // node 3 has nothing
	c.fire(3, TimerElection)
	c.queue = nil            // term 2, lost
	c.fire(3, TimerElection) // term 3
	c.run(only(3, 2))        // node 2 sees term 3 and refuses: its log is ahead
	if c.r(2).Term() != 3 || c.r(2).hard.votedFor != noVote || c.r(3).Role() == Leader {
		t.Fatalf("node 2 term %d voted for %d; node 3 role %d", c.r(2).Term(), c.r(2).hard.votedFor, c.r(3).Role())
	}
	// A leader ignores candidates (the lease); it learns of term 3 from
	// node 2's answer to its heartbeat, and steps down.
	c.fire(1, TimerHeartbeat)
	c.run(only(1, 2))
	if c.r(1).Role() != Follower || c.r(1).Term() != 3 {
		t.Fatalf("node 1 role %d term %d", c.r(1).Role(), c.r(1).Term())
	}
	if _, err := c.r(1).Propose([]byte("z")); !errors.Is(err, ErrNotLeader) { // bug 10
		t.Fatalf("propose on a deposed leader: %v", err)
	}
	// The new term and the cleared vote survive a power loss.
	c.crash(1)
	c.start(1)
	if c.r(1).Term() != 3 || c.r(1).hard.votedFor != noVote {
		t.Fatalf("after restart: term %d, voted for %d", c.r(1).Term(), c.r(1).hard.votedFor)
	}
	// Node 2 (log: noop a b, last term 1; it saw term 3) asks for a vote
	// in term 4. Node 1 has the same log and grants it, once.
	c.fire(2, TimerElection)
	c.run(only(1, 2))
	if c.r(2).Role() != Leader || c.r(2).Term() != 4 || c.r(1).hard.votedFor != 2 {
		t.Fatalf("node 2 role %d; node 1 voted for %d", c.r(2).Role(), c.r(1).hard.votedFor)
	}
	// A candidate with a longer log of an older term loses to a shorter
	// log of a newer term.
	ask := func(lastIndex, lastTerm int64) bool {
		c.queue = nil
		c.passTime() // node 1 has heard from leader 2: let that age
		term := c.r(1).Term() + 1
		c.r(1).Step(3, (&message{typ: msgRequestVote, term: term, lastIndex: lastIndex, lastTerm: lastTerm}).encode())
		m, err := decodeMessage(c.queue[len(c.queue)-1].raw, 100)
		if err != nil {
			t.Fatal(err)
		}
		return m.ok
	}
	c.run(nil) // node 1 now holds node 2's term-4 no-op: last term 4, index 3
	if ask(50, 2) {
		t.Error("vote granted to a longer log with an older last term")
	}
	if !ask(0, 5) {
		t.Error("vote refused to a shorter log with a newer last term")
	}
}

// seedLog writes a log with the given entry terms, and a hard state, to a
// node's disk before it starts.
func (c *cluster) seedLog(id sys.NodeID, term int64, terms ...int64) {
	c.t.Helper()
	disk := c.nodes[id].disk
	r := New(Config{ID: id, Peers: []sys.NodeID{id, 99}, Dir: "raft", Log: storage.Config{SegmentBytes: 512, MaxPayloadBytes: 128}}, c.nodes[id], disk)
	c.nodes[id].armed = map[Timer]bool{}
	if err := r.Start(); err != nil {
		c.t.Fatal(err)
	}
	for i, t := range terms {
		if _, err := r.append(entry{term: t, cmd: fmt.Appendf(nil, "%d@%d", t, i)}); err != nil {
			c.t.Fatal(err)
		}
	}
	r.Sync()
	r.setHard(term, noVote, true)
	if r.Err() != nil {
		c.t.Fatal(r.Err())
	}
}

// The paper's Figure 7: followers that are missing entries, hold extra
// uncommitted ones, or both. Each must end up with the leader's log.
func TestFigure7(t *testing.T) {
	c := newCluster(t, 7, nil)
	c.seedLog(1, 7, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6)       // the leader-to-be
	c.seedLog(2, 7, 1, 1, 1, 4, 4, 5, 5, 6, 6)          // (a) one entry short
	c.seedLog(3, 7, 1, 1, 1, 4)                         // (b) far behind
	c.seedLog(4, 7, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6)    // (c) one extra
	c.seedLog(5, 7, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7) // (d) extra from a later term
	c.seedLog(6, 7, 1, 1, 1, 4, 4, 4, 4)                // (e) short, with stale entries
	c.seedLog(7, 7, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3)    // (f) a long stale branch
	c.startAll()
	c.elect(1) // term 8, with the votes of a, b, e and f
	for range 40 {
		c.fire(1, TimerHeartbeat)
		c.run(nil)
	}
	want := c.logOf(1)
	if len(want) != 11 || want[10] != "8:noop" {
		t.Fatalf("leader's log: %v", want)
	}
	for _, id := range c.ids {
		if got := c.logOf(id); !slices.Equal(got, want) {
			t.Errorf("node %d: log %v, want %v", id, got, want)
		}
		if c.r(id).Committed() != 11 {
			t.Errorf("node %d: committed %d, want 11", id, c.r(id).Committed())
		}
	}
}

// A follower that lost entries it had acknowledged (here: it does not wait
// for the sync, and loses power) is sent them again. The leader must not
// insist on continuing from what was acknowledged: found by the simulator
// as a run that never ended (BUGS.md).
func TestFollowerThatLostAcknowledgedEntriesIsRepaired(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.AckBeforeSync = true })
	c.startAll()
	c.elect(1)
	c.propose(1, "a")
	c.propose(1, "b")
	c.run(nil)
	c.r(1).Sync() // the leader keeps its log; the follower does not
	want := c.logOf(1)
	c.crash(2)
	c.start(2)
	if got := c.logOf(2); len(got) >= len(want) {
		t.Fatalf("setup: node 2 still holds %v after power loss", got)
	}
	c.fire(1, TimerHeartbeat)
	c.run(nil)
	if got := c.logOf(2); !slices.Equal(got, want) {
		t.Fatalf("node 2 holds %v, the leader %v", got, want)
	}
}
