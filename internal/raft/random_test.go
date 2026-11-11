package raft

import (
	"bytes"
	"flag"
	"fmt"
	"slices"
	"testing"

	"github.com/amritangshudey/samsa/internal/sys"
)

var randomSeeds = flag.Int("raft-seeds", 300, "number of seeds for the random schedule test")

// randomRun drives a cluster through a random schedule: messages are
// delivered in any order, repeated or lost; timers fire at any moment;
// nodes sync late, lose power and come back. It returns the first
// violation of:
//
//	S1  at most one leader per term;
//	S2  two logs that agree on an entry's term agree on everything before;
//	S3  an entry seen committed anywhere is never anything else anywhere
//	    (which covers leader completeness and state machine safety);
//	S5  a node's term never decreases, across restarts too;
//
// and, once faults stop, that the cluster elects a leader and every log
// becomes the same.
func randomRun(seed uint64, mutate func(*Config)) error {
	// Each seed picks its own mix of faults: a bug that needs many crashes
	// or one entry per message hides from any single fixed mix.
	crashes := []int{2, 8, 30}[seed/2%3]
	c := newSeededCluster(nil, seed, 3+2*int(seed%2), func(cfg *Config) {
		if seed/6%2 == 1 {
			cfg.MaxAppendBytes = 1 // one entry per message
		}
		if mutate != nil {
			mutate(cfg)
		}
	})
	rng := c.rng
	type seen struct {
		term int64
		cmd  []byte
	}
	var (
		leaders   = map[int64]sys.NodeID{}
		committed []seen // by index: the first sighting of each committed entry
		checked   = map[sys.NodeID]int64{}
		terms     = map[sys.NodeID]int64{}
		proposals int
	)
	check := func(id sys.NodeID) error {
		r := c.r(id)
		if err := r.Err(); err != nil {
			return err
		}
		if r.Term() < terms[id] {
			return fmt.Errorf("S5: node %d went from term %d back to %d", id, terms[id], r.Term())
		}
		terms[id] = r.Term()
		if r.Role() == Leader {
			if other, ok := leaders[r.Term()]; ok && other != id {
				return fmt.Errorf("S1: nodes %d and %d both led term %d", other, id, r.Term())
			}
			leaders[r.Term()] = id
		}
		for i := checked[id]; i < r.Committed(); i++ {
			cmd, _, err := r.Read(i)
			if err != nil {
				return fmt.Errorf("node %d: committed entry %d: %w", id, i, err)
			}
			got := seen{r.EntryTerm(i), slices.Clone(cmd)}
			if int(i) == len(committed) {
				committed = append(committed, got)
			} else if w := committed[i]; w.term != got.term || !bytes.Equal(w.cmd, got.cmd) {
				return fmt.Errorf("S3: node %d committed %d:%q at index %d, where %d:%q was committed before", id, got.term, got.cmd, i, w.term, w.cmd)
			}
		}
		checked[id] = r.Committed()
		return nil
	}
	matching := func() error {
		for _, a := range c.ids {
			for _, b := range c.ids {
				if c.nodes[a].down || c.nodes[b].down {
					continue // a dead node's object describes a disk that no longer exists
				}
				ra, rb := c.r(a), c.r(b)
				for i := min(ra.LastIndex(), rb.LastIndex()); a < b && i >= 0; i-- {
					if ra.EntryTerm(i) != rb.EntryTerm(i) {
						continue
					}
					for ; i >= 0; i-- { // from the highest shared (index, term) down, all must be equal
						ca, _, _ := ra.Read(i)
						cb, _, _ := rb.Read(i)
						if ra.EntryTerm(i) != rb.EntryTerm(i) || !bytes.Equal(ca, cb) {
							return fmt.Errorf("S2: nodes %d and %d differ at index %d below a matching entry", a, b, i)
						}
					}
				}
			}
		}
		return nil
	}
	restart := func(id sys.NodeID) error {
		checked[id] = 0
		if err := c.open(id); err != nil {
			return fmt.Errorf("restart node %d: %w", id, err)
		}
		return check(id)
	}
	for _, id := range c.ids {
		if err := restart(id); err != nil {
			return err
		}
	}

	pick := func() *tnode { return c.nodes[c.ids[rng.Intn(len(c.ids))]] }
	for step := range 4000 {
		n := pick()
		switch r := rng.Intn(1000); {
		case n.down:
			if r < 100 {
				if err := restart(n.id); err != nil {
					return err
				}
			}
			continue
		case r < crashes: // power loss: whatever was not synced is gone
			c.crash(n.id)
			continue
		case r < crashes+12 && n.armed[TimerElection]:
			n.armed[TimerElection] = false
			c.passTime()
			n.r.OnTimer(TimerElection)
		case r < 120 && n.armed[TimerHeartbeat]:
			n.armed[TimerHeartbeat] = false
			n.r.OnTimer(TimerHeartbeat)
		case r < 250:
			if n.r.Role() == Leader {
				proposals++
				if _, err := n.r.Propose(fmt.Appendf(nil, "c%d", proposals)); err != nil {
					return fmt.Errorf("propose on leader %d: %w", n.id, err)
				}
			}
		case r < 450:
			n.r.Flush()
		case len(c.queue) > 0:
			i := rng.Intn(len(c.queue))
			m := c.queue[i]
			switch {
			case r < 500: // lost
				c.queue = slices.Delete(c.queue, i, i+1)
			case r < 540: // delivered, and delivered again later
			default:
				c.queue = slices.Delete(c.queue, i, i+1)
			}
			if r >= 500 && !c.nodes[m.to].down {
				n = c.nodes[m.to]
				n.r.Step(m.from, m.raw)
			}
		}
		if err := check(n.id); err != nil {
			return fmt.Errorf("step %d: %w", step, err)
		}
		if step%500 == 0 {
			if err := matching(); err != nil {
				return fmt.Errorf("step %d: %w", step, err)
			}
		}
	}

	// Faults stop. Everyone comes back; messages arrive; timers fire.
	for _, id := range c.ids {
		if c.nodes[id].down {
			if err := restart(id); err != nil {
				return err
			}
		}
	}
	for round := range 300 {
		for len(c.queue) > 0 {
			m := c.queue[0]
			c.queue = c.queue[1:]
			c.r(m.to).Step(m.from, m.raw)
		}
		leader := false
		for _, id := range c.ids {
			n := c.nodes[id]
			n.r.Flush()
			if n.r.Role() == Leader && n.armed[TimerHeartbeat] {
				leader = true
				n.r.OnTimer(TimerHeartbeat)
			}
			if err := check(id); err != nil {
				return fmt.Errorf("healing: %w", err)
			}
		}
		if !leader && len(c.queue) == 0 {
			c.passTime()
			c.r(c.ids[round%len(c.ids)]).OnTimer(TimerElection)
		}
	}
	if err := matching(); err != nil {
		return err
	}
	last := c.r(c.ids[0]).LastIndex()
	for _, id := range c.ids {
		if r := c.r(id); r.LastIndex() != last || r.Committed() != last+1 {
			return fmt.Errorf("after healing node %d has last index %d and %d committed; node %d has last index %d",
				id, r.LastIndex(), r.Committed(), c.ids[0], last)
		}
	}
	if len(committed) == 0 { // a crash-heavy seed commits little; none at all means healing failed
		return fmt.Errorf("nothing was ever committed")
	}
	return nil
}

func TestRandomSchedules(t *testing.T) {
	for seed := range uint64(*randomSeeds) {
		if err := randomRun(seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// Each deliberate bug must be caught by some schedule, or green runs of
// TestRandomSchedules mean nothing.
func TestRandomSchedulesCatchBrokenRaft(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"commit by counting replicas of any term": func(c *Config) { c.Unsafe = MutCommitAnyTerm },
		"vote granted before it is on disk":       func(c *Config) { c.Unsafe = MutVoteWithoutPersist },
		"entries acknowledged before sync":        func(c *Config) { c.AckBeforeSync = true },
		"vote by log length only":                 func(c *Config) { c.Unsafe = MutVoteByLength },
		"follower truncates on a stale message":   func(c *Config) { c.Unsafe = MutTruncateOnStale },
	} {
		caught := false
		for seed := range uint64(3000) {
			if err := randomRun(seed, mutate); err != nil {
				t.Logf("%s: caught at seed %d: %v", name, seed, err)
				caught = true
				break
			}
		}
		if !caught {
			t.Errorf("%s: 3000 seeds passed", name)
		}
	}
}

func FuzzDecodeMessage(f *testing.F) {
	for _, m := range []message{
		{typ: msgRequestVote, term: 3, lastIndex: 7, lastTerm: 2},
		{typ: msgVoteResponse, term: 3, ok: true},
		{typ: msgAppendEntries, term: 3, prevIndex: 1, prevTerm: 1, commit: 1, entries: []entry{{term: 2, cmd: []byte("x")}, {term: 3, kind: kindNoop}}},
		{typ: msgAppendResponse, term: 3, ok: true, echo: 1, index: 3},
	} {
		f.Add(m.encode())
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := decodeMessage(b, 64)
		if err != nil {
			return
		}
		// Whatever decodes must encode to the same bytes: one meaning per message.
		if again := m.encode(); !bytes.Equal(again, b) {
			t.Fatalf("decode then encode changed the message:\n%x\n%x", b, again)
		}
	})
}

// FuzzStep feeds arbitrary bytes to a follower and to a leader as peer
// messages. Nothing a peer sends may crash a node or make it give up.
func FuzzStep(f *testing.F) {
	f.Add((&message{typ: msgAppendEntries, term: 9, prevIndex: 0, prevTerm: 1, commit: 5, entries: []entry{{term: 9, cmd: []byte("x")}}}).encode())
	f.Add((&message{typ: msgAppendResponse, term: 1, ok: true, index: 1 << 40}).encode())
	f.Add((&message{typ: msgAppendResponse, term: 1, echo: -1, index: -1}).encode())
	f.Add((&message{typ: msgAppendResponse, term: 1, echo: 1, index: 7}).encode())
	f.Add((&message{typ: msgRequestVote, term: 1 << 50, lastIndex: 1 << 50, lastTerm: 1 << 50}).encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		c := newCluster(t, 3, nil)
		c.startAll()
		c.elect(1)
		c.propose(1, "a")
		c.run(nil)
		for _, to := range []sys.NodeID{1, 2} {
			term := c.r(to).Term()
			c.r(to).Step(3, b)
			c.r(to).Flush()
			// The one thing a lying peer can do is claim to lead a term,
			// the present one (which node 1 leads) or a later one, with
			// entries that contradict what is committed. An honest
			// leader of any term holds every committed entry, so this
			// proves a broken peer, and a node rightly stops rather than
			// act on it (peers are not Byzantine; design phase 6,
			// section 2). Anything else must leave it running.
			m, err := decodeMessage(b, 64)
			claim := err == nil && m.typ == msgAppendEntries && m.term >= term
			if err := c.r(to).Err(); err != nil && !claim {
				t.Fatalf("node %d stopped: %v", to, err)
			}
		}
	})
}
