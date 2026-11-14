package raft

import (
	"bytes"
	"crypto/sha256"
	"flag"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Exhaustive search of a small Raft cluster (docs/design/b2-exhaustive.md):
// every schedule of deliveries, losses, copies, timers, the passing of
// time, proposals, crashes and restarts, within per-node budgets, breadth
// first, so that the first violation found is a shortest one.
//
// Each node has its own clock, moved only by its own events, so that time
// passing at one node is an event of its own (design, section 2).

var (
	exploreElections  = flag.Int("explore-elections", 1, "per node: election timer firings in the exhaustive search")
	exploreTicks      = flag.Int("explore-ticks", 0, "per node: times its clock jumps past every timeout without a timer firing")
	exploreHeartbeats = flag.Int("explore-heartbeats", 0, "per node: heartbeat timer firings")
	exploreProposals  = flag.Int("explore-proposals", 1, "per node: client proposals while it leads")
	exploreCrashes    = flag.Int("explore-crashes", 0, "per node: power losses")
	exploreDrops      = flag.Int("explore-drops", 0, "per node: messages to it lost")
	exploreDups       = flag.Int("explore-dups", 0, "per node: messages to it delivered twice")
	exploreDepth      = flag.Int("explore-depth", 10, "events per path (0: no limit)")
	exploreMode       = flag.String("explore-mode", "both", "default, classic or both: which Raft is searched")
)

// budget is what one node may do, or has done.
type budget struct {
	elections, ticks, heartbeats, proposals, crashes, drops, dups int
}

// exploreBound is the per-node budget and an optional path length limit.
type exploreBound struct {
	budget
	depth int
}

type evKind byte

const (
	evDeliver evKind = iota
	evDrop
	evDuplicate
	evElection
	evTick
	evHeartbeat
	evPropose
	evCrash
	evRestart
)

// event is one step of a path.
type event struct {
	kind evKind
	node sys.NodeID // where it happens: for a message, its receiver
	msg  tmsg
}

func (e event) String() string {
	switch e.kind {
	case evDeliver, evDrop, evDuplicate:
		m, _ := decodeMessage(e.msg.raw, 1<<20)
		verb := map[evKind]string{evDeliver: "deliver", evDrop: "lose", evDuplicate: "deliver a copy of"}[e.kind]
		return fmt.Sprintf("%s %s %d->%d (term %d, last %d/%d, prev %d/%d, %d entries, commit %d, ok %v, index %d)",
			verb, msgName(m.typ), e.msg.from, e.msg.to, m.term, m.lastIndex, m.lastTerm, m.prevIndex, m.prevTerm, len(m.entries), m.commit, m.ok, m.index)
	case evElection:
		return fmt.Sprintf("node %d: election timer", e.node)
	case evTick:
		return fmt.Sprintf("node %d: time passes", e.node)
	case evHeartbeat:
		return fmt.Sprintf("node %d: heartbeat timer", e.node)
	case evPropose:
		return fmt.Sprintf("node %d: propose", e.node)
	case evCrash:
		return fmt.Sprintf("node %d: power loss", e.node)
	default:
		return fmt.Sprintf("node %d: restart", e.node)
	}
}

func msgName(t byte) string {
	if s, ok := map[byte]string{msgRequestVote: "RequestVote", msgVoteResponse: "VoteResponse", msgAppendEntries: "AppendEntries",
		msgAppendResponse: "AppendResponse", msgPreVote: "PreVote", msgPreVoteResponse: "PreVoteResponse", msgTimeoutNow: "TimeoutNow"}[t]; ok {
		return s
	}
	return fmt.Sprintf("type %d", t)
}

// world is a cluster in the middle of a path, with the ghost state the
// history properties need and each node's budget used.
type world struct {
	c         *cluster
	leaders   map[int64]sys.NodeID // S1: who led each term, ever
	committed []string             // S3: "term:cmd" of every entry seen committed
	terms     map[sys.NodeID]int64 // S5: each node's highest term
	used      map[sys.NodeID]*budget
}

func newWorld(mutate func(*Config)) (*world, error) {
	c := newSeededCluster(nil, 1, 3, mutate)
	c.ownClocks = true
	w := &world{c: c, leaders: map[int64]sys.NodeID{}, terms: map[sys.NodeID]int64{}, used: map[sys.NodeID]*budget{}}
	for _, id := range c.ids {
		w.used[id] = &budget{}
		if err := c.open(id); err != nil {
			return nil, err
		}
		// A node ignores candidates for an election timeout after it
		// starts. Start the search in a cluster that has been up that
		// long, or no vote is granted without a tick (BUGS.md M11).
		jump(c.nodes[id])
	}
	return w, w.check()
}

// enabled lists the events possible now, in a fixed order: by node, then
// by kind. Copies of one message in flight are offered once.
func (w *world) enabled(b exploreBound) []event {
	var out []event
	q := slices.Clone(w.c.queue)
	slices.SortFunc(q, cmpMsg)
	q = slices.CompactFunc(q, func(a, b tmsg) bool { return cmpMsg(a, b) == 0 })
	for _, id := range w.c.ids {
		n, u := w.c.nodes[id], w.used[id]
		for _, m := range q {
			if m.to != id {
				continue
			}
			out = append(out, event{kind: evDeliver, node: id, msg: m})
			if u.drops < b.drops {
				out = append(out, event{kind: evDrop, node: id, msg: m})
			}
			if u.dups < b.dups {
				out = append(out, event{kind: evDuplicate, node: id, msg: m})
			}
		}
		if n.down {
			out = append(out, event{kind: evRestart, node: id})
			continue
		}
		if n.armed[TimerElection] && u.elections < b.elections {
			out = append(out, event{kind: evElection, node: id})
		}
		if u.ticks < b.ticks {
			out = append(out, event{kind: evTick, node: id})
		}
		// A heartbeat only repeats what may have been lost; it has a
		// budget like any timer, or the copies of its answers in flight
		// would grow without end (design, 2).
		if n.armed[TimerHeartbeat] && u.heartbeats < b.heartbeats {
			out = append(out, event{kind: evHeartbeat, node: id})
		}
		if n.r.Role() == Leader && !n.r.Transferring() && u.proposals < b.proposals {
			out = append(out, event{kind: evPropose, node: id})
		}
		if u.crashes < b.crashes {
			out = append(out, event{kind: evCrash, node: id})
		}
	}
	return out
}

func cmpMsg(a, b tmsg) int {
	if a.from != b.from {
		return int(a.from) - int(b.from)
	}
	if a.to != b.to {
		return int(a.to) - int(b.to)
	}
	return bytes.Compare(a.raw, b.raw)
}

// jump moves a node's clock past every timeout, as an election timer that
// fires implies (cluster.passTime, for one node).
func jump(n *tnode) { n.now += sys.Instant(2 * 300 * time.Millisecond) }

// apply performs e and checks every property afterwards.
func (w *world) apply(e event) error {
	c := w.c
	n, u := c.nodes[e.node], w.used[e.node]
	switch e.kind {
	case evDeliver, evDrop, evDuplicate:
		i := slices.IndexFunc(c.queue, func(m tmsg) bool { return cmpMsg(m, e.msg) == 0 })
		if i < 0 {
			return fmt.Errorf("replay: message not in flight: %v", e)
		}
		switch e.kind {
		case evDrop:
			u.drops++
		case evDuplicate:
			u.dups++ // delivered, and still in flight
		}
		if e.kind != evDuplicate {
			c.queue = slices.Delete(c.queue, i, i+1)
		}
		if e.kind != evDrop && !n.down {
			n.r.Step(e.msg.from, e.msg.raw)
		}
	case evElection:
		u.elections++
		n.armed[TimerElection] = false
		jump(n)
		n.r.OnTimer(TimerElection)
	case evTick:
		u.ticks++
		jump(n)
	case evHeartbeat:
		u.heartbeats++
		n.armed[TimerHeartbeat] = false
		n.r.OnTimer(TimerHeartbeat)
	case evPropose:
		u.proposals++
		// Named by node and count, so that the name does not depend on
		// the order of other nodes' events.
		if _, err := n.r.Propose(fmt.Appendf(nil, "c%d.%d", e.node, u.proposals)); err != nil {
			return fmt.Errorf("propose on leader %d: %w", e.node, err)
		}
	case evCrash:
		u.crashes++
		c.crash(e.node)
	case evRestart:
		if err := c.open(e.node); err != nil {
			return fmt.Errorf("restart node %d: %w", e.node, err)
		}
	}
	if !n.down && n.r.NeedsFlush() {
		n.r.Flush()
	}
	w.dropDead()
	return w.check()
}

// dropDead delivers at once every message that its receiver ignores and
// will always ignore: a response from an older term (a PreVote response
// from no newer one). Step does nothing with those, and a node's term
// never goes down (S5, checked), so delivering one later changes nothing
// either. Removing them is a legal schedule, not a cut: without it, every
// combination of ignored messages in flight is a state of its own
// (design, section 2).
func (w *world) dropDead() {
	w.c.queue = slices.DeleteFunc(w.c.queue, func(m tmsg) bool {
		n := w.c.nodes[m.to]
		if n.down {
			return false
		}
		d, err := decodeMessage(m.raw, n.r.cfg.Log.MaxPayloadBytes-entryHeader)
		if err != nil {
			return false
		}
		switch d.typ {
		case msgVoteResponse, msgAppendResponse, msgInstallResponse, msgTimeoutNow:
			return d.term < n.r.hard.term
		case msgPreVoteResponse:
			return d.term <= n.r.hard.term
		}
		return false
	})
}

// check verifies S1, S2, S3, S5 and that no node stopped, and updates the
// ghost state.
func (w *world) check() error {
	c := w.c
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.down {
			continue
		}
		r := n.r
		if err := r.Err(); err != nil {
			return fmt.Errorf("node %d stopped: %w", id, err)
		}
		if r.Term() < w.terms[id] {
			return fmt.Errorf("S5: node %d went from term %d back to %d", id, w.terms[id], r.Term())
		}
		w.terms[id] = r.Term()
		if r.Role() == Leader {
			if other, ok := w.leaders[r.Term()]; ok && other != id {
				return fmt.Errorf("S1: nodes %d and %d both led term %d", other, id, r.Term())
			}
			w.leaders[r.Term()] = id
		}
		for i := int64(0); i < r.Committed(); i++ {
			cmd, _, err := r.Read(i)
			if err != nil {
				return fmt.Errorf("node %d: committed entry %d: %w", id, i, err)
			}
			got := fmt.Sprintf("%d:%s", r.EntryTerm(i), cmd)
			if int(i) == len(w.committed) {
				w.committed = append(w.committed, got)
			} else if want := w.committed[i]; want != got {
				return fmt.Errorf("S3: node %d committed %s at index %d, where %s was committed before", id, got, i, want)
			}
		}
	}
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a >= b || c.nodes[a].down || c.nodes[b].down {
				continue
			}
			ra, rb := c.r(a), c.r(b)
			for i := min(ra.LastIndex(), rb.LastIndex()); i >= 0; i-- {
				if ra.EntryTerm(i) != rb.EntryTerm(i) {
					continue
				}
				for ; i >= 0; i-- {
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

// fingerprint identifies the state for pruning: everything a later step
// or property can read (design, section 2).
func (w *world) fingerprint() [16]byte {
	var b strings.Builder
	c := w.c
	for _, id := range c.ids {
		n := c.nodes[id]
		fmt.Fprintf(&b, "node %d now %d used %v down %v armed %v %v\n", id, n.now, *w.used[id], n.down, n.armed[TimerElection], n.armed[TimerHeartbeat])
		if n.down {
			continue
		}
		r := n.r
		fmt.Fprintf(&b, " %d %d %d %d %d %v %v %d %d %d %v %v %d %d %d %d %d %v\n",
			r.hard.term, r.hard.votedFor, r.role, r.leader, r.commit, sortedTrue(r.votes), sortedTrue(r.preVotes),
			r.leaderSince, r.transferee, r.transferUntil, r.timeoutSent, r.dirty, r.ackTo, r.ackIndex, r.ackEcho, r.ackStamp, r.contact, r.learners)
		for _, p := range r.targets() {
			pr := r.peers[p]
			fmt.Fprintf(&b, " peer %d %d %d %v %d\n", p, pr.next, pr.match, pr.inflight, pr.acked)
		}
		for i := int64(0); i <= r.LastIndex(); i++ {
			cmd, noop, _ := r.Read(i)
			fmt.Fprintf(&b, " e%d:%s:%v", r.EntryTerm(i), cmd, noop)
		}
		b.WriteByte('\n')
	}
	q := slices.Clone(c.queue)
	slices.SortFunc(q, cmpMsg)
	for _, m := range q {
		fmt.Fprintf(&b, "m %d %d %x\n", m.from, m.to, m.raw)
	}
	terms := make([]int64, 0, len(w.leaders))
	for t := range w.leaders {
		terms = append(terms, t)
	}
	slices.Sort(terms)
	for _, t := range terms {
		fmt.Fprintf(&b, "L%d=%d ", t, w.leaders[t])
	}
	fmt.Fprintf(&b, "\nC %v\nT %d %d %d\n", w.committed, w.terms[1], w.terms[2], w.terms[3])
	sum := sha256.Sum256([]byte(b.String()))
	var out [16]byte
	copy(out[:], sum[:])
	return out
}

func sortedTrue(m map[sys.NodeID]bool) []sys.NodeID {
	var out []sys.NodeID
	for id, ok := range m {
		if ok {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// exploreResult is what a search found.
type exploreResult struct {
	states, deepest int
	proposed        bool // some state committed a client proposal: the search was not vacuous
	violation       error
	path            []event // the shortest path to the violation
}

// explore searches every path within b, breadth-first, replaying each
// path from the start. A path is stored as the index of each event in the
// list enabled offers at that step: replay is deterministic, so the list
// is the same every time, and a path costs one byte per step.
//
// Every enabled event is followed from every state. A one-node partial-order
// reduction was tried and was unsound (BUGS.md M11).
func explore(b exploreBound, mutate func(*Config), progress func(depth, frontier, states int)) (exploreResult, error) {
	replay := func(path []byte) (*world, []event, error) {
		w, err := newWorld(mutate)
		if err != nil {
			return nil, nil, err
		}
		var events []event
		for _, i := range path {
			en := w.enabled(b)
			if int(i) >= len(en) {
				return nil, nil, fmt.Errorf("replay: event %d of %d", i, len(en))
			}
			events = append(events, en[i])
			if err := w.apply(en[i]); err != nil {
				return nil, nil, err
			}
		}
		return w, events, nil
	}
	root, err := newWorld(mutate)
	if err != nil {
		return exploreResult{violation: err}, nil
	}
	res := exploreResult{states: 1}
	seen := map[[16]byte]bool{root.fingerprint(): true}
	frontier := [][]byte{nil}
	type child struct {
		fp        [16]byte
		path      []byte
		violation error
		events    []event
		proposed  bool // a client proposal is committed in this state
	}
	// expand returns a path's children, in the order enabled offers them.
	expand := func(path []byte) ([]child, error) {
		w, events, err := replay(path)
		if err != nil {
			return nil, fmt.Errorf("replay of a path that passed before failed: %w", err)
		}
		en := w.enabled(b)
		if len(en) > 255 {
			return nil, fmt.Errorf("%d events enabled: more than a byte can name", len(en))
		}
		// Each child starts from a copy of the replayed parent: copying is
		// cheaper than replaying the path again (deepcopy_test.go).
		parent := w
		visit := func(i int) (child, error) {
			w := deepCopy(parent)
			cp := append(slices.Clip(path), byte(i))
			if err := w.apply(en[i]); err != nil {
				return child{path: cp, violation: err, events: append(slices.Clone(events), en[i])}, nil
			}
			return child{fp: w.fingerprint(), path: cp, proposed: slices.ContainsFunc(w.committed, func(e string) bool { return strings.Contains(e, ":c") })}, nil
		}
		var out []child
		for i := range en {
			ch, err := visit(i)
			if err != nil {
				return nil, err
			}
			out = append(out, ch)
			if ch.violation != nil {
				break
			}
		}
		return out, nil
	}
	workers := runtime.GOMAXPROCS(0)
	for len(frontier) > 0 && (b.depth == 0 || res.deepest < b.depth) {
		results := make([][]child, len(frontier))
		errs := make([]error, len(frontier))
		var wg sync.WaitGroup
		var nextJob atomic.Int64
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					j := int(nextJob.Add(1)) - 1
					if j >= len(frontier) {
						return
					}
					results[j], errs[j] = expand(frontier[j])
				}
			}()
		}
		wg.Wait()
		// Sequential and in frontier order: the result, the first
		// violation included, does not depend on scheduling.
		var next [][]byte
		for j := range frontier {
			if errs[j] != nil {
				return res, errs[j]
			}
			for _, ch := range results[j] {
				if ch.violation != nil {
					res.violation, res.path = ch.violation, ch.events
					return res, nil
				}
				if seen[ch.fp] {
					continue
				}
				seen[ch.fp] = true
				res.states++
				res.deepest = max(res.deepest, len(ch.path))
				res.proposed = res.proposed || ch.proposed
				next = append(next, ch.path)
			}
		}
		frontier = next
		if progress != nil {
			progress(res.deepest, len(frontier), res.states)
		}
	}
	return res, nil
}

func formatPath(path []event) string {
	var b strings.Builder
	for i, e := range path {
		fmt.Fprintf(&b, "\n  %2d. %v", i+1, e)
	}
	return b.String()
}

func flagBound() exploreBound {
	return exploreBound{budget: budget{elections: *exploreElections, ticks: *exploreTicks, heartbeats: *exploreHeartbeats, proposals: *exploreProposals,
		crashes: *exploreCrashes, drops: *exploreDrops, dups: *exploreDups}, depth: *exploreDepth}
}

func modes() []string {
	if *exploreMode == "both" {
		return []string{"default", "classic"}
	}
	return []string{*exploreMode}
}

func modeConfig(mode string, mutate func(*Config)) func(*Config) {
	return func(c *Config) {
		if mode == "classic" {
			classic(c)
		}
		if mutate != nil {
			mutate(c)
		}
	}
}

// The search completes within its bound with no violation, for Raft as
// configured by default and for classic Raft (no PreVote, no CheckQuorum).
func TestExhaustive(t *testing.T) {
	if testing.Short() {
		t.Skip("exhaustive search")
	}
	for _, mode := range modes() {
		start := time.Now()
		res, err := explore(flagBound(), modeConfig(mode, nil), func(d, f, s int) {
			if testing.Verbose() {
				t.Logf("%s: depth %d, frontier %d, states %d, %v", mode, d, f, s, time.Since(start).Round(time.Millisecond))
			}
		})
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if res.violation != nil {
			t.Fatalf("%s: %v after %d events:%s", mode, res.violation, len(res.path), formatPath(res.path))
		}
		if !res.proposed && flagBound().proposals > 0 {
			t.Fatalf("%s: no state committed a proposal: the bound is too small to test anything", mode)
		}
		t.Logf("%s: complete: %d distinct states, deepest path %d events, in %v; per-node budget %+v",
			mode, res.states, res.deepest, time.Since(start).Round(time.Millisecond), flagBound())
	}
}

// exploreMutations are the Raft mutations that three nodes reach with
// these events. Snapshots, compaction, transfers and membership changes are
// outside the search; the random schedules cover them.
var exploreMutations = map[string]func(*Config){
	"vote-by-length":   func(c *Config) { c.Unsafe |= MutVoteByLength },
	"vote-unpersisted": func(c *Config) { c.Unsafe |= MutVoteWithoutPersist },
	"commit-any-term":  func(c *Config) { c.Unsafe |= MutCommitAnyTerm },
	"truncate-stale":   func(c *Config) { c.Unsafe |= MutTruncateOnStale },
	"ack-before-sync":  func(c *Config) { c.AckBeforeSync = true },
}

// shortestAckBeforeSync is the length of the shortest path to a violation
// with AckBeforeSync: three nodes, classic Raft, one election and one power
// loss per node.
const shortestAckBeforeSync = 11

var exploreShortest = flag.Bool("explore-shortest", false, "find the shortest AckBeforeSync counterexample (about 30 s on 10 cores)")

// TestExhaustiveShortest finds a mutation with a shortest counterexample:
// a follower that answers before its log is durable (AckBeforeSync) loses
// a committed entry in a power loss. Breadth-first order makes the first
// violation a shortest one. CI runs it in the exhaustive job.
func TestExhaustiveShortest(t *testing.T) {
	if !*exploreShortest {
		t.Skip("-explore-shortest not set")
	}
	b := exploreBound{budget: budget{elections: 1, crashes: 1}}
	start := time.Now()
	res, err := explore(b, modeConfig("classic", exploreMutations["ack-before-sync"]), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.violation == nil || len(res.path) != shortestAckBeforeSync {
		t.Fatalf("want a violation in %d events, got %v in %d:%s", shortestAckBeforeSync, res.violation, len(res.path), formatPath(res.path))
	}
	t.Logf("%v after %d states in %v; shortest path, %d events:%s", res.violation, res.states,
		time.Since(start).Round(time.Second), len(res.path), formatPath(res.path))
}

var exploreMutation = flag.String("explore-mutation", "", "search with this mutation (a key of exploreMutations) and report the shortest violation")

// TestExploreMutation is the manual form: one mutation, the bound from the
// flags.
func TestExploreMutation(t *testing.T) {
	if *exploreMutation == "" {
		t.Skip("-explore-mutation not set")
	}
	for _, mode := range modes() {
		start := time.Now()
		res, err := explore(flagBound(), modeConfig(mode, exploreMutations[*exploreMutation]), func(d, f, n int) {
			if testing.Verbose() {
				t.Logf("%s: depth %d, frontier %d, states %d, %v", mode, d, f, n, time.Since(start).Round(time.Millisecond))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.violation == nil {
			t.Errorf("%s: not found in %d states (%v)", mode, res.states, time.Since(start).Round(time.Millisecond))
			continue
		}
		t.Logf("%s: %v\n  after %d states in %v; shortest path, %d events:%s", mode, res.violation, res.states,
			time.Since(start).Round(time.Millisecond), len(res.path), formatPath(res.path))
	}
}
