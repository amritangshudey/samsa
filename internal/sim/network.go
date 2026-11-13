package sim

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// The network between nodes (docs/design/phase-6.md, section 2). Each
// message is dropped, delivered, or delivered twice, after independent
// random delays; a directed link can be cut. Clients are never cut off.

// sendPeer decides the fate of one message from a node to node to.
func (w *world) sendPeer(from *simNode, to int32, msg []byte) {
	nd := w.node(to)
	if nd == nil || w.cut[[2]int32{from.id, to}] || w.rng.Chance(w.opts.drop) {
		return
	}
	copies := 1
	if w.rng.Chance(w.opts.dup) {
		copies = 2
	}
	for range copies {
		// Usually as quick as a client's bytes; now and then slow enough
		// to be overtaken by many messages and to outlast a heartbeat.
		delay := w.netDelay()
		if w.rng.Chance(0.1) {
			delay = time.Duration(w.rng.Intn(int(w.bcfg.ElectionTimeout / 2)))
		}
		sent := w.now
		w.after(delay, func() {
			if t := nd.inc; t != nil && !t.dead {
				w.tl.msg(from.id, to, sent, w.now)
				w.note(nd.tag("peer"), uint64(from.id), msg)
				w.call(t, t.n.OnPeerMessage(sys.NodeID(from.id), msg))
			}
		})
	}
}

func (w *world) node(id int32) *simNode {
	for _, nd := range w.nodes {
		if nd.id == id {
			return nd
		}
	}
	return nil
}

// partition installs one scenario of link cuts, heals it after a while,
// and schedules the next.
func (w *world) partition() {
	if w.quiet {
		return
	}
	ids := make([]int32, len(w.nodes))
	for i, nd := range w.nodes {
		ids[i] = nd.id
	}
	victim := w.target().id
	others := slices.DeleteFunc(slices.Clone(ids), func(id int32) bool { return id == victim })
	var cuts [][2]int32
	both := func(a, b int32) { cuts = append(cuts, [2]int32{a, b}, [2]int32{b, a}) }
	var what string
	switch w.rng.Intn(4) {
	case 0, 1: // isolate: in a cluster of three, also the minority/majority split
		what = fmt.Sprintf("isolate %d", victim)
		for _, o := range others {
			both(victim, o)
		}
	case 2:
		o := others[w.rng.Intn(len(others))]
		what = fmt.Sprintf("one-way %d->%d", victim, o)
		cuts = append(cuts, [2]int32{victim, o})
	case 3: // the victim sees both sides; they do not see each other
		what = fmt.Sprintf("bridge via %d", victim)
		both(others[0], others[1])
	}
	if w.faultAllowed(-1, "partition: "+what) {
		if nd := w.node(victim); strings.HasPrefix(what, "isolate") && nd.inc != nil && nd.inc.n.Serving() {
			w.stats.LeaderIsolated++
		}
		for _, c := range cuts {
			w.cut[c] = true
		}
		w.tl.cut(w.now, what)
		w.note("partition: "+what, 0, nil)
		w.stats.Partitions++
	}
	w.after(time.Duration(50+w.rng.Intn(1950))*time.Millisecond, func() {
		w.heal()
		w.after(time.Duration(w.rng.Intn(500))*time.Millisecond, w.partition)
	})
}

// target picks the node a fault hits: half the time the leader, where it
// hurts, so that leaders change often and leave diverging logs behind.
func (w *world) target() *simNode {
	if w.rng.Chance(0.5) {
		for _, nd := range w.nodes {
			if nd.inc != nil && nd.inc.n.Serving() {
				return nd
			}
		}
	}
	return w.nodes[w.rng.Intn(len(w.nodes))]
}

func (w *world) heal() {
	if len(w.cut) > 0 {
		w.note("heal", 0, nil)
		clear(w.cut)
		w.tl.heal(w.now)
	}
}
