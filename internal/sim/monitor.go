package sim

import (
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/raft"
)

// The monitor checks what no client can see: Raft's own safety properties,
// read from inside the nodes. It reads memory only, never the disk, so it
// cannot disturb the faults being injected.
//
//	S1  at most one leader per term;
//	S3  an entry seen committed on any node is never a different entry on
//	    any node. Entries are compared by term; by log matching (S2, which
//	    is checked on contents whenever the cluster settles) an index and
//	    a term name one entry;
//	S4  nodes that have applied the same number of entries have the same
//	    state hash: sampled while running, required when settled;
//	S5  a node's term never decreases and a vote it gave in a term never
//	    changes, not even across restarts; its commit index never
//	    decreases while it runs.

// monitor runs after every handler call that leaves inc alive.
func (w *world) monitor(inc *incarnation) {
	nd, r := inc.node, inc.n.Raft()
	if r == nil || w.err != nil {
		return
	}
	term := r.Term()
	if term < nd.term {
		w.fail("S5: node %d went from term %d back to term %d", nd.id, nd.term, term)
		return
	}
	// Two leaders of one term (S1) need this and three more accidents; a
	// forgotten vote is the fault itself, so it is checked directly.
	vote, voted := r.Vote()
	if term == nd.term && nd.voted && vote != nd.vote {
		now := "has no vote"
		if voted {
			now = fmt.Sprintf("votes for node %d", vote)
		}
		w.fail("S5: node %d voted for node %d in term %d and now %s", nd.id, nd.vote, term, now)
		return
	}
	nd.term, nd.vote, nd.voted = term, vote, voted
	if r.Role() == raft.Leader {
		if other, ok := w.leaders[term]; ok && other != nd.id {
			w.fail("S1: nodes %d and %d both led term %d", other, nd.id, term)
			return
		}
		if _, ok := w.leaders[term]; !ok {
			w.leaders[term] = nd.id
			w.stats.Leaders++
		}
	}
	commit := r.Committed()
	if commit < inc.commit {
		w.fail("S5: node %d's commit index went from %d back to %d", nd.id, inc.commit, commit)
		return
	}
	for i := inc.commit; i < commit; i++ {
		got := r.EntryTerm(i)
		if int(i) == len(w.committed) {
			w.committed = append(w.committed, got)
		} else if want := w.committed[i]; want != got {
			w.fail("S3: node %d committed an entry of term %d at index %d, where an entry of term %d was committed before", nd.id, got, i, want)
			return
		}
	}
	inc.commit = commit
	if s := r.LogStart(); s > inc.logStart {
		if inc.logStart > 0 || inc.installing || !r.Installing() {
			w.stats.Trims++
		}
		inc.logStart = s
	}
	if r.Installing() && !inc.installing {
		w.stats.Installs++
	}
	inc.installing = r.Installing()
}

// compareReplicas samples S4 every 100ms: whichever running nodes happen
// to have applied equally many entries must have equal state.
func (w *world) compareReplicas() {
	if w.quiet {
		return
	}
	for i, a := range w.nodes {
		for _, b := range w.nodes[i+1:] {
			if a.inc == nil || b.inc == nil {
				continue
			}
			na, ha := a.inc.n.StateHash()
			nb, hb := b.inc.n.StateHash()
			if na != nb {
				continue
			}
			w.stats.ReplicasCompared++
			if ha != hb {
				w.fail("S4: nodes %d and %d applied the same %d entries and differ: state hash %08x and %08x", a.id, b.id, na, ha, hb)
				return
			}
		}
	}
	w.after(100*time.Millisecond, w.compareReplicas)
}
