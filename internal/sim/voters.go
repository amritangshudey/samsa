package sim

import (
	"slices"

	"github.com/amritangshudey/samsa/internal/wire"
)

// Membership changes under faults (docs/design/c1-membership.md, section
// 5). A run with changes has spares: nodes that start with no
// configuration and wait to be added. One client adds and removes voters
// through AddRaftVoter and RemoveRaftVoter, one request at a time, with
// every node it can reach as a candidate: so nodes come and go, a removed
// node may come back with its disk intact, and the leader may remove
// itself. Ids are never reused with a new disk (design, section 6, finding 3).

// spares is how many nodes a run with changes adds to its cluster.
const spares = 2

func voterClients(changes bool) int {
	if changes {
		return 1
	}
	return 0
}

// voters returns the nodes that leader's configuration names, in id order:
// only they must converge (a removed node is no longer replicated to).
func (w *world) voters(leader *simNode) []*simNode {
	var out []*simNode
	for _, m := range leader.inc.n.Raft().Members() {
		if nd := w.node(int32(m.ID)); nd != nil && nd.inc != nil {
			out = append(out, nd)
		}
	}
	return out
}

// changeVoters asks, now and then, for one node to be added or removed.
// Every answer must be one Kafka's leader gives (AddVoterHandler,
// RemoveVoterHandler) or a non-leader's refusal.
func (cl *client) changeVoters() {
	if cl.changing || !cl.rng.Chance(0.2) {
		return
	}
	w := cl.w
	id := int32(1 + cl.rng.Intn(len(w.nodes)))
	resp := &wire.VoterChangeResponse{}
	var key int16
	var req wire.Message
	if cl.rng.Chance(0.5) {
		key = wire.KeyAddRaftVoter
		// Short timeouts too: a change that times out uncommitted frees the
		// leader for the next one, which only Raft's own rule then stops.
		timeout := int32(20 + cl.rng.Intn(3000))
		req = &wire.AddRaftVoterRequest{TimeoutMs: timeout, VoterID: id, Listeners: []wire.Listener{{Name: "CLIENT", Host: "sim", Port: uint16(9091 + id)}}}
	} else {
		key = wire.KeyRemoveRaftVoter
		req = &wire.RemoveRaftVoterRequest{VoterID: id}
	}
	cl.changing = true
	cl.request(key, cl.version(key), req, resp, func(ok bool) {
		cl.changing = false
		if !ok {
			return
		}
		allowed := []int16{wire.ErrNone, wire.ErrNotController, wire.ErrRequestTimedOut, wire.ErrDuplicateVoter, wire.ErrVoterNotFound, wire.ErrInvalidRequest}
		if !slices.Contains(allowed, resp.ErrorCode) {
			w.fail("voter change of node %d (key %d): error %d %q", id, key, resp.ErrorCode, resp.ErrorMessage)
			return
		}
		if resp.ErrorCode == wire.ErrNone {
			w.stats.VoterChanges++
		}
	})
}

// replication is the replication factor clients ask for: the cluster size
// while it is fixed, the broker's default (-1) once it can change.
func (w *world) replication() int16 {
	if w.opts.changes {
		return -1
	}
	return int16(len(w.nodes))
}
