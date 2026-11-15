package broker

import (
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Fetch from the nearest replica (KIP-392, docs/design/c19-follower-fetch.md).
// A consumer says which rack it is in (Fetch v11 RackId). The leader names a
// caught-up replica in that rack and sends no records; the consumer then
// fetches from that node. A follower answers fetches from what it has
// applied, which is only what is committed, so nothing it returns can be
// lost; if the consumer is ahead of it, it says OFFSET_NOT_AVAILABLE.

// canServeFetch reports whether this node, not the leader, may answer a
// consumer's fetch: it follows a leader it has heard from lately, and is not
// repairing its log or receiving a snapshot (which resets the log under the
// state it has applied: found by seed 749).
func (n *Node) canServeFetch() bool {
	return !n.serving && n.fatal == nil && !n.stopping && n.applied > 0 &&
		n.raft.HeardFromLeader() && !n.raft.Recovering() && !n.raft.Installing()
}

// preferReplica names, in resp, a replica in rack for the consumer to read
// from next, and removes the records: the consumer will ask that node. It
// reports whether it did. A replica is named only if it is not this node and
// the leader knows it holds every committed entry.
func (n *Node) preferReplica(rack string, resp *wire.FetchResponse) bool {
	if own, ok := n.bootPeer(n.cfg.NodeID); ok && own.Rack == rack {
		return false // the leader is in the consumer's rack
	}
	var pick *Peer
	for _, p := range n.members() {
		if p.ID == n.cfg.NodeID || p.Rack != rack {
			continue
		}
		if m, ok := n.raft.Match(sys.NodeID(p.ID)); !ok || m < n.raft.Committed()-1 {
			continue
		}
		if pick == nil || p.ID < pick.ID {
			p := p
			pick = &p
		}
	}
	if pick == nil {
		return false
	}
	named := false
	for _, t := range resp.Topics {
		for i := range t.Partitions {
			if p := &t.Partitions[i]; p.ErrorCode == wire.ErrNone {
				p.Records, p.PreferredReadReplica, p.HasPreferred = nil, pick.ID, true
				named = true
			}
		}
	}
	return named
}
