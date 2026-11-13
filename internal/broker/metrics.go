package broker

import (
	"fmt"

	"github.com/amritangshudey/samsa/internal/raft"
)

// Metrics is a copy of a node's counters and gauges. The driver takes one
// on the node's own goroutine and publishes it; nothing else reads the node.
type Metrics struct {
	// Requests counts decoded requests by API key (every key is below 128).
	Requests [128]int64
	// BytesIn and BytesOut count client bytes received and responses sent.
	BytesIn, BytesOut int64
	// Connections counts client connections accepted, Dropped those the
	// node closed for bad input or limits; Open is the number open now.
	Connections, Dropped int64
	Open                 int
	// Faults counts faults injected on purpose (Config.Faults).
	Faults int64

	Leader, Serving                               bool
	Term, Committed, Applied, LogStart, LastIndex int64
	Snapshot                                      int64 // -1: none
	Topics, Partitions, Groups                    int
	// Ready is false, with the reason in NotReady, while the node cannot
	// serve: see Metrics on Node.
	Ready    bool
	NotReady string
}

// counters are the node's running totals behind Metrics.
type counters struct {
	requests                        [128]int64
	bytesIn, bytesOut, conns, drops int64
}

// Metrics returns the node's counters and gauges. A node is ready when it
// serves as leader, or follows a known leader and has applied everything it
// knows to be committed; not while it fails, stops, or has no leader.
func (n *Node) Metrics() Metrics {
	m := Metrics{
		Requests: n.stats.requests, BytesIn: n.stats.bytesIn, BytesOut: n.stats.bytesOut,
		Connections: n.stats.conns, Dropped: n.stats.drops, Open: len(n.conns), Faults: n.faults.count,
		Leader: n.raft.Role() == raft.Leader, Serving: n.serving,
		Term: n.raft.Term(), Committed: n.raft.Committed(), Applied: n.applied,
		LogStart: n.raft.LogStart(), LastIndex: n.raft.LastIndex(), Snapshot: n.raft.SnapshotIndex(),
		Topics: len(n.st.topics), Groups: len(n.st.groups),
	}
	for _, t := range n.st.topics {
		m.Partitions += len(t.parts)
	}
	leader, known := n.raft.Leader()
	switch {
	case n.fatal != nil:
		m.NotReady = "failed: " + n.fatal.Error()
	case n.stopping:
		m.NotReady = "stopping"
	case n.serving:
		m.Ready = true
	case m.Leader:
		m.NotReady = "elected, not serving until an entry of its term is applied"
	case !known:
		m.NotReady = "no known leader"
	case n.applied < m.Committed:
		m.NotReady = fmt.Sprintf("following node %d, applied %d of %d committed entries", leader, n.applied, m.Committed)
	default:
		m.Ready = true
	}
	return m
}
