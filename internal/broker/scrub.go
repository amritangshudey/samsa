package broker

import (
	"fmt"

	"github.com/amritangshudey/samsa/internal/storage"
	"github.com/amritangshudey/samsa/internal/sys"
)

// scrub reads a little of the sealed segments of the Raft log, once a
// second, to find damage before a client or a follower needs the entries
// (docs/design/b1-storage-faults.md, section 3.4). It changes nothing; on
// damage the node stops, and the repair is its next start.
func (n *Node) scrub() {
	if n.fatal != nil || n.cfg.ScrubInterval < 0 || n.clock.Now() < n.scrubRestAt {
		return
	}
	if n.scrubAt < 0 { // a pass is done
		n.scrubAt = 0
	}
	n.scrubAt = max(n.scrubAt, n.raft.LogStart())
	next, read, damage, found, err := n.raft.Scrub(n.scrubAt, n.cfg.ScrubBytesPerSecond)
	n.scrubbed += int64(read)
	switch {
	case err != nil:
		n.fatal = fmt.Errorf("scrub of the raft log: %w", err)
	case found:
		n.damaged(damage)
	default:
		if next < 0 {
			n.scrubRestAt = n.clock.Now() + sys.Instant(n.cfg.ScrubInterval)
		}
		n.scrubAt = next
	}
}

// damaged stops the node on damage to its log, after recording it so that
// the next start repairs it from the other nodes, wherever the damage is.
func (n *Node) damaged(offset int64) {
	cause := fmt.Errorf("the raft log is damaged (offset %d): restart the node, which repairs it from its peers: %w", offset, storage.ErrCorrupt)
	if err := n.raft.MarkDamage(offset); err != nil {
		cause = fmt.Errorf("%w; and the damage could not be recorded: %w", cause, err)
	}
	if n.fatal == nil {
		n.fatal = cause
	}
}
