package broker

import (
	"fmt"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// The cluster's format level (docs/design/g7-rolling-upgrade.md). Every
// node tells the leader the highest level it supports; once every voter
// has, recently, the leader raises the cluster's level with cmdSetLevel.
// Nothing that needs a level is written below it, so an older build in the
// cluster during a rolling upgrade never meets a format it cannot read.

const (
	msgLevel byte = 3 // a broker message: the sender's formatLevel (1 byte)

	levelEvery = time.Second     // how often a node below its level reports
	levelFresh = 3 * levelEvery // a report older than this does not count
)

// levelReport is what the leader last heard from one node.
type levelReport struct {
	level byte
	at    sys.Instant
}

// onLevelTimer reports this node's level to the leader, and lets a leader
// check whether it can raise the cluster's, until the cluster is at this
// build's level.
func (n *Node) onLevelTimer() {
	if n.st.level >= formatLevel {
		return
	}
	n.reportLevel()
	n.raiseLevel()
	n.clock.SetTimer(timerLevel, levelEvery)
}

// reportLevel sends this build's level to the leader, if another node
// leads.
func (n *Node) reportLevel() {
	if leader, ok := n.raft.Leader(); ok && leader != sys.NodeID(n.cfg.NodeID) {
		n.net.SendPeer(leader, []byte{brokerMsg, msgLevel, formatLevel})
		n.reportedTo = leader
	}
}

// onLevelMessage records a node's report.
func (n *Node) onLevelMessage(from sys.NodeID, msg []byte) {
	if len(msg) != 3 {
		return
	}
	n.levels[from] = levelReport{level: msg[2], at: n.clock.Now()}
	n.raiseLevel()
}

// levelOf returns the level id reported within levelFresh; 0, the level of
// a build that never reports, otherwise.
func (n *Node) levelOf(id sys.NodeID) byte {
	if id == sys.NodeID(n.cfg.NodeID) {
		return formatLevel
	}
	r, ok := n.levels[id]
	if !ok || n.clock.Now()-r.at > sys.Instant(levelFresh) {
		return 0
	}
	return r.level
}

// raiseLevel proposes this build's level once every voter in force
// supports it.
func (n *Node) raiseLevel() {
	if !n.serving || n.st.level >= formatLevel || n.levelProposed {
		return
	}
	if !n.cfg.UnsafeRaiseLevelEarly {
		for _, m := range n.raft.Members() {
			if n.levelOf(m.ID) < formatLevel {
				return
			}
		}
	}
	n.levelProposed = true // until this node serves again: a lost proposal is never answered
	n.propose([]byte{cmdSetLevel, formatLevel}, func(result) { n.levelProposed = false })
}

// levelRefused returns the refusal for a request that writes something of
// level need, or "" if the cluster is there.
func (n *Node) levelRefused(need byte) string {
	if n.st.level >= need {
		return ""
	}
	return fmt.Sprintf("the cluster runs at format level %d until every node runs a release that supports level %d", n.st.level, need)
}

// knowsLeader reports whether some node is known to lead. Without one, a
// follower's level may lag, and NOT_CONTROLLER is the better answer.
func (n *Node) knowsLeader() bool {
	_, ok := n.raft.Leader()
	return ok
}

// voterLevelRefused is levelRefused as a voter change response.
func (n *Node) voterLevelRefused() *wire.VoterChangeResponse {
	if msg := n.levelRefused(1); msg != "" {
		return voterChangeRefusal(wire.ErrUnsupportedVersion, msg)
	}
	return nil
}
