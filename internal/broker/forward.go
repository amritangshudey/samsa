package broker

import (
	"encoding/binary"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Forwarding ACL changes from a follower to the leader
// (docs/design/c8b-credentials-forwarding.md, section 3).
//
// Broker messages share the peer channel with Raft's. Raft messages start
// with their version byte, 1; broker messages with brokerMsg, which an
// older node's Raft discards as an unknown version.
const (
	brokerMsg byte = 0xB0

	msgForward        byte = 1 // tag (8 bytes), then the command
	msgForwardRefused byte = 2 // tag: the receiver is not serving

	forwardTimeout = 10 * time.Second
)

// forward is a follower's ACL change waiting for its command to apply.
type forward struct {
	done  func(result)
	timer sys.TimerID
}

// proposeOrForward proposes cmd if this node serves, and otherwise asks
// the leader to. Only idempotent ACL commands come here.
func (n *Node) proposeOrForward(cmd []byte, done func(result)) {
	if n.canRead() {
		n.propose(cmd, done)
		return
	}
	notController := func() { done(result{errCode: wire.ErrNotController}) }
	leader, ok := n.raft.Leader()
	if !ok || leader == sys.NodeID(n.cfg.NodeID) {
		notController()
		return
	}
	tag := n.rand.Uint64()
	f := &forward{done: done}
	f.timer = n.after(forwardTimeout, func() {
		delete(n.forwards, tag)
		notController()
	})
	n.forwards[tag] = f
	msg := binary.BigEndian.AppendUint64([]byte{brokerMsg, msgForward}, tag)
	n.net.SendPeer(leader, append(msg, cmd...))
}

// completeForward answers a forwarded change, once.
func (n *Node) completeForward(tag uint64, res result) {
	if f := n.forwards[tag]; f != nil {
		delete(n.forwards, tag)
		n.cancel(f.timer)
		f.done(res)
	}
}

// onBrokerMessage handles a broker message from a peer. Peers are cluster
// members, but their bytes are still checked like any input (rule 4).
func (n *Node) onBrokerMessage(from sys.NodeID, msg []byte) {
	if len(msg) >= 2 && msg[1] == msgLevel {
		n.onLevelMessage(from, msg)
		return
	}
	if len(msg) < 10 {
		return
	}
	tag := binary.BigEndian.Uint64(msg[2:10])
	switch msg[1] {
	case msgForward:
		cmd := msg[10:]
		if !validForwarded(cmd) {
			n.log.Warn("dropped a malformed forwarded command", "from", uint64(from))
			return
		}
		if !n.canRead() || n.levelRefused(1) != "" { // cmdForwarded is level 1
			n.net.SendPeer(from, binary.BigEndian.AppendUint64([]byte{brokerMsg, msgForwardRefused}, tag))
			return
		}
		n.propose(encodeForwarded(int32(from), tag, cmd), func(result) {}) // the origin learns the result from its own apply
	case msgForwardRefused:
		n.completeForward(tag, result{errCode: wire.ErrNotController})
	}
}

// validForwarded reports whether cmd is an ACL command that apply accepts.
func validForwarded(cmd []byte) bool {
	if len(cmd) == 0 {
		return false
	}
	r := wire.NewReader(cmd[1:])
	switch cmd[0] {
	case cmdCreateACL:
		a := readACL(r)
		return r.Err() == nil && r.Len() == 0 && validACL(a) == ""
	case cmdDeleteACLs:
		f := readFilter(r)
		return r.Err() == nil && r.Len() == 0 && validFilter(f)
	}
	return false
}
