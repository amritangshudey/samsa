package broker

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/amritangshudey/samsa/internal/raft"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Membership changes through AddRaftVoter and RemoveRaftVoter
// (docs/design/c1-membership.md, section 4). The voters live in Raft's
// configuration; each carries its listeners, so that a node joining later
// learns every address from the log.

const (
	maxListeners        = 8
	voterChangeDefault  = 30 * time.Second
	voterChangeMaxDelay = 5 * time.Minute
	voterChangePoll     = 50 * time.Millisecond
)

// encodeMemberInfo is what a configuration entry holds for one voter:
// version 1, then its listeners.
func encodeMemberInfo(ls []wire.Listener) []byte {
	b := []byte{1, byte(len(ls))}
	for _, l := range ls {
		b = append(b, byte(len(l.Name)))
		b = append(b, l.Name...)
		b = append(b, byte(len(l.Host)))
		b = append(b, l.Host...)
		b = append(b, byte(l.Port>>8), byte(l.Port))
	}
	return b
}

// decodeMemberInfo checks every length against the input (rule 4).
func decodeMemberInfo(b []byte) ([]wire.Listener, error) {
	bad := errors.New("malformed member info")
	if len(b) < 2 || b[0] != 1 || int(b[1]) > maxListeners {
		return nil, bad
	}
	n, b := int(b[1]), b[2:]
	str := func() (string, bool) {
		if len(b) < 1 || len(b) < 1+int(b[0]) {
			return "", false
		}
		s := string(b[1 : 1+int(b[0])])
		b = b[1+int(b[0]):]
		return s, true
	}
	var ls []wire.Listener
	for range n {
		name, ok1 := str()
		host, ok2 := str()
		if !ok1 || !ok2 || len(b) < 2 {
			return nil, bad
		}
		ls = append(ls, wire.Listener{Name: name, Host: host, Port: uint16(b[0])<<8 | uint16(b[1])})
		b = b[2:]
	}
	if len(b) != 0 {
		return nil, bad
	}
	return ls, nil
}

// bootPeer returns the -peers entry for id, if there is one.
func (n *Node) bootPeer(id int32) (Peer, bool) {
	for _, p := range n.cfg.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

// members returns the voters in force with their client addresses, by id.
func (n *Node) members() []Peer {
	var out []Peer
	for _, m := range n.raft.Members() {
		p := Peer{ID: int32(m.ID)}
		if ls, err := decodeMemberInfo(m.Info); err == nil && len(ls) > 0 {
			p.Host, p.Port = ls[0].Host, int32(ls[0].Port)
		} else if q, ok := n.bootPeer(p.ID); ok {
			p = q
		}
		out = append(out, p)
	}
	return out
}

// peerAddr tells the transport where node id listens for peers.
func (n *Node) peerAddr(id int32, addr string) {
	if addr != "" && id != n.cfg.NodeID {
		n.net.PeerAddr(sys.NodeID(id), addr)
	}
}

// peerListener returns the address of the PEER listener in ls, if any.
func peerListener(ls []wire.Listener) string {
	for _, l := range ls {
		if l.Name == "PEER" {
			return joinHostPort(l.Host, l.Port)
		}
	}
	return ""
}

// joinHostPort and splitHostPort do what net's functions do, for a core
// that may not import net (ADR 0003): an IPv6 host goes in brackets.
func joinHostPort(host string, port uint16) string {
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return host + ":" + strconv.Itoa(int(port))
}

func splitHostPort(addr string) (string, uint16, bool) {
	i := strings.LastIndexByte(addr, ':')
	if i < 1 {
		return "", 0, false
	}
	host := strings.TrimSuffix(strings.TrimPrefix(addr[:i], "["), "]")
	port, err := strconv.ParseUint(addr[i+1:], 10, 16)
	return host, uint16(port), err == nil && port != 0 && host != ""
}

// peerAddrs tells the transport every voter's peer address whenever the
// configuration in force may have changed.
func (n *Node) peerAddrs() {
	if i := n.raft.ConfIndex(); i != n.confIndex {
		n.confIndex = i
		for _, m := range n.raft.Members() {
			if ls, err := decodeMemberInfo(m.Info); err == nil {
				n.peerAddr(int32(m.ID), peerListener(ls))
			}
		}
	}
}

// described returns the configuration in force with every voter's info
// filled in: a bootstrap voter has none until the first change writes the
// address this node knows for it.
func (n *Node) described() []raft.Member {
	ms := slices.Clone(n.raft.Members())
	for i, m := range ms {
		if len(m.Info) == 0 {
			if p, ok := n.bootPeer(int32(m.ID)); ok {
				ls := []wire.Listener{{Name: "CLIENT", Host: p.Host, Port: uint16(p.Port)}}
				if host, port, ok := splitHostPort(p.PeerAddr); ok {
					ls = append(ls, wire.Listener{Name: "PEER", Host: host, Port: port})
				}
				ms[i].Info = encodeMemberInfo(ls)
			}
		}
	}
	return ms
}

// voterChange is the one AddRaftVoter or RemoveRaftVoter a leader works
// on; another gets REQUEST_TIMED_OUT, as in Kafka.
type voterChange struct {
	c       *conn
	h       wire.RequestHeader
	members []raft.Member // the configuration to propose
	adding  sys.NodeID    // the voter to add; 0 for a removal
	index   int64         // of its entry; -1 until appended
	timer   sys.TimerID
	poll    sys.TimerID // retries ChangeMembers while the cluster is idle
}

func voterChangeRefusal(code int16, msg string) *wire.VoterChangeResponse {
	return &wire.VoterChangeResponse{ErrorCode: code, ErrorMessage: msg}
}

// checkVoterChange applies the checks both requests share, in Kafka's
// order: authorization, leadership, cluster id, one change at a time.
func (n *Node) checkVoterChange(c *conn, clusterID string) *wire.VoterChangeResponse {
	switch {
	case !n.may(c, opAlter, resCluster, clusterName):
		return voterChangeRefusal(wire.ErrClusterAuthorizationFailed, "needs ALTER permission on the cluster")
	case !n.serving:
		return voterChangeRefusal(wire.ErrNotController, "this node is not the leader")
	case clusterID != "" && clusterID != n.cfg.ClusterID:
		return voterChangeRefusal(wire.ErrInconsistentClusterID, "cluster id "+clusterID+" is not "+n.cfg.ClusterID)
	case n.change != nil:
		return voterChangeRefusal(wire.ErrRequestTimedOut, "a previous voter change is in progress")
	}
	return n.voterLevelRefused()
}

func (n *Node) addVoter(c *conn, h wire.RequestHeader, req *wire.AddRaftVoterRequest) {
	if resp := n.checkVoterChange(c, req.ClusterID); resp != nil {
		n.respond(c, h, resp)
		return
	}
	id := sys.NodeID(req.VoterID)
	valid := len(req.Listeners) > 0 && len(req.Listeners) <= maxListeners
	for _, l := range req.Listeners {
		valid = valid && l.Host != "" && len(l.Host) < 256 && len(l.Name) < 256 && l.Port != 0
	}
	switch {
	case req.VoterID <= 0 || req.VoterID > maxNodeID || !valid:
		n.respond(c, h, voterChangeRefusal(wire.ErrInvalidRequest, "a voter needs an id and at least one listener with a host and port"))
		return
	case slices.ContainsFunc(n.raft.Members(), func(m raft.Member) bool { return m.ID == id }):
		n.respond(c, h, voterChangeRefusal(wire.ErrDuplicateVoter, fmt.Sprintf("node %d is already a voter", id)))
		return
	}
	n.peerAddr(req.VoterID, peerListener(req.Listeners))
	if err := n.raft.AddLearner(id); err != nil {
		n.respond(c, h, voterChangeRefusal(wire.ErrNotController, err.Error()))
		return
	}
	ms := append(n.described(), raft.Member{ID: id, Info: encodeMemberInfo(req.Listeners)})
	slices.SortFunc(ms, func(a, b raft.Member) int { return int(a.ID) - int(b.ID) })
	n.startVoterChange(c, h, ms, id, req.TimeoutMs)
}

func (n *Node) removeVoter(c *conn, h wire.RequestHeader, req *wire.RemoveRaftVoterRequest) {
	if resp := n.checkVoterChange(c, req.ClusterID); resp != nil {
		n.respond(c, h, resp)
		return
	}
	ms := n.described()
	i := slices.IndexFunc(ms, func(m raft.Member) bool { return m.ID == sys.NodeID(req.VoterID) })
	switch {
	case i < 0:
		n.respond(c, h, voterChangeRefusal(wire.ErrVoterNotFound, fmt.Sprintf("node %d is not a voter", req.VoterID)))
		return
	case len(ms) == 1:
		n.respond(c, h, voterChangeRefusal(wire.ErrInvalidRequest, "cannot remove the last voter"))
		return
	}
	n.startVoterChange(c, h, slices.Delete(ms, i, i+1), 0, 0)
}

func (n *Node) startVoterChange(c *conn, h wire.RequestHeader, conf []raft.Member, adding sys.NodeID, timeoutMs int32) {
	d := voterChangeDefault
	if timeoutMs > 0 {
		d = min(ms(timeoutMs), voterChangeMaxDelay)
	}
	vc := &voterChange{c: c, h: h, members: conf, adding: adding, index: -1}
	vc.timer = n.after(d, func() {
		vc.timer = 0
		n.finishVoterChange(wire.ErrRequestTimedOut, "the change did not complete in time")
	})
	n.change = vc
	n.stepVoterChange()
}

// stepVoterChange moves the change on: appended once Raft accepts it (the
// learner has caught up, the previous change committed), answered once
// committed. It runs after every apply.
func (n *Node) stepVoterChange() {
	vc := n.change
	if vc == nil || !n.serving {
		return
	}
	if vc.index < 0 {
		var err error
		index := int64(-1)
		if vc.adding != 0 && n.levelOf(vc.adding) < n.st.level {
			err = raft.ErrNotCaughtUp // it must report a level that reads the log first
		} else {
			index, err = n.raft.ChangeMembers(vc.members)
		}
		switch {
		case errors.Is(err, raft.ErrNotCaughtUp), errors.Is(err, raft.ErrChangePending):
			// Retried after the next apply, or soon if nothing applies.
			if vc.poll == 0 {
				vc.poll = n.after(voterChangePoll, func() {
					vc.poll = 0
					n.stepVoterChange()
				})
			}
			return
		case errors.Is(err, raft.ErrBadChange):
			n.finishVoterChange(wire.ErrInvalidRequest, err.Error())
			return
		case err != nil:
			n.finishVoterChange(wire.ErrNotController, err.Error())
			return
		}
		vc.index = index
	}
	if n.raft.Committed() > vc.index {
		n.finishVoterChange(wire.ErrNone, "")
	}
}

func (n *Node) finishVoterChange(code int16, msg string) {
	vc := n.change
	if vc == nil {
		return
	}
	n.change = nil
	n.cancel(vc.timer)
	n.cancel(vc.poll)
	n.respond(vc.c, vc.h, voterChangeRefusal(code, msg))
}

// voterChangeLost runs when leadership ends. A change already committed
// is answered: a leader that removed itself steps down at that very
// moment, and its client must hear that it worked.
func (n *Node) voterChangeLost() {
	if vc := n.change; vc != nil {
		if vc.index >= 0 && n.raft.Committed() > vc.index {
			n.finishVoterChange(wire.ErrNone, "")
			return
		}
		n.change = nil // its connection is dropped with the others
	}
}
