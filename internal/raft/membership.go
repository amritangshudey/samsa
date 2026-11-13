package raft

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Membership changes, one voter at a time (docs/design/c1-membership.md).
// A configuration is a log entry of kind kindConfig. It is in force on a
// node as soon as it is in that node's log, committed or not; truncating
// the entry falls back to the one before. Below the log, the snapshot holds
// the configuration as of its index; with neither, the bootstrap
// configuration from Config.Peers applies.

// Member is one voter: its id and an opaque description that Raft stores
// and replicates but never reads (the broker keeps addresses there).
type Member struct {
	ID   sys.NodeID
	Info []byte
}

// Bounds checked before allocating when a configuration is decoded.
const (
	maxMembers    = 64
	maxMemberInfo = 1024
)

// Errors of AddLearner and ChangeMembers.
var (
	// ErrChangePending: a configuration entry is not yet committed, or no
	// entry of the leader's term is (Ongaro's 2015 erratum). Retry later.
	ErrChangePending = errors.New("raft: a membership change is in progress")
	// ErrBadChange: the new configuration does not differ from the one in
	// force by exactly one voter, or is malformed.
	ErrBadChange = errors.New("raft: a membership change must add or remove exactly one voter")
	// ErrNotCaughtUp: the voter to add is not a learner that holds every
	// committed entry yet. Retry later.
	ErrNotCaughtUp = errors.New("raft: the node to add has not caught up")
)

// confAt is a configuration and the index of the entry that set it; -1 for
// the bootstrap configuration.
type confAt struct {
	index   int64
	members []Member
}

func encodeMembers(ms []Member) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(ms)))
	for _, m := range ms {
		b = binary.BigEndian.AppendUint32(b, uint32(m.ID))
		b = binary.BigEndian.AppendUint16(b, uint16(len(m.Info)))
		b = append(b, m.Info...)
	}
	return b
}

// decodeMembers checks every length against the input and the bounds, and
// requires ids positive, ascending and unique, and at least one member.
func decodeMembers(b []byte) ([]Member, error) {
	bad := func(what string) ([]Member, error) {
		return nil, fmt.Errorf("%w: configuration %s", ErrBadChange, what)
	}
	if len(b) < 2 {
		return bad("truncated")
	}
	n := int(binary.BigEndian.Uint16(b))
	b = b[2:]
	if n < 1 || n > maxMembers || n > len(b)/6 {
		return bad(fmt.Sprintf("with %d members", n))
	}
	ms := make([]Member, 0, n)
	for range n {
		if len(b) < 6 {
			return bad("truncated")
		}
		m := Member{ID: sys.NodeID(binary.BigEndian.Uint32(b))}
		size := int(binary.BigEndian.Uint16(b[4:]))
		b = b[6:]
		if size > maxMemberInfo || size > len(b) {
			return bad("member info too long")
		}
		m.Info, b = b[:size:size], b[size:]
		if m.ID <= 0 || (len(ms) > 0 && m.ID <= ms[len(ms)-1].ID) {
			return bad("ids not positive, ascending and unique")
		}
		ms = append(ms, m)
	}
	if len(b) != 0 {
		return bad("has trailing bytes")
	}
	return ms, nil
}

func ids(ms []Member) []sys.NodeID {
	out := make([]sys.NodeID, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

// conf returns the configuration in force: the last configuration entry
// above the snapshot's, else the snapshot's (or bootstrap) one.
func (r *Raft) conf() confAt { return r.confAsOf(r.LastIndex()) }

// confAsOf returns the configuration in force after entry index.
func (r *Raft) confAsOf(index int64) confAt {
	for i := len(r.confs) - 1; i >= 0; i-- {
		if c := r.confs[i]; c.index <= index && c.index > r.base.index {
			return c
		}
	}
	return r.base
}

// Members returns the voters in force, sorted by id. They must not be
// modified.
func (r *Raft) Members() []Member { return r.conf().members }

// voters returns the ids of the voters in force, sorted.
func (r *Raft) voters() []sys.NodeID { return ids(r.conf().members) }

// isVoter reports whether id is a voter in force.
func (r *Raft) isVoter(id sys.NodeID) bool {
	_, ok := slices.BinarySearchFunc(r.conf().members, id, func(m Member, id sys.NodeID) int { return cmp.Compare(m.ID, id) })
	return ok
}

// majority returns the value that a majority of the voters in force have
// reached at least: own for this node, of(p) for every other voter, zero
// for a voter the leader has no progress for.
func majority[T cmp.Ordered](r *Raft, own T, zero T, of func(*progress) T) T {
	vs := r.voters()
	vals := make([]T, 0, len(vs))
	for _, id := range vs {
		switch p := r.peers[id]; {
		case id == r.cfg.ID:
			vals = append(vals, own)
		case p != nil:
			vals = append(vals, of(p))
		default:
			vals = append(vals, zero)
		}
	}
	slices.Sort(vals)
	return vals[len(vals)-1-len(vals)/2] // the majority-th highest
}

// counted reports whether the ids in set make a majority of the voters.
func (r *Raft) counted(set map[sys.NodeID]bool) bool {
	vs, n := r.voters(), 0
	for _, id := range vs {
		if set[id] {
			n++
		}
	}
	return n > len(vs)/2
}

// noteEntry records a configuration entry just stored at index.
func (r *Raft) noteEntry(index int64, kind byte, cmd []byte) error {
	if kind != kindConfig {
		return nil
	}
	ms, err := decodeMembers(cmd)
	if err != nil {
		return fmt.Errorf("raft entry %d: %w", index, err)
	}
	r.confs = append(r.confs, confAt{index, ms})
	r.confChanged()
	return nil
}

// dropConfsFrom forgets configuration entries at index and above, which
// the log no longer holds.
func (r *Raft) dropConfsFrom(index int64) {
	n := len(r.confs)
	for n > 0 && r.confs[n-1].index >= index {
		n--
	}
	if n != len(r.confs) {
		r.confs = r.confs[:n]
		r.confChanged()
	}
}

// confChanged brings a leader's followers in line with the configuration
// in force: progress for every voter, kept for learners and for voters of
// the committed configuration until it moves past them.
func (r *Raft) confChanged() {
	if r.role != Leader {
		return
	}
	for _, id := range r.voters() {
		if id != r.cfg.ID && r.peers[id] == nil {
			r.addPeer(id)
		}
	}
}

// addPeer starts replicating to id. A leader that was alone has no
// heartbeat yet, and only the heartbeat repairs a lost message (found by
// the random-schedule test, seed 181).
func (r *Raft) addPeer(id sys.NodeID) {
	if len(r.peers) == 0 {
		r.env.SetTimer(TimerHeartbeat, r.cfg.HeartbeatInterval)
	}
	r.peers[id] = &progress{next: r.LastIndex() + 1, match: -1}
}

// pruneAfterCommit runs when the commit index moves: nodes that are
// neither voters in force, nor learners, nor voters of the committed
// configuration are no longer contacted, and a leader that the committed
// configuration leaves out steps down (thesis 4.2.2).
func (r *Raft) pruneAfterCommit() {
	if r.role != Leader {
		return
	}
	committed := ids(r.confAsOf(r.commit).members)
	for _, id := range r.targets() {
		if !r.isVoter(id) && !slices.Contains(r.learners, id) && !slices.Contains(committed, id) {
			delete(r.peers, id)
		}
	}
	if !slices.Contains(committed, r.cfg.ID) && !r.isVoter(r.cfg.ID) {
		r.follow(r.hard.term)
		r.leader = noVote
	}
}

// targets returns the ids the leader replicates to, sorted: never the
// map's order.
func (r *Raft) targets() []sys.NodeID {
	out := make([]sys.NodeID, 0, len(r.peers))
	for id := range r.peers {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// AddLearner makes the leader replicate to id without counting it, so
// that it catches up before ChangeMembers makes it a voter. Learners are
// leader state: they are forgotten when leadership is lost.
func (r *Raft) AddLearner(id sys.NodeID) error {
	switch {
	case r.err != nil:
		return r.err
	case r.role != Leader, r.handingOver():
		return ErrNotLeader
	case id <= 0 || id == r.cfg.ID || r.isVoter(id):
		return ErrBadChange
	}
	if !slices.Contains(r.learners, id) {
		r.learners = append(r.learners, id)
		slices.Sort(r.learners)
	}
	if r.peers[id] == nil {
		r.addPeer(id)
		r.replicate(id, r.peers[id])
	}
	return nil
}

// ChangeMembers appends a configuration that differs from the one in force
// by exactly one voter, and returns its index. It is in force at once and
// complete once committed. Refused while another change is uncommitted or
// before an entry of this term has committed (ErrChangePending), and for a
// voter to add that is not a caught-up learner (ErrNotCaughtUp).
func (r *Raft) ChangeMembers(ms []Member) (int64, error) {
	switch {
	case r.err != nil:
		return 0, r.err
	case r.role != Leader, r.handingOver():
		return 0, ErrNotLeader
	case !r.mut(MutChangeBeforeCommit) && (r.commit < 0 || r.terms[r.commit] != r.hard.term):
		return 0, ErrChangePending
	case !r.mut(MutTwoChanges) && len(r.confs) > 0 && r.confs[len(r.confs)-1].index > r.commit:
		return 0, ErrChangePending
	}
	b := encodeMembers(ms)
	if _, err := decodeMembers(b); err != nil {
		return 0, err
	}
	old, now := r.voters(), ids(ms)
	var added, removed []sys.NodeID
	for _, id := range now {
		if !slices.Contains(old, id) {
			added = append(added, id)
		}
	}
	for _, id := range old {
		if !slices.Contains(now, id) {
			removed = append(removed, id)
		}
	}
	if len(added)+len(removed) != 1 {
		return 0, ErrBadChange
	}
	if len(added) == 1 {
		p := r.peers[added[0]]
		if !slices.Contains(r.learners, added[0]) || p == nil || p.match < r.commit {
			return 0, ErrNotCaughtUp
		}
		r.learners = slices.DeleteFunc(r.learners, func(id sys.NodeID) bool { return id == added[0] })
	}
	index, err := r.append(entry{term: r.hard.term, kind: kindConfig, cmd: b})
	if err != nil {
		return 0, err
	}
	r.broadcast(false)
	r.advanceCommit()
	return index, nil
}

// mayStand reports whether this node should stand for election: unless the
// configuration in force and the committed one both leave it out. A node
// whose removal is only appended may still be needed (thesis 4.2.2); once
// the removal commits, the remaining voters can always elect a leader
// among themselves, and a removed node that kept winning would only make
// way again (design, section 6, finding 10).
func (r *Raft) mayStand() bool {
	if r.isVoter(r.cfg.ID) {
		return true
	}
	_, ok := slices.BinarySearchFunc(r.confAsOf(r.commit).members, r.cfg.ID, func(m Member, id sys.NodeID) int { return cmp.Compare(m.ID, id) })
	return ok
}
