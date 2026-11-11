package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/amritangshudey/samsa/internal/storage"
	"github.com/amritangshudey/samsa/internal/sys"
)

// ErrNotLeader is returned by Propose on a node that is not the leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Role is what a node currently is in its term.
type Role uint8

// The three roles of the Raft paper.
const (
	Follower Role = iota
	Candidate
	Leader
)

// Timer names one of the two timers a Raft instance uses.
type Timer uint8

// The timers. Arming one replaces its earlier arming.
const (
	// TimerElection fires when a follower or candidate has heard from no
	// leader for an election timeout.
	TimerElection Timer = iota
	// TimerHeartbeat fires on a leader, which then contacts every follower.
	TimerHeartbeat
)

// Env is what a Raft instance needs from the node that drives it. It never
// reaches the network, the clock or a random source any other way.
type Env interface {
	// Send delivers msg to a peer, at best: it may be lost.
	Send(to sys.NodeID, msg []byte)
	// SetTimer arranges a call to OnTimer(t) after d, replacing any
	// earlier arming of t.
	SetTimer(t Timer, d time.Duration)
	// CancelTimer disarms t.
	CancelTimer(t Timer)
	// Rand returns a uniformly distributed value.
	Rand() uint64
}

// Config configures a Raft instance.
type Config struct {
	// ID is this node. Peers lists every node of the cluster, ID included.
	ID    sys.NodeID
	Peers []sys.NodeID
	// Dir is the directory of the log and the hard state on the disk.
	Dir string
	// Log configures the entry log. Its SyncBytes is ignored: Raft decides
	// when to sync.
	Log storage.Config
	// ElectionTimeout is the least time without a leader before a node
	// stands for election; the actual timeout is drawn from
	// [ElectionTimeout, 2*ElectionTimeout). Default 300ms.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often a leader contacts each follower, and
	// so how long a lost message goes unrepaired. Default 50ms.
	HeartbeatInterval time.Duration
	// MaxAppendBytes bounds the commands in one AppendEntries. Default 1 MiB.
	MaxAppendBytes int

	// AckBeforeSync makes the node treat an entry as durable once it is
	// appended, not once it is synced. Acknowledged entries can then be
	// lost in a power cut, and with them Raft's guarantees. It is what
	// the weaker fsync policies mean, and under the strict policy it is a
	// deliberate bug for the simulator to find.
	AckBeforeSync bool
	// UnsafeCommitAnyTerm and UnsafeVoteWithoutPersist are deliberate
	// bugs: committing entries of earlier terms by counting replicas
	// (the paper's Figure 8), and granting a vote before it is on disk.
	UnsafeCommitAnyTerm, UnsafeVoteWithoutPersist bool
}

// progress is what a leader knows about one follower.
type progress struct {
	next  int64 // index of the next entry to send
	match int64 // highest index known to be durable there; never decreases
	// inflight is set while an AppendEntries awaits its response. It is
	// cleared by the response or, if that never comes, by the heartbeat.
	inflight bool
}

// Raft is one node's consensus state. It is driven by exactly one caller,
// which must check Err after every call: once it is non-nil the instance
// does nothing more.
type Raft struct {
	cfg  Config
	env  Env
	disk sys.Disk
	hsf  hardStateFile

	log   *storage.Log
	terms []int64 // term of every entry, by index
	hard  hardState

	role   Role
	leader sys.NodeID // of the current term, if known; else noVote
	commit int64      // highest index known committed; -1 if none
	votes  map[sys.NodeID]bool
	peers  map[sys.NodeID]*progress // leader only: every node but this one

	dirty bool // entries appended since the last Flush
	// A follower answers AppendEntries only once the entries are durable.
	// ackTo is the leader awaiting that answer (noVote if none), ackIndex
	// the highest index its messages confirmed, ackEcho what to echo.
	ackTo             sys.NodeID
	ackIndex, ackEcho int64

	err error
}

// New returns an instance that does nothing until Start is called.
func New(cfg Config, env Env, disk sys.Disk) *Raft {
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 300 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.MaxAppendBytes <= 0 {
		cfg.MaxAppendBytes = 1 << 20
	}
	if cfg.Log.MaxPayloadBytes <= 0 {
		cfg.Log.MaxPayloadBytes = 1 << 20
	}
	cfg.Log.SyncBytes = -1
	return &Raft{cfg: cfg, env: env, disk: disk, hsf: hardStateFile{disk: disk, dir: cfg.Dir},
		leader: noVote, commit: -1, ackTo: noVote}
}

// Start recovers the log and the hard state from disk and begins as a
// follower. A node that is alone in its cluster stands for election at
// once, since nobody else can.
func (r *Raft) Start() error {
	if !slices.Contains(r.cfg.Peers, r.cfg.ID) || r.cfg.ID < 0 {
		return fmt.Errorf("raft: node %d is not in its own peer list %v", r.cfg.ID, r.cfg.Peers)
	}
	log, err := storage.Open(r.disk, r.cfg.Dir, r.cfg.Log)
	if err != nil {
		return fmt.Errorf("open raft log: %w", err)
	}
	r.log = log
	for off := log.StartOffset(); off < log.EndOffset(); {
		batches, err := log.Read(off, 4<<20)
		if err != nil {
			return fmt.Errorf("scan raft log at %d: %w", off, err)
		}
		for _, b := range batches {
			if len(b.Payload) < entryHeader || b.Count != 1 {
				return fmt.Errorf("%w: raft log entry %d has no header", storage.ErrCorrupt, b.Base)
			}
			r.terms = append(r.terms, int64(binary.BigEndian.Uint64(b.Payload)))
			off = b.Base + 1
		}
	}
	if r.hard, err = r.hsf.load(); err != nil {
		return err
	}
	// The term is saved before any entry of that term is stored.
	if last := r.lastTerm(); last > r.hard.term {
		return fmt.Errorf("%w: log holds term %d, hard state says %d", errBadHardState, last, r.hard.term)
	}
	if len(r.cfg.Peers) == 1 {
		r.campaign()
	} else {
		r.armElection()
	}
	return r.err
}

// Err returns the error after which the instance stopped, if any: a failed
// write or sync, or evidence that safety was violated.
func (r *Raft) Err() error { return r.err }

// Role returns the node's role, Term its current term, and Leader the
// leader of that term if the node knows it.
func (r *Raft) Role() Role { return r.role }

// Term returns the node's current term.
func (r *Raft) Term() int64 { return r.hard.term }

// Leader returns the leader of the current term, if this node knows it.
func (r *Raft) Leader() (id sys.NodeID, known bool) { return r.leader, r.leader != noVote }

// Committed returns the index below which every entry is committed.
func (r *Raft) Committed() int64 { return r.commit + 1 }

// Durable returns the index below which every entry has been synced.
func (r *Raft) Durable() int64 { return r.log.DurableOffset() }

// LastIndex returns the index of the last entry in the log, or -1.
func (r *Raft) LastIndex() int64 { return int64(len(r.terms)) - 1 }

// EntryTerm returns the term of the entry at index, which must exist.
func (r *Raft) EntryTerm(index int64) int64 { return r.terms[index] }

// NeedsFlush reports whether entries have been appended since the last
// Flush. The caller should then call Flush soon: until it does, followers
// are not answered and the leader's own entries do not count.
func (r *Raft) NeedsFlush() bool { return r.dirty && r.err == nil }

func (r *Raft) lastTerm() int64 {
	if len(r.terms) == 0 {
		return 0
	}
	return r.terms[len(r.terms)-1]
}

// stable returns the index up to which this node's log counts as durable.
func (r *Raft) stable() int64 {
	if r.cfg.AckBeforeSync {
		return r.log.EndOffset() - 1
	}
	return r.log.DurableOffset() - 1
}

func (r *Raft) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

// setHard changes the term and vote and makes them durable before
// returning: the caller may send messages that depend on them afterwards.
func (r *Raft) setHard(term int64, vote sys.NodeID, persist bool) {
	if r.hard == (hardState{term, vote}) {
		return
	}
	r.hard = hardState{term, vote}
	if persist {
		if err := r.hsf.save(r.hard); err != nil {
			r.fail(err)
		}
	}
}

func (r *Raft) armElection() {
	t := r.cfg.ElectionTimeout
	r.env.SetTimer(TimerElection, t+time.Duration(r.env.Rand()%uint64(t)))
}

func (r *Raft) send(to sys.NodeID, m message) {
	if r.err == nil { // a node that cannot vouch for its state says nothing
		m.term = r.hard.term
		r.env.Send(to, m.encode())
	}
}

// follow makes the node a follower in term, which is at least its own.
func (r *Raft) follow(term int64) {
	if term > r.hard.term {
		r.setHard(term, noVote, true)
		r.leader, r.ackTo = noVote, noVote // an answer owed to an old leader is void
	}
	if r.role != Follower {
		r.role = Follower
		r.votes, r.peers = nil, nil
		r.env.CancelTimer(TimerHeartbeat)
		r.armElection()
	}
}

// campaign starts an election in a new term.
func (r *Raft) campaign() {
	r.setHard(r.hard.term+1, r.cfg.ID, true)
	r.role, r.leader, r.peers, r.ackTo = Candidate, noVote, nil, noVote
	r.votes = map[sys.NodeID]bool{r.cfg.ID: true}
	r.armElection()
	for _, p := range r.cfg.Peers {
		if p != r.cfg.ID {
			r.send(p, message{typ: msgRequestVote, lastIndex: r.LastIndex(), lastTerm: r.lastTerm()})
		}
	}
	r.maybeLead()
}

func (r *Raft) maybeLead() {
	if r.role != Candidate || len(r.votes) <= len(r.cfg.Peers)/2 || r.err != nil {
		return
	}
	r.role, r.leader, r.votes = Leader, r.cfg.ID, nil
	r.peers = map[sys.NodeID]*progress{}
	for _, p := range r.cfg.Peers {
		if p != r.cfg.ID {
			r.peers[p] = &progress{next: r.LastIndex() + 1, match: -1}
		}
	}
	r.env.CancelTimer(TimerElection)
	if len(r.peers) > 0 { // alone, there is nobody to tell
		r.env.SetTimer(TimerHeartbeat, r.cfg.HeartbeatInterval)
	}
	// An entry of this term: until it commits, nothing from earlier terms
	// can be known to be committed (the paper's Figure 8).
	if _, err := r.append(entry{term: r.hard.term, kind: kindNoop}); err != nil {
		r.fail(err)
		return
	}
	r.broadcast(false)
	r.advanceCommit()
}

// append stores one entry at the end of the log.
func (r *Raft) append(e entry) (int64, error) {
	payload := make([]byte, entryHeader, entryHeader+len(e.cmd))
	binary.BigEndian.PutUint64(payload, uint64(e.term))
	payload[8] = e.kind
	index, err := r.log.Append(append(payload, e.cmd...), 1)
	if errors.Is(err, storage.ErrTooLarge) {
		return 0, err
	}
	if err != nil {
		r.fail(fmt.Errorf("append raft entry: %w", err))
		return 0, r.err
	}
	r.terms = append(r.terms, e.term)
	r.dirty = true
	return index, nil
}

// Propose appends cmd to the leader's log and returns its index. The
// command is committed once Committed exceeds that index, provided this
// node has been leader in the same term throughout.
func (r *Raft) Propose(cmd []byte) (int64, error) {
	switch {
	case r.err != nil:
		return 0, r.err
	case r.role != Leader:
		return 0, ErrNotLeader
	}
	index, err := r.append(entry{term: r.hard.term, cmd: cmd})
	if err != nil {
		return 0, err
	}
	r.broadcast(false)
	r.advanceCommit()
	return index, nil
}

// Read returns the command of the entry at index, which must be at most
// LastIndex. noop is set for an entry that carries no command.
func (r *Raft) Read(index int64) (cmd []byte, noop bool, err error) {
	batches, err := r.log.Read(index, 0)
	if err != nil {
		return nil, false, err
	}
	if len(batches) == 0 || batches[0].Base != index || len(batches[0].Payload) < entryHeader {
		return nil, false, fmt.Errorf("%w: no raft entry at index %d", storage.ErrCorrupt, index)
	}
	p := batches[0].Payload
	return p[entryHeader:], p[8] == kindNoop, nil
}

// Sync makes the log durable without anything else happening: for callers
// that sync on a schedule of their own under AckBeforeSync.
func (r *Raft) Sync() {
	if r.err == nil {
		if err := r.log.Sync(); err != nil {
			r.fail(fmt.Errorf("sync raft log: %w", err))
		}
	}
}

// Flush makes appended entries durable (unless AckBeforeSync says not to
// wait for that) and then does what was waiting for it: a follower
// answers its leader, a leader counts its own log towards commit.
func (r *Raft) Flush() {
	if r.err != nil {
		return
	}
	if !r.cfg.AckBeforeSync {
		r.Sync()
	}
	r.dirty = false
	r.sendAck()
	r.advanceCommit()
}

// sendAck answers the leader's AppendEntries if what they confirmed is
// durable now.
func (r *Raft) sendAck() {
	if r.err == nil && r.ackTo != noVote && r.ackIndex <= r.stable() {
		r.send(r.ackTo, message{typ: msgAppendResponse, ok: true, echo: r.ackEcho, index: r.ackIndex})
		r.ackTo = noVote
	}
}

// broadcast sends AppendEntries to every follower that is not waiting for
// a response, or to all of them if force is set.
func (r *Raft) broadcast(force bool) {
	for _, id := range r.cfg.Peers { // the configured order, never the map's
		if p := r.peers[id]; p != nil && (force || !p.inflight) {
			r.replicate(id, p)
		}
	}
}

// replicate sends one AppendEntries to a follower: the entries from its
// next index, as many as fit, or none if it is up to date (a heartbeat).
func (r *Raft) replicate(to sys.NodeID, p *progress) {
	if r.err != nil {
		return
	}
	m := message{typ: msgAppendEntries, prevIndex: p.next - 1, commit: r.commit}
	if m.prevIndex >= 0 {
		m.prevTerm = r.terms[m.prevIndex]
	}
	if p.next <= r.LastIndex() {
		batches, err := r.log.Read(p.next, r.cfg.MaxAppendBytes)
		if err != nil {
			r.fail(fmt.Errorf("read raft log at %d: %w", p.next, err))
			return
		}
		for _, b := range batches {
			m.entries = append(m.entries, entry{term: r.terms[b.Base], kind: b.Payload[8], cmd: b.Payload[entryHeader:]})
		}
	}
	p.inflight = true
	r.send(to, m)
}

// advanceCommit moves the leader's commit index to the highest entry of
// its own term that a majority holds durably.
func (r *Raft) advanceCommit() {
	if r.role != Leader || r.err != nil {
		return
	}
	held := []int64{r.stable()}
	for _, id := range r.cfg.Peers {
		if p := r.peers[id]; p != nil {
			held = append(held, p.match)
		}
	}
	slices.Sort(held)
	n := held[len(held)-1-len(r.cfg.Peers)/2] // the majority-th highest
	// Only an entry of the current term is committed by counting. An
	// older one that is on a majority can still be overwritten by a
	// leader that never saw it (Figure 8); it commits along with the
	// first current-term entry after it.
	if n > r.commit && (r.terms[n] == r.hard.term || r.cfg.UnsafeCommitAnyTerm) {
		r.commit = n
	}
}

// OnTimer handles the expiry of one of the instance's timers.
func (r *Raft) OnTimer(t Timer) {
	switch {
	case r.err != nil:
	case t == TimerElection && r.role != Leader:
		r.campaign()
	case t == TimerHeartbeat && r.role == Leader:
		r.broadcast(true) // also repairs a lost request or response
		r.env.SetTimer(TimerHeartbeat, r.cfg.HeartbeatInterval)
	}
}

// Step handles one message from a peer. Malformed messages and messages
// from outside the cluster are dropped.
func (r *Raft) Step(from sys.NodeID, raw []byte) {
	if r.err != nil || from == r.cfg.ID || !slices.Contains(r.cfg.Peers, from) {
		return
	}
	m, err := decodeMessage(raw, r.cfg.Log.MaxPayloadBytes-entryHeader)
	if err != nil {
		return
	}
	// A higher term always wins: whatever this node was, it is a follower
	// of that term now, and that is on disk before anything else happens.
	if m.term > r.hard.term {
		r.follow(m.term)
		if r.err != nil {
			return
		}
	}
	stale := m.term < r.hard.term
	switch m.typ {
	case msgRequestVote:
		r.onRequestVote(from, m, stale)
	case msgVoteResponse:
		if !stale && r.role == Candidate && m.ok {
			r.votes[from] = true
			r.maybeLead()
		}
	case msgAppendEntries:
		if stale {
			// Tell the sender it has been superseded: only the term in
			// this answer matters. Echo -1 keeps it from being taken for
			// a refusal if the sender has since become leader of our term.
			r.send(from, message{typ: msgAppendResponse, echo: -1, index: -1})
			return
		}
		r.onAppendEntries(from, m)
	case msgAppendResponse:
		if !stale && r.role == Leader {
			r.onAppendResponse(from, m)
		}
	}
}

func (r *Raft) onRequestVote(from sys.NodeID, m message, stale bool) {
	// The candidate's log must be at least as up to date as ours: a later
	// last term wins, and only between equal terms does length decide.
	upToDate := m.lastTerm > r.lastTerm() || (m.lastTerm == r.lastTerm() && m.lastIndex >= r.LastIndex())
	grant := !stale && upToDate && (r.hard.votedFor == noVote || r.hard.votedFor == from)
	if grant {
		// On disk before the answer leaves: a node that forgot its vote
		// could give a second one in the same term.
		r.setHard(r.hard.term, from, !r.cfg.UnsafeVoteWithoutPersist)
		r.armElection()
	}
	r.send(from, message{typ: msgVoteResponse, ok: grant})
}

func (r *Raft) onAppendEntries(from sys.NodeID, m message) {
	if r.role == Leader {
		r.fail(fmt.Errorf("raft: node %d and node %d are both leader in term %d", r.cfg.ID, from, m.term))
		return
	}
	r.follow(m.term) // a candidate yields to the leader of its term
	r.leader = from
	r.armElection()

	// The entry before the new ones must be here with the same term;
	// otherwise the logs differ somewhere before it.
	if m.prevIndex > r.LastIndex() || (m.prevIndex >= 0 && r.terms[m.prevIndex] != m.prevTerm) {
		// shortcut: the leader steps back one entry per round trip when
		// terms differ. Send the conflicting term's first index if
		// catching up a long-diverged follower is ever slow.
		r.send(from, message{typ: msgAppendResponse, echo: m.prevIndex, index: min(r.LastIndex(), m.prevIndex-1)})
		return
	}
	for i, e := range m.entries {
		index := m.prevIndex + 1 + int64(i)
		if index <= r.LastIndex() {
			if r.terms[index] == e.term {
				continue // already here: an old or repeated message changes nothing
			}
			// A real conflict: same index, different term. Everything
			// from here on is from a leader that lost.
			if index <= r.commit {
				r.fail(fmt.Errorf("raft: node %d was asked to replace committed entry %d", r.cfg.ID, index))
				return
			}
			if err := r.log.TruncateSuffix(index); err != nil {
				r.fail(fmt.Errorf("truncate raft log at %d: %w", index, err))
				return
			}
			r.terms = r.terms[:index]
		}
		if _, err := r.append(e); err != nil {
			r.fail(fmt.Errorf("append raft entry %d: %w", index, err))
			return
		}
	}
	// Only up to the last entry this message vouched for: anything beyond
	// it in our log may be a stale suffix.
	confirmed := m.prevIndex + int64(len(m.entries))
	r.commit = max(r.commit, min(m.commit, confirmed))
	if r.ackTo != from || confirmed > r.ackIndex {
		r.ackTo, r.ackIndex, r.ackEcho = from, confirmed, m.prevIndex
	}
	r.sendAck() // at once if nothing new needs syncing; otherwise after Flush
}

func (r *Raft) onAppendResponse(from sys.NodeID, m message) {
	p := r.peers[from]
	switch {
	case m.ok && m.index <= r.LastIndex():
		p.match = max(p.match, m.index) // responses can arrive out of order
		p.next = max(p.next, p.match+1)
		p.inflight = false
		r.advanceCommit()
		if p.next <= r.LastIndex() {
			r.replicate(from, p)
		}
	// A refusal of the request we are waiting on. A real one names an
	// index before the one it refused; anything else is noise.
	case !m.ok && m.echo == p.next-1 && m.echo >= 0 && m.index < m.echo:
		// Not clamped to match+1: a follower that refuses below what it
		// once acknowledged has lost entries (its disk lied, or it runs
		// with AckBeforeSync and lost power). What that already cost
		// cannot be undone here, but sending the entries again restores
		// the copy; insisting on match+1 would refuse it for ever.
		p.next = min(m.index+1, p.next-1)
		r.replicate(from, p)
	}
}
