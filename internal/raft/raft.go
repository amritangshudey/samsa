package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path"
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
	// Now returns the current time; used only for the read lease.
	Now() sys.Instant
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
	// Unsafe switches on deliberate bugs, so that tests can prove they
	// notice each one. Zero in production.
	Unsafe Mutation
}

// Mutation is a set of deliberate bugs; see the Mut constants.
type Mutation uint32

// The deliberate bugs. Each removes one step that safety depends on.
const (
	// MutCommitAnyTerm commits an earlier term's entry by counting
	// replicas (the paper's Figure 8).
	MutCommitAnyTerm Mutation = 1 << iota
	// MutVoteWithoutPersist grants a vote before it is on disk.
	MutVoteWithoutPersist
	// MutVoteByLength grants a vote to any candidate whose log is at
	// least as long, whatever its last term.
	MutVoteByLength
	// MutTruncateOnStale makes a follower drop every entry beyond the
	// ones a message carries, even when there is no conflict.
	MutTruncateOnStale
	// MutSnapshotUnsynced writes a snapshot without first syncing the log
	// up to its index.
	MutSnapshotUnsynced
	// MutTrimPastSnapshot lets Compact remove entries that no snapshot
	// on disk covers yet.
	MutTrimPastSnapshot
	// MutNoPreVote starts a real election, with a new term, whenever the
	// election timer fires.
	MutNoPreVote
	// MutNoCheckQuorum keeps a leader leading after it has lost touch with
	// its majority.
	MutNoCheckQuorum
	// MutTransferKeepsLease keeps the lease, and proposals, during a
	// leadership transfer; the broker also keeps serving.
	MutTransferKeepsLease
)

// ErrTransferring is returned by TransferLeadership when it cannot start.
var ErrTransferring = errors.New("raft: no follower to transfer leadership to")

func (r *Raft) mut(m Mutation) bool { return r.cfg.Unsafe&m != 0 }

// progress is what a leader knows about one follower.
type progress struct {
	next  int64 // index of the next entry to send
	match int64 // highest index known to be durable there; never decreases
	// inflight is set while an AppendEntries awaits its response. It is
	// cleared by the response or, if that never comes, by the heartbeat.
	inflight bool
	// acked is the send time of the latest request this follower has
	// answered: it will not vote for anyone until ElectionTimeout after.
	acked sys.Instant
	// snapIndex and snapOff track a snapshot being sent: which one, and
	// the next byte to send.
	snapIndex, snapOff int64
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
	// preVotes is set while the node asks for PreVotes (for term+1).
	preVotes map[sys.NodeID]bool
	// leaderSince is when this node became leader of its term.
	leaderSince sys.Instant
	// A leadership transfer in progress: the target (noVote if none), when
	// it is abandoned, and whether TimeoutNow has been sent.
	transferee    sys.NodeID
	transferUntil sys.Instant
	timeoutSent   bool

	dirty bool // entries appended since the last Flush
	// A follower answers AppendEntries only once the entries are durable.
	// ackTo is the leader awaiting that answer (noVote if none), ackIndex
	// the highest index its messages confirmed, ackEcho what to echo.
	ackTo                       sys.NodeID
	ackIndex, ackEcho, ackStamp int64
	// contact is when this node last heard from the leader of its term,
	// or started. Until ElectionTimeout after it, it ignores candidates.
	contact sys.Instant

	latest      snapshot // the newest snapshot on disk; index -1 if none
	latestBytes []byte   // its encoding, for sending to followers
	// pending is the snapshot being installed while the log is fetched
	// again from its cut; nil otherwise. inst is one being received.
	pending      *snapshot
	pendingBytes []byte
	inst         *transfer

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
		leader: noVote, commit: -1, ackTo: noVote, transferee: noVote, latest: snapshot{index: -1}}
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
	r.contact = r.env.Now() // it may have answered a leader just before it stopped
	if r.latest, r.latestBytes, err = loadSnapshot(r.disk, r.cfg.Dir); err != nil {
		return err
	}
	marker, err := ReadFile(r.disk, path.Join(r.cfg.Dir, installName))
	if err != nil {
		return err
	}
	from := log.StartOffset()
	if marker != nil {
		cut, s, raw, err := decodeInstall(marker)
		if err != nil {
			return err
		}
		if s.index <= r.latest.index { // promoted just before a crash
			if err := r.disk.Remove(path.Join(r.cfg.Dir, installName)); err != nil {
				return fmt.Errorf("remove install marker: %w", err)
			}
			marker = nil
		} else {
			if err := r.beginInstall(cut, s, raw); err != nil {
				return err
			}
			from = cut
		}
	}
	if s := r.latest; marker == nil && s.index >= 0 {
		if err := r.checkSnapshot(s); err != nil {
			return err
		}
		r.terms, from = s.expand(), s.index+1
		r.commit = s.index // the caller applied it, so it was committed
	} else if marker == nil && log.StartOffset() > 0 {
		return fmt.Errorf("%w: log starts at %d and there is no snapshot", errBadSnapshot, log.StartOffset())
	}
	for off := from; off < log.EndOffset(); {
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
	r.maybePromote()
	if len(r.cfg.Peers) == 1 {
		r.campaign(false)
	} else {
		r.armElection()
	}
	return r.err
}

// checkSnapshot refuses a snapshot that the log cannot back: one that
// reaches past the log's end, or whose last entry has another term.
func (r *Raft) checkSnapshot(s snapshot) error {
	if start := r.log.StartOffset(); start > s.index+1 || s.index >= r.log.EndOffset() {
		return fmt.Errorf("%w: snapshot at %d, log holds %d to %d", errBadSnapshot, s.index, start, r.log.EndOffset())
	} else if s.index < start {
		return nil // the log starts just after it
	}
	batches, err := r.log.Read(s.index, 0)
	if err != nil {
		return fmt.Errorf("read raft log at snapshot %d: %w", s.index, err)
	}
	if len(batches) == 0 || len(batches[0].Payload) < entryHeader {
		return fmt.Errorf("%w: raft log entry %d has no header", storage.ErrCorrupt, s.index)
	}
	if term := int64(binary.BigEndian.Uint64(batches[0].Payload)); term != s.runs[len(s.runs)-1].term {
		return fmt.Errorf("%w: entry %d has term %d, snapshot says %d", errBadSnapshot, s.index, term, s.runs[len(s.runs)-1].term)
	}
	return nil
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

// Vote returns the node this node voted for in the current term, if any.
func (r *Raft) Vote() (id sys.NodeID, voted bool) { return r.hard.votedFor, r.hard.votedFor != noVote }

// Committed returns the index below which every entry is committed.
func (r *Raft) Committed() int64 { return r.commit + 1 }

// Lease reports whether this node leads and may answer reads from its own
// state: a majority, counting itself, has answered a request it sent less
// than three quarters of an election timeout ago. Each of them ignores
// candidates for a full election timeout after such a request, so no
// other leader can exist yet. The quarter is margin for clocks that run at
// slightly different rates; the clocks need not agree on the time.
func (r *Raft) Lease() bool {
	if r.role != Leader || r.err != nil || r.handingOver() {
		return false
	}
	now := r.env.Now()
	times := []sys.Instant{now}
	for _, id := range r.cfg.Peers {
		if p := r.peers[id]; p != nil {
			times = append(times, p.acked)
		}
	}
	slices.Sort(times)
	held := times[len(times)-1-len(r.cfg.Peers)/2] // the majority-th most recent
	return now < held+sys.Instant(r.cfg.ElectionTimeout*3/4)
}

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

func (r *Raft) send(to sys.NodeID, m message) { r.sendAs(to, m, r.hard.term) }

// sendAs sends m carrying term, which only PreVote messages set to
// anything but the node's own.
func (r *Raft) sendAs(to sys.NodeID, m message, term int64) {
	if r.err == nil { // a node that cannot vouch for its state says nothing
		m.term = term
		r.env.Send(to, m.encode())
	}
}

// follow makes the node a follower in term, which is at least its own.
func (r *Raft) follow(term int64) {
	if term > r.hard.term {
		r.setHard(term, noVote, true)
		r.leader, r.ackTo = noVote, noVote // an answer owed to an old leader is void
	}
	r.preVotes = nil
	if r.role != Follower {
		r.role = Follower
		r.votes, r.peers, r.transferee = nil, nil, noVote
		r.env.CancelTimer(TimerHeartbeat)
		r.armElection()
	}
}

// preCampaign asks every peer whether it would vote for this node in the
// next term, without starting that term (thesis 9.6).
func (r *Raft) preCampaign() {
	r.preVotes = map[sys.NodeID]bool{r.cfg.ID: true}
	r.armElection()
	for _, p := range r.cfg.Peers {
		if p != r.cfg.ID {
			r.sendAs(p, message{typ: msgPreVote, lastIndex: r.LastIndex(), lastTerm: r.lastTerm()}, r.hard.term+1)
		}
	}
}

// heardFromLeader reports whether a leader was heard within an election
// timeout. Such a node refuses PreVotes and ignores RequestVotes: one check
// for both, or a node could pass PreVote and then lose every election.
func (r *Raft) heardFromLeader() bool {
	return r.role == Leader || r.env.Now() < r.contact+sys.Instant(r.cfg.ElectionTimeout)
}

// upToDate reports whether a candidate's last entry is at least as up to
// date as ours: a later last term wins, and only between equal terms does
// length decide.
func (r *Raft) upToDate(m message) bool {
	if r.mut(MutVoteByLength) {
		return m.lastIndex >= r.LastIndex()
	}
	return m.lastTerm > r.lastTerm() || (m.lastTerm == r.lastTerm() && m.lastIndex >= r.LastIndex())
}

// Transferring reports whether this leader is handing leadership over; it
// then holds no lease and takes no proposals.
func (r *Raft) Transferring() bool { return r.role == Leader && r.transferee != noVote }

// handingOver is Transferring, unless the mutation keeps serving.
func (r *Raft) handingOver() bool { return r.Transferring() && !r.mut(MutTransferKeepsLease) }

// TransferLeadership starts handing leadership to the follower to, or with
// to = 0 to the most up-to-date one. The lease is void from now on. The
// caller learns of the outcome through Role: if this node still leads an
// election timeout later, the transfer was abandoned and it may serve again.
func (r *Raft) TransferLeadership(to sys.NodeID) error {
	switch {
	case r.err != nil:
		return r.err
	case r.role != Leader:
		return ErrNotLeader
	}
	if to == 0 {
		best := int64(-2)
		for _, id := range r.cfg.Peers { // configured order: ties go to the first
			if p := r.peers[id]; p != nil && p.match > best {
				to, best = id, p.match
			}
		}
	}
	if r.peers[to] == nil {
		return ErrTransferring
	}
	r.transferee, r.transferUntil, r.timeoutSent = to, r.env.Now()+sys.Instant(r.cfg.ElectionTimeout), false
	r.maybeTimeoutNow()
	return nil
}

// maybeTimeoutNow sends TimeoutNow once the target holds the whole log.
func (r *Raft) maybeTimeoutNow() {
	if r.role != Leader || r.transferee == noVote || r.timeoutSent {
		return
	}
	if p := r.peers[r.transferee]; p.match >= r.LastIndex() {
		r.timeoutSent = true
		r.send(r.transferee, message{typ: msgTimeoutNow})
	} else if !p.inflight {
		r.replicate(r.transferee, p)
	}
}

// quorumActive reports whether a majority, this node included, answered
// within an election timeout; a follower's clock starts at the election.
func (r *Raft) quorumActive() bool {
	now, n := r.env.Now(), 1
	for _, id := range r.cfg.Peers {
		if p := r.peers[id]; p != nil && now < max(p.acked, r.leaderSince)+sys.Instant(r.cfg.ElectionTimeout) {
			n++
		}
	}
	return n > len(r.cfg.Peers)/2
}

// campaign starts an election in a new term; transfer marks one that a
// leader asked for with TimeoutNow.
func (r *Raft) campaign(transfer bool) {
	r.setHard(r.hard.term+1, r.cfg.ID, true)
	r.role, r.leader, r.peers, r.ackTo, r.preVotes = Candidate, noVote, nil, noVote, nil
	r.votes = map[sys.NodeID]bool{r.cfg.ID: true}
	r.armElection()
	typ := msgRequestVote
	if transfer {
		typ = msgTransferVote
	}
	for _, p := range r.cfg.Peers {
		if p != r.cfg.ID {
			r.send(p, message{typ: typ, lastIndex: r.LastIndex(), lastTerm: r.lastTerm()})
		}
	}
	r.maybeLead()
}

func (r *Raft) maybeLead() {
	if r.role != Candidate || len(r.votes) <= len(r.cfg.Peers)/2 || r.err != nil {
		return
	}
	r.role, r.leader, r.votes = Leader, r.cfg.ID, nil
	r.leaderSince, r.transferee = r.env.Now(), noVote
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
	case r.role != Leader, r.handingOver():
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
	r.maybePromote()
	r.dirty = false
	r.sendAck()
	r.advanceCommit()
}

// sendAck answers the leader's AppendEntries if what they confirmed is
// durable now.
func (r *Raft) sendAck() {
	if r.err == nil && r.ackTo != noVote && r.ackIndex <= r.stable() {
		r.send(r.ackTo, message{typ: msgAppendResponse, ok: true, echo: r.ackEcho, index: r.ackIndex, stamp: r.ackStamp})
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
	if p.next < r.log.StartOffset() {
		r.sendSnapshot(to, p)
		return
	}
	m := message{typ: msgAppendEntries, prevIndex: p.next - 1, commit: r.commit, stamp: int64(r.env.Now())}
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
	if n > r.commit && (r.terms[n] == r.hard.term || r.mut(MutCommitAnyTerm)) {
		r.commit = n
	}
}

// OnTimer handles the expiry of one of the instance's timers.
func (r *Raft) OnTimer(t Timer) {
	switch {
	case r.err != nil:
	case t == TimerElection && r.role != Leader:
		if len(r.cfg.Peers) == 1 || r.mut(MutNoPreVote) {
			r.campaign(false)
		} else {
			r.preCampaign()
		}
	case t == TimerHeartbeat && r.role == Leader:
		if !r.mut(MutNoCheckQuorum) && !r.quorumActive() {
			// Lost touch with the majority (thesis 6.2): stop leading, so
			// that the nodes it still reaches stop ignoring candidates.
			r.follow(r.hard.term)
			r.leader = noVote
			return
		}
		if r.transferee != noVote && r.env.Now() >= r.transferUntil {
			r.transferee = noVote // abandoned: serve again
		}
		r.broadcast(true) // also repairs a lost request or response
		r.maybeTimeoutNow()
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
	// PreVotes change no term, theirs or ours.
	switch m.typ {
	case msgPreVote:
		if !r.heardFromLeader() {
			grant := m.term > r.hard.term && r.upToDate(m)
			term := r.hard.term // a refusal tells the asker our term
			if grant {
				term = m.term
			}
			r.sendAs(from, message{typ: msgPreVoteResponse, ok: grant}, term)
		}
		return
	case msgPreVoteResponse:
		switch {
		case r.preVotes == nil || r.role == Leader:
		case m.ok && m.term == r.hard.term+1:
			r.preVotes[from] = true
			if len(r.preVotes) > len(r.cfg.Peers)/2 {
				r.campaign(false)
			}
		case !m.ok && m.term > r.hard.term:
			r.follow(m.term) // a later term exists: join it
		}
		return
	}
	// A node that has a live leader ignores candidates altogether, even
	// their term: that is what makes the leader's read lease sound, and it
	// keeps a node returning from a partition from deposing a working
	// leader (the Raft thesis, section 4.2.3). A transfer vote is the
	// exception: the only leader it could depose asked for it, and gave
	// up its lease first (docs/design/b3-raft-hardening.md, 2.3).
	if m.typ == msgRequestVote && r.heardFromLeader() {
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
	case msgRequestVote, msgTransferVote:
		r.onRequestVote(from, m, stale)
	case msgTimeoutNow:
		if !stale && r.role == Follower && from == r.leader {
			r.campaign(true)
		}
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
	case msgInstallSnapshot:
		if stale {
			r.send(from, message{typ: msgAppendResponse, echo: -1, index: -1})
			return
		}
		r.onInstallSnapshot(from, m)
	case msgInstallResponse:
		if !stale && r.role == Leader {
			r.onInstallResponse(from, m)
		}
	}
}

func (r *Raft) onRequestVote(from sys.NodeID, m message, stale bool) {
	grant := !stale && r.upToDate(m) && (r.hard.votedFor == noVote || r.hard.votedFor == from)
	if grant {
		// On disk before the answer leaves: a node that forgot its vote
		// could give a second one in the same term.
		r.setHard(r.hard.term, from, !r.mut(MutVoteWithoutPersist))
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
	r.contact = r.env.Now()
	r.armElection()

	// The entry before the new ones must be here with the same term;
	// otherwise the logs differ somewhere before it.
	if m.prevIndex > r.LastIndex() || (m.prevIndex >= 0 && r.terms[m.prevIndex] != m.prevTerm) {
		// shortcut: the leader steps back one entry per round trip when
		// terms differ. Send the conflicting term's first index if
		// catching up a long-diverged follower is ever slow.
		r.send(from, message{typ: msgAppendResponse, echo: m.prevIndex, index: min(r.LastIndex(), m.prevIndex-1), stamp: m.stamp})
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
	confirmed := m.prevIndex + int64(len(m.entries))
	if r.mut(MutTruncateOnStale) && confirmed < r.LastIndex() && confirmed+1 > r.commit {
		if err := r.log.TruncateSuffix(confirmed + 1); err != nil {
			r.fail(fmt.Errorf("truncate raft log at %d: %w", confirmed+1, err))
			return
		}
		r.terms = r.terms[:confirmed+1]
	}
	// Only up to the last entry this message vouched for: anything beyond
	// it in our log may be a stale suffix.
	r.commit = max(r.commit, min(m.commit, confirmed))
	if r.ackTo != from || confirmed > r.ackIndex {
		r.ackTo, r.ackIndex, r.ackEcho = from, confirmed, m.prevIndex
	}
	r.ackStamp = max(r.ackStamp, m.stamp) // only the newest contact matters to the lease
	r.maybePromote()                      // under AckBeforeSync, appended is enough
	r.sendAck()                           // at once if nothing new needs syncing; otherwise after Flush
}

func (r *Raft) onAppendResponse(from sys.NodeID, m message) {
	p := r.peers[from]
	// Refusals count too: the follower heard us. A stamp from the future
	// can only come from a broken peer; it buys nothing.
	p.acked = max(p.acked, min(sys.Instant(m.stamp), r.env.Now()))
	switch {
	case m.ok && m.index <= r.LastIndex():
		p.match = max(p.match, m.index) // responses can arrive out of order
		p.next = max(p.next, p.match+1)
		p.inflight = false
		r.advanceCommit()
		if p.next <= r.LastIndex() {
			r.replicate(from, p)
		}
		if from == r.transferee {
			r.maybeTimeoutNow()
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
