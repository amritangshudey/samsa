package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/amritangshudey/samsa/internal/raft"
	"github.com/amritangshudey/samsa/internal/storage"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// SyncPolicy says when a command counts as committed, and so when a client
// may be told its write succeeded.
type SyncPolicy uint8

const (
	// SyncAlways commits a command once it is synced to disk. Requests that
	// arrive together share one sync. An acknowledged write survives power
	// loss.
	SyncAlways SyncPolicy = iota
	// SyncInterval commits a command as soon as it is appended and syncs on
	// a timer. Up to one interval of acknowledged writes can be lost.
	SyncInterval
	// SyncNever commits on append and syncs only when a segment is sealed.
	SyncNever
)

// Peer is one node of the cluster as clients see it.
type Peer struct {
	ID   int32
	Host string // where Kafka clients connect
	Port int32
}

// Config configures a Node. Zero values select the defaults named below.
type Config struct {
	// NodeID, Host and Port are what Metadata responses tell clients.
	NodeID int32
	Host   string
	Port   int32
	// Peers lists every node of the cluster, this one included. The list
	// is the same on every node and never changes. Default: this node
	// alone.
	Peers []Peer
	// ElectionTimeout, HeartbeatInterval and MaxAppendBytes tune Raft;
	// see raft.Config.
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	MaxAppendBytes    int
	// ClusterID is reported in Metadata. Default "samsa".
	ClusterID string
	// Dir is the directory of the Raft log on the node's disk. Default "raft".
	Dir string
	// Sync is the fsync policy; SyncInterval is its period (default 1s).
	Sync         SyncPolicy
	SyncInterval time.Duration
	// SegmentBytes is the command log's segment size (storage default if 0).
	SegmentBytes int64
	// SnapshotEvery is how many applied entries pass between snapshots of
	// the state machine (docs/design/a1-snapshots.md). Default 10 000;
	// negative disables them.
	SnapshotEvery int64
	// RetentionBytes bounds the log: past it, the leader proposes to drop
	// the oldest segments (docs/design/a1-snapshots.md, section 3). Zero
	// keeps everything.
	RetentionBytes int64
	// ProducerExpiry is how long an idempotent producer may write nothing
	// before every partition forgets it (docs/design/a2-idempotent-producer.md,
	// section 7). Default 24 h; negative never expires.
	ProducerExpiry time.Duration
	// UnsafeExpireEarly expires producers as soon as the leader samples
	// the log, without waiting for ProducerExpiry.
	UnsafeExpireEarly bool
	// MaxRequestBytes bounds one request frame. Default 32 MiB.
	MaxRequestBytes int
	// MaxBatchBytes bounds one record batch. Default 1 MiB.
	MaxBatchBytes int
	// MaxFetchBytes bounds the record bytes of one fetch response, whatever
	// the client asks for. Default 8 MiB.
	MaxFetchBytes int
	// MaxPartitions bounds the partitions of one topic. Default 1024.
	MaxPartitions int32
	// MinSessionTimeout and MaxSessionTimeout bound the session timeout a
	// group member may ask for. Defaults 6s and 30m, as in Kafka.
	MinSessionTimeout time.Duration
	MaxSessionTimeout time.Duration
	// MaxGroupSize bounds the members of one consumer group. Default 1000.
	MaxGroupSize int
	// Logger receives structured logs. Default: discard.
	Logger *slog.Logger

	// UnsafeAckBeforeSync breaks the commit rule on purpose: under
	// SyncAlways, commands count as durable here when appended, not when
	// synced. It exists so that the simulator can prove it notices.
	UnsafeAckBeforeSync bool
	// UnsafeRaft switches on Raft's deliberate bugs; see raft.Mutation.
	UnsafeRaft raft.Mutation
	// UnsafeServeEarly serves clients as soon as the node is elected,
	// before it has applied an entry of its term. UnsafeNoLease answers
	// reads without checking the leader's lease.
	UnsafeServeEarly, UnsafeNoLease bool
	// UnsafeTrimEarly deletes log segments as soon as a Trim is applied,
	// before a snapshot covers it.
	UnsafeTrimEarly bool
	// UnsafeNoSequenceCheck appends every batch of an idempotent producer,
	// retries and gaps included.
	UnsafeNoSequenceCheck bool
	// UnsafeNoFencing breaks zombie fencing on purpose: an offset commit
	// from a member is accepted whatever generation it carries.
	UnsafeNoFencing bool
}

func (c Config) withDefaults() Config {
	if c.ClusterID == "" {
		c.ClusterID = "samsa"
	}
	if c.Dir == "" {
		c.Dir = "raft"
	}
	if len(c.Peers) == 0 {
		c.Peers = []Peer{{ID: c.NodeID, Host: c.Host, Port: c.Port}}
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = time.Second
	}
	if c.ProducerExpiry == 0 {
		c.ProducerExpiry = 24 * time.Hour
	}
	if c.SnapshotEvery == 0 {
		c.SnapshotEvery = 10_000
	}
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = 32 << 20
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 1 << 20
	}
	if c.MaxFetchBytes <= 0 {
		c.MaxFetchBytes = 8 << 20
	}
	if c.MaxPartitions <= 0 {
		c.MaxPartitions = 1024
	}
	if c.MinSessionTimeout <= 0 {
		c.MinSessionTimeout = 6 * time.Second
	}
	if c.MaxSessionTimeout <= 0 {
		c.MaxSessionTimeout = 30 * time.Minute
	}
	if c.MaxGroupSize <= 0 {
		c.MaxGroupSize = 1000
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

// commandOverhead is room for an Append command's fields around its batch:
// type, topic name (at most 249 bytes plus length) and partition.
const commandOverhead = 512

// Timer ids. Delayed fetches and the group coordinator take ids from
// firstFreeTimer upwards.
const (
	timerFlush     sys.TimerID = 1
	timerSync      sys.TimerID = 2
	timerElection  sys.TimerID = 3
	timerHeartbeat sys.TimerID = 4

	firstFreeTimer sys.TimerID = 16
)

// conn is the node's view of one client connection.
type conn struct {
	id sys.ConnID
	in []byte // received bytes not yet consumed
	// busy is set while a request from this connection is in progress.
	// Later frames wait in the buffer, which keeps responses in request
	// order and a producer's batches in send order.
	busy    bool
	pumping bool // pump is on the stack; finish must not re-enter it
	closed  bool
	// onClose, if set, withdraws the request the group coordinator is
	// holding for this connection. A connection has one request in
	// progress at a time, so one slot is enough.
	onClose func()
}

// waiter is a proposed command that is not yet applied.
type waiter struct {
	index int64
	cmd   []byte
	done  func(result)
}

// Node is a single broker. It implements sys.Handler and must be driven by
// one caller at a time.
type Node struct {
	cfg   Config
	clock sys.Clock
	net   sys.Network
	disk  sys.Disk
	rand  sys.Random
	log   *slog.Logger

	raft    *raft.Raft
	pids    producerIDs
	st      *state
	applied int64 // every log entry below this index has been applied
	// serving is set while this node leads and has applied an entry of
	// its own term: only then does it know that its state holds
	// everything clients were ever told. servingTerm is that term.
	serving     bool
	servingTerm int64
	// trimmedAt is the index of the last Trim applied (-1: none since the
	// last restore); trimProposed the highest cut this node proposed.
	trimmedAt, trimProposed int64
	// expirySamples are (time, applied index) pairs the serving leader
	// wrote down, oldest first: soft state, so a new leader starts empty
	// and expiry is only ever late.
	expirySamples []expirySample

	waiters    []waiter // ascending by index; non-empty only while serving
	flushArmed bool
	conns      map[sys.ConnID]*conn
	parked     []*parkedFetch // in arrival order
	nextTimer  sys.TimerID
	timers     map[sys.TimerID]func() // armed by after; see groups.go
	groups     map[string]*softGroup

	// fatal is the first error after which the node cannot be trusted: a
	// failed write or sync, or an undecodable command. Every handler
	// returns it, and the driver stops the node.
	fatal error
}

// New returns a node that will use the given clock, network, disk and
// random source. It does nothing until the driver calls OnStart.
func New(cfg Config, clock sys.Clock, net sys.Network, disk sys.Disk, rand sys.Random) *Node {
	cfg = cfg.withDefaults()
	n := &Node{
		cfg: cfg, clock: clock, net: net, disk: disk, rand: rand, log: cfg.Logger,
		trimmedAt: -1, conns: map[sys.ConnID]*conn{}, nextTimer: firstFreeTimer,
		timers: map[sys.TimerID]func(){}, groups: map[string]*softGroup{},
	}
	n.adopt(newState())
	return n
}

// adopt makes st the node's state. The deliberate bugs are switches of the
// configuration, not state: they are set again on whatever state arrives.
func (n *Node) adopt(st *state) {
	st.noFencing, st.noSequences = n.cfg.UnsafeNoFencing, n.cfg.UnsafeNoSequenceCheck
	n.st = st
}

// raftEnv is the node as Raft sees it.
type raftEnv struct{ n *Node }

func (e raftEnv) Send(to sys.NodeID, msg []byte) { e.n.net.SendPeer(to, msg) }
func (e raftEnv) Rand() uint64                   { return e.n.rand.Uint64() }
func (e raftEnv) Now() sys.Instant               { return e.n.clock.Now() }
func (e raftEnv) SetTimer(t raft.Timer, d time.Duration) {
	e.n.clock.SetTimer(raftTimer(t), d)
}
func (e raftEnv) CancelTimer(t raft.Timer) { e.n.clock.CancelTimer(raftTimer(t)) }

func raftTimer(t raft.Timer) sys.TimerID {
	if t == raft.TimerElection {
		return timerElection
	}
	return timerHeartbeat
}

// ackBeforeSync reports whether an entry counts as durable on this node
// before it is synced: the meaning of the weaker fsync policies.
func (n *Node) ackBeforeSync() bool {
	return n.cfg.Sync != SyncAlways || n.cfg.UnsafeAckBeforeSync
}

// OnStart opens the Raft log, recovering it if needed. The state machine
// starts empty and is rebuilt as Raft reports entries committed: a node
// alone in its cluster learns that before OnStart returns, any other node
// once it hears from, or becomes, a leader.
func (n *Node) OnStart() error {
	if n.cfg.NodeID < 0 || n.cfg.NodeID > maxNodeID {
		return errNodeIDForProducers
	}
	peers := make([]sys.NodeID, len(n.cfg.Peers))
	for i, p := range n.cfg.Peers {
		peers[i] = sys.NodeID(p.ID)
	}
	n.raft = raft.New(raft.Config{
		ID: sys.NodeID(n.cfg.NodeID), Peers: peers, Dir: n.cfg.Dir,
		Log: storage.Config{
			SegmentBytes:    n.cfg.SegmentBytes,
			MaxPayloadBytes: n.cfg.MaxBatchBytes + commandOverhead,
		},
		ElectionTimeout: n.cfg.ElectionTimeout, HeartbeatInterval: n.cfg.HeartbeatInterval, MaxAppendBytes: n.cfg.MaxAppendBytes,
		AckBeforeSync: n.ackBeforeSync(),
		Unsafe:        n.cfg.UnsafeRaft | n.trimMutation(),
	}, raftEnv{n}, n.disk)
	if err := n.raft.Start(); err != nil {
		return err
	}
	// Before any request: an id from a block that is not on disk could be
	// handed out again by the next life of this node.
	n.pids = producerIDs{disk: n.disk, dir: n.cfg.Dir, node: int64(n.cfg.NodeID)}
	if err := n.pids.reserve(); err != nil {
		return err
	}
	if i, b := n.raft.Snapshot(); i >= 0 {
		st, err := decodeState(b)
		if err != nil {
			return fmt.Errorf("snapshot at %d: %w", i, err)
		}
		n.adopt(st)
		n.applied = i + 1
	}
	// A single node has elected itself; one sync makes its whole log
	// committed, so it serves from the first request on.
	if len(peers) == 1 {
		n.raft.Flush()
	}
	if n.cfg.Sync == SyncInterval {
		n.clock.SetTimer(timerSync, n.cfg.SyncInterval)
	}
	err := n.settle()
	n.log.Info("started", "entries", n.raft.LastIndex()+1, "applied", n.applied, "topics", len(n.st.names), "groups", len(n.st.groups))
	return err
}

// OnPeerMessage implements sys.Handler.
func (n *Node) OnPeerMessage(from sys.NodeID, msg []byte) error {
	n.raft.Step(from, msg)
	return n.settle()
}

// OnConnOpen implements sys.Handler.
func (n *Node) OnConnOpen(id sys.ConnID) error {
	n.conns[id] = &conn{id: id}
	return n.settle()
}

// OnConnData implements sys.Handler.
func (n *Node) OnConnData(id sys.ConnID, p []byte) error {
	c := n.conns[id]
	if c == nil || c.closed {
		return n.settle()
	}
	c.in = append(c.in, p...) // p is only valid during this call
	if len(c.in) > 2*n.cfg.MaxRequestBytes {
		n.drop(c, "input buffer limit exceeded")
		return n.settle()
	}
	n.pump(c)
	return n.settle()
}

// OnConnClose implements sys.Handler.
func (n *Node) OnConnClose(id sys.ConnID) error {
	if c := n.conns[id]; c != nil {
		c.closed = true // proposals in flight still complete, but send nothing
		delete(n.conns, id)
		if c.onClose != nil {
			c.onClose()
			c.onClose = nil
		}
		kept := n.parked[:0]
		for _, f := range n.parked {
			if f.c == c {
				n.clock.CancelTimer(f.timer)
			} else {
				kept = append(kept, f)
			}
		}
		clear(n.parked[len(kept):])
		n.parked = kept
	}
	return n.settle()
}

// OnTimer implements sys.Handler.
func (n *Node) OnTimer(id sys.TimerID) error {
	switch id {
	case timerFlush:
		n.flushArmed = false
		n.raft.Flush()
	case timerSync:
		n.raft.Sync()
		n.clock.SetTimer(timerSync, n.cfg.SyncInterval)
	case timerElection:
		n.raft.OnTimer(raft.TimerElection)
	case timerHeartbeat:
		n.raft.OnTimer(raft.TimerHeartbeat)
	default:
		if fn := n.timers[id]; fn != nil {
			delete(n.timers, id)
			fn()
			break
		}
		for _, f := range n.parked {
			if f.timer == id {
				n.completeFetch(f)
				break
			}
		}
	}
	return n.settle()
}

// settle ends every handler: it notices a change of leadership, applies
// whatever has become committed, which may complete requests, which may
// start further requests, until nothing moves. It returns the node's
// fatal error, if any.
func (n *Node) settle() error {
	for n.fatal == nil {
		if n.fatal = n.raft.Err(); n.fatal != nil {
			break
		}
		applied, serving := n.applied, n.serving
		n.lead()
		n.advance()
		if n.raft.NeedsFlush() {
			if n.ackBeforeSync() {
				n.raft.Flush() // nothing to wait for
				continue
			}
			// A zero-delay timer fires after the events already waiting,
			// so everything that is ready now shares one sync: group commit.
			if !n.flushArmed {
				n.flushArmed = true
				n.clock.SetTimer(timerFlush, 0)
			}
		}
		if n.applied == applied && n.serving == serving {
			break
		}
	}
	return n.fatal
}

// lead starts or stops serving clients as leadership comes and goes.
func (n *Node) lead() {
	leader, term := n.raft.Role() == raft.Leader, n.raft.Term()
	if n.serving && (!leader || term != n.servingTerm) {
		n.serving = false
		n.stopServing()
	}
	// The leader's first entry of its term is a no-op; once that is
	// applied, so is everything before it.
	if !n.serving && leader && (n.cfg.UnsafeServeEarly || n.applied > 0 && n.raft.EntryTerm(n.applied-1) == term) {
		n.serving, n.servingTerm = true, term
		n.trimProposed = n.st.logStart // a Trim proposed in an earlier term may have been lost
		n.expirySamples = nil
		n.startGroups()
		n.log.Info("serving as leader", "term", term, "applied", n.applied)
	}
}

// stopServing drops everything a leader holds for clients (roadmap rule
// R5): commands awaiting commit, parked fetches, group rounds and timers.
// The connections with a request in progress are closed, which every
// client treats as "ask again"; the next answer names the new leader.
//
// shortcut: closes the connection instead of answering each held request
// with its not-leader error; answer in place if reconnect storms on a
// leader change ever matter.
func (n *Node) stopServing() {
	n.log.Info("no longer serving", "term", n.servingTerm, "waiters", len(n.waiters))
	n.waiters = nil // whether these commit is unknown; nobody is told they did
	for _, f := range n.parked {
		n.clock.CancelTimer(f.timer)
	}
	n.parked = nil
	for _, id := range sortedKeys(n.timers) {
		n.cancel(id)
	}
	n.groups = map[string]*softGroup{}
	for _, id := range sortedKeys(n.conns) { // in a fixed order, for the simulator
		if c := n.conns[id]; c.busy {
			c.onClose = nil
			n.drop(c, "leadership lost while a request was in progress")
		}
	}
}

// propose appends cmd to the log and arranges for done to be called with
// the result once the command has been committed and applied. A success
// response may only be sent from done (roadmap rule R4). It is called only
// while serving; if leadership is lost first, done is never called and the
// connection is closed instead.
func (n *Node) propose(cmd []byte, done func(result)) {
	if n.fatal != nil {
		return
	}
	index, err := n.raft.Propose(cmd)
	if errors.Is(err, storage.ErrTooLarge) {
		done(result{errCode: wire.ErrMessageTooLarge, baseOffset: -1})
		return
	}
	if err != nil {
		n.fatal = fmt.Errorf("propose command: %w", err)
		return
	}
	n.waiters = append(n.waiters, waiter{index: index, cmd: cmd, done: done})
}

// advance applies committed entries in order and completes their waiters.
// trimMutation is the Raft switch behind UnsafeTrimEarly.
func (n *Node) trimMutation() raft.Mutation {
	if n.cfg.UnsafeTrimEarly {
		return raft.MutTrimPastSnapshot
	}
	return 0
}

func (n *Node) advance() {
	if n.raft.Installing() {
		return // the log restarts after a snapshot this node does not yet hold
	}
	if i, b := n.raft.Snapshot(); i >= n.applied { // installed from the leader
		st, err := decodeState(b)
		if err != nil {
			n.fatal = fmt.Errorf("installed snapshot at %d: %w", i, err)
			return
		}
		n.adopt(st)
		n.applied, n.trimmedAt = i+1, -1
		n.log.Info("installed snapshot", "index", i)
	}
	appended := false
	for n.fatal == nil && n.applied < n.raft.Committed() {
		var w waiter
		if len(n.waiters) > 0 && n.waiters[0].index <= n.applied {
			w = n.waiters[0]
			n.waiters[0] = waiter{}
			n.waiters = n.waiters[1:]
			if w.index != n.applied {
				n.fatal = fmt.Errorf("broker: waiter for command %d was passed over at %d", w.index, n.applied)
				return
			}
		}
		cmd, noop := w.cmd, false
		if w.done == nil {
			var err error
			if cmd, noop, err = n.raft.Read(n.applied); err != nil {
				n.fatal = fmt.Errorf("read committed entry %d: %w", n.applied, err)
				return
			}
		}
		if noop {
			n.applied++
			continue
		}
		expired := n.st.expired
		res, err := n.st.apply(n.applied, cmd)
		if err != nil {
			n.fatal = err
			return
		}
		if n.st.expired > expired {
			n.log.Info("expired idempotent producers", "count", n.st.expired-expired, "index", n.applied)
		}
		if cmd[0] == cmdTrim {
			n.trimmedAt = n.applied
		}
		n.applied++
		appended = appended || (cmd[0] == cmdAppend && res.errCode == wire.ErrNone)
		if w.done != nil {
			w.done(res)
		}
	}
	if len(n.waiters) == 0 {
		n.waiters = nil // release the backing array
	}
	n.raft.Compact(n.st.logStart) // effective only below a snapshot, unless UnsafeTrimEarly
	// A Trim frees disk only once a snapshot after it exists: take one now.
	trimPending := n.trimmedAt > n.raft.SnapshotIndex() && !n.cfg.UnsafeTrimEarly
	if every := n.cfg.SnapshotEvery; n.fatal == nil && (trimPending || every > 0 && n.applied-1-n.raft.SnapshotIndex() >= every) {
		n.raft.SaveSnapshot(n.applied-1, n.st.encode())
		n.raft.Compact(n.st.logStart)
	}
	if n.serving && n.cfg.RetentionBytes > 0 && n.fatal == nil {
		if cut := n.raft.RetentionCut(n.cfg.RetentionBytes); cut > n.st.logStart && cut > n.trimProposed {
			n.trimProposed = cut
			n.propose(encodeTrim(cut), func(result) {})
		}
	}
	if n.serving && n.fatal == nil {
		n.expireProducers()
	}
	if appended {
		n.wakeFetches()
	}
}

// ExpiredProducers returns how many producers this node has forgotten
// since it started, for the simulator's coverage counts.
func (n *Node) ExpiredProducers() int { return n.st.expired }

// expirySample is the leader's note that every entry below index had been
// applied at time at.
type expirySample struct {
	at    sys.Instant
	index int64
}

// expireProducers samples the applied index every eighth of the expiry
// time and, once a sample is that old, proposes to forget the producers
// that wrote nothing since it (docs/design/a2-idempotent-producer.md,
// section 7.1).
func (n *Node) expireProducers() {
	every := n.cfg.ProducerExpiry
	if every < 0 {
		return
	}
	now := n.clock.Now()
	if k := len(n.expirySamples); k == 0 || now-n.expirySamples[k-1].at >= sys.Instant(every/8) {
		s := expirySample{now, n.applied}
		if n.cfg.UnsafeExpireEarly { // a fresh sample, used at once
			n.expirySamples = []expirySample{s}
			n.proposeExpiry(s.index)
			return
		}
		n.expirySamples = append(n.expirySamples, s)
	}
	if s := n.expirySamples[0]; now-s.at >= sys.Instant(every) && !n.cfg.UnsafeExpireEarly {
		n.expirySamples = n.expirySamples[1:]
		n.proposeExpiry(s.index)
	}
}

func (n *Node) proposeExpiry(before int64) {
	if before > 0 {
		n.propose(encodeExpireProducers(before), func(result) {})
	}
}

// pump handles complete request frames from c's buffer, one at a time,
// stopping while a request is in progress.
func (n *Node) pump(c *conn) {
	c.pumping = true
	for !c.busy && !c.closed && len(c.in) >= 4 {
		size := int(int32(binary.BigEndian.Uint32(c.in)))
		// The declared size is checked before waiting for, or buffering,
		// that many bytes.
		if size < 0 || size > n.cfg.MaxRequestBytes {
			n.drop(c, "request frame size out of range")
			break
		}
		if len(c.in) < 4+size {
			break
		}
		frame := c.in[4 : 4+size]
		c.in = c.in[4+size:]
		c.busy = true
		n.dispatch(c, frame)
	}
	if len(c.in) == 0 {
		c.in = nil
	}
	c.pumping = false
}

// finish marks c's current request as done and resumes its buffered frames.
func (n *Node) finish(c *conn) {
	c.busy = false
	c.onClose = nil
	if !c.pumping {
		n.pump(c)
	}
}

// drop closes a connection that sent something unacceptable.
func (n *Node) drop(c *conn, reason string) {
	if !c.closed {
		c.closed = true
		n.log.Warn("closing connection", "conn", uint64(c.id), "reason", reason)
		n.net.Close(c.id) // the driver answers with OnConnClose
	}
}

// respond sends the response to the request with header h and finishes it.
func (n *Node) respond(c *conn, h wire.RequestHeader, body wire.Message) {
	// A node that has hit a fatal error says nothing more: whatever it
	// would send was computed from state it can no longer vouch for.
	if !c.closed && n.fatal == nil {
		w := wire.Writer{B: make([]byte, 4, 256)}
		wire.EncodeResponseHeader(&w, h.Key, h.Version, h.CorrelationID)
		body.Encode(&w, h.Version)
		binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
		n.net.Send(c.id, w.B)
	}
	n.finish(c)
}

// dispatch decodes one request and hands it to its handler. frame aliases
// the connection buffer: handlers copy whatever they keep.
func (n *Node) dispatch(c *conn, frame []byte) {
	r := wire.NewReader(frame)
	var h wire.RequestHeader
	h.Decode(r)
	if r.Err() != nil {
		n.drop(c, "malformed request header")
		return
	}
	n.log.Debug("request", "client", h.ClientID, "key", h.Key, "version", h.Version)

	if !wire.Supported(h.Key, h.Version) {
		if h.Key != wire.KeyAPIVersions {
			n.drop(c, fmt.Sprintf("unsupported request: key %d version %d", h.Key, h.Version))
			return
		}
		// The client is probing with a version we lack. Answer in version
		// 0, which every client can read, with the ranges we do support.
		h.Version = 0
		n.respond(c, h, &wire.APIVersionsResponse{ErrorCode: wire.ErrUnsupportedVersion, APIs: advertised()})
		return
	}

	var req wire.Message
	var handle func()
	switch h.Key {
	case wire.KeyAPIVersions:
		m := &wire.APIVersionsRequest{}
		req, handle = m, func() {
			n.log.Debug("client software", "client", h.ClientID, "name", m.ClientSoftwareName, "version", m.ClientSoftwareVersion)
			n.respond(c, h, &wire.APIVersionsResponse{APIs: advertised()})
		}
	case wire.KeyMetadata:
		m := &wire.MetadataRequest{}
		req, handle = m, func() { n.respond(c, h, n.metadata(m)) }
	case wire.KeyCreateTopics:
		m := &wire.CreateTopicsRequest{}
		req, handle = m, func() { n.createTopics(c, h, m) }
	case wire.KeyProduce:
		m := &wire.ProduceRequest{}
		req, handle = m, func() { n.produce(c, h, m) }
	case wire.KeyFetch:
		m := &wire.FetchRequest{}
		req, handle = m, func() { n.fetch(c, h, m) }
	case wire.KeyListOffsets:
		m := &wire.ListOffsetsRequest{}
		req, handle = m, func() { n.respond(c, h, n.listOffsets(m)) }
	case wire.KeyFindCoord:
		m := &wire.FindCoordinatorRequest{}
		req, handle = m, func() { n.respond(c, h, n.findCoordinator(m)) }
	case wire.KeyJoinGroup:
		m := &wire.JoinGroupRequest{}
		req, handle = m, func() { n.joinGroup(c, h, m) }
	case wire.KeySyncGroup:
		m := &wire.SyncGroupRequest{}
		req, handle = m, func() { n.syncGroup(c, h, m) }
	case wire.KeyHeartbeat:
		m := &wire.HeartbeatRequest{}
		req, handle = m, func() { n.respond(c, h, n.heartbeat(m)) }
	case wire.KeyLeaveGroup:
		m := &wire.LeaveGroupRequest{}
		req, handle = m, func() { n.leaveGroup(c, h, m) }
	case wire.KeyOffsetCommit:
		m := &wire.OffsetCommitRequest{}
		req, handle = m, func() { n.offsetCommit(c, h, m) }
	case wire.KeyOffsetFetch:
		m := &wire.OffsetFetchRequest{}
		req, handle = m, func() { n.respond(c, h, n.offsetFetch(m)) }
	case wire.KeyInitProducerID:
		m := &wire.InitProducerIDRequest{}
		req, handle = m, func() { n.respond(c, h, n.initProducerID(m)) }
	default: // unreachable while wire.APIs and this switch agree
		n.drop(c, fmt.Sprintf("no handler for key %d", h.Key))
		return
	}
	req.Decode(r, h.Version)
	if r.Err() != nil {
		n.drop(c, fmt.Sprintf("malformed request body: key %d version %d", h.Key, h.Version))
		return
	}
	if !n.canRead() {
		if resp := refusal(req); resp != nil {
			if p, ok := req.(*wire.ProduceRequest); ok && p.Acks == 0 {
				n.finish(c) // the client asked for no answer, and gets none
			} else {
				n.respond(c, h, resp)
			}
			return
		}
	}
	handle()
}

func advertised() []wire.APIRange {
	out := make([]wire.APIRange, len(wire.APIs))
	for i, a := range wire.APIs {
		out[i] = wire.APIRange{Key: a.Key, Min: a.AdvertisedMin, Max: a.Max}
	}
	return out
}

// Serving reports whether the node is answering clients as leader.
func (n *Node) Serving() bool { return n.serving }

// Raft exposes the node's consensus state to the simulator's monitor,
// which only reads it. Nil before OnStart.
func (n *Node) Raft() *raft.Raft { return n.raft }

// StateHash returns a checksum of everything the node has applied, together
// with the index up to which it has applied.
func (n *Node) StateHash() (applied int64, hash uint32) { return n.applied, n.st.hash() }
