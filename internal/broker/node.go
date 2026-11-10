package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

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

// Config configures a Node. Zero values select the defaults named below.
type Config struct {
	// NodeID, Host and Port are what Metadata responses tell clients.
	NodeID int32
	Host   string
	Port   int32
	// ClusterID is reported in Metadata. Default "samsa".
	ClusterID string
	// Dir is the directory of the command log on the node's disk. Default "log".
	Dir string
	// Sync is the fsync policy; SyncInterval is its period (default 1s).
	Sync         SyncPolicy
	SyncInterval time.Duration
	// SegmentBytes is the command log's segment size (storage default if 0).
	SegmentBytes int64
	// MaxRequestBytes bounds one request frame. Default 32 MiB.
	MaxRequestBytes int
	// MaxBatchBytes bounds one record batch. Default 1 MiB.
	MaxBatchBytes int
	// MaxFetchBytes bounds the record bytes of one fetch response, whatever
	// the client asks for. Default 8 MiB.
	MaxFetchBytes int
	// MaxPartitions bounds the partitions of one topic. Default 1024.
	MaxPartitions int32
	// Logger receives structured logs. Default: discard.
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.ClusterID == "" {
		c.ClusterID = "samsa"
	}
	if c.Dir == "" {
		c.Dir = "log"
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = time.Second
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
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return c
}

// commandOverhead is room for an Append command's fields around its batch:
// type, topic name (at most 249 bytes plus length) and partition.
const commandOverhead = 512

// Timer ids. Delayed fetches take ids from firstFetchTimer upwards.
const (
	timerFlush sys.TimerID = 1
	timerSync  sys.TimerID = 2

	firstFetchTimer sys.TimerID = 16
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
	log   *slog.Logger

	cmds    *storage.Log
	st      *state
	applied int64 // every command below this index has been applied

	waiters    []waiter // ascending by index
	flushArmed bool
	conns      map[sys.ConnID]*conn
	parked     []*parkedFetch // in arrival order
	nextTimer  sys.TimerID

	// fatal is the first error after which the node cannot be trusted: a
	// failed write or sync, or an undecodable command. Every handler
	// returns it, and the driver stops the node.
	fatal error
}

// New returns a node that will use the given clock, network and disk. It
// does nothing until the driver calls OnStart.
func New(cfg Config, clock sys.Clock, net sys.Network, disk sys.Disk) *Node {
	cfg = cfg.withDefaults()
	return &Node{
		cfg: cfg, clock: clock, net: net, disk: disk, log: cfg.Logger,
		st: newState(), conns: map[sys.ConnID]*conn{}, nextTimer: firstFetchTimer,
	}
}

// OnStart opens the command log, recovering it if needed, and rebuilds the
// state machine by applying every command in it.
func (n *Node) OnStart() error {
	cmds, err := storage.Open(n.disk, n.cfg.Dir, storage.Config{
		SegmentBytes:    n.cfg.SegmentBytes,
		MaxPayloadBytes: n.cfg.MaxBatchBytes + commandOverhead,
		SyncBytes:       -1, // the node decides when to sync
	})
	if err != nil {
		return fmt.Errorf("open command log: %w", err)
	}
	n.cmds = cmds
	for off := cmds.StartOffset(); off < cmds.EndOffset(); {
		batches, err := cmds.Read(off, 4<<20)
		if err != nil {
			return fmt.Errorf("replay command log at %d: %w", off, err)
		}
		for _, b := range batches {
			if _, err := n.st.apply(b.Base, b.Payload); err != nil {
				return err
			}
			off = b.Base + int64(b.Count)
		}
	}
	n.applied = cmds.EndOffset()
	if n.cfg.Sync == SyncInterval {
		n.clock.SetTimer(timerSync, n.cfg.SyncInterval)
	}
	n.log.Info("started", "commands", n.applied, "topics", len(n.st.names))
	return nil
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
	switch {
	case id == timerFlush:
		n.flushArmed = false
		n.sync()
	case id == timerSync:
		n.sync()
		n.clock.SetTimer(timerSync, n.cfg.SyncInterval)
	default:
		for _, f := range n.parked {
			if f.timer == id {
				n.completeFetch(f)
				break
			}
		}
	}
	return n.settle()
}

func (n *Node) sync() {
	if n.fatal == nil {
		if err := n.cmds.Sync(); err != nil {
			n.fatal = fmt.Errorf("sync command log: %w", err)
		}
	}
}

// settle ends every handler: it applies whatever has become committed,
// which may complete requests, which may start further requests, until
// nothing moves. It returns the node's fatal error, if any.
func (n *Node) settle() error {
	for n.fatal == nil {
		before := n.applied
		n.advance()
		if n.applied == before {
			break
		}
	}
	return n.fatal
}

// committed returns the index below which commands are committed under the
// configured policy.
func (n *Node) committed() int64 {
	if n.cfg.Sync == SyncAlways {
		return n.cmds.DurableOffset()
	}
	return n.cmds.EndOffset()
}

// propose appends cmd to the log and arranges for done to be called with
// the result once the command has been committed and applied. A success
// response may only be sent from done (roadmap rule R4).
func (n *Node) propose(cmd []byte, done func(result)) {
	if n.fatal != nil {
		return
	}
	index, err := n.cmds.Append(cmd, 1)
	if errors.Is(err, storage.ErrTooLarge) {
		done(result{errCode: wire.ErrMessageTooLarge, baseOffset: -1})
		return
	}
	if err != nil {
		n.fatal = fmt.Errorf("append command: %w", err)
		return
	}
	n.waiters = append(n.waiters, waiter{index: index, cmd: cmd, done: done})
	// A zero-delay timer fires after the events already waiting, so every
	// request that is ready now shares the one sync: group commit.
	if n.cfg.Sync == SyncAlways && !n.flushArmed {
		n.flushArmed = true
		n.clock.SetTimer(timerFlush, 0)
	}
}

// advance applies committed commands in order and completes their waiters.
func (n *Node) advance() {
	appended := false
	for n.fatal == nil && n.applied < n.committed() {
		if len(n.waiters) == 0 || n.waiters[0].index != n.applied {
			n.fatal = fmt.Errorf("broker: no waiter for committed command %d", n.applied)
			return
		}
		w := n.waiters[0]
		n.waiters[0] = waiter{}
		n.waiters = n.waiters[1:]
		res, err := n.st.apply(w.index, w.cmd)
		if err != nil {
			n.fatal = err
			return
		}
		n.applied++
		appended = appended || (w.cmd[0] == cmdAppend && res.errCode == wire.ErrNone)
		w.done(res)
	}
	if len(n.waiters) == 0 {
		n.waiters = nil // release the backing array
	}
	if appended {
		n.wakeFetches()
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
	if !c.closed {
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
	default: // unreachable while wire.APIs and this switch agree
		n.drop(c, fmt.Sprintf("no handler for key %d", h.Key))
		return
	}
	req.Decode(r, h.Version)
	if r.Err() != nil {
		n.drop(c, fmt.Sprintf("malformed request body: key %d version %d", h.Key, h.Version))
		return
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
