package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/raft"
	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// event is something that happens at a virtual instant. Events at the same
// instant run in the order they were scheduled.
type event struct {
	at  sys.Instant
	seq uint64
	fn  func()
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	return h[i].at < h[j].at || (h[i].at == h[j].at && h[i].seq < h[j].seq)
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = event{}
	*h = old[:len(old)-1]
	return e
}

// Stats counts what happened in a run, so that a suite can check its seeds
// did real work.
type Stats struct {
	Requests, RecordsAcked, RecordsRead, FetchesChecked int
	PowerLosses, Kills, SyncFailures, RecoveriesCut     int
	Starts, Disconnects                                 int
	GroupJoins, GroupSyncs, OffsetCommits               int
	OffsetFetches, ZombiesRefused                       int
	// Clusters only: terms that had a leader, requests refused by a node
	// that was not serving, and replica comparisons made (S4).
	Leaders, NotLeader, ReplicasCompared int
	Partitions                           int
	// Starts that began from a snapshot instead of the whole log.
	SnapshotStarts int
	// Log prefixes deleted by retention, and snapshots installed from a
	// leader (counted when they begin).
	Trims, Installs int
	// Idempotent producers: records acknowledged, batches sent again,
	// ids handed out, and partitions moved to a new epoch.
	IdempotentAcked, Retries, ProducerIDs, EpochBumps int
	// Producers the leader had forgotten by the end, and refusals of a
	// producer that had legitimately expired.
	ProducersExpired, ExpiredRefusals int
	// Graceful stops of a leader that ended with another node leading.
	Handovers int
	// Admin operations acknowledged: DeleteRecords, partitions added,
	// topics deleted.
	RecordDeletes, PartitionsAdded, TopicsDeleted int
	// Coverage of states that are hard to reach (docs/design/a3-sim-tools.md,
	// 2.3): a fault that hit the serving leader, a partition that isolated
	// it, and terms that ended without electing anyone (split votes, or a
	// candidate cut off).
	LeaderFaults, LeaderIsolated, TermsWithoutLeader int
}

// simNode is one machine of the cluster: a disk that outlives the
// processes (incarnations) that run on it.
type simNode struct {
	id        int32
	cfg       broker.Config
	disk      *simsys.Disk
	inc       *incarnation // nil while the node is down
	powerLost bool         // the disk must be restarted before the node
	term      int64        // S5: the highest term seen on this node, ever
	vote      sys.NodeID   // S5: whom it voted for in that term, if voted
	voted     bool
}

// world owns virtual time and everything outside the nodes.
type world struct {
	opts  options
	bcfg  broker.Config // what every node's configuration is made from
	rng   *simsys.Rand
	now   sys.Instant
	seq   uint64
	queue eventHeap

	nodes    []*simNode
	conns    map[sys.ConnID]*conn
	nextConn sys.ConnID
	// The monitor's memory; see monitor.go.
	leaders   map[int64]int32   // S1: who led each term
	committed []int64           // S3: the term of each entry first seen committed
	cut       map[[2]int32]bool // directed links that drop everything; see network.go
	// Faults, numbered in the order they happen (docs/design/a3-sim-tools.md).
	faults []Fault
	skip   map[int]bool
	tl     *timeline // nil unless Config.Timeline is set

	model   *Model
	clients []*client
	// I9: every producer id handed out
	producerIDs map[int64]bool
	quiet       bool // no new faults, no new client requests
	err         error
	stats       Stats
	hash        uint32
	trace       io.Writer
}

// incarnation is one life of the node, from start to crash. It is the
// node's clock and network.
type incarnation struct {
	w      *world
	node   *simNode
	n      *broker.Node
	timers map[sys.TimerID]uint64 // id -> seq of the arming that counts
	dead   bool
	// S5 and S3: how far this life has been seen to commit, and checked.
	commit int64
	// What retention and InstallSnapshot have been seen to do.
	logStart   int64
	installing bool
}

// conn is one client connection: two ordered byte streams.
type conn struct {
	id  sys.ConnID
	cl  *client
	inc *incarnation
	// toNodeAt and toClientAt are when the last scheduled bytes arrive in
	// each direction; later bytes never arrive earlier, as with TCP.
	toNodeAt, toClientAt sys.Instant
	nodeClosed           bool // the node closed it, or was told it closed
	clientGone           bool // the client closed it, or was told it closed
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// note adds an event to the trace hash and, if asked, prints it.
func (w *world) note(what string, id uint64, p []byte) {
	var b [16]byte
	for i := range 8 {
		b[i] = byte(uint64(w.now) >> (8 * i))
		b[8+i] = byte(id >> (8 * i))
	}
	w.hash = crc32.Update(w.hash, crcTable, b[:])
	w.hash = crc32.Update(w.hash, crcTable, []byte(what))
	w.hash = crc32.Update(w.hash, crcTable, p)
	if w.trace != nil {
		size := ""
		if p != nil {
			size = fmt.Sprintf(" %d bytes", len(p))
		}
		// A trace that cannot be written is not worth stopping a run for.
		_, _ = fmt.Fprintf(w.trace, "%13.6f %-12s %d%s\n", float64(w.now-epochStart)/1e9, what, id, size)
	}
}

// fail records the first violation; the run stops at the next event.
func (w *world) fail(format string, args ...any) {
	if w.err == nil {
		w.err = fmt.Errorf(format, args...)
		w.note("VIOLATION: "+w.err.Error(), 0, nil)
		w.tl.fail(w.now, w.err)
	}
}

func (w *world) after(d time.Duration, fn func()) { w.at(w.now+sys.Instant(d), fn) }

func (w *world) at(t sys.Instant, fn func()) {
	w.seq++
	heap.Push(&w.queue, event{at: t, seq: w.seq, fn: fn})
}

// runUntil runs events up to the given instant, or until a violation, or
// until stop reports true.
func (w *world) runUntil(t sys.Instant, stop func() bool) {
	for w.err == nil && len(w.queue) > 0 && w.queue[0].at <= t && (stop == nil || !stop()) {
		e := heap.Pop(&w.queue).(event)
		w.now = e.at
		e.fn()
	}
	if w.err == nil && (stop == nil || !stop()) {
		w.now = t
	}
}

// netDelay is how long bytes take to cross the simulated network.
func (w *world) netDelay() time.Duration {
	if w.rng.Chance(0.3) {
		return 0
	}
	return time.Duration(w.rng.Intn(int(3 * time.Millisecond)))
}

// fragments cuts p at random places, often into single bytes, and calls
// deliver for each piece at a time that keeps the stream in order.
func (w *world) fragments(p []byte, last *sys.Instant, deliver func(piece []byte)) {
	for len(p) > 0 {
		n := 1
		if !w.rng.Chance(0.2) {
			n += w.rng.Intn(len(p))
		}
		piece := p[:n]
		p = p[n:]
		*last = max(*last, w.now+sys.Instant(w.netDelay()))
		w.at(*last, func() { deliver(piece) })
	}
}

// --- the node's side -------------------------------------------------------

// Now implements sys.Clock.
func (inc *incarnation) Now() sys.Instant { return inc.w.now }

// SetTimer implements sys.Clock.
func (inc *incarnation) SetTimer(id sys.TimerID, after time.Duration) {
	w := inc.w
	seq := w.seq + 1 // the sequence number the event below will get
	inc.timers[id] = seq
	w.after(after, func() {
		if inc.dead || inc.timers[id] != seq {
			return // cancelled, replaced, or the node died
		}
		delete(inc.timers, id)
		w.note(inc.node.tag("timer"), uint64(id), nil)
		w.call(inc, inc.n.OnTimer(id))
	})
}

// CancelTimer implements sys.Clock.
func (inc *incarnation) CancelTimer(id sys.TimerID) { delete(inc.timers, id) }

// Send implements sys.Network.
func (inc *incarnation) Send(id sys.ConnID, p []byte) {
	w := inc.w
	c := w.conns[id]
	if c == nil || c.nodeClosed || c.inc != inc {
		return
	}
	w.note("send", uint64(id), p)
	own := append([]byte(nil), p...) // the node may reuse p
	w.fragments(own, &c.toClientAt, func(piece []byte) {
		if !c.clientGone {
			c.cl.onData(c, piece)
		}
	})
}

// SendPeer implements sys.Network; network.go decides the message's fate.
func (inc *incarnation) SendPeer(to sys.NodeID, msg []byte) {
	inc.w.sendPeer(inc.node, int32(to), append([]byte(nil), msg...)) // the node may reuse msg
}

// Close implements sys.Network.
func (inc *incarnation) Close(id sys.ConnID) {
	w := inc.w
	c := w.conns[id]
	if c == nil || c.nodeClosed || c.inc != inc {
		return
	}
	w.note("node-close", uint64(id), nil)
	c.nodeClosed = true
	delete(w.conns, id)
	w.after(0, func() {
		if !inc.dead {
			w.call(inc, inc.n.OnConnClose(id))
		}
	})
	w.hangUp(c)
}

// hangUp tells the client its connection is gone, after the bytes already
// on their way to it.
func (w *world) hangUp(c *conn) {
	c.toClientAt = max(c.toClientAt, w.now+sys.Instant(w.netDelay()))
	w.at(c.toClientAt, func() {
		if !c.clientGone {
			c.clientGone = true
			c.cl.onClose(c)
		}
	})
}

// call handles the result of a handler: any error is the node's death.
func (w *world) call(inc *incarnation, err error) {
	if inc.dead {
		return
	}
	nd := inc.node
	switch {
	case err == nil && !nd.disk.Crashed():
		w.monitor(inc)
	case nd.disk.Crashed() || errors.Is(err, simsys.ErrCrashed):
		w.down(nd, true, "power loss during a disk operation")
	case errors.Is(err, simsys.ErrInjected):
		// The node stopped because a sync failed, as it must. What the
		// unsynced data now holds is unknown: model it as a power loss.
		w.stats.SyncFailures++
		w.down(nd, true, "failed sync")
	default:
		w.fail("node %d stopped with an error no fault explains: %v", nd.id, err)
	}
}

// powerLoss tells the checkers that a node lost power. Under a policy
// that acknowledges before syncing, entries that were committed may be
// gone, and others may be committed in their place: S3 starts over. (Only
// a single node runs that combination; see Run.)
func (w *world) powerLoss() {
	w.model.PowerLoss()
	if !w.model.strict {
		w.committed = nil
	}
}

// tag names an event of this node in the trace.
func (nd *simNode) tag(what string) string { return fmt.Sprintf("%s@%d", what, nd.id) }

// down ends the node's current life. power says whether unsynced data is
// lost (power loss) or kept (the process was killed).
func (w *world) down(nd *simNode, power bool, why string) {
	inc := nd.inc
	if inc == nil {
		return
	}
	w.note(nd.tag("node-down: "+why), 0, nil)
	w.tl.down(nd.id, w.now, why)
	inc.dead = true
	nd.inc = nil
	if power {
		w.stats.PowerLosses++
		nd.disk.Crash()
		nd.powerLost = true
		w.powerLoss()
	} else {
		w.stats.Kills++
	}
	ids := make([]sys.ConnID, 0, len(w.conns))
	for id, c := range w.conns {
		if c.inc == inc {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] }) // map order must not reach the schedule
	for _, id := range ids {
		c := w.conns[id]
		c.nodeClosed = true
		delete(w.conns, id)
		w.hangUp(c)
	}
	w.after(time.Duration(1+w.rng.Intn(int(300*time.Millisecond))), func() { w.start(nd) })
}

// start brings a node up on whatever its disk now holds.
func (w *world) start(nd *simNode) {
	if nd.inc != nil || w.err != nil {
		return
	}
	if nd.powerLost {
		nd.disk.Restart()
		nd.powerLost = false
	}
	nd.disk.SetSyncErrorRate(0)
	if !w.quiet && w.opts.diskCrash && w.rng.Chance(0.25) {
		n := 1 + w.rng.Intn(40) // lands inside recovery, or soon after
		if w.faultAllowed(int(nd.id-1), fmt.Sprintf("diskcrash in recovery node %d", nd.id)) {
			nd.disk.CrashAfter(n)
		}
	}
	inc := &incarnation{w: w, node: nd, timers: map[sys.TimerID]uint64{}}
	inc.n = broker.New(nd.cfg, inc, inc, nd.disk, w.rng.Fork())
	w.note(nd.tag("node-start"), 0, nil)
	w.tl.up(nd.id, w.now)
	if err := inc.n.OnStart(); err != nil {
		inc.dead = true // it may have armed timers before it failed
		if !nd.disk.Crashed() {
			w.fail("recovery of node %d failed: %v", nd.id, err)
			return
		}
		w.note(nd.tag("node-down: power loss during recovery"), 0, nil)
		w.stats.RecoveriesCut++
		w.stats.PowerLosses++
		nd.powerLost = true
		w.powerLoss()
		w.after(time.Duration(1+w.rng.Intn(int(300*time.Millisecond))), func() { w.start(nd) })
		return
	}
	nd.inc = inc
	w.stats.Starts++
	if inc.n.Raft().SnapshotIndex() >= 0 {
		w.stats.SnapshotStarts++
	}
	w.monitor(inc)
}

// faultAllowed numbers a fault about to happen, records it, and reports
// whether it may act: false if the run was told to skip it. The caller has
// already made the draws that chose it.
func (w *world) faultAllowed(lane int, what string) bool {
	f := Fault{Seq: len(w.faults) + 1, At: time.Duration(w.now - epochStart), What: what, Skipped: w.skip[len(w.faults)+1]}
	w.faults = append(w.faults, f)
	kind := "fault"
	if f.Skipped {
		w.note("skipped: "+what, 0, nil)
		kind = "skipped"
	}
	w.tl.mark(lane, w.now, kind, fmt.Sprintf("#%d %s", f.Seq, what))
	return !f.Skipped
}

// chaos injects one fault and schedules the next.
func (w *world) chaos() {
	if w.quiet || len(w.opts.faults) == 0 {
		return
	}
	if nd := w.target(); nd.inc != nil {
		kind := w.opts.faults[w.rng.Intn(len(w.opts.faults))]
		if !w.faultAllowed(int(nd.id-1), fmt.Sprintf("%s node %d", kind, nd.id)) {
			kind = faultNone
		} else if nd.inc.n.Serving() {
			w.stats.LeaderFaults++
		}
		switch kind {
		case faultPower:
			w.down(nd, true, "power loss")
		case faultDiskCrash:
			nd.disk.CrashAfter(1 + w.rng.Intn(30))
		case faultKill:
			w.down(nd, false, "process killed")
		case faultSync:
			nd.disk.SetSyncErrorRate(1) // the next sync fails
		case faultStop:
			w.stop(nd)
		}
	}
	w.after(time.Duration(1+w.rng.Intn(2*int(w.opts.faultEvery))), w.chaos)
}

// stop is a graceful stop, as the real loop does it on SIGTERM: OnStop,
// events until Stopped (or the real loop's limit of ten seconds), then the
// process ends. Ending loses nothing, so it is a kill.
func (w *world) stop(nd *simNode) {
	inc := nd.inc
	w.note(nd.tag("stop"), 0, nil)
	leading := inc.n.Serving()
	w.call(inc, inc.n.OnStop())
	deadline := w.now + sys.Instant(10*time.Second)
	// Now and then it stops hearing from the others mid-handover: its
	// TimeoutNow still goes out, but nothing of the successor comes back,
	// which is when serving on would hurt.
	isolated := false
	if leading && w.opts.partitions && w.rng.Chance(0.5) {
		w.after(time.Duration(w.rng.Intn(2*int(w.bcfg.HeartbeatInterval))), func() {
			if nd.inc == inc {
				isolated = true
				w.note(fmt.Sprintf("partition: nothing reaches %d while it hands over", nd.id), 0, nil)
				for _, o := range w.nodes {
					w.cut[[2]int32{o.id, nd.id}] = true
				}
			}
		})
	}
	var wait func()
	wait = func() {
		switch {
		case nd.inc != inc: // it died some other way meanwhile
		case inc.n.Stopped() || w.now >= deadline:
			if leading && !inc.n.Raft().Transferring() && inc.n.Raft().Role() != raft.Leader {
				w.stats.Handovers++
			}
			w.down(nd, false, "graceful stop")
			if isolated {
				for _, o := range w.nodes {
					delete(w.cut, [2]int32{o.id, nd.id})
				}
			}
		default:
			w.after(5*time.Millisecond, wait)
		}
	}
	wait()
}

// --- the clients' side -----------------------------------------------------

// connect opens a connection for cl to the node it is pinned to, or else
// to any node, and returns nil if that node is down. A client that picked
// a node which is not the leader is told so, hangs up, and picks again.
func (w *world) connect(cl *client) *conn {
	nd := cl.pin
	if nd == nil {
		nd = w.nodes[cl.rng.Intn(len(w.nodes))]
	}
	inc := nd.inc
	if inc == nil {
		return nil
	}
	w.nextConn++
	c := &conn{id: w.nextConn, cl: cl, inc: inc}
	w.conns[c.id] = c
	c.toNodeAt = w.now + sys.Instant(w.netDelay())
	w.at(c.toNodeAt, func() {
		if !inc.dead && !c.nodeClosed {
			w.note("open", uint64(c.id), nil)
			w.call(inc, inc.n.OnConnOpen(c.id))
		}
	})
	return c
}

// send delivers p from the client to the node, in fragments.
func (w *world) send(c *conn, p []byte) {
	w.fragments(p, &c.toNodeAt, func(piece []byte) {
		if c.inc.dead || c.nodeClosed {
			return
		}
		w.note("data", uint64(c.id), piece)
		err := c.inc.n.OnConnData(c.id, piece)
		// The node may not keep the slice: make any kept alias visible.
		for i := range piece {
			piece[i] = 0xAA
		}
		w.call(c.inc, err)
	})
}

// closeByClient closes c from the client's end.
func (w *world) closeByClient(c *conn) {
	c.clientGone = true
	c.toNodeAt = max(c.toNodeAt, w.now+sys.Instant(w.netDelay()))
	w.at(c.toNodeAt, func() {
		if c.inc.dead || c.nodeClosed {
			return
		}
		c.nodeClosed = true
		delete(w.conns, c.id)
		w.note("client-close", uint64(c.id), nil)
		w.call(c.inc, c.inc.n.OnConnClose(c.id))
	})
}

// epochStart is the virtual time at which every run begins.
const epochStart sys.Instant = 1_700_000_000_000_000_000

// nodeLogger returns the node's logger: discarded, or written into the
// trace without timestamps (they are real time, and would differ per run).
func nodeLogger(trace io.Writer) *slog.Logger {
	if trace == nil {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(trace, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
}
