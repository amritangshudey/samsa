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
}

// world owns virtual time and everything outside the node.
type world struct {
	opts  options
	bcfg  broker.Config
	rng   *simsys.Rand
	now   sys.Instant
	seq   uint64
	queue eventHeap

	disk      *simsys.Disk
	node      *incarnation // nil while the node is down
	powerLost bool         // the disk must be restarted before the node
	conns     map[sys.ConnID]*conn
	nextConn  sys.ConnID

	model   *Model
	clients []*client
	quiet   bool // no new faults, no new client requests
	err     error
	stats   Stats
	hash    uint32
	trace   io.Writer
}

// incarnation is one life of the node, from start to crash. It is the
// node's clock and network.
type incarnation struct {
	w      *world
	n      *broker.Node
	timers map[sys.TimerID]uint64 // id -> seq of the arming that counts
	dead   bool
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
		w.note("timer", uint64(id), nil)
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
	switch {
	case err == nil && !w.disk.Crashed():
	case w.disk.Crashed() || errors.Is(err, simsys.ErrCrashed):
		w.down(true, "power loss during a disk operation")
	case errors.Is(err, simsys.ErrInjected):
		// The node stopped because a sync failed, as it must. What the
		// unsynced data now holds is unknown: model it as a power loss.
		w.stats.SyncFailures++
		w.down(true, "failed sync")
	default:
		w.fail("node stopped with an error no fault explains: %v", err)
	}
}

// down ends the node's current life. power says whether unsynced data is
// lost (power loss) or kept (the process was killed).
func (w *world) down(power bool, why string) {
	inc := w.node
	if inc == nil {
		return
	}
	w.note("node-down: "+why, 0, nil)
	inc.dead = true
	w.node = nil
	if power {
		w.stats.PowerLosses++
		w.disk.Crash()
		w.powerLost = true
		w.model.PowerLoss()
	} else {
		w.stats.Kills++
	}
	ids := make([]sys.ConnID, 0, len(w.conns))
	for id := range w.conns {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] }) // map order must not reach the schedule
	for _, id := range ids {
		c := w.conns[id]
		c.nodeClosed = true
		w.hangUp(c)
	}
	clear(w.conns)
	w.after(time.Duration(1+w.rng.Intn(int(300*time.Millisecond))), w.start)
}

// start brings the node up on whatever the disk now holds.
func (w *world) start() {
	if w.node != nil || w.err != nil {
		return
	}
	if w.powerLost {
		w.disk.Restart()
		w.powerLost = false
	}
	w.disk.SetSyncErrorRate(0)
	if !w.quiet && w.opts.diskCrash && w.rng.Chance(0.25) {
		w.disk.CrashAfter(1 + w.rng.Intn(40)) // lands inside recovery, or soon after
	}
	inc := &incarnation{w: w, timers: map[sys.TimerID]uint64{}}
	inc.n = broker.New(w.bcfg, inc, inc, w.disk, w.rng.Fork())
	w.note("node-start", 0, nil)
	if err := inc.n.OnStart(); err != nil {
		if !w.disk.Crashed() {
			w.fail("recovery failed: %v", err)
			return
		}
		w.note("node-down: power loss during recovery", 0, nil)
		w.stats.RecoveriesCut++
		w.stats.PowerLosses++
		w.powerLost = true
		w.model.PowerLoss()
		w.after(time.Duration(1+w.rng.Intn(int(300*time.Millisecond))), w.start)
		return
	}
	w.node = inc
	w.stats.Starts++
}

// chaos injects one fault and schedules the next.
func (w *world) chaos() {
	if w.quiet || len(w.opts.faults) == 0 {
		return
	}
	if w.node != nil {
		switch w.opts.faults[w.rng.Intn(len(w.opts.faults))] {
		case faultPower:
			w.down(true, "power loss")
		case faultDiskCrash:
			w.disk.CrashAfter(1 + w.rng.Intn(30))
		case faultKill:
			w.down(false, "process killed")
		case faultSync:
			w.disk.SetSyncErrorRate(1) // the next sync fails
		}
	}
	w.after(time.Duration(1+w.rng.Intn(2*int(w.opts.faultEvery))), w.chaos)
}

// --- the clients' side -----------------------------------------------------

// connect opens a connection for cl, or returns nil if the node is down.
func (w *world) connect(cl *client) *conn {
	inc := w.node
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
