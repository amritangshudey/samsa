package realsys

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Loop drives a sys.Handler with real TCP connections and real timers. It
// is the production counterpart of the simulator's scheduler.
//
// Goroutines: one accepts, one reads and one writes per connection, and one
// (the caller of Run) owns the handler. Everything the handler touches is
// reached only from that last goroutine; the others talk to it through one
// bounded channel of events. Because the channel is bounded, a handler that
// falls behind stops the readers, and TCP pushes back on the clients.
type Loop struct {
	ln     net.Listener
	maxOut int // bytes that may wait to be written to one connection

	events chan event
	done   chan struct{} // closed when Run is returning
	wg     sync.WaitGroup

	// Owned by the Run goroutine.
	conns  map[sys.ConnID]*tcpConn
	timers map[sys.TimerID]armed
	gen    uint64
	local  []event // zero-delay timers: served only when events is empty

	start     time.Time // carries a monotonic reading
	startWall int64

	// Set by Peers before Run; see peer.go.
	self     sys.NodeID
	peerLn   net.Listener
	peerOut  map[sys.NodeID]chan []byte
	peerMu   sync.Mutex
	peerIn   map[net.Conn]struct{} // inbound peer connections, closed at shutdown
	peerAddr map[sys.NodeID]string
}

type eventKind uint8

const (
	evOpen eventKind = iota
	evData
	evClose
	evTimer
	evPeer
)

type event struct {
	kind  eventKind
	conn  *tcpConn
	data  []byte
	timer sys.TimerID
	gen   uint64
	from  sys.NodeID
}

// armed is one arming of a timer. gen tells a firing that is still wanted
// from one that was cancelled or replaced after it was already queued.
type armed struct {
	gen uint64
	t   *time.Timer
}

type tcpConn struct {
	id sys.ConnID
	c  net.Conn

	mu      sync.Mutex
	out     []byte
	closing bool          // close once out is written
	wake    chan struct{} // capacity 1: "out or closing changed"
}

// NewLoop returns a loop that will accept connections on ln. maxOutBytes
// bounds the data queued for one connection whose peer is not reading; a
// connection that exceeds it is closed. Zero means 64 MiB.
func NewLoop(ln net.Listener, maxOutBytes int) *Loop {
	if maxOutBytes <= 0 {
		maxOutBytes = 64 << 20
	}
	now := time.Now()
	return &Loop{
		ln: ln, maxOut: maxOutBytes,
		events: make(chan event, 1024), done: make(chan struct{}),
		conns: map[sys.ConnID]*tcpConn{}, timers: map[sys.TimerID]armed{},
		start: now, startWall: now.UnixNano(),
	}
}

// stopLimit bounds a graceful stop: a handler that has not stopped by then
// is abandoned.
const stopLimit = 10 * time.Second

// Run starts h and delivers events to it until ctx is cancelled or a
// handler method fails (returning its error). When ctx is cancelled, a
// handler that is a sys.Stopper is told so and served until it reports
// Stopped, for at most stopLimit; then Run returns nil. It closes the
// listener and every connection before returning.
func (l *Loop) Run(ctx context.Context, h sys.Handler) error {
	defer l.shutdown()
	if err := h.OnStart(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	l.wg.Add(1)
	go l.accept()
	l.startPeers()

	cancelled := ctx.Done()
	var stopper sys.Stopper
	var limit <-chan time.Time
	stop := func() (bool, error) { // reports whether Run should return now
		cancelled = nil
		s, ok := h.(sys.Stopper)
		if !ok {
			return true, nil
		}
		if err := s.OnStop(); err != nil {
			return true, err
		}
		stopper, limit = s, time.After(stopLimit)
		return s.Stopped(), nil
	}
	for {
		var ev event
		if len(l.local) > 0 {
			// A zero-delay timer runs after the events already queued,
			// which is what lets one sync cover many requests.
			select {
			case ev = <-l.events:
			default:
				ev, l.local = l.local[0], l.local[1:]
			}
		} else {
			select {
			case ev = <-l.events:
			case <-cancelled:
				if done, err := stop(); done || err != nil {
					return err
				}
				continue
			case <-limit:
				return nil
			}
		}
		if err := l.deliver(h, ev); err != nil {
			return err
		}
		if stopper != nil && stopper.Stopped() {
			return nil
		}
		if cancelled != nil && ctx.Err() != nil {
			if done, err := stop(); done || err != nil {
				return err
			}
		}
	}
}

func (l *Loop) deliver(h sys.Handler, ev event) error {
	switch ev.kind {
	case evOpen:
		l.conns[ev.conn.id] = ev.conn
		return h.OnConnOpen(ev.conn.id)
	case evData:
		return h.OnConnData(ev.conn.id, ev.data)
	case evClose:
		delete(l.conns, ev.conn.id)
		return h.OnConnClose(ev.conn.id)
	case evTimer:
		if l.timers[ev.timer].gen != ev.gen {
			return nil // cancelled or re-armed after this firing was queued
		}
		delete(l.timers, ev.timer)
		return h.OnTimer(ev.timer)
	case evPeer:
		return h.OnPeerMessage(ev.from, ev.data)
	}
	return nil
}

func (l *Loop) shutdown() {
	close(l.done)
	l.ln.Close() //nolint:errcheck // shutting down
	for _, a := range l.timers {
		if a.t != nil {
			a.t.Stop()
		}
	}
	// Connections are closed by their own goroutines when they see done;
	// closing here as well unblocks readers stuck in Read.
	for _, tc := range l.conns {
		tc.c.Close() //nolint:errcheck // shutting down
	}
	l.stopPeers()
	// The events channel is never closed: a timer callback may still post
	// after this returns, and a send on a closed channel would panic.
	l.wg.Wait()
}

// post hands an event to the handler goroutine, giving up at shutdown.
func (l *Loop) post(ev event) bool {
	select {
	case l.events <- ev:
		return true
	case <-l.done:
		return false
	}
}

func (l *Loop) accept() {
	defer l.wg.Done()
	var next sys.ConnID
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return // listener closed (shutdown), or broken beyond use
		}
		next++
		tc := &tcpConn{id: next, c: c, wake: make(chan struct{}, 1)}
		if !l.post(event{kind: evOpen, conn: tc}) {
			c.Close() //nolint:errcheck // shutting down
			return
		}
		l.wg.Add(2)
		go l.read(tc) // started after the open event, so data never precedes it
		go l.write(tc)
	}
}

func (l *Loop) read(tc *tcpConn) {
	defer l.wg.Done()
	buf := make([]byte, 64<<10)
	for {
		n, err := tc.c.Read(buf)
		if n > 0 && !l.post(event{kind: evData, conn: tc, data: append([]byte(nil), buf[:n]...)}) {
			return
		}
		if err != nil {
			tc.c.Close() //nolint:errcheck // already failed or closed
			tc.mu.Lock()
			tc.closing = true // nothing more can be written: release the writer
			tc.mu.Unlock()
			tc.signal()
			l.post(event{kind: evClose, conn: tc})
			return
		}
	}
}

func (l *Loop) write(tc *tcpConn) {
	defer l.wg.Done()
	for {
		select {
		case <-tc.wake:
		case <-l.done:
			tc.c.Close() //nolint:errcheck // shutting down
			return
		}
		tc.mu.Lock()
		out, closing := tc.out, tc.closing
		tc.out = nil
		tc.mu.Unlock()
		if len(out) > 0 {
			if _, err := tc.c.Write(out); err != nil {
				tc.c.Close() //nolint:errcheck // the reader reports the close
				return
			}
		}
		if closing {
			tc.c.Close() //nolint:errcheck // the reader reports the close
			return
		}
	}
}

func (tc *tcpConn) signal() {
	select {
	case tc.wake <- struct{}{}:
	default:
	}
}

// Send implements sys.Network. It must be called from a handler.
func (l *Loop) Send(id sys.ConnID, p []byte) {
	tc := l.conns[id]
	if tc == nil {
		return
	}
	tc.mu.Lock()
	overflow := len(tc.out)+len(p) > l.maxOut
	if !overflow && !tc.closing {
		tc.out = append(tc.out, p...)
	}
	tc.mu.Unlock()
	if overflow {
		tc.c.Close() //nolint:errcheck // the peer is not reading; the reader reports the close
		return
	}
	tc.signal()
}

// Close implements sys.Network. It must be called from a handler.
func (l *Loop) Close(id sys.ConnID) {
	if tc := l.conns[id]; tc != nil {
		tc.mu.Lock()
		tc.closing = true
		tc.mu.Unlock()
		tc.signal()
	}
}

// Now implements sys.Clock: the wall time at start plus monotonic elapsed
// time, so it never goes backwards even if the system clock is set back.
func (l *Loop) Now() sys.Instant {
	return sys.Instant(l.startWall + int64(time.Since(l.start)))
}

// SetTimer implements sys.Clock. It must be called from a handler.
func (l *Loop) SetTimer(id sys.TimerID, after time.Duration) {
	l.CancelTimer(id)
	l.gen++
	ev := event{kind: evTimer, timer: id, gen: l.gen}
	a := armed{gen: l.gen}
	if after <= 0 {
		l.local = append(l.local, ev)
	} else {
		a.t = time.AfterFunc(after, func() { l.post(ev) })
	}
	l.timers[id] = a
}

// CancelTimer implements sys.Clock. It must be called from a handler.
func (l *Loop) CancelTimer(id sys.TimerID) {
	if a, ok := l.timers[id]; ok {
		if a.t != nil {
			a.t.Stop()
		}
		delete(l.timers, id) // a firing already queued now fails the gen check
	}
}
