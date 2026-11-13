package realsys

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Peer messages travel on their own TCP connections, one per direction per
// pair of nodes. A connection starts with the sender's node id (4 bytes);
// then each message is a 4-byte length and that many bytes.
//
// Nothing here is reliable, and nothing needs to be: sys.Network.SendPeer
// promises only whole messages. A message is dropped when the peer cannot
// be reached, when the connection breaks, or when maxPeerQueue messages are
// already waiting.
//
// The id a connection announces is believed. Without PeerTLS, peers are
// assumed to be on a network where only cluster nodes can connect. With it,
// every peer connection is mutual TLS: only a holder of a certificate the
// cluster's CA signed can connect, or be connected to (ADR 0019).
const (
	maxPeerMessage = 16 << 20 // a length above this closes the connection
	maxPeerQueue   = 256
	peerDialEvery  = 200 * time.Millisecond
	peerIOTimeout  = 2 * time.Second
)

// Peers makes this loop node self of a cluster, accepting peer connections
// on ln. The node names the other nodes' addresses with PeerAddr. It must
// be called before Run.
func (l *Loop) Peers(self sys.NodeID, ln net.Listener) {
	l.self, l.peerLn = self, ln
	l.peerAddr = map[sys.NodeID]string{}
	l.peerOut = map[sys.NodeID]chan []byte{}
	l.peerIn = map[net.Conn]struct{}{}
}

// PeerAddr implements sys.Network. A new id gets its queue and sender at
// once; a known one is dialled at the new address from its next
// connection on. It must be called from a handler.
func (l *Loop) PeerAddr(id sys.NodeID, addr string) {
	if l.peerLn == nil || id == l.self || id <= 0 {
		return
	}
	l.peerMu.Lock()
	l.peerAddr[id] = addr
	l.peerMu.Unlock()
	if l.peerOut[id] == nil {
		ch := make(chan []byte, maxPeerQueue)
		l.peerOut[id] = ch
		if l.peersStarted {
			l.wg.Add(1)
			go l.sendPeer(id, ch)
		}
	}
}

// addrOf returns where id listens; "" for an id this node never heard of.
func (l *Loop) addrOf(id sys.NodeID) string {
	l.peerMu.Lock()
	defer l.peerMu.Unlock()
	return l.peerAddr[id]
}

// PeerTLS makes peer connections mutual TLS. cfg is used to dial: it must
// hold this node's certificate and the cluster CA as RootCAs; the caller
// wraps the peer listener with tls.NewListener and a config that requires
// and verifies client certificates. It must be called before Run.
func (l *Loop) PeerTLS(cfg *tls.Config) { l.peerTLS = cfg }

// SendPeer implements sys.Network. It must be called from a handler.
func (l *Loop) SendPeer(to sys.NodeID, msg []byte) {
	if ch := l.peerOut[to]; ch != nil && len(msg) <= maxPeerMessage {
		frame := make([]byte, 4, 4+len(msg))
		binary.BigEndian.PutUint32(frame, uint32(len(msg)))
		select {
		case ch <- append(frame, msg...):
		default: // the peer is slow or away: this message is lost
		}
	}
}

func (l *Loop) startPeers() {
	if l.peerLn == nil {
		return
	}
	l.wg.Add(1)
	go l.acceptPeers()
	l.peersStarted = true
	for id, ch := range l.peerOut {
		l.wg.Add(1)
		go l.sendPeer(id, ch)
	}
}

func (l *Loop) stopPeers() {
	if l.peerLn == nil {
		return
	}
	l.peerLn.Close() //nolint:errcheck // shutting down
	l.peerMu.Lock()
	for c := range l.peerIn {
		c.Close() //nolint:errcheck // shutting down
	}
	l.peerMu.Unlock()
}

// sendPeer writes the queued messages for one peer, connecting as needed.
func (l *Loop) sendPeer(id sys.NodeID, ch chan []byte) {
	defer l.wg.Done()
	var c net.Conn
	defer func() {
		if c != nil {
			c.Close() //nolint:errcheck // shutting down
		}
	}()
	var lastDial time.Time
	for {
		var frame []byte
		select {
		case frame = <-ch:
		case <-l.done:
			return
		}
		if c == nil {
			if time.Since(lastDial) < peerDialEvery {
				continue // the peer was just unreachable: drop, do not hammer it
			}
			lastDial = time.Now()
			addr := l.addrOf(id)
			var err error
			d := &net.Dialer{Timeout: peerIOTimeout} // with TLS, the handshake is within the timeout too
			if l.peerTLS != nil {
				c, err = tls.DialWithDialer(d, "tcp", addr, l.peerTLS)
			} else {
				c, err = d.Dial("tcp", addr)
			}
			if err != nil {
				c = nil
				continue
			}
			frame = append(binary.BigEndian.AppendUint32(nil, uint32(l.self)), frame...)
		}
		c.SetWriteDeadline(time.Now().Add(peerIOTimeout)) //nolint:errcheck // a failed deadline shows up as a failed write
		if _, err := c.Write(frame); err != nil {
			c.Close() //nolint:errcheck // already failed
			c = nil
		}
	}
}

func (l *Loop) acceptPeers() {
	defer l.wg.Done()
	for {
		c, err := l.peerLn.Accept()
		if err != nil {
			return // listener closed (shutdown), or broken beyond use
		}
		l.peerMu.Lock()
		select {
		case <-l.done:
			l.peerMu.Unlock()
			c.Close() //nolint:errcheck // shutting down
			return
		default:
		}
		l.peerIn[c] = struct{}{}
		l.peerMu.Unlock()
		l.wg.Add(1)
		go l.readPeer(c)
	}
}

// readPeer turns one inbound connection into peer message events.
func (l *Loop) readPeer(c net.Conn) {
	defer l.wg.Done()
	defer func() {
		c.Close() //nolint:errcheck // done with it either way
		l.peerMu.Lock()
		delete(l.peerIn, c)
		l.peerMu.Unlock()
	}()
	var head [4]byte
	c.SetReadDeadline(time.Now().Add(peerIOTimeout)) //nolint:errcheck // a failed deadline shows up as a failed read
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return
	}
	from := sys.NodeID(binary.BigEndian.Uint32(head[:]))
	if l.addrOf(from) == "" {
		return // not a node of this cluster
	}
	c.SetReadDeadline(time.Time{}) //nolint:errcheck // as above
	for {
		if _, err := io.ReadFull(c, head[:]); err != nil {
			return
		}
		// The declared size is checked before that many bytes are allocated.
		size := binary.BigEndian.Uint32(head[:])
		if size > maxPeerMessage {
			return
		}
		msg := make([]byte, size)
		if _, err := io.ReadFull(c, msg); err != nil {
			return
		}
		if !l.post(event{kind: evPeer, from: from, data: msg}) {
			return
		}
	}
}
