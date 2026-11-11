package realsys

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// peerNode runs a loop that is node id of a two-node cluster. Whatever a
// client writes to it, it sends to the other node as a peer message.
func peerNode(t *testing.T, id, other sys.NodeID, peerLn net.Listener, otherAddr string) (s *script, clientAddr string, stop func() error) {
	t.Helper()
	clientAddr, stop = startLoop(t, 0, func(l *Loop) sys.Handler {
		l.Peers(id, peerLn, map[sys.NodeID]string{other: otherAddr})
		s = &script{loop: l}
		s.onData = func(_ sys.ConnID, p []byte) error {
			l.SendPeer(other, p)
			return nil
		}
		return s
	})
	return s, clientAddr, stop
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func TestPeerMessages(t *testing.T) {
	ln1, ln2 := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
	addr1, addr2 := ln1.Addr().String(), ln2.Addr().String()
	s1, client1, _ := peerNode(t, 1, 2, ln1, addr2)
	s2, client2, stop2 := peerNode(t, 2, 1, ln2, addr1)

	// Whole messages arrive, in both directions, with the sender's id.
	c1, c2 := dial(t, client1), dial(t, client2)
	c1.Write([]byte("ping"))
	waitFor(t, "node 2 to get ping", func() bool { return contains(s2.events(), "peer 1 ping") })
	c2.Write([]byte("pong"))
	waitFor(t, "node 1 to get pong", func() bool { return contains(s1.events(), "peer 2 pong") })

	// The peer goes away: messages are lost, and the sender is unharmed.
	if err := stop2(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		c1.Write([]byte("lost"))
		time.Sleep(10 * time.Millisecond)
	}

	// It comes back on the same address: the sender reconnects by itself.
	s2, _, _ = peerNode(t, 2, 1, listen(t, addr2), addr1)
	waitFor(t, "node 2 to hear from node 1 again", func() bool {
		c1.Write([]byte("again"))
		time.Sleep(20 * time.Millisecond)
		return contains(s2.events(), "peer 1 again")
	})
}

// What arrives on the peer port is untrusted: a stranger, or a frame that
// claims an absurd size, is disconnected and delivers nothing.
func TestPeerPortRejectsStrangers(t *testing.T) {
	ln := listen(t, "127.0.0.1:0")
	s, _, _ := peerNode(t, 1, 2, ln, "127.0.0.1:1")
	for name, bytes := range map[string][]byte{
		"unknown node id": binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 7), 1),
		"own node id":     binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 1), 1),
		"huge frame":      binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 2), maxPeerMessage+1),
	} {
		c := dial(t, ln.Addr().String())
		c.Write(append(bytes, 'x'))
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		// Closed with our bytes unread, so EOF or a reset: either way, an error.
		if _, err := c.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("%s: connection not closed: %v", name, err)
		}
	}
	// A real peer still gets through afterwards.
	c := dial(t, ln.Addr().String())
	c.Write([]byte{0, 0, 0, 2, 0, 0, 0, 2, 'o', 'k'})
	waitFor(t, "the genuine message", func() bool { return contains(s.events(), "peer 2 ok") })
	if got := s.events(); len(got) != 1 {
		t.Fatalf("delivered %q, want only the genuine message", got)
	}
}
