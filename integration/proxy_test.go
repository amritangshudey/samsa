package integration

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// proxy forwards TCP connections from its own address to target. It fails
// only as TCP itself can: it closes connections, refuses new ones, or is
// slow. It never drops or reorders bytes inside a connection.
type proxy struct {
	ln     net.Listener
	target string

	mu    sync.Mutex
	cut   bool
	delay time.Duration
	conns map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(func() { ln.Close(); p.reset() })
	return p
}

func (p *proxy) addr() string { return p.ln.Addr().String() }

func (p *proxy) accept() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return // closed at cleanup
		}
		p.mu.Lock()
		cut := p.cut
		p.mu.Unlock()
		if cut {
			in.Close()
			continue
		}
		out, err := net.DialTimeout("tcp", p.target, time.Second)
		if err != nil {
			in.Close() // the target is down: as a refused connection
			continue
		}
		p.mu.Lock()
		if p.cut { // cut while dialling
			p.mu.Unlock()
			in.Close()
			out.Close()
			continue
		}
		p.conns[in], p.conns[out] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(in, out)
		go p.pipe(out, in)
	}
}

// pipe copies from src to dst, holding each chunk for the delay, and closes
// both when either side ends.
func (p *proxy) pipe(src, dst net.Conn) {
	defer func() {
		src.Close()
		dst.Close()
		p.mu.Lock()
		delete(p.conns, src)
		delete(p.conns, dst)
		p.mu.Unlock()
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			d := p.delay
			p.mu.Unlock()
			time.Sleep(d)
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// reset closes every open connection; new ones are accepted.
func (p *proxy) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		c.Close()
	}
}

// setCut cuts (closes every connection and refuses new ones) or heals.
func (p *proxy) setCut(cut bool) {
	p.mu.Lock()
	p.cut = cut
	p.mu.Unlock()
	if cut {
		p.reset()
	}
}

func (p *proxy) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

func TestProxy(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go io.Copy(c, c)
		}
	}()
	p := newProxy(t, echo.Addr().String())
	roundTrip := func() error {
		c, err := net.Dial("tcp", p.addr())
		if err != nil {
			return err
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(time.Second))
		if _, err := c.Write([]byte("ping")); err != nil {
			return err
		}
		b := make([]byte, 4)
		_, err = io.ReadFull(c, b)
		return err
	}
	if err := roundTrip(); err != nil {
		t.Fatalf("pass: %v", err)
	}
	p.setCut(true)
	if err := roundTrip(); err == nil {
		t.Fatal("a cut proxy passed bytes")
	}
	p.setCut(false)
	p.setDelay(20 * time.Millisecond)
	start := time.Now()
	if err := roundTrip(); err != nil || time.Since(start) < 40*time.Millisecond {
		t.Fatalf("delay: %v after %v", err, time.Since(start))
	}
}
