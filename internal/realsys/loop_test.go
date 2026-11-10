package realsys

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/broker"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// script is a handler whose behaviour each test supplies. Every method runs
// on the loop goroutine; log is guarded for the test goroutine's reads.
type script struct {
	loop   *Loop
	onData func(c sys.ConnID, p []byte) error
	mu     sync.Mutex
	log    []string
}

func (s *script) note(e string) {
	s.mu.Lock()
	s.log = append(s.log, e)
	s.mu.Unlock()
}

func (s *script) events() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

func (s *script) OnStart() error               { return nil }
func (s *script) OnConnOpen(sys.ConnID) error  { s.note("open"); return nil }
func (s *script) OnConnClose(sys.ConnID) error { s.note("close"); return nil }
func (s *script) OnTimer(id sys.TimerID) error {
	s.note("timer" + string(rune('0'+id)))
	return nil
}
func (s *script) OnConnData(c sys.ConnID, p []byte) error {
	s.note("data:" + string(p))
	if s.onData != nil {
		return s.onData(c, p)
	}
	return nil
}

// startLoop runs a loop with h and returns its address and a stop function
// that returns Run's error.
func startLoop(t *testing.T, maxOut int, mk func(l *Loop) sys.Handler) (addr string, stop func() error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	loop := NewLoop(ln, maxOut)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	h := mk(loop) // built here, not on the loop goroutine, so tests may keep a reference
	go func() { done <- loop.Run(ctx, h) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() { cancel(); runErr = <-done })
		return runErr
	}
	t.Cleanup(func() { stop() })
	return ln.Addr().String(), stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestLoopEchoTimersAndClose(t *testing.T) {
	var s *script
	addr, stop := startLoop(t, 0, func(l *Loop) sys.Handler {
		s = &script{loop: l}
		s.onData = func(c sys.ConnID, p []byte) error {
			switch string(p) {
			case "timers":
				l.SetTimer(1, 30*time.Millisecond)
				l.SetTimer(2, time.Hour)
				l.SetTimer(3, 10*time.Millisecond)
				l.SetTimer(3, time.Millisecond) // replaces the arming above
				l.CancelTimer(2)
				l.SetTimer(4, 0)
				l.SetTimer(5, 0)
				l.CancelTimer(5) // a zero-delay timer can be cancelled too
			case "bye":
				l.Send(c, []byte("closing"))
				l.Close(c)
			default:
				l.Send(c, bytes.ToUpper(p))
			}
			return nil
		}
		return s
	})

	c := dial(t, addr)
	c.Write([]byte("hello"))
	buf := make([]byte, 16)
	if n, err := io.ReadFull(c, buf[:5]); err != nil || string(buf[:n]) != "HELLO" {
		t.Fatalf("echo: %q, %v", buf[:n], err)
	}

	before := s.loop.Now()
	c.Write([]byte("timers"))
	waitFor(t, "timer 1", func() bool { return contains(s.events(), "timer1") })
	got := s.events()
	if contains(got, "timer2") || contains(got, "timer5") {
		t.Errorf("a cancelled timer fired: %v", got)
	}
	if index(got, "timer4") > index(got, "timer3") || index(got, "timer3") > index(got, "timer1") {
		t.Errorf("timers fired out of order: %v", got)
	}
	if s.loop.Now() <= before {
		t.Error("clock did not advance")
	}

	c.Write([]byte("bye"))
	if rest, err := io.ReadAll(c); err != nil || string(rest) != "closing" {
		t.Fatalf("close after send: %q, %v", rest, err)
	}
	waitFor(t, "close event", func() bool { return contains(s.events(), "close") })

	// The peer closing is reported as well.
	c2 := dial(t, addr)
	c2.Write([]byte("x"))
	io.ReadFull(c2, buf[:1])
	c2.Close()
	waitFor(t, "second close event", func() bool { return count(s.events(), "close") == 2 })
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v after cancel", err)
	}
}

func TestLoopStopsWhenHandlerFails(t *testing.T) {
	boom := errors.New("node is dead")
	addr, stop := startLoop(t, 0, func(l *Loop) sys.Handler {
		return &script{loop: l, onData: func(sys.ConnID, []byte) error { return boom }}
	})
	c := dial(t, addr)
	c.Write([]byte("x"))
	if _, err := io.ReadAll(c); err != nil {
		t.Fatalf("connection not closed cleanly at shutdown: %v", err)
	}
	if err := stop(); !errors.Is(err, boom) {
		t.Fatalf("Run returned %v, want the handler's error", err)
	}
}

// A peer that never reads must not make the broker hold unbounded data.
func TestLoopClosesConnectionThatDoesNotRead(t *testing.T) {
	var s *script
	addr, _ := startLoop(t, 1<<20, func(l *Loop) sys.Handler {
		s = &script{loop: l}
		s.onData = func(c sys.ConnID, _ []byte) error {
			chunk := make([]byte, 256<<10)
			for range 200 { // 50 MiB against a 1 MiB limit
				l.Send(c, chunk)
			}
			return nil
		}
		return s
	})
	c := dial(t, addr)
	c.Write([]byte("go"))
	waitFor(t, "overflowing connection to be closed", func() bool { return contains(s.events(), "close") })
}

func contains(xs []string, x string) bool { return index(xs, x) >= 0 }

func index(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func count(xs []string, x string) (n int) {
	for _, v := range xs {
		if v == x {
			n++
		}
	}
	return n
}

// The broker behind real TCP and a real disk, driven by this repository's
// own encoder. It proves the plumbing; the real-client tests in
// integration/ prove compatibility.
func TestBrokerOverTCP(t *testing.T) {
	dir := t.TempDir()
	run := func() (string, func() error) {
		return startLoop(t, 0, func(l *Loop) sys.Handler {
			disk, err := OpenDisk(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { disk.Close() })
			return broker.New(broker.Config{NodeID: 1, Host: "127.0.0.1", Port: 1}, l, l, disk)
		})
	}
	var corr int32
	call := func(c net.Conn, key, version int16, req, resp wire.Message) {
		t.Helper()
		corr++
		w := wire.Writer{B: make([]byte, 4)}
		(&wire.RequestHeader{Key: key, Version: version, CorrelationID: corr, ClientID: "e2e"}).Encode(&w)
		req.Encode(&w, version)
		binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
		c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Write(w.B); err != nil {
			t.Fatal(err)
		}
		var size [4]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			t.Fatalf("read response to key %d: %v", key, err)
		}
		body := make([]byte, binary.BigEndian.Uint32(size[:]))
		if _, err := io.ReadFull(c, body); err != nil {
			t.Fatal(err)
		}
		r := wire.NewReader(body)
		if got := wire.DecodeResponseHeader(r, key, version); got != corr {
			t.Fatalf("correlation id %d, want %d", got, corr)
		}
		resp.Decode(r, version)
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
	}
	fetchAll := func(c net.Conn) (hw int64, values []string) {
		t.Helper()
		var resp wire.FetchResponse
		call(c, wire.KeyFetch, 11, &wire.FetchRequest{MaxWaitMs: 100, MinBytes: 1, MaxBytes: 1 << 20, Topics: []wire.FetchTopic{{Name: "t", Partitions: []wire.FetchPartition{{Offset: 0, MaxBytes: 1 << 20}}}}}, &resp)
		p := resp.Topics[0].Partitions[0]
		for rest := p.Records; len(rest) > 0; {
			info, code := wire.ParseBatch(rest, 1<<20)
			if code != 0 {
				t.Fatalf("bad batch: %d", code)
			}
			_, recs, _ := wire.DecodeBatch(rest[:info.Size])
			for _, r := range recs {
				values = append(values, string(r.Value))
			}
			rest = rest[info.Size:]
		}
		return p.HighWatermark, values
	}

	addr, stop := run()
	c := dial(t, addr)
	var av wire.APIVersionsResponse
	call(c, wire.KeyAPIVersions, 3, &wire.APIVersionsRequest{ClientSoftwareName: "e2e"}, &av)
	var ct wire.CreateTopicsResponse
	call(c, wire.KeyCreateTopics, 4, &wire.CreateTopicsRequest{Topics: []wire.CreateTopic{{Name: "t", NumPartitions: 1, ReplicationFactor: 1}}}, &ct)
	if len(av.APIs) == 0 || ct.Topics[0].ErrorCode != 0 {
		t.Fatalf("setup: %+v %+v", av, ct)
	}

	// Concurrent producers, each on its own connection.
	const producers, each = 8, 50
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer pc.Close()
			for i := range each {
				w := wire.Writer{B: make([]byte, 4)}
				(&wire.RequestHeader{Key: wire.KeyProduce, Version: 8, CorrelationID: int32(i), ClientID: "p"}).Encode(&w)
				batch := wire.EncodeBatch([]wire.Record{{Value: []byte{byte('a' + p)}, Timestamp: int64(i)}})
				(&wire.ProduceRequest{Acks: -1, Topics: []wire.ProduceTopic{{Name: "t", Partitions: []wire.ProducePartition{{Records: batch}}}}}).Encode(&w, 8)
				binary.BigEndian.PutUint32(w.B, uint32(len(w.B)-4))
				pc.SetDeadline(time.Now().Add(5 * time.Second))
				pc.Write(w.B)
				var size [4]byte
				if _, err := io.ReadFull(pc, size[:]); err != nil {
					t.Error(err)
					return
				}
				io.CopyN(io.Discard, pc, int64(binary.BigEndian.Uint32(size[:])))
			}
		}()
	}
	wg.Wait()
	hw, values := fetchAll(c)
	if hw != producers*each || len(values) == 0 {
		t.Fatalf("after concurrent produce: high watermark %d, %d records in first fetch", hw, len(values))
	}

	// Stop the broker and start a new one on the same directory.
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	addr, _ = run()
	hw2, _ := fetchAll(dial(t, addr))
	if hw2 != hw {
		t.Fatalf("after restart: high watermark %d, was %d", hw2, hw)
	}
}
