package raft

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

func TestSnapshotEncoding(t *testing.T) {
	terms := []int64{1, 1, 1, 3, 3, 4, 7, 7}
	s := snapshot{index: 6, runs: runsOf(terms, 6), state: []byte("state")}
	if want := []termRun{{0, 1}, {3, 3}, {5, 4}, {6, 7}}; !reflect.DeepEqual(s.runs, want) {
		t.Fatalf("runs %v, want %v", s.runs, want)
	}
	if got := s.expand(); !reflect.DeepEqual(got, terms[:7]) {
		t.Fatalf("expand %v, want %v", got, terms[:7])
	}
	b := s.encode()
	got, err := decodeSnapshot(b)
	if err != nil || !reflect.DeepEqual(got, s) || b[4] != 1 {
		t.Fatalf("round trip: %+v, %v, version %d (want 1: no configuration, readable by v0.2.0)", got, err, b[4])
	}
	withConf := s
	withConf.members = []Member{{ID: 1, Info: []byte("a")}, {ID: 2}}
	if b2 := withConf.encode(); b2[4] != snapshotVersion {
		t.Fatalf("with a configuration: version %d, want %d", b2[4], snapshotVersion)
	} else if got, err := decodeSnapshot(b2); err != nil || len(got.members) != 2 || got.members[0].ID != 1 || string(got.members[0].Info) != "a" {
		t.Fatalf("with a configuration: %+v, %v", got, err)
	}
	for name, mangle := range map[string]func([]byte) []byte{
		"empty":       func([]byte) []byte { return nil },
		"truncated":   func(b []byte) []byte { return b[:len(b)-1] },
		"bit flipped": func(b []byte) []byte { b[10] ^= 1; return b },
		"too long":    func(b []byte) []byte { return append(b, 0) },
	} {
		if _, err := decodeSnapshot(mangle(bytes.Clone(b))); !errors.Is(err, errBadSnapshot) {
			t.Errorf("%s: %v, want errBadSnapshot", name, err)
		}
	}
	for name, bad := range map[string]snapshot{
		"runs not from 0":       {index: 2, runs: []termRun{{1, 1}}},
		"terms going down":      {index: 2, runs: []termRun{{0, 2}, {1, 1}}},
		"index before last run": {index: 0, runs: []termRun{{0, 1}, {1, 2}}},
		"no runs":               {index: 0},
		"negative index":        {index: -1, runs: []termRun{{0, 1}}},
		"run starting twice at": {index: 3, runs: []termRun{{0, 1}, {0, 2}}},
	} {
		if _, err := decodeSnapshot(bad.encode()); !errors.Is(err, errBadSnapshot) {
			t.Errorf("%s: %v, want errBadSnapshot", name, err)
		}
	}
}

// A snapshot survives a restart, and a node that starts from one reads
// only the entries after it but knows the terms of all.
func TestRestartFromSnapshot(t *testing.T) {
	c := newCluster(t, 1, nil)
	n := c.start(1)
	for i := range 5 {
		if _, err := n.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		n.Flush()
	}
	before := n.Committed()
	n.SaveSnapshot(3, []byte("at 3"))
	if err := n.Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.open(1); err != nil {
		t.Fatal(err)
	}
	n = c.r(1)
	if i, state := n.Snapshot(); i != 3 || string(state) != "at 3" {
		t.Fatalf("Snapshot() = %d, %q", i, state)
	}
	// Restarted alone, it elected itself again and appended a no-op.
	if n.SnapshotIndex() != 3 || n.Committed() < 4 || n.LastIndex() != before {
		t.Fatalf("after restart: snapshot %d, committed %d, last %d (before: %d committed)", n.SnapshotIndex(), n.Committed(), n.LastIndex(), before)
	}
	for i := range before {
		if n.EntryTerm(i) != 1 {
			t.Fatalf("entry %d has term %d after restart", i, n.EntryTerm(i))
		}
	}
	n.SaveSnapshot(n.Committed(), nil) // not committed
	if n.Err() == nil {
		t.Fatal("a snapshot past the commit index was accepted")
	}
}

func FuzzDecodeSnapshot(f *testing.F) {
	f.Add((&snapshot{index: 4, runs: []termRun{{0, 1}, {2, 5}}, state: []byte("x")}).encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := decodeSnapshot(b)
		if err != nil {
			return
		}
		again, err := decodeSnapshot(s.encode())
		if err != nil || !reflect.DeepEqual(again, s) {
			t.Fatalf("not stable: %+v then %+v, %v", s, again, err)
		}
		if int64(len(s.expand())) != s.index+1 {
			t.Fatalf("expand gave %d terms for index %d", len(s.expand()), s.index)
		}
	})
}

// Every heartbeat resends the current chunk, and the network may duplicate
// answers. Only an answer that shows progress may send the next chunk, or
// each resend and each copy starts a chain of its own (BUGS.md S7).
func TestSnapshotChunksDoNotMultiply(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	for i := range 60 { // several 512-byte segments, so whole ones can go
		c.propose(1, fmt.Sprintf("command %d", i))
		c.run(cutOff(3))
	}
	l := c.r(1)
	l.SaveSnapshot(l.Committed()-1, bytes.Repeat([]byte("s"), 3000))
	c.settle(c.nodes[1])
	l.Compact(l.Committed())
	if l.LogStart() == 0 {
		t.Fatal("nothing compacted")
	}
	if l.terms.from != l.LogStart()-1 { // BUGS.md L1
		t.Fatalf("leader keeps one term per entry from %d, log starts at %d", l.terms.from, l.LogStart())
	}
	for range 5 { // resends while node 3 is away; each would start a chain
		c.now += sys.Instant(50 * time.Millisecond)
		l.OnTimer(TimerHeartbeat)
	}
	chunks, copies := 0, map[*byte]bool{}
	for limit := 0; len(c.queue) > 0; limit++ {
		if limit == 5000 {
			t.Fatalf("still sending after %d chunks", chunks)
		}
		m := c.queue[0]
		switch m.raw[1] {
		case msgInstallSnapshot:
			chunks++
		case msgInstallResponse:
			if !copies[&m.raw[0]] { // every answer arrives twice, the copy last
				dup := slices.Clone(m.raw)
				copies[&dup[0]] = true
				c.queue = append(c.queue, tmsg{m.from, m.to, dup})
			}
		}
		c.step(0)
	}
	if c.r(3).LastIndex() != l.LastIndex() {
		t.Fatalf("node 3 holds %d, leader %d", c.r(3).LastIndex(), l.LastIndex())
	}
	if f := c.r(3); f.terms.from != f.LogStart()-1 {
		t.Fatalf("after install node 3 keeps one term per entry from %d, log starts at %d", f.terms.from, f.LogStart())
	}
	if chunks > 3000/100+10 {
		t.Fatalf("%d chunks sent for a snapshot of about 30", chunks)
	}
}

// A transfer that takes longer than the leader takes between snapshots
// still ends. Before BUGS.md S21 every newer snapshot started it again from
// its first byte, so a follower that needed one never caught up while the
// leader kept writing (simulator seed 1668, with a snapshot per entry).
func TestSnapshotTransferOutlivesNewerSnapshots(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	for i := range 60 {
		c.propose(1, fmt.Sprintf("command %d", i))
		c.run(cutOff(3))
	}
	l := c.r(1)
	snap := func() { // what the broker does after it applies entries, with a snapshot per entry
		l.SaveSnapshot(l.Committed()-1, bytes.Repeat([]byte("s"), 3000))
		l.Compact(l.Committed())
	}
	snap()
	if l.LogStart() == 0 {
		t.Fatal("nothing compacted")
	}
	l.OnTimer(TimerHeartbeat)
	c.settle(c.nodes[1])
	chunks, writes := 0, 0
	for step := 0; len(c.queue) > 0; step++ {
		if step == 20000 {
			t.Fatalf("node 3 holds %d, installing %v, after %d chunks and %d writes", c.r(3).LastIndex(), c.r(3).Installing(), chunks, writes)
		}
		m := c.queue[0]
		c.step(0)
		switch m.raw[1] {
		case msgInstallSnapshot:
			chunks++
		case msgInstallResponse: // the leader writes on while node 3 takes each chunk
			writes++
			c.propose(1, fmt.Sprintf("later %d", writes))
		}
		if l.Committed()-1 > l.SnapshotIndex() {
			snap()
		}
	}
	if f := c.r(3); f.Installing() || f.LastIndex() != l.LastIndex() {
		t.Fatalf("node 3 holds %d, installing %v; the leader %d", f.LastIndex(), f.Installing(), l.LastIndex())
	}
	if chunks > 60 { // one transfer is about 26
		t.Fatalf("%d chunks sent: the transfer started again", chunks)
	}
	snap() // node 3 holds the log past the snapshot: the leader compacts again
	if l.LogStart() != l.SnapshotIndex()+1 && l.LogStart() < l.LastIndex()-20 {
		t.Fatalf("the leader's log still starts at %d, snapshot %d, last %d", l.LogStart(), l.SnapshotIndex(), l.LastIndex())
	}
}

// A transfer that no hold protects starts again once the leader compacts.
// The caller compacts as its newest snapshot allows (the broker's
// compactTo), and an older snapshot may need entries from below the new log
// start. The first fix of S21 went on with the older snapshot and sent it
// with the newer cut (BUGS.md S22, simulator seeds 690, 916 and 4079).
func TestSnapshotTransferStartsAgainAfterCompaction(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.startAll()
	c.elect(1)
	for i := range 60 {
		c.propose(1, fmt.Sprintf("command %d", i))
		c.run(cutOff(3))
	}
	l := c.r(1)
	for range 8 { // node 3, away, no longer holds the log; node 2 keeps the leader
		c.now += sys.Instant(50 * time.Millisecond)
		l.OnTimer(TimerHeartbeat)
		c.run(cutOff(3))
	}
	l.SaveSnapshot(l.Committed()-1, []byte("first"))
	first := l.SnapshotIndex()
	l.Compact(25) // what the caller keeps for this snapshot
	cut := l.LogStart()
	l.OnTimer(TimerHeartbeat) // the snapshot goes to node 3, which is away
	c.run(cutOff(3))
	c.propose(1, "later")
	c.run(cutOff(3))
	l.SaveSnapshot(l.Committed()-1, []byte("second"))
	l.Compact(first) // the newer snapshot needs less of the log
	if l.LogStart() <= cut || l.LogStart() > first+1 {
		t.Fatalf("setup: the log starts at %d, before at %d; the first snapshot is at %d", l.LogStart(), cut, first)
	}
	l.OnTimer(TimerHeartbeat)
	c.run(nil) // node 3 is back
	f := c.r(3)
	if f.SnapshotIndex() == first && f.LogStart() > cut {
		t.Fatalf("node 3 has the snapshot at %d with its log from %d; the leader's log started at %d while that snapshot was its newest", first, f.LogStart(), cut)
	}
	if f.Installing() || f.LastIndex() != l.LastIndex() {
		t.Fatalf("node 3 holds %d, installing %v; the leader %d", f.LastIndex(), f.Installing(), l.LastIndex())
	}
}

// runsOf compresses terms[0..index] into runs.
func runsOf(terms []int64, index int64) []termRun {
	var runs []termRun
	for i := int64(0); i <= index; i++ {
		if len(runs) == 0 || runs[len(runs)-1].term != terms[i] {
			runs = append(runs, termRun{i, terms[i]})
		}
	}
	return runs
}

// expand returns the term of every entry 0..s.index.
func (s *snapshot) expand() []int64 { t := termsOf(s.runs, s.index, 0); return t.tail }
