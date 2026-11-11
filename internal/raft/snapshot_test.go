package raft

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
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
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip: %+v, %v", got, err)
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
