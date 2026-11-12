package broker

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/amritangshudey/samsa/internal/wire"
)

// sampleState has topics, batches, a group with members, protocols, an
// assignment and committed offsets, and a group with offsets only.
func sampleState(t *testing.T) *state {
	t.Helper()
	s := newState()
	g := &group{generation: 1, protocolType: "consumer", protocol: "range", leader: "a",
		members: []*member{{id: "a", sessionTimeout: 6000, rebalanceTimeout: 9000,
			protocols: []wire.GroupProtocol{{Name: "range", Metadata: []byte("m")}, {Name: "rr"}}}, {id: "b"}}}
	cmds := [][]byte{
		encodeCreateTopic("b", 2),
		encodeCreateTopic("a", 1),
		encodeAppend("a", 0, batch("x", "y")),
		encodeAppend("b", 1, batch("z")),
		encodeAppend("a", 0, batch("w")),
		encodeAppend("a", 0, batchAs(1<<47|3, 2, 0, "i", "j")),
		encodeAppend("a", 0, batchAs(1<<47|3, 2, 2, "k")),
		encodeAppend("b", 0, batchAs(9, 0, 0, "l")),
		encodeAppend("a", 0, batchAs(9, 0, 0, "m")),
		encodeOffsetCommit("solo", -1, "", 5, []offsetEntry{{"a", 0, committedOffset{offset: 2, metadata: "md"}}}),
		encodeGroupGeneration("g", g),
		encodeGroupAssignment("g", 1, []wire.MemberAssignment{{MemberID: "a", Assignment: []byte("x")}, {MemberID: "b", Assignment: []byte{}}}),
		encodeOffsetCommit("g", 1, "a", 7, []offsetEntry{{"a", 0, committedOffset{offset: 3, leaderEpoch: 4}}, {"b", 1, committedOffset{offset: 1}}}),
	}
	for i, cmd := range cmds {
		if res, err := s.apply(int64(i), cmd); err != nil || res.errCode != 0 {
			t.Fatalf("command %d: %+v, %v", i, res, err)
		}
	}
	return s
}

func TestStateEncoding(t *testing.T) {
	for name, s := range map[string]*state{"empty": newState(), "sample": sampleState(t)} {
		b := s.encode()
		got, err := decodeState(b)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.hash() != s.hash() || !bytes.Equal(got.encode(), b) {
			t.Fatalf("%s: state changed through encode and decode", name)
		}
		for cut := range len(b) {
			if _, err := decodeState(b[:cut]); err == nil {
				t.Fatalf("%s: accepted %d of %d bytes", name, cut, len(b))
			}
		}
	}
	// The decoded state must go on behaving like the original.
	s, got := sampleState(t), sampleState(t)
	got, _ = decodeState(got.encode())
	for _, st := range []*state{s, got} {
		if res, _ := st.apply(100, encodeAppend("b", 1, batch("q"))); res.baseOffset != 1 {
			t.Fatalf("append after decode got offset %d", res.baseOffset)
		}
		if res, _ := st.apply(101, encodeOffsetCommit("g", 0, "a", 0, nil)); res.errCode != wire.ErrIllegalGeneration {
			t.Fatalf("a fenced commit after decode: %d", res.errCode)
		}
	}
	if s.hash() != got.hash() {
		t.Fatal("decoded state diverged after more commands")
	}
}

func FuzzDecodeState(f *testing.F) {
	f.Add(newState().encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := decodeState(b)
		if err != nil {
			return
		}
		again, err := decodeState(s.encode())
		if err != nil || again.hash() != s.hash() {
			t.Fatalf("not stable: %v", err)
		}
	})
}

// BenchmarkRestart times a restart after n produced batches, with
// snapshots off and every 1000 entries. With them, the time must stay flat
// as the log grows (docs/design/a1-snapshots.md); without, it grows.
//
//	go test ./internal/broker -run '^$' -bench Restart
func BenchmarkRestart(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		for _, every := range []int64{-1, 1000} {
			b.Run(fmt.Sprintf("entries=%d/snapshot=%d", n, every), func(b *testing.B) {
				// shortcut: the harness wants a *testing.T; a failure in it
				// ends the benchmark with 0 iterations instead of an error.
				h := newHarness(&testing.T{}, Config{SnapshotEvery: every, SegmentBytes: 64 << 10, MaxBatchBytes: 1000})
				h.open(1)
				h.createTopic(1, "t", 1)
				for i := range n {
					h.produce(1, "t", 0, fmt.Sprint(i))
				}
				b.ResetTimer()
				for range b.N {
					h.start()
				}
				b.StopTimer()
				if every > 0 && h.node.applied-1-h.node.raft.SnapshotIndex() > every+1 {
					b.Fatalf("restart replayed %d entries with a snapshot every %d", h.node.applied-1-h.node.raft.SnapshotIndex(), every)
				}
			})
		}
	}
}
