package sim

import (
	"strings"
	"testing"

	"github.com/amritangshudey/samsa/internal/wire"
)

// batchAt encodes the n records from first as they would be read at base.
func batchAt(first uint64, n int, base int64) []byte {
	recs := make([]wire.Record, n)
	for i := range recs {
		recs[i] = wire.Record{Value: Value(first + uint64(i))}
	}
	b := wire.EncodeBatch(recs)
	wire.SetBatchBaseOffset(b, base)
	return b
}

// TestModel feeds the checker hand-made histories. Each bad one must be
// refused for the stated reason: a checker that accepts everything would
// make the whole simulator worthless.
func TestModel(t *testing.T) {
	tp := TP{"t", 0}
	// Every history starts the same way: topic created, batch A (records
	// 1-2) acknowledged at offset 0, batch B (records 3-5) sent.
	type world struct {
		m    *Model
		a, b uint64
	}
	read := func(w world, offset, floor, hw int64, records []byte) error {
		_, err := w.m.Fetched(Fetch{TP: tp, Offset: offset, Floor: floor, Epoch: w.m.Epoch(), Known: true, MustProgress: true, HW: hw, Records: records})
		return err
	}
	tests := []struct {
		name   string
		strict bool
		run    func(w world) error
		want   string // substring of the error; "" means the history is fine
	}{
		{"good read", true, func(w world) error {
			return read(w, 0, w.m.Floor(tp), 5, append(batchAt(w.a, 2, 0), batchAt(w.b, 3, 2)...))
		}, ""},
		{"acknowledged record missing from the log end", true, func(w world) error {
			return read(w, 0, w.m.Floor(tp), 1, nil)
		}, "I1/I3"},
		{"acknowledged offset holds another record", true, func(w world) error {
			return read(w, 0, 0, 3, batchAt(w.b, 3, 0))
		}, "I1/I3"},
		{"acknowledged record at another offset", true, func(w world) error {
			return read(w, 2, 0, 7, batchAt(w.a, 2, 5))
		}, "I2"},
		{"record moved", true, func(w world) error {
			if err := read(w, 2, 0, 5, batchAt(w.b, 3, 2)); err != nil {
				return err
			}
			return read(w, 5, 0, 8, batchAt(w.b, 3, 5))
		}, "I1/I4"},
		{"gap between batches", true, func(w world) error {
			return read(w, 0, 0, 9, append(batchAt(w.a, 2, 0), batchAt(w.b, 3, 3)...))
		}, "I2"},
		{"first batch does not cover the offset", true, func(w world) error {
			return read(w, 0, 0, 5, batchAt(w.b, 3, 2))
		}, "I2"},
		{"records beyond the high watermark", true, func(w world) error {
			return read(w, 0, 0, 1, batchAt(w.a, 2, 0))
		}, "I2"},
		{"nothing returned below the high watermark", true, func(w world) error {
			return read(w, 0, 0, 2, nil)
		}, "I2"},
		{"record nobody sent", true, func(w world) error {
			return read(w, 2, 0, 3, batchAt(999, 1, 2))
		}, "I4"},
		{"batch split", true, func(w world) error {
			return read(w, 2, 0, 4, batchAt(w.b, 2, 2))
		}, "I4"},
		{"record in the wrong partition", true, func(w world) error {
			w.m.TopicAttempted("u", 1)
			_, err := w.m.Fetched(Fetch{TP: TP{"u", 0}, HW: 2, Records: batchAt(w.a, 2, 0)})
			return err
		}, "I4"},
		{"rejected write is readable", true, func(w world) error {
			if err := w.m.Rejected(w.b, 3); err != nil {
				return err
			}
			return read(w, 2, 0, 5, batchAt(w.b, 3, 2))
		}, "I5"},
		{"read write is then rejected", true, func(w world) error {
			if err := read(w, 2, 0, 5, batchAt(w.b, 3, 2)); err != nil {
				return err
			}
			return w.m.Rejected(w.b, 3)
		}, "I5"},
		{"high watermark goes backwards", true, func(w world) error {
			if err := read(w, 0, 0, 5, batchAt(w.a, 2, 0)); err != nil {
				return err
			}
			return read(w, 0, w.m.Floor(tp), 4, batchAt(w.a, 2, 0))
		}, "I1/I3"},
		{"stale response may show an older high watermark", true, func(w world) error {
			if err := read(w, 0, 0, 5, batchAt(w.a, 2, 0)); err != nil {
				return err
			}
			return read(w, 0, 2, 4, batchAt(w.a, 2, 0)) // sent when the floor was 2
		}, ""},
		{"created topic unknown", true, func(w world) error {
			_, err := w.m.Fetched(Fetch{TP: tp, Known: true, Err: wire.ErrUnknownTopicOrPartition})
			return err
		}, "I1"},
		{"offset below the log end reported out of range", true, func(w world) error {
			_, err := w.m.Fetched(Fetch{TP: tp, Offset: 1, Floor: 2, Known: true, Err: wire.ErrOffsetOutOfRange})
			return err
		}, "I1"},
		{"topic nobody created", true, func(w world) error {
			_, err := w.m.Fetched(Fetch{TP: TP{"nobody", 0}})
			return err
		}, "I4"},
		{"acknowledged record lost at the end", true, func(w world) error {
			w.m.BeginFinal()
			return w.m.CheckFinal()
		}, "I1"},
		{"overlapping acknowledgements", true, func(w world) error {
			return w.m.Acked(w.b, 3, 1, w.m.Epoch())
		}, "I1/I3"},

		// Under a policy that permits loss, a power loss may cut a suffix.
		{"strict: power loss must not lose the acknowledged", true, func(w world) error {
			w.m.PowerLoss()
			return read(w, 0, w.m.Floor(tp), 3, batchAt(w.b, 3, 0))
		}, "I1/I3"},
		{"weak: power loss may lose the acknowledged", false, func(w world) error {
			w.m.PowerLoss()
			if err := read(w, 0, w.m.Floor(tp), 3, batchAt(w.b, 3, 0)); err != nil {
				return err
			}
			w.m.BeginFinal()
			if err := read(w, 0, w.m.Floor(tp), 3, batchAt(w.b, 3, 0)); err != nil {
				return err
			}
			return w.m.CheckFinal()
		}, ""},
		{"weak: a lost record must stay lost", false, func(w world) error {
			w.m.PowerLoss()
			if err := read(w, 0, 0, 3, batchAt(w.b, 3, 0)); err != nil {
				return err
			}
			return read(w, 3, 3, 5, batchAt(w.a, 2, 3))
		}, "I3"},
		{"weak: without a power loss nothing may be lost", false, func(w world) error {
			return read(w, 0, 0, 3, batchAt(w.b, 3, 0))
		}, "I1/I3"},
		{"weak: a later crash does not condemn what an earlier one spared", false, func(w world) error {
			// Epoch 0: C is read at 5. Crash; the log is cut to 2 and D takes
			// offset 2..4 unseen, E is read at 5 in epoch 1. Crash again,
			// nothing lost. Reading D at 2 must not mark E lost.
			c, d, e := w.m.Send(tp, 1), w.m.Send(tp, 3), w.m.Send(tp, 1)
			if err := read(w, 2, 0, 6, append(batchAt(w.b, 3, 2), batchAt(c, 1, 5)...)); err != nil {
				return err
			}
			w.m.PowerLoss()
			if err := read(w, 5, 0, 6, batchAt(e, 1, 5)); err != nil {
				return err
			}
			w.m.PowerLoss()
			return read(w, 2, 0, 6, append(batchAt(d, 3, 2), batchAt(e, 1, 5)...))
		}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewModel(tc.strict)
			m.TopicAttempted("t", 1)
			m.TopicCreated("t", 0)
			a, b := m.Send(tp, 2), m.Send(tp, 3)
			if err := m.Acked(a, 2, 0, 0); err != nil {
				t.Fatal(err)
			}
			err := tc.run(world{m, a, b})
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("a correct history was refused: %v", err)
			case tc.want != "" && err == nil:
				t.Fatal("a bad history was accepted")
			case tc.want != "" && !strings.HasPrefix(err.Error(), tc.want+":"):
				t.Fatalf("refused for the wrong reason: %v (want %s)", err, tc.want)
			}
		})
	}
}
