package storage

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"slices"
	"testing"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

var crashSeeds = flag.Int("crash-seeds", 300, "number of seeds for the crash property test")

// rec is what the model remembers about one append.
type rec struct {
	base    int64
	count   uint32
	payload []byte
}

// crashRun drives one log through random appends, syncs, reads, clean
// reopens and crashes at random disk operations, and checks it against a
// model after every reopen. It returns the first violated invariant.
//
// The invariants:
//  1. no record below a DurableOffset that the log reported is ever lost;
//  2. whatever the log returns is byte-identical to what was appended there;
//  3. offsets are contiguous from StartOffset to EndOffset;
//  4. reopening without a crash changes nothing.
func crashRun(seed uint64, mutate func(*Config)) error {
	rng := simsys.NewRand(seed)
	disk := simsys.NewDisk(rng.Fork(), simsys.DiskConfig{BlockSize: 1 + rng.Intn(40), SyncErrorRate: 0.002})
	cfg := Config{
		SegmentBytes:       int64(150 + rng.Intn(600)),
		IndexIntervalBytes: int64(1 + rng.Intn(120)),
		MaxPayloadBytes:    64,
		SyncBytes:          []int64{0, -1, 100}[rng.Intn(3)],
	}
	if mutate != nil {
		mutate(&cfg)
	}
	fixed := rng.Chance(0.5) // same-sized frames make stale bytes line up with new ones

	var (
		model   []rec // every append not known to be lost, in offset order
		durable int64 // highest DurableOffset the log has reported
		l       *Log
	)

	// verify compares the freshly opened log with the model.
	verify := func() error {
		if l.EndOffset() < durable {
			return fmt.Errorf("invariant 1: end offset %d is below acknowledged durable offset %d", l.EndOffset(), durable)
		}
		if l.DurableOffset() != l.EndOffset() {
			return fmt.Errorf("after open, durable offset %d != end offset %d", l.DurableOffset(), l.EndOffset())
		}
		off, i := l.StartOffset(), 0
		for off < l.EndOffset() {
			batches, err := l.Read(off, rng.Intn(200))
			if err != nil {
				return fmt.Errorf("read at %d: %w", off, err)
			}
			if len(batches) == 0 {
				return fmt.Errorf("invariant 3: nothing readable at %d, end is %d", off, l.EndOffset())
			}
			for _, b := range batches {
				if i >= len(model) {
					return fmt.Errorf("invariant 2: log has a batch at %d that was never appended", b.Base)
				}
				if m := model[i]; b.Base != m.base || b.Count != m.count || !bytes.Equal(b.Payload, m.payload) {
					return fmt.Errorf("invariant 2: at offset %d log has (%d,%d,%q), appended was (%d,%d,%q)",
						off, b.Base, b.Count, b.Payload, m.base, m.count, m.payload)
				}
				off += int64(b.Count)
				i++
			}
		}
		if off != l.EndOffset() {
			return fmt.Errorf("invariant 3: batches end at %d, log says %d", off, l.EndOffset())
		}
		model = model[:i] // appends beyond the end were lost in the crash; they were never durable
		durable = l.EndOffset()
		return nil
	}

	// reopen brings the log back, crashing again during recovery sometimes.
	reopen := func() error {
		for {
			var err error
			if l, err = Open(disk, "log", cfg); err == nil {
				err = verify() // a crash armed during recovery can also land here
			}
			if err == nil {
				return nil
			}
			if !errors.Is(err, simsys.ErrCrashed) && !errors.Is(err, simsys.ErrInjected) {
				return err
			}
			disk.Restart()
			if rng.Chance(0.3) {
				disk.CrashAfter(1 + rng.Intn(30))
			}
		}
	}

	if err := reopen(); err != nil {
		return err
	}
	for range 400 {
		if rng.Chance(0.04) {
			disk.CrashAfter(1 + rng.Intn(12))
		}
		var err error
		switch r := rng.Intn(100); {
		case r < 70:
			size := 24
			if !fixed {
				size = rng.Intn(65)
			}
			p := make([]byte, size)
			for i := range p {
				p[i] = byte(rng.Uint64())
			}
			m := rec{base: l.EndOffset(), count: uint32(1 + rng.Intn(3)), payload: p}
			model = append(model, m) // before the call: a failed append may still reach the disk
			var base int64
			if base, err = l.Append(p, m.count); err == nil && base != m.base {
				return fmt.Errorf("append returned base %d, want %d", base, m.base)
			}
		case r < 80:
			err = l.Sync()
		case r < 95:
			if n := len(model); n > 0 {
				m := model[rng.Intn(n)]
				var got []Batch
				if got, err = l.Read(m.base+int64(rng.Intn(int(m.count))), 0); err == nil &&
					(len(got) == 0 || got[0].Base != m.base || !bytes.Equal(got[0].Payload, m.payload)) {
					return fmt.Errorf("invariant 2: live read at %d returned %+v", m.base, got)
				}
			}
		default: // clean close and reopen: invariant 4
			end := l.EndOffset()
			if err = l.Sync(); err == nil {
				l.Close()
				if err := reopen(); err != nil {
					return err
				}
				if l.EndOffset() != end {
					return fmt.Errorf("invariant 4: clean reopen moved end offset from %d to %d", end, l.EndOffset())
				}
			}
		}
		if err == nil {
			durable = max(durable, l.DurableOffset())
			continue
		}
		if !errors.Is(err, simsys.ErrCrashed) && !errors.Is(err, simsys.ErrInjected) {
			return fmt.Errorf("unexpected error: %w", err)
		}
		// A crash, or a failed sync after which the node must stop.
		durable = max(durable, l.DurableOffset())
		disk.Restart()
		if err := reopen(); err != nil {
			return err
		}
	}
	return nil
}

func TestCrashRecoveryProperty(t *testing.T) {
	for seed := range uint64(*crashSeeds) {
		if err := crashRun(seed, nil); err != nil {
			t.Fatalf("seed %d: %v\nreproduce: go test ./internal/storage -run 'TestCrashSeed' -seed %d", seed, err, seed)
		}
	}
}

var oneSeed = flag.Uint64("seed", 0, "seed for TestCrashSeed")

// TestCrashSeed replays a single seed of the crash property test.
func TestCrashSeed(t *testing.T) {
	if err := crashRun(*oneSeed, nil); err != nil {
		t.Fatalf("seed %d: %v", *oneSeed, err)
	}
}

// A test that cannot fail proves nothing. Each case removes one step of the
// crash-safety protocol and requires the property test to notice.
func TestCrashPropertyCatchesBrokenProtocol(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"segment created without directory sync", func(c *Config) { c.skipRollDirSync = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for seed := range uint64(2000) {
				if err := crashRun(seed, tt.mutate); err != nil {
					t.Logf("caught at seed %d: %v", seed, err)
					return
				}
			}
			t.Fatal("2000 seeds passed with the protocol broken: the property test is too weak")
		})
	}
}

// The sync after truncating a torn tail is what stops old bytes from coming
// back. The random property test reaches the needed sequence too rarely to
// rely on, so this test builds it directly: a torn frame followed by a whole
// one, recovery, one new unsynced append of the same size, then a crash.
// Without the sync, some crash outcome splices the new frame in front of the
// old whole one and a record that recovery had discarded reappears.
func TestRecoverySyncPreventsResurrection(t *testing.T) {
	frame := func(base int64, p string) []byte { return appendFrame(nil, base, 1, []byte(p)) }
	resurrected := func(skip bool) (found bool) {
		for seed := range uint64(400) {
			disk := simsys.NewDisk(simsys.NewRand(seed), simsys.DiskConfig{BlockSize: 8})
			disk.MkdirAll("d")
			f, _ := disk.Open(segName("d", 0, ".log"), sys.Create)
			f.WriteAt(slices.Concat(frame(0, "zero"), flip(frame(1, "lost"), headerSize), frame(2, "gone")), 0)
			f.Sync()
			disk.SyncDir("d")

			cfg := Config{SyncBytes: -1, skipRecoverySync: skip}
			l, err := Open(disk, "d", cfg)
			if err != nil || l.EndOffset() != 1 {
				t.Fatalf("setup: end %d, %v", l.EndOffset(), err)
			}
			l.Append([]byte("new!"), 1)
			disk.Restart()

			l, err = Open(disk, "d", cfg)
			if err != nil {
				t.Fatal(err)
			}
			if l.EndOffset() > 2 { // offset 2 was never appended in this life of the log
				found = true
			}
		}
		return found
	}
	if resurrected(false) {
		t.Error("a discarded record came back although recovery synced its truncation")
	}
	if !resurrected(true) {
		t.Error("no resurrection even without the sync: this test proves nothing")
	}
}
