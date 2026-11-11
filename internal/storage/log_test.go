package storage

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/amritangshudey/samsa/internal/realsys"
	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// disks runs a test once against the simulated disk and once against real
// files, so both implementations are held to the same behaviour.
func disks(t *testing.T, test func(t *testing.T, disk sys.Disk)) {
	t.Run("sim", func(t *testing.T) {
		test(t, simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{}))
	})
	t.Run("real", func(t *testing.T) {
		d, err := realsys.OpenDisk(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		test(t, d)
	})
}

func open(t *testing.T, disk sys.Disk, cfg Config) *Log {
	t.Helper()
	l, err := Open(disk, "topic-0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func payload(i int) []byte { return fmt.Appendf(nil, "record-%04d", i) }

// small forces frequent rolls and index entries.
var small = Config{SegmentBytes: 200, IndexIntervalBytes: 60, MaxPayloadBytes: 100}

func TestAppendReadAcrossSegmentsAndReopen(t *testing.T) {
	disks(t, func(t *testing.T, disk sys.Disk) {
		l := open(t, disk, small)
		const n = 100
		next := int64(0)
		var bases []int64
		for i := range n {
			count := uint32(i%3 + 1)
			base, err := l.Append(payload(i), count)
			if err != nil {
				t.Fatal(err)
			}
			if base != next {
				t.Fatalf("append %d got base %d, want %d", i, base, next)
			}
			bases = append(bases, base)
			next += int64(count)
		}
		if len(l.segs) < 5 {
			t.Fatalf("expected many segments, got %d", len(l.segs))
		}

		check := func(l *Log) {
			t.Helper()
			if l.EndOffset() != next || l.DurableOffset() != next || l.StartOffset() != 0 {
				t.Fatalf("start/end/durable = %d/%d/%d, want 0/%d/%d", l.StartOffset(), l.EndOffset(), l.DurableOffset(), next, next)
			}
			for i, base := range bases {
				// Every offset inside a batch must find that batch.
				for off := base; off < base+int64(i%3+1); off++ {
					got, err := l.Read(off, 1)
					if err != nil {
						t.Fatalf("read %d: %v", off, err)
					}
					if len(got) != 1 || got[0].Base != base || !bytes.Equal(got[0].Payload, payload(i)) {
						t.Fatalf("read %d = %+v, want batch %d at base %d", off, got, i, base)
					}
				}
			}
			if got, err := l.Read(next, 100); err != nil || len(got) != 0 {
				t.Fatalf("read at end = %v, %v; want nothing", got, err)
			}
			for _, off := range []int64{-1, next + 1} {
				if _, err := l.Read(off, 100); !errors.Is(err, ErrOffsetOutOfRange) {
					t.Fatalf("read %d: got %v, want ErrOffsetOutOfRange", off, err)
				}
			}
		}
		check(l)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		check(open(t, disk, small))
	})
}

func TestReadHonoursMaxBytes(t *testing.T) {
	disks(t, func(t *testing.T, disk sys.Disk) {
		l := open(t, disk, Config{})
		for i := range 10 {
			l.Append(payload(i), 1) // 11 bytes each
		}
		tests := []struct{ maxBytes, want int }{
			{0, 1}, // the first batch is always returned
			{11, 1},
			{21, 1},
			{22, 2},
			{1000, 10},
		}
		for _, tt := range tests {
			got, err := l.Read(0, tt.maxBytes)
			if err != nil || len(got) != tt.want {
				t.Errorf("Read(0, %d) = %d batches, %v; want %d", tt.maxBytes, len(got), err, tt.want)
			}
		}
	})
}

func TestAppendRejects(t *testing.T) {
	l := open(t, simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{}), small)
	if _, err := l.Append(make([]byte, 101), 1); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversized payload: got %v, want ErrTooLarge", err)
	}
	if _, err := l.Append([]byte("x"), 0); err == nil {
		t.Error("zero count accepted")
	}
	// Neither rejection may poison the log.
	if _, err := l.Append([]byte("x"), 1); err != nil {
		t.Errorf("valid append after rejections: %v", err)
	}
}

func TestConfigRejects(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
	for _, cfg := range []Config{
		{SegmentBytes: 1 << 33},
		{SegmentBytes: 100, MaxPayloadBytes: 100},
		{MaxPayloadBytes: -1},
	} {
		if _, err := Open(disk, "d", cfg); err == nil {
			t.Errorf("config %+v accepted", cfg)
		}
	}
}

func TestSyncPolicy(t *testing.T) {
	tests := []struct {
		name      string
		syncBytes int64
		// durable offset expected after each of four 31-byte frames
		want [4]int64
	}{
		{"every append", 0, [4]int64{1, 2, 3, 4}},
		{"every 60 bytes", 60, [4]int64{0, 2, 2, 4}},
		{"never", -1, [4]int64{0, 0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := open(t, simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{}), Config{SyncBytes: tt.syncBytes})
			for i, want := range tt.want {
				l.Append(payload(i), 1)
				if got := l.DurableOffset(); got != want {
					t.Errorf("after append %d: durable = %d, want %d", i, got, want)
				}
			}
			if err := l.Sync(); err != nil || l.DurableOffset() != 4 {
				t.Errorf("after Sync: durable = %d, %v; want 4", l.DurableOffset(), err)
			}
		})
	}
}

func TestSyncFailurePoisonsLog(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
	l := open(t, disk, Config{SyncBytes: -1})
	l.Append([]byte("a"), 1)
	disk.SetSyncErrorRate(1)
	if err := l.Sync(); !errors.Is(err, simsys.ErrInjected) {
		t.Fatalf("got %v, want the injected sync failure", err)
	}
	disk.SetSyncErrorRate(0) // the disk recovers; the log must not
	if err := l.Sync(); !errors.Is(err, simsys.ErrInjected) {
		t.Errorf("retried sync: got %v, want the original failure", err)
	}
	if _, err := l.Append([]byte("b"), 1); !errors.Is(err, simsys.ErrInjected) {
		t.Errorf("append after failed sync: got %v, want the original failure", err)
	}
	if _, err := l.Read(0, 1); !errors.Is(err, simsys.ErrInjected) {
		t.Errorf("read after failed sync: got %v, want the original failure", err)
	}
	if l.DurableOffset() != 0 {
		t.Errorf("durable offset advanced to %d by a failed sync", l.DurableOffset())
	}
}

func TestRecoveryTruncatesTornTail(t *testing.T) {
	frame := func(base int64, p string) []byte { return appendFrame(nil, base, 1, []byte(p)) }
	good := append(frame(0, "zero"), frame(1, "one")...)
	tests := []struct {
		name    string
		tail    []byte
		wantEnd int64
	}{
		{"clean", nil, 2},
		{"half a header", frame(2, "two")[:7], 2},
		{"half a payload", frame(2, "two")[:headerSize+1], 2},
		{"flipped payload bit", flip(frame(2, "two"), headerSize), 2},
		{"flipped header bit", flip(frame(2, "two"), 9), 2},
		{"huge length", []byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1}, 2},
		{"zero count", appendFrame(nil, 2, 0, []byte("x")), 2},
		{"valid frame, wrong offset", frame(7, "seven"), 2},
		{"valid frame after garbage", append([]byte("garbage!"), frame(2, "two")...), 2},
		{"another good frame", frame(2, "two"), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
			disk.MkdirAll("d")
			f, _ := disk.Open(segName("d", 0, ".log"), sys.Create)
			f.WriteAt(append(bytes.Clone(good), tt.tail...), 0)

			for pass := range 2 { // recovering twice must equal recovering once
				l, err := Open(disk, "d", small)
				if err != nil {
					t.Fatal(err)
				}
				if l.EndOffset() != tt.wantEnd || l.DurableOffset() != tt.wantEnd {
					t.Fatalf("pass %d: end/durable = %d/%d, want %d", pass, l.EndOffset(), l.DurableOffset(), tt.wantEnd)
				}
				got, err := l.Read(0, 1000)
				if err != nil || len(got) != int(tt.wantEnd) {
					t.Fatalf("pass %d: read %d batches, %v; want %d", pass, len(got), err, tt.wantEnd)
				}
				l.Close()
			}
			// And the log must be appendable right where it was cut.
			l := open2(t, disk)
			if base, err := l.Append([]byte("next"), 1); err != nil || base != tt.wantEnd {
				t.Fatalf("append after recovery = %d, %v; want %d", base, err, tt.wantEnd)
			}
		})
	}
}

func open2(t *testing.T, disk sys.Disk) *Log {
	t.Helper()
	l, err := Open(disk, "d", small)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func flip(b []byte, i int) []byte {
	b[i] ^= 0x40
	return b
}

func TestIndexFileIsRebuiltWhenUnusable(t *testing.T) {
	damage := map[string]func(disk sys.Disk, name string){
		"missing":   func(disk sys.Disk, name string) { disk.Remove(name) },
		"truncated": func(disk sys.Disk, name string) { f, _ := disk.Open(name, sys.ReadWrite); f.Truncate(5) },
		"bit flip": func(disk sys.Disk, name string) {
			f, _ := disk.Open(name, sys.ReadWrite)
			f.WriteAt([]byte{0xff}, 9)
		},
	}
	for name, hurt := range damage {
		t.Run(name, func(t *testing.T) {
			disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
			l := open2(t, disk)
			for i := range 40 {
				l.Append(payload(i), 1)
			}
			l.Close()
			idx := segName("d", 0, ".index")
			before, err := l.readFile(idx)
			if err != nil {
				t.Fatal(err)
			}
			hurt(disk, idx)

			l = open2(t, disk)
			for i := range 40 {
				got, err := l.Read(int64(i), 1)
				if err != nil || !bytes.Equal(got[0].Payload, payload(i)) {
					t.Fatalf("read %d after index damage: %v, %v", i, got, err)
				}
			}
			after, err := l.readFile(idx)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("index was not rebuilt identically: %v", err)
			}
		})
	}
}

func TestCorruptSyncedDataIsReportedNotReturned(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
	l := open2(t, disk)
	for i := range 40 {
		l.Append(payload(i), 1)
	}
	// Damage a payload byte in the first (sealed) segment behind the log's back.
	f, _ := disk.Open(segName("d", 0, ".log"), sys.ReadWrite)
	f.WriteAt([]byte{'X'}, headerSize+2)
	if _, err := l.Read(0, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read of damaged frame: got %v, want ErrCorrupt", err)
	}
	// With the index gone as well, reopening must refuse, not truncate
	// synced data away.
	l.Close()
	disk.Remove(segName("d", 0, ".index"))
	if _, err := Open(disk, "d", small); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open with damaged sealed segment: got %v, want ErrCorrupt", err)
	}
}

func TestIndexRoundTripAndRejects(t *testing.T) {
	entries := []indexEntry{{0, 0}, {5, 100}, {9, 220}}
	enc := encodeIndex(entries)
	got, err := decodeIndex(enc, 300)
	if err != nil || len(got) != 3 || got[2] != entries[2] {
		t.Fatalf("round trip = %v, %v", got, err)
	}
	bad := map[string][]byte{
		"empty":             nil,
		"short":             enc[:len(enc)-1],
		"long":              append(bytes.Clone(enc), 0),
		"bad magic":         flip(bytes.Clone(enc), 0),
		"bad count":         flip(bytes.Clone(enc), 7),
		"bad crc":           flip(bytes.Clone(enc), len(enc)-1),
		"not increasing":    encodeIndex([]indexEntry{{5, 100}, {5, 200}}),
		"position past end": encodeIndex([]indexEntry{{0, 300}}),
	}
	for name, b := range bad {
		if _, err := decodeIndex(b, 300); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestTruncateSuffix(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
	cfg := Config{SegmentBytes: 100, MaxPayloadBytes: 40, SyncBytes: -1} // a few records per segment
	l, err := Open(disk, "log", cfg)
	if err != nil {
		t.Fatal(err)
	}
	appendN := func(from, n int, tag string) {
		t.Helper()
		for i := from; i < from+n; i++ {
			if _, err := l.Append([]byte(fmt.Sprintf("%s-%02d", tag, i)), 1); err != nil {
				t.Fatal(err)
			}
		}
	}
	// check reads the whole log and compares it with want.
	check := func(want ...string) {
		t.Helper()
		var got []string
		for off := l.StartOffset(); off < l.EndOffset(); {
			batches, err := l.Read(off, 1<<20)
			if err != nil {
				t.Fatalf("read at %d: %v", off, err)
			}
			for _, b := range batches {
				got = append(got, string(b.Payload))
				off = b.Base + int64(b.Count)
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("log holds %v, want %v", got, want)
		}
	}
	appendN(0, 20, "old")
	if len(l.segs) < 3 {
		t.Fatalf("test needs several segments, got %d", len(l.segs))
	}
	if err := l.TruncateSuffix(21); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("truncate past the end: %v", err)
	}
	if err := l.TruncateSuffix(20); err != nil || l.EndOffset() != 20 {
		t.Fatalf("truncate at the end must do nothing: %v, end %d", err, l.EndOffset())
	}
	// Across segment boundaries, into the middle of an earlier segment.
	if err := l.TruncateSuffix(4); err != nil {
		t.Fatal(err)
	}
	if l.EndOffset() != 4 || l.DurableOffset() != 4 {
		t.Fatalf("after truncate: end %d, durable %d", l.EndOffset(), l.DurableOffset())
	}
	appendN(4, 10, "new")
	want := []string{"old-00", "old-01", "old-02", "old-03"}
	for i := 4; i < 14; i++ {
		want = append(want, fmt.Sprintf("new-%02d", i))
	}
	check(want...)
	// A power loss now may lose the unsynced new records, and nothing else:
	// in particular no old record past the cut comes back.
	disk.Restart()
	if l, err = Open(disk, "log", cfg); err != nil {
		t.Fatal(err)
	}
	if l.EndOffset() < 4 {
		t.Fatalf("records below the cut were lost: end %d", l.EndOffset())
	}
	check(want[:l.EndOffset()]...)
	// Everything can go.
	if err := l.TruncateSuffix(0); err != nil || l.EndOffset() != 0 {
		t.Fatalf("truncate to empty: %v, end %d", err, l.EndOffset())
	}
	appendN(0, 1, "again")
	check("again-00")
}
