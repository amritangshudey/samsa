package storage

import (
	"bytes"
	"testing"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// FuzzRecovery opens a log whose only segment is arbitrary bytes. Whatever
// they are, recovery must not panic, must leave a log whose every offset is
// readable, and must be idempotent.
func FuzzRecovery(f *testing.F) {
	good := appendFrame(appendFrame(nil, 0, 2, []byte("hello")), 2, 1, []byte("world"))
	f.Add(good)
	f.Add(good[:len(good)-3])
	f.Add([]byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
		disk.MkdirAll("d")
		file, _ := disk.Open(segName("d", 0, ".log"), sys.Create)
		file.WriteAt(data, 0)

		var ends [2]int64
		for pass := range ends {
			l, err := Open(disk, "d", Config{MaxPayloadBytes: 1024})
			if err != nil {
				t.Fatalf("pass %d: open: %v", pass, err)
			}
			ends[pass] = l.EndOffset()
			for off := int64(0); off < l.EndOffset(); {
				batches, err := l.Read(off, 1<<20)
				if err != nil || len(batches) == 0 {
					t.Fatalf("pass %d: read at %d of %d: %v", pass, off, l.EndOffset(), err)
				}
				for _, b := range batches {
					off += int64(b.Count)
				}
			}
			l.Close()
		}
		if ends[0] != ends[1] {
			t.Fatalf("recovery not idempotent: end %d then %d", ends[0], ends[1])
		}
	})
}

// FuzzDecodeIndex checks that the index decoder never panics and that
// anything it accepts re-encodes to the same bytes.
func FuzzDecodeIndex(f *testing.F) {
	f.Add(encodeIndex([]indexEntry{{0, 0}, {3, 90}}), int64(200))
	f.Add([]byte{}, int64(0))
	f.Add([]byte{0x53, 0x41, 0x49, 0x58, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}, int64(10))
	f.Fuzz(func(t *testing.T, data []byte, segSize int64) {
		entries, err := decodeIndex(data, segSize)
		if err != nil {
			return
		}
		if !bytes.Equal(encodeIndex(entries), data) {
			t.Fatalf("accepted index does not round-trip: %x", data)
		}
	})
}
