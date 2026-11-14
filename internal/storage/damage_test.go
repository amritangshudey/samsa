package storage

import (
	"testing"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// frameStarts returns the file position and first offset of every frame of
// a sealed segment.
func frameStarts(t *testing.T, l *Log, i int) (pos []int64, base []int64) {
	t.Helper()
	s := l.segs[i]
	f, err := l.disk.Open(segName(l.dir, s.base, ".log"), sys.ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for p := int64(0); p < s.size; {
		h, err := readHeader(f, p, s.size, l.cfg.MaxPayloadBytes)
		if err != nil {
			t.Fatal(err)
		}
		pos, base = append(pos, p), append(base, h.base)
		p += headerSize + h.length
	}
	return pos, base
}

// Rot in a sealed segment is damage, found by FirstDamage and by the scrub,
// and a read of it fails; rot is not mistaken for a torn tail, and a torn
// tail is not mistaken for rot.
func TestDamageInSealedSegment(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
	l := open(t, disk, small)
	for i := range 60 {
		if _, err := l.Append(payload(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.segs) < 4 {
		t.Fatalf("only %d segments", len(l.segs))
	}
	if _, found, err := l.FirstDamage(0); err != nil || found {
		t.Fatalf("a healthy log has damage: %v %v", found, err)
	}
	// A full scrub of a healthy log, in small steps, ends with -1.
	var steps int
	for cursor := int64(0); cursor >= 0; steps++ {
		next, _, _, found, err := l.Scrub(cursor, 50)
		if err != nil || found || steps > 1000 {
			t.Fatalf("scrub of a healthy log: next %d found %v err %v after %d steps", next, found, err, steps)
		}
		cursor = next
	}
	if steps < 5 {
		t.Fatalf("scrub finished in %d steps; the budget is not honoured", steps)
	}

	// Flip a byte inside the payload of the third frame of the second segment.
	pos, bases := frameStarts(t, l, 1)
	if len(pos) < 3 {
		t.Fatalf("segment 1 has %d frames", len(pos))
	}
	name := segName(l.dir, l.segs[1].base, ".log")
	if !disk.Corrupt(name, int(pos[2])+headerSize+3, []byte{0xFF}) {
		t.Fatal("could not damage the segment")
	}
	l2 := open(t, disk, small) // reopening does not scan sealed segments that have an index
	off, found, err := l2.FirstDamage(0)
	if err != nil || !found || off != bases[2] {
		t.Fatalf("FirstDamage: offset %d found %v err %v, want %d", off, found, err, bases[2])
	}
	if _, found, _ := l2.FirstDamage(l2.segs[2].base); found {
		t.Fatal("damage reported in a segment that ends before `from`")
	}
	// The scrub finds it from the start, and from just before it.
	for _, cursor := range []int64{0, bases[0], bases[1]} {
		var damage int64
		for c := cursor; ; {
			next, _, d, found, err := l2.Scrub(c, 80)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				damage = d
				break
			}
			if next < 0 {
				t.Fatalf("scrub from %d did not find the damage", cursor)
			}
			c = next
		}
		if damage != bases[2] {
			t.Fatalf("scrub from %d: damage at %d, want %d", cursor, damage, bases[2])
		}
	}
	if _, err := l2.Read(bases[2], 1<<20); err == nil {
		t.Fatal("a read of a damaged frame succeeded")
	}
	if got, err := l2.Read(bases[0], 1); err != nil || len(got) == 0 {
		t.Fatalf("a read before the damage: %v", err)
	}

	// Cutting at the damage leaves a log that can be appended to.
	if err := l2.TruncateSuffix(bases[2]); err != nil {
		t.Fatal(err)
	}
	if _, found, err := l2.FirstDamage(0); err != nil || found {
		t.Fatalf("damage after the cut: %v %v", found, err)
	}
	if _, err := l2.Append(payload(1000), 1); err != nil {
		t.Fatal(err)
	}
}

// Garbage after the last frame of the active segment is a torn write, cut
// at open, never damage.
func TestTornTailIsNotDamage(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(2), simsys.DiskConfig{})
	l := open(t, disk, small)
	for i := range 30 {
		if _, err := l.Append(payload(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	end := l.EndOffset()
	last := segName(l.dir, l.active().base, ".log")
	f, err := disk.Open(last, sys.ReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	size, _ := f.Size()
	if _, err := f.WriteAt([]byte("garbage garbage garbage"), size); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	l2 := open(t, disk, small)
	if _, found, err := l2.FirstDamage(0); err != nil || found || l2.EndOffset() != end {
		t.Fatalf("torn tail: found %v err %v, end %d want %d", found, err, l2.EndOffset(), end)
	}
}
