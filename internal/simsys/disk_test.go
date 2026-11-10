package simsys

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/amritangshudey/samsa/internal/sys"
)

func mustOpen(t *testing.T, d *Disk, name string, flag sys.OpenFlag) sys.File {
	t.Helper()
	f, err := d.Open(name, flag)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func readAll(t *testing.T, d *Disk, name string) []byte {
	t.Helper()
	f := mustOpen(t, d, name, sys.ReadOnly)
	n, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, n)
	if _, err := f.ReadAt(p, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return p
}

// Whatever the seed, data that was synced under a synced name survives.
func TestSyncedDataSurvives(t *testing.T) {
	for seed := uint64(0); seed < 200; seed++ {
		d := NewDisk(NewRand(seed), DiskConfig{BlockSize: 4})
		f := mustOpen(t, d, "a", sys.Create)
		f.WriteAt([]byte("durable!"), 0)
		if err := errors.Join(f.Sync(), d.SyncDir(".")); err != nil {
			t.Fatal(err)
		}
		f.WriteAt([]byte("unsynced tail that may tear"), 8)
		d.Restart()
		got := readAll(t, d, "a")
		if !bytes.HasPrefix(got, []byte("durable!")) {
			t.Fatalf("seed %d: synced prefix lost, file is %q", seed, got)
		}
	}
}

// Across seeds, each outcome the contract permits must actually occur;
// otherwise the model is kinder than it claims.
func TestCrashOutcomesAllOccur(t *testing.T) {
	var lostAll, keptAll, torn, vanished, renamed, notRenamed bool
	for seed := uint64(0); seed < 500; seed++ {
		d := NewDisk(NewRand(seed), DiskConfig{BlockSize: 4})
		f := mustOpen(t, d, "a", sys.Create)
		d.SyncDir(".")
		data := []byte("0123456789abcdefghij")
		f.WriteAt(data, 0)
		g := mustOpen(t, d, "b", sys.Create) // name never synced
		g.WriteAt(data, 0)
		g.Sync()
		d.Restart()

		switch got := readAll(t, d, "a"); {
		case len(got) == 0:
			lostAll = true
		case bytes.Equal(got, data):
			keptAll = true
		default:
			torn = true
		}
		if _, err := d.Open("b", sys.ReadOnly); errors.Is(err, sys.ErrNotExist) {
			vanished = true
		} else if !bytes.Equal(readAll(t, d, "b"), data) {
			t.Fatalf("seed %d: b exists but its synced data is damaged", seed)
		}

		// Rename is atomic: afterwards exactly one of the names exists.
		d.SyncDir(".")
		if _, err := d.Open("a", sys.ReadOnly); err != nil {
			t.Fatal(err)
		}
		if err := d.Rename("a", "c"); err != nil {
			t.Fatal(err)
		}
		d.Restart()
		_, errA := d.Open("a", sys.ReadOnly)
		_, errC := d.Open("c", sys.ReadOnly)
		switch {
		case errA == nil && errC != nil:
			notRenamed = true
		case errA != nil && errC == nil:
			renamed = true
		default:
			t.Fatalf("seed %d: rename not atomic: a: %v, c: %v", seed, errA, errC)
		}
	}
	for name, seen := range map[string]bool{
		"all unsynced data lost": lostAll, "all unsynced data kept": keptAll, "torn": torn,
		"unsynced name vanished": vanished, "rename survived": renamed, "rename lost": notRenamed,
	} {
		if !seen {
			t.Errorf("outcome never occurred in 500 seeds: %s", name)
		}
	}
}

func TestCrashPointStopsEverything(t *testing.T) {
	d := NewDisk(NewRand(1), DiskConfig{})
	f := mustOpen(t, d, "a", sys.Create)
	d.CrashAfter(2)
	if _, err := f.WriteAt([]byte("x"), 0); err != nil {
		t.Fatalf("operation before the crash point failed: %v", err)
	}
	if _, err := f.WriteAt([]byte("y"), 1); !errors.Is(err, ErrCrashed) {
		t.Fatalf("operation at the crash point: got %v, want ErrCrashed", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrCrashed) {
		t.Fatalf("operation after the crash: got %v, want ErrCrashed", err)
	}
	d.Restart()
	if _, err := f.Size(); !errors.Is(err, ErrCrashed) {
		t.Fatalf("handle from before the restart still works: %v", err)
	}
}

func TestSameSeedSameDisk(t *testing.T) {
	run := func() []byte {
		d := NewDisk(NewRand(42), DiskConfig{BlockSize: 2})
		for _, name := range []string{"x", "y", "z"} {
			f := mustOpen(t, d, name, sys.Create)
			f.WriteAt([]byte("some unsynced content "+name), 0)
		}
		d.SyncDir(".")
		d.Restart()
		var all []byte
		for _, name := range []string{"x", "y", "z"} {
			all = append(all, readAll(t, d, name)...)
		}
		return all
	}
	if a, b := run(), run(); !bytes.Equal(a, b) {
		t.Fatalf("same seed gave different disks:\n%q\n%q", a, b)
	}
}

func TestInjectedSyncErrorSyncsNothing(t *testing.T) {
	d := NewDisk(NewRand(7), DiskConfig{SyncErrorRate: 1})
	f := mustOpen(t, d, "a", sys.Create)
	f.WriteAt([]byte("data"), 0)
	if err := f.Sync(); !errors.Is(err, ErrInjected) {
		t.Fatalf("got %v, want ErrInjected", err)
	}
	if err := d.SyncDir("."); !errors.Is(err, ErrInjected) {
		t.Fatalf("got %v, want ErrInjected", err)
	}
	if got := d.files["a"].durable; len(got) != 0 {
		t.Fatalf("failed sync made %q durable", got)
	}
	if len(d.journal) == 0 {
		t.Fatal("failed SyncDir committed the journal")
	}
}

func TestPathsCannotEscape(t *testing.T) {
	d := NewDisk(NewRand(1), DiskConfig{})
	if err := d.MkdirAll("/abs/../../up"); err != nil {
		t.Fatal(err)
	}
	names, err := d.List(".")
	if err != nil || len(names) != 1 || names[0] != "up" {
		t.Fatalf("List = %v, %v; want [up]", names, err)
	}
}
