package raft

import (
	"errors"
	"testing"

	"github.com/amritangshudey/samsa/internal/simsys"
	"github.com/amritangshudey/samsa/internal/sys"
)

// hardStateRun saves a rising sequence of states with crashes at random
// disk operations. After every crash the file must hold the last state
// whose save returned, or the one being saved when the power went: never
// an older one and never damage. It returns the first violation.
func hardStateRun(seed uint64, mutate func(*hardStateFile)) error {
	rng := simsys.NewRand(seed)
	disk := simsys.NewDisk(rng.Fork(), simsys.DiskConfig{BlockSize: 1 + rng.Intn(24)})
	if err := disk.MkdirAll("raft"); err != nil {
		return err
	}
	if err := disk.SyncDir("."); err != nil {
		return err
	}
	f := &hardStateFile{disk: disk, dir: "raft"}
	if mutate != nil {
		mutate(f)
	}
	saved := hardState{votedFor: noVote} // the last state whose save returned
	for range 60 {
		next := hardState{term: saved.term + int64(rng.Intn(3)), votedFor: sys.NodeID(rng.Intn(4)) - 1}
		if rng.Chance(0.3) {
			disk.CrashAfter(1 + rng.Intn(8))
		}
		err := f.save(next)
		if err == nil {
			saved = next
			if rng.Chance(0.2) {
				disk.Crash() // power loss between saves loses nothing
				err = simsys.ErrCrashed
			}
		}
		if err == nil {
			continue
		}
		if !errors.Is(err, simsys.ErrCrashed) {
			return err
		}
		disk.Restart()
		got, err := f.load()
		if err != nil {
			return err
		}
		if got != saved && got != next {
			return errors.New("after a crash the file holds a state that is neither the last saved nor the one being saved")
		}
		saved = got
	}
	return nil
}

func TestHardStateSurvivesCrashes(t *testing.T) {
	for seed := range uint64(2000) {
		if err := hardStateRun(seed, nil); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// Each case removes one step of the atomic replace; the crash test must
// notice, or it proves nothing.
func TestHardStateCrashTestCatchesBrokenProtocol(t *testing.T) {
	for name, mutate := range map[string]func(*hardStateFile){
		"no sync before the rename":          func(f *hardStateFile) { f.skipSync = true },
		"no directory sync after the rename": func(f *hardStateFile) { f.skipDirSync = true },
	} {
		caught := false
		for seed := range uint64(2000) {
			if err := hardStateRun(seed, mutate); err != nil {
				t.Logf("%s: caught at seed %d: %v", name, seed, err)
				caught = true
				break
			}
		}
		if !caught {
			t.Errorf("%s: 2000 seeds passed with the protocol broken", name)
		}
	}
}

func TestHardStateFile(t *testing.T) {
	disk := simsys.NewDisk(simsys.NewRand(1), simsys.DiskConfig{})
	if err := disk.MkdirAll("raft"); err != nil {
		t.Fatal(err)
	}
	f := &hardStateFile{disk: disk, dir: "raft"}
	if hs, err := f.load(); err != nil || hs != (hardState{votedFor: noVote}) {
		t.Fatalf("no file: %+v, %v", hs, err)
	}
	want := hardState{term: 7, votedFor: 2}
	if err := f.save(want); err != nil {
		t.Fatal(err)
	}
	if hs, err := f.load(); err != nil || hs != want {
		t.Fatalf("round trip: %+v, %v", hs, err)
	}
	// Damage of any kind is an error, never a guess.
	for name, damage := range map[string]func(sys.File){
		"flipped bit": func(file sys.File) { file.WriteAt([]byte{0xff}, 9) },
		"too short":   func(file sys.File) { file.Truncate(hardStateSize - 1) },
		"too long":    func(file sys.File) { file.WriteAt([]byte{0}, hardStateSize) },
	} {
		if err := f.save(want); err != nil {
			t.Fatal(err)
		}
		file, err := disk.Open("raft/hardstate", sys.ReadWrite)
		if err != nil {
			t.Fatal(err)
		}
		damage(file)
		if _, err := f.load(); !errors.Is(err, errBadHardState) {
			t.Errorf("%s: load returned %v, want errBadHardState", name, err)
		}
	}
}
