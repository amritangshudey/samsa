// Package raft implements the Raft consensus algorithm as a deterministic
// state machine driven by one node: see docs/design/phase-5.md.
package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path"

	"github.com/amritangshudey/samsa/internal/sys"
)

// noVote is hardState.votedFor when the node has not voted in its term.
const noVote sys.NodeID = -1

// hardState is what a node must never forget: the latest term it has
// seen, and whom it voted for in that term. Forgetting either lets it
// vote twice in one term, which can elect two leaders.
type hardState struct {
	term     int64
	votedFor sys.NodeID
}

// errBadHardState means the hard-state file exists and is not intact.
// Under the disk contract that cannot come from a crash (the file is
// replaced by an atomic rename of a synced file), so it is corruption, and
// the node must not guess a term.
var errBadHardState = errors.New("raft: hard state file is corrupt")

const (
	hardStateMagic = 0x53414853 // "SAHS"
	hardStateSize  = 4 + 8 + 4 + 4
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// hardStateFile stores a hardState in dir on disk.
type hardStateFile struct {
	disk sys.Disk
	dir  string
	// skipSync and skipDirSync each remove one step of save. Tests set
	// them to prove that the crash test notices.
	skipSync, skipDirSync bool
}

// load returns the stored state, or term 0 and no vote if none was ever
// stored.
func (f *hardStateFile) load() (hardState, error) {
	file, err := f.disk.Open(path.Join(f.dir, "hardstate"), sys.ReadOnly)
	if errors.Is(err, sys.ErrNotExist) {
		return hardState{votedFor: noVote}, nil
	}
	if err != nil {
		return hardState{}, fmt.Errorf("open hard state: %w", err)
	}
	defer file.Close()            //nolint:errcheck // read-only handle
	var b [hardStateSize + 1]byte // one more, to notice a file that is too long
	n, err := file.ReadAt(b[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return hardState{}, fmt.Errorf("read hard state: %w", err)
	}
	hs := hardState{term: int64(binary.BigEndian.Uint64(b[4:])), votedFor: sys.NodeID(binary.BigEndian.Uint32(b[12:]))}
	if n != hardStateSize || binary.BigEndian.Uint32(b[:]) != hardStateMagic ||
		crc32.Checksum(b[:16], castagnoli) != binary.BigEndian.Uint32(b[16:]) || hs.term < 0 || hs.votedFor < noVote {
		return hardState{}, errBadHardState
	}
	return hs, nil
}

// save makes hs durable before it returns. After a crash during save, load
// returns either hs or the state stored before, never anything else.
func (f *hardStateFile) save(hs hardState) error {
	b := binary.BigEndian.AppendUint32(make([]byte, 0, hardStateSize), hardStateMagic)
	b = binary.BigEndian.AppendUint64(b, uint64(hs.term))
	b = binary.BigEndian.AppendUint32(b, uint32(hs.votedFor))
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))

	return writeAtomic(f.disk, f.dir, "hardstate", b, f.skipSync, f.skipDirSync)
}

// writeAtomic replaces dir/name with b, durably: after a crash the file
// holds either b or what it held before, never anything else. skipSync and
// skipDirSync each remove one step, for tests that prove a crash test notices.
func writeAtomic(disk sys.Disk, dir, name string, b []byte, skipSync, skipDirSync bool) error {
	tmp, final := path.Join(dir, name+".tmp"), path.Join(dir, name)
	file, err := disk.Open(tmp, sys.Create)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	defer file.Close()                       //nolint:errcheck // synced below; close cannot lose data
	if err := file.Truncate(0); err != nil { // a crash may have left an old one
		return fmt.Errorf("truncate %s: %w", name, err)
	}
	if _, err := file.WriteAt(b, 0); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	// The content must be durable before the name points at it, or the
	// rename could survive a crash that the bytes did not.
	if !skipSync {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync %s: %w", name, err)
		}
	}
	if err := disk.Rename(tmp, final); err != nil {
		return fmt.Errorf("replace %s: %w", name, err)
	}
	// And the rename itself, or the old file could come back.
	if !skipDirSync {
		if err := disk.SyncDir(dir); err != nil {
			return fmt.Errorf("sync %s directory: %w", name, err)
		}
	}
	return nil
}
