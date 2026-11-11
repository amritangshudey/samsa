package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"path"

	"github.com/amritangshudey/samsa/internal/sys"
)

// A snapshot replaces entries 0..index for the purposes of starting up: the
// terms of those entries, and the state the caller had after applying them.
// The entries themselves stay in the log. See docs/design/a1-snapshots.md.
type snapshot struct {
	index int64     // last entry included; -1 for none
	runs  []termRun // terms of entries 0..index, ascending by first
	state []byte    // the caller's, uninterpreted
}

// termRun says that entries from first up to the next run's first have term.
type termRun struct{ first, term int64 }

const (
	snapshotMagic   = 0x5341534e // "SASN"
	snapshotVersion = 1
	snapshotName    = "snapshot"
)

// errBadSnapshot means the snapshot file exists and is not intact, or does
// not fit the log. A crash cannot cause either (the file is replaced by an
// atomic rename of a synced file, and the log is synced up to the snapshot
// before it is written), so the node must not guess.
var errBadSnapshot = errors.New("raft: snapshot file is corrupt or does not match the log")

// runsOf compresses terms[0..index] into runs.
func runsOf(terms []int64, index int64) []termRun {
	var runs []termRun
	for i := int64(0); i <= index; i++ {
		if len(runs) == 0 || runs[len(runs)-1].term != terms[i] {
			runs = append(runs, termRun{i, terms[i]})
		}
	}
	return runs
}

func (s *snapshot) encode() []byte {
	b := binary.BigEndian.AppendUint32(nil, snapshotMagic)
	b = append(b, snapshotVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(s.index))
	b = binary.BigEndian.AppendUint32(b, uint32(len(s.runs)))
	for _, r := range s.runs {
		b = binary.BigEndian.AppendUint64(b, uint64(r.first))
		b = binary.BigEndian.AppendUint64(b, uint64(r.term))
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(s.state)))
	b = append(b, s.state...)
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeSnapshot checks every length against the input before using it.
func decodeSnapshot(b []byte) (snapshot, error) {
	const head = 4 + 1 + 8 + 4
	if len(b) < head+4+4 || crc32.Checksum(b[:len(b)-4], castagnoli) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return snapshot{}, fmt.Errorf("%w: bad length or checksum", errBadSnapshot)
	}
	body := b[:len(b)-4]
	if binary.BigEndian.Uint32(body) != snapshotMagic || body[4] != snapshotVersion {
		return snapshot{}, fmt.Errorf("%w: bad magic or version", errBadSnapshot)
	}
	s := snapshot{index: int64(binary.BigEndian.Uint64(body[5:]))}
	n := uint64(binary.BigEndian.Uint32(body[13:]))
	rest := body[head:]
	if n > uint64(len(rest))/16 {
		return snapshot{}, fmt.Errorf("%w: %d term runs do not fit", errBadSnapshot, n)
	}
	for range n {
		r := termRun{int64(binary.BigEndian.Uint64(rest)), int64(binary.BigEndian.Uint64(rest[8:]))}
		rest = rest[16:]
		// Runs start at 0, ascend, and change term (upwards) each time.
		if last := len(s.runs) - 1; (last < 0 && r.first != 0) || (last >= 0 && (r.first <= s.runs[last].first || r.term <= s.runs[last].term)) || r.term < 0 {
			return snapshot{}, fmt.Errorf("%w: term runs out of order", errBadSnapshot)
		}
		s.runs = append(s.runs, r)
	}
	if len(rest) < 4 || uint64(binary.BigEndian.Uint32(rest)) != uint64(len(rest)-4) {
		return snapshot{}, fmt.Errorf("%w: state length", errBadSnapshot)
	}
	s.state = rest[4:]
	if s.index < 0 || len(s.runs) == 0 || s.runs[len(s.runs)-1].first > s.index {
		return snapshot{}, fmt.Errorf("%w: index %d does not fit its term runs", errBadSnapshot, s.index)
	}
	return s, nil
}

// expand returns the term of every entry 0..index.
//
// shortcut: keeps one term per entry in memory, as Raft did before
// snapshots; replace r.terms by runs when the log is cut (A1 part b).
func (s *snapshot) expand() []int64 {
	terms := make([]int64, 0, s.index+1)
	for i, r := range s.runs {
		end := s.index + 1
		if i+1 < len(s.runs) {
			end = s.runs[i+1].first
		}
		for range end - r.first {
			terms = append(terms, r.term)
		}
	}
	return terms
}

// loadSnapshot returns the stored snapshot, or one with index -1 if none
// was ever stored.
func loadSnapshot(disk sys.Disk, dir string) (snapshot, error) {
	f, err := disk.Open(path.Join(dir, snapshotName), sys.ReadOnly)
	if errors.Is(err, sys.ErrNotExist) {
		return snapshot{index: -1}, nil
	}
	if err != nil {
		return snapshot{}, fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	size, err := f.Size()
	if err != nil {
		return snapshot{}, fmt.Errorf("snapshot size: %w", err)
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && size > 0 {
		return snapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	return decodeSnapshot(b)
}

// SaveSnapshot records that state is the caller's state after applying
// every entry up to and including index, which must be committed. It first
// makes the log durable up to index (unless MutSnapshotUnsynced), so that
// no crash can leave a snapshot ahead of the log. A failure stops the node.
func (r *Raft) SaveSnapshot(index int64, state []byte) {
	if r.err != nil {
		return
	}
	if index < 0 || index > r.commit {
		r.fail(fmt.Errorf("raft: snapshot at %d, but only %d is committed", index, r.commit))
		return
	}
	if r.log.DurableOffset() <= index && !r.mut(MutSnapshotUnsynced) {
		r.Sync()
		if r.err != nil {
			return
		}
	}
	s := snapshot{index: index, runs: runsOf(r.terms, index), state: state}
	if err := writeAtomic(r.disk, r.cfg.Dir, snapshotName, s.encode(), false, false); err != nil {
		r.fail(err)
		return
	}
	r.snapIndex = index
}

// Snapshot returns the index and state of the snapshot loaded by Start, or
// index -1 if there was none. The state is valid until the next call into
// Raft.
func (r *Raft) Snapshot() (index int64, state []byte) { return r.loaded.index, r.loaded.state }

// SnapshotIndex returns the index of the newest snapshot on disk, or -1.
func (r *Raft) SnapshotIndex() int64 { return r.snapIndex }
