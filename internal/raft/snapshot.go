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

// loadSnapshot returns the stored snapshot and its bytes, or one with index
// -1 if none was ever stored.
func loadSnapshot(disk sys.Disk, dir string) (snapshot, []byte, error) {
	b, err := ReadFile(disk, path.Join(dir, snapshotName))
	if err != nil || b == nil {
		return snapshot{index: -1}, nil, err
	}
	s, err := decodeSnapshot(b)
	return s, b, err
}

// SaveSnapshot records that state is the caller's state after applying
// every entry up to and including index, which must be committed. It first
// makes the log durable up to index (unless MutSnapshotUnsynced), so that
// no crash can leave a snapshot ahead of the log. A failure stops the node.
func (r *Raft) SaveSnapshot(index int64, state []byte) {
	if r.err != nil {
		return
	}
	if index < 0 || index > r.commit || index > r.LastIndex() {
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
	b := s.encode()
	if err := writeAtomic(r.disk, r.cfg.Dir, snapshotName, b, false, false); err != nil {
		r.fail(err)
		return
	}
	r.latest, r.latestBytes = s, b
}

// Snapshot returns the index and state of the newest snapshot: the one
// loaded by Start, saved since, or installed from the leader. Index is -1
// if there is none. The state must not be modified.
func (r *Raft) Snapshot() (index int64, state []byte) { return r.latest.index, r.latest.state }

// SnapshotIndex returns the index of the newest snapshot, or -1.
func (r *Raft) SnapshotIndex() int64 { return r.latest.index }

// LogStart returns the index of the first entry still in the log. Entries
// below it are known only through the snapshot.
func (r *Raft) LogStart() int64 { return r.log.StartOffset() }

// Installing reports whether the node is receiving its log afresh after a
// snapshot from the leader. Meanwhile entries are committed but must not be
// applied: the caller's state is older than the log, which starts after it.
func (r *Raft) Installing() bool { return r.pending != nil }

// Compact removes log entries below offset, as far as whole segments allow.
// It never removes an entry the newest snapshot does not cover (unless
// MutTrimPastSnapshot): offset is clamped to the snapshot index + 1.
func (r *Raft) Compact(offset int64) {
	if r.err != nil || r.pending != nil {
		return
	}
	if !r.mut(MutTrimPastSnapshot) {
		offset = min(offset, r.latest.index+1)
	}
	if err := r.log.TruncatePrefix(min(offset, r.commit+1)); err != nil {
		r.fail(fmt.Errorf("compact raft log below %d: %w", offset, err))
	}
}

// RetentionCut returns where to cut the log so that about maxBytes remain,
// never past the commit index.
func (r *Raft) RetentionCut(maxBytes int64) int64 {
	return min(r.log.CutFor(maxBytes), r.commit+1)
}

// The install marker: the snapshot a follower is installing, and the index
// its log starts at. Its presence means the node is installing.
const (
	installMagic = 0x5341494e // "SAIN"
	installName  = "install"
)

func encodeInstall(cut int64, snap []byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, installMagic)
	b = binary.BigEndian.AppendUint64(b, uint64(cut))
	b = append(b, snap...)
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

func decodeInstall(b []byte) (cut int64, s snapshot, raw []byte, err error) {
	if len(b) < 16 || binary.BigEndian.Uint32(b) != installMagic || crc32.Checksum(b[:len(b)-4], castagnoli) != binary.BigEndian.Uint32(b[len(b)-4:]) {
		return 0, snapshot{}, nil, fmt.Errorf("%w: install marker", errBadSnapshot)
	}
	cut, raw = int64(binary.BigEndian.Uint64(b[4:])), b[12:len(b)-4]
	if s, err = decodeSnapshot(raw); err == nil && (cut < 1 || cut > s.index+1) {
		err = fmt.Errorf("%w: install cut %d for a snapshot at %d", errBadSnapshot, cut, s.index)
	}
	return cut, s, raw, err
}

// ReadFile returns a whole file, or nil if it does not exist.
func ReadFile(disk sys.Disk, name string) ([]byte, error) {
	f, err := disk.Open(name, sys.ReadOnly)
	if errors.Is(err, sys.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	defer f.Close() //nolint:errcheck // read-only handle
	size, err := f.Size()
	if err != nil {
		return nil, fmt.Errorf("size of %s: %w", name, err)
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && size > 0 {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	return b, nil
}

// beginInstall makes the node a follower whose log starts at cut, with the
// terms below cut from s, after the marker is durable. Entries below cut
// are committed: they are below the leader's log start.
func (r *Raft) beginInstall(cut int64, s snapshot, raw []byte) error {
	if r.log.StartOffset() != cut || r.log.EndOffset() < cut {
		if err := r.log.Reset(cut); err != nil {
			return fmt.Errorf("reset raft log to %d: %w", cut, err)
		}
	}
	r.terms = s.expand()[:cut]
	r.commit = max(r.commit, cut-1)
	r.pending, r.pendingBytes = &s, raw
	return nil
}

// maybePromote ends an install once the log is durable past the snapshot:
// the snapshot becomes the node's own, and the marker goes.
func (r *Raft) maybePromote() {
	if r.err != nil || r.pending == nil || r.stable() < r.pending.index {
		return
	}
	if err := writeAtomic(r.disk, r.cfg.Dir, snapshotName, r.pendingBytes, false, false); err != nil {
		r.fail(err)
		return
	}
	// A crash before the marker is gone leaves both; Start then sees the
	// snapshot is at least as new and removes the marker.
	if err := r.disk.Remove(path.Join(r.cfg.Dir, installName)); err != nil {
		r.fail(fmt.Errorf("remove install marker: %w", err))
		return
	}
	r.latest, r.latestBytes = *r.pending, r.pendingBytes
	r.commit = max(r.commit, r.latest.index)
	r.pending, r.pendingBytes = nil, nil
}

// sendSnapshot sends the next chunk of the newest snapshot to a follower
// whose next entry is below the log start.
//
// shortcut: a follower buffers the snapshot in memory. It holds no record
// data, so it is small; stream it to a file if it ever is not.
func (r *Raft) sendSnapshot(to sys.NodeID, p *progress) {
	s, b := r.latest, r.latestBytes
	if r.pending != nil && r.pending.index > s.index {
		s, b = *r.pending, r.pendingBytes
	}
	if len(b) == 0 || s.index+1 < r.log.StartOffset() {
		return // nothing that covers the gap; cannot happen while Compact clamps
	}
	if p.snapIndex != s.index || p.snapOff >= int64(len(b)) {
		p.snapIndex, p.snapOff = s.index, 0
	}
	// No bigger than one command, the bound every receiver checks.
	chunk := min(max(r.cfg.MaxAppendBytes, 256), r.cfg.Log.MaxPayloadBytes-entryHeader)
	n := min(int64(len(b))-p.snapOff, int64(chunk))
	p.inflight = true
	cut := r.log.StartOffset()
	r.send(to, message{typ: msgInstallSnapshot, stamp: int64(r.env.Now()), cut: cut, prevTerm: r.terms[cut-1], snapIndex: s.index,
		total: int64(len(b)), offset: p.snapOff, data: b[p.snapOff : p.snapOff+n]})
}

// transfer is a snapshot being received.
type transfer struct {
	term, index, total int64
	buf                []byte
}

func (r *Raft) onInstallSnapshot(from sys.NodeID, m message) {
	if r.role == Leader {
		r.fail(fmt.Errorf("raft: node %d and node %d are both leader in term %d", r.cfg.ID, from, m.term))
		return
	}
	r.follow(m.term)
	r.leader = from
	r.contact = r.env.Now()
	r.armElection()
	reply := message{typ: msgInstallResponse, stamp: m.stamp, index: m.cut - 1}
	// Only a node that cannot continue from its own log needs it: one
	// whose entry before the cut is missing or from another term. A copy
	// that arrives after the node caught up must not reset its log.
	if m.cut-1 <= r.LastIndex() && r.terms[m.cut-1] == m.prevTerm {
		reply.offset = -1
		r.send(from, reply)
		return
	}
	if t := r.inst; t == nil || t.term != m.term || t.index != m.snapIndex || t.total != m.total {
		r.inst = &transfer{term: m.term, index: m.snapIndex, total: m.total}
	}
	t := r.inst
	if m.offset == int64(len(t.buf)) {
		t.buf = append(t.buf, m.data...)
	}
	if int64(len(t.buf)) < t.total {
		reply.offset = int64(len(t.buf))
		r.send(from, reply)
		return
	}
	r.inst = nil
	s, err := decodeSnapshot(t.buf)
	if err != nil || s.index != m.snapIndex {
		return // a broken peer; a working leader sends it again
	}
	if err := writeAtomic(r.disk, r.cfg.Dir, installName, encodeInstall(m.cut, t.buf), false, false); err != nil {
		r.fail(err)
		return
	}
	if err := r.beginInstall(m.cut, s, t.buf); err != nil {
		r.fail(err)
		return
	}
	r.ackTo, r.dirty = noVote, false // whatever was owed refers to the old log
	r.maybePromote()
	r.send(from, message{typ: msgAppendResponse, ok: true, echo: m.cut - 1, index: m.cut - 1, stamp: m.stamp})
}

func (r *Raft) onInstallResponse(from sys.NodeID, m message) {
	p := r.peers[from]
	p.acked = max(p.acked, min(sys.Instant(m.stamp), r.env.Now()))
	if p.next >= r.log.StartOffset() {
		return // no install under way: a late copy
	}
	p.inflight = false
	if m.offset < 0 {
		p.next = max(p.next, m.index+1) // it can continue from its own log
	} else {
		p.snapOff = m.offset
	}
	r.replicate(from, p)
}
