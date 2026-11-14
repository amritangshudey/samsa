package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"path"

	"github.com/amritangshudey/samsa/internal/storage"
	"github.com/amritangshudey/samsa/internal/sys"
)

// Recovery from damage to the log (docs/design/b1-storage-faults.md). A node
// that finds a bad frame in a sealed segment of its log cuts the log there,
// and until its commit index reaches what the log held before, it is
// recovering: it holds entries it may have acknowledged no longer, so it
// grants no vote and does not campaign. Every election then needs a majority
// of the others, and every committed entry is on one of them.

const (
	recoveringName  = "recovering"
	recoveringMagic = 0x53415243 // "SARC"
	recoveringSize  = 4 + 8 + 4
)

func encodeRecovering(until int64) []byte {
	b := binary.BigEndian.AppendUint32(nil, recoveringMagic)
	b = binary.BigEndian.AppendUint64(b, uint64(until))
	return binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeRecovering returns the offset the marker holds; a damaged marker
// counts as one that is never satisfied.
func decodeRecovering(b []byte) int64 {
	if len(b) != recoveringSize || binary.BigEndian.Uint32(b) != recoveringMagic ||
		crc32.Checksum(b[:recoveringSize-4], castagnoli) != binary.BigEndian.Uint32(b[recoveringSize-4:]) {
		return math.MaxInt64
	}
	return int64(binary.BigEndian.Uint64(b[4:]))
}

// Recovering reports whether the node holds no vote while it fetches back
// what damage cost it.
func (r *Raft) Recovering() bool { return r.recovering }

// Repaired counts the damage this node has repaired since it started.
func (r *Raft) Repaired() int { return r.repaired }

// damagedName marks, durably, that a run found damage to the log. The repair
// is the next start.
const damagedName = "damaged"

// MarkDamage records that the log holds damage, found at offset (or
// somewhere, if offset is -1), and returns once the record is durable. The
// node should then stop: its next start repairs.
func (r *Raft) MarkDamage(offset int64) error {
	if b, err := ReadFile(r.disk, path.Join(r.cfg.Dir, damagedName)); err != nil || b != nil {
		return err // already marked
	}
	return writeAtomic(r.disk, r.cfg.Dir, damagedName, encodeRecovering(max(offset, 0)), false, false)
}

// repairMarked runs at start, before the log is read, when a run found
// damage: it looks for it again, and cuts it out.
func (r *Raft) repairMarked() error {
	name := path.Join(r.cfg.Dir, damagedName)
	b, err := ReadFile(r.disk, name)
	if err != nil || b == nil {
		return err
	}
	_, _, damage, found, err := r.log.Scrub(r.log.StartOffset(), math.MaxInt)
	if err != nil {
		return err
	}
	if found {
		if err := r.cutAt(damage, nil); err != nil {
			return err
		}
	}
	if err := r.disk.Remove(name); err != nil {
		return fmt.Errorf("remove damage marker: %w", err)
	}
	return r.disk.SyncDir(r.cfg.Dir)
}

// repairDamage handles a read of the log at from, during the start-up scan,
// that failed with storage.ErrCorrupt. It reports whether it cut the log at
// damage, in which case the caller scans again.
func (r *Raft) repairDamage(from int64, cause error) (bool, error) {
	_, _, damage, found, err := r.log.Scrub(from, math.MaxInt)
	if err != nil {
		return false, err
	}
	if !found {
		return false, cause // not damage a sealed segment explains
	}
	return true, r.cutAt(damage, cause)
}

// cutAt repairs damage that starts at offset damage: it writes the
// recovery marker, then removes what the node cannot trust. Damage after the
// snapshot costs the log from there on; damage under it, or a snapshot that
// does not read, costs the whole log and the snapshot, which the leader then
// sends again.
func (r *Raft) cutAt(damage int64, cause error) error {
	if len(r.cfg.Peers) == 1 {
		return fmt.Errorf("raft: the log is damaged at offset %d and this node is the only voter, so there is no copy to repair it from: %w", damage, storage.ErrCorrupt)
	}
	snap, _, serr := loadSnapshot(r.disk, r.cfg.Dir)
	until := max(r.until, r.log.EndOffset())
	if !r.mut(MutTruncateAtDamage) {
		// The marker is durable before anything is cut: a crash between
		// them must leave a node that knows it lost something.
		if err := writeAtomic(r.disk, r.cfg.Dir, recoveringName, encodeRecovering(until), false, false); err != nil {
			return err
		}
		r.recovering, r.until = true, until
	}
	if serr == nil && damage > snap.index && (snap.index >= 0 || r.log.StartOffset() == 0) {
		if err := r.log.TruncateSuffix(damage); err != nil {
			return fmt.Errorf("cut the raft log at damage, offset %d: %w", damage, err)
		}
	} else {
		for _, n := range []string{snapshotName, installName} {
			if err := r.disk.Remove(path.Join(r.cfg.Dir, n)); err != nil && !errors.Is(err, sys.ErrNotExist) {
				return fmt.Errorf("remove %s: %w", n, err)
			}
		}
		if err := r.disk.SyncDir(r.cfg.Dir); err != nil {
			return err
		}
		if err := r.log.Reset(0); err != nil {
			return fmt.Errorf("empty the raft log: %w", err)
		}
	}
	r.repaired++
	return nil
}

// loadRecovering reads the marker at start.
func (r *Raft) loadRecovering() error {
	b, err := ReadFile(r.disk, path.Join(r.cfg.Dir, recoveringName))
	if err != nil {
		return err
	}
	if b == nil || r.mut(MutRecoveredEarly) {
		return nil
	}
	r.recovering, r.until = true, decodeRecovering(b)
	return nil
}

// maybeRecovered ends recovery once everything the log held before the
// damage is committed here.
func (r *Raft) maybeRecovered() {
	if !r.recovering || r.commit < r.until-1 {
		return
	}
	if err := r.disk.Remove(path.Join(r.cfg.Dir, recoveringName)); err != nil && !errors.Is(err, sys.ErrNotExist) {
		r.fail(fmt.Errorf("remove recovery marker: %w", err))
		return
	}
	if err := r.disk.SyncDir(r.cfg.Dir); err != nil {
		r.fail(fmt.Errorf("sync raft directory after recovery: %w", err))
		return
	}
	r.recovering, r.until = false, 0
}

// IsDamage reports whether err is the storage layer's report of damage.
func IsDamage(err error) bool { return errors.Is(err, storage.ErrCorrupt) }

// Scrub reads the sealed segments of the log from cursor on, at most about
// budget bytes, and returns where to go on from (-1 after the last), or the
// offset of damage. See storage.Log.Scrub.
func (r *Raft) Scrub(cursor int64, budget int) (next int64, read int, damage int64, found bool, err error) {
	return r.log.Scrub(cursor, budget)
}
