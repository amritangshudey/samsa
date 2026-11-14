package broker

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Key compaction (docs/design/c2b-compaction.md). Like retention it runs on
// the serving leader and only proposes commands: a batch with records that a
// newer record of the same key replaces is rewritten by a cmdCompact. Apply
// stays pure and knows no keys.

const (
	// scanBatchBytes bounds the log read to extend key maps in one round,
	// and sweepBatchBytes the bytes of batches rewritten in one round, so
	// that neither crowds out clients for long.
	scanBatchBytes  = 8 << 20
	sweepBatchBytes = 4 << 20
	// keyMapEntryBytes is what one key of a key map is counted as taking.
	keyMapEntryBytes = 64
	// dirtyRatio is the share of a partition's records that a newer record
	// of the same key replaces, from which a sweep starts. Kafka's
	// min.cleanable.dirty.ratio, whose default is the same.
	// shortcut: fixed, not a topic config. Upgrade when a workload needs it.
	dirtyRatio = 0.5
	// sweepBackoff is how long a partition waits after a sweep that removed
	// nothing, as when records are still younger than min.compaction.lag.ms.
	sweepBackoff = 30 * time.Second
)

// Deliberate compaction bugs, for the simulator's mutation suite
// (Config.UnsafeCompact).
const (
	// UnsafeCompactLatest removes the newest record of a key, not the
	// older ones.
	UnsafeCompactLatest = 1 + iota
	// UnsafeCompactNoLag ignores min.compaction.lag.ms.
	UnsafeCompactNoLag
	// UnsafeTombstoneEarly removes a tombstone at once, ignoring
	// delete.retention.ms.
	UnsafeTombstoneEarly
	// UnsafeTombstoneOrphan removes a tombstone while older records of its
	// key are still in the log, which brings the key back to life.
	UnsafeTombstoneOrphan
	// UnsafeCompactWrongKey tells keys apart by their first byte only.
	UnsafeCompactWrongKey
)

type keyDigest [16]byte

// digest identifies a key: the first 16 bytes of its SHA-256.
// shortcut: a collision, about 2^-64 for two keys, would remove a live
// record. Kafka's cleaner uses MD5 for the same purpose.
func (n *Node) digest(key []byte) keyDigest {
	if n.cfg.UnsafeCompact == UnsafeCompactWrongKey && len(key) > 1 {
		key = key[:1]
	}
	var d keyDigest
	sum := sha256.Sum256(key)
	copy(d[:], sum[:])
	return d
}

type keyInfo struct {
	latest int64 // the highest offset holding the key
	count  int32 // the records holding the key that are still in the log
	tomb   bool  // the record at latest is a tombstone
}

// keyMap is a leader's knowledge of the keys of one compacted partition. It
// is soft state: built by reading the log, dropped when the node stops
// leading, never saved. It covers every batch below scanned.
type keyMap struct {
	keys    map[keyDigest]keyInfo
	total   int64 // records in the log that the map has counted
	tombs   int64 // keys whose latest record is a tombstone
	scanned int64 // batches with a base offset below this are counted
	start   int64 // the partition's start when counting began
	full    bool  // the keys outgrew -compaction-map-bytes: no more compaction
	// A sweep rewrites the batches the map covers, oldest first.
	sweeping  bool
	cursor    int64 // the next batch to look at has at least this base
	removed   int64 // records removed by this sweep
	nextSweep sys.Instant
}

// dirty reports whether a sweep is worth its reads: enough of the counted
// records have a newer record of the same key, or a tombstone may be old
// enough to go.
// shortcut: a partition with a tombstone is read in full every sweepBackoff.
// Upgrade by remembering the oldest tombstone's time.
func (km *keyMap) dirty() bool {
	return km.total > 0 && (float64(km.total-int64(len(km.keys))) >= dirtyRatio*float64(km.total) || km.tombs > 0)
}

// remove forgets one record of a key after a command that removes it has
// applied.
func (km *keyMap) remove(d keyDigest) {
	km.total--
	if k, ok := km.keys[d]; ok {
		if k.count--; k.count <= 0 {
			if k.tomb {
				km.tombs--
			}
			delete(km.keys, d)
		} else {
			km.keys[d] = k
		}
	}
}

// checkCompactable returns the error code for a batch that must not be
// written to a compacted topic: one with a record without a key, or one the
// cleaner could not read. See the design, section 3.2.
func checkCompactable(b []byte) int16 {
	if wire.BatchCompressed(b) {
		// shortcut: the core has no decompressor. Upgrade by decompressing
		// outside apply, as the real system could.
		return wire.ErrUnsupportedCompressionType
	}
	recs, err := wire.SplitBatch(b)
	if err != nil {
		return wire.ErrInvalidRecord
	}
	for _, r := range recs {
		if r.Key == nil {
			return wire.ErrInvalidRecord
		}
	}
	return wire.ErrNone
}

// configMs returns a topic's setting of a config in milliseconds, or def.
func configMs(t *topic, name string, def int64) int64 {
	if v, ok := t.configs[name]; ok {
		ms, _ := strconv.ParseInt(v, 10, 64) // checked at apply
		return ms
	}
	return def
}

// compact runs after apply and every retainEvery on the serving leader at
// level 3, once the last round's commands have applied. It extends the key
// map of every compacted partition, and rewrites batches that hold records
// a newer one replaces. It reports whether another round has work.
func (n *Node) compact(now sys.Instant) bool {
	more, budget, scan := false, sweepBatchBytes, scanBatchBytes
	live := map[*partition]bool{}
	for _, name := range n.st.names {
		t := n.st.topics[name]
		if !compacts(t) {
			continue
		}
		for i, p := range t.parts {
			live[p] = true
			km := n.keyMaps[p]
			if km == nil || km.start != p.start {
				// New, or the start moved: what lay below it is gone.
				if n.keyMaps == nil {
					n.keyMaps = map[*partition]*keyMap{}
				}
				km = &keyMap{keys: map[keyDigest]keyInfo{}, scanned: p.start, start: p.start, cursor: p.start}
				n.keyMaps[p] = km
			}
			if km.full {
				continue
			}
			if before := km.scanned; n.extendKeyMap(name, i, p, km, &scan) || km.scanned != before {
				more = true
			}
			if !km.sweeping && now >= km.nextSweep && km.dirty() {
				km.sweeping, km.cursor, km.removed = true, p.start, 0
			}
			if !km.sweeping {
				continue
			}
			if n.sweep(name, i, t, p, km, now, &budget) {
				more = true
			}
		}
	}
	for p := range n.keyMaps { // forget partitions that are gone
		if !live[p] {
			delete(n.keyMaps, p)
		}
	}
	return more
}

// extendKeyMap counts the batches of p that km does not cover yet, up to
// *budget bytes of log. It reports whether it read anything.
func (n *Node) extendKeyMap(name string, part int, p *partition, km *keyMap, budget *int) bool {
	maxKeys := int(n.cfg.CompactionMapBytes / keyMapEntryBytes)
	read := false
	for i := searchBase(p.batches, km.scanned); i < len(p.batches); i++ {
		b := p.batches[i]
		if *budget <= 0 {
			return true
		}
		cmd, _, err := n.raft.Read(b.logIndex)
		if err != nil {
			n.fatal = fmt.Errorf("read committed batch at log index %d to compact it: %w", b.logIndex, err)
			return false
		}
		data := appendBatch(cmd)
		*budget -= len(data)
		read = true
		recs, err := wire.SplitBatch(data)
		if err != nil {
			// Compressed (written before the topic was compacted) or damaged:
			// its records are not counted and it is never rewritten.
			km.scanned = b.base + 1
			continue
		}
		for _, r := range recs {
			if r.Key == nil {
				continue // written before the topic was compacted
			}
			d, off := n.digest(r.Key), b.base+int64(r.OffsetDelta)
			k := km.keys[d]
			if off >= k.latest || k.count == 0 {
				if k.tomb {
					km.tombs--
				}
				k.latest, k.tomb = off, r.Value == nil
				if k.tomb {
					km.tombs++
				}
			}
			k.count++
			km.keys[d] = k
			km.total++
		}
		km.scanned = b.base + 1
		if len(km.keys) > maxKeys {
			km.full = true
			n.log.Warn("compaction stopped: a partition has more keys than -compaction-map-bytes holds", "topic", name, "partition", part)
			return true
		}
	}
	// Everything is counted: offsets up to the end are covered.
	if len(p.batches) == 0 || km.scanned > p.batches[len(p.batches)-1].base {
		km.scanned = max(km.scanned, p.next)
	}
	return read
}

// searchBase returns the index of the first batch with a base offset of at
// least base.
func searchBase(batches []batchRef, base int64) int {
	lo, hi := 0, len(batches)
	for lo < hi {
		mid := (lo + hi) / 2
		if batches[mid].base < base {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// sweep rewrites the batches below km.scanned, from km.cursor on, that hold
// records the map says are dead, within *budget bytes. It reports whether it
// proposed anything or has more to read.
func (n *Node) sweep(name string, part int, t *topic, p *partition, km *keyMap, now sys.Instant, budget *int) bool {
	nowMs := int64(now) / int64(time.Millisecond)
	lag := configMs(t, "min.compaction.lag.ms", 0)
	retain := configMs(t, "delete.retention.ms", n.cfg.DeleteRetentionTime.Milliseconds())
	old := func(b batchRef, ms int64) bool { // is the batch at least ms old?
		return ms <= 0 || b.maxTimestamp >= 0 && nowMs-b.maxTimestamp >= ms
	}
	proposed := false
	for i := searchBase(p.batches, km.cursor); i < len(p.batches); i++ {
		b := p.batches[i]
		if b.base >= km.scanned {
			break
		}
		if *budget <= 0 {
			return true
		}
		km.cursor = b.base + 1
		if n.cfg.UnsafeCompact != UnsafeCompactNoLag && !old(b, lag) {
			continue
		}
		cmd, _, err := n.raft.Read(b.logIndex)
		if err != nil {
			n.fatal = fmt.Errorf("read committed batch at log index %d to compact it: %w", b.logIndex, err)
			return false
		}
		data := appendBatch(cmd)
		recs, err := wire.SplitBatch(data)
		if err != nil {
			continue
		}
		keep, gone := make([]bool, len(recs)), []keyDigest(nil)
		for j, r := range recs {
			keep[j] = true
			if r.Key == nil {
				continue
			}
			d, off := n.digest(r.Key), b.base+int64(r.OffsetDelta)
			k, ok := km.keys[d]
			dead := ok && k.latest > off
			if n.cfg.UnsafeCompact == UnsafeCompactLatest {
				dead = ok && k.latest == off && k.count > 1
			}
			if n.cfg.UnsafeCompact == UnsafeTombstoneOrphan && ok && k.tomb && r.Value != nil {
				dead = false // what a tombstone replaces is left behind
			}
			if ok && k.latest == off && r.Value == nil && (k.count == 1 || n.cfg.UnsafeCompact == UnsafeTombstoneOrphan) &&
				(old(b, retain) || n.cfg.UnsafeCompact == UnsafeTombstoneEarly) {
				dead = true
			}
			if dead {
				keep[j] = false
				gone = append(gone, d)
			}
		}
		if len(gone) == 0 {
			continue
		}
		out := wire.KeepRecords(data, recs, keep)
		*budget -= len(data)
		n.compacting++
		proposed = true
		n.propose(encodeCompact(name, int32(part), b.base, b.logIndex, out), func(r result) {
			n.compacting--
			if r.applied {
				n.stats.relocatedBytes += int64(len(out))
				n.stats.compactedRecords += int64(len(gone))
				km.removed += int64(len(gone))
				for _, d := range gone {
					km.remove(d)
				}
			}
		})
	}
	if proposed {
		return true
	}
	// Reached the end of what the map covers.
	if km.scanned < p.next {
		return true // more to count, then more to sweep
	}
	km.sweeping = false
	if km.removed == 0 {
		km.nextSweep = now + sys.Instant(sweepBackoff)
	}
	return km.removed > 0 && km.dirty()
}
