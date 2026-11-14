package sim

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/amritangshudey/samsa/internal/wire"
)

// What the model knows of a compacted topic (docs/design/c2b-compaction.md,
// section 4). Records sent to it carry a key, and some are tombstones; the
// broker may remove a record only if a newer record of its key exists, or if
// it is an old tombstone. The checks:
//
//	C1  the latest record of every key survives (a tombstone, once old enough, may go)
//	C2  records that are read are the ones sent, byte for byte (observeCompacted)
//	C3  a record is removed only once it is old enough, and only if superseded
//	C5  once a tombstone is gone, so is everything older with its key
//
// All of them are held back unless nothing a power loss could have taken is
// in doubt (strict, or no power loss yet), and for a key unless every record
// of it has been answered: an unanswered one may have been written and may
// supersede what the model believes is the latest.

// compactKeys is how many keys the clients use.
const compactKeys = 8

type compactRules struct {
	lagMs, retainMs int64 // min.compaction.lag.ms and delete.retention.ms
}

// blindRecord is a tombstone read in the final pass that nobody learned the
// acknowledgement of.
type blindRecord struct {
	topic string
	key   int
	off   int64
	pass  int
}

// keyState is what is known of one key of a compacted topic.
type keyState struct {
	latest  int64 // the highest offset at which a record of the key was acknowledged or read; -1 if none
	pending int   // records of the key sent and not yet answered, or never answered
}

// keyIndex gives most records one of six keys, which are updated all the
// time, and tombstones and a few puts two others, which are deleted and
// then left alone for a while, so that a tombstone is often the latest
// record of its key.
func keyIndex(id uint64) int {
	switch {
	case Tombstone(id):
		return 6 + int(id/7%2)
	case id%16 == 0:
		return 6 + int(id/16%2)
	}
	return int(id * 2654435761 >> 7 % 6)
}

// Key returns the key of the record with the given id, for compacted topics.
func Key(id uint64) []byte { return []byte(fmt.Sprintf("key-%d", keyIndex(id))) }

// Tombstone reports whether the record with the given id has no value.
func Tombstone(id uint64) bool { return id%7 == 3 }

// RecordValue is the value of a record sent to a compacted topic: nil for a
// tombstone.
func RecordValue(id uint64) []byte {
	if Tombstone(id) {
		return nil
	}
	return Value(id)
}

// Compaction turns on the checks C1 to C5 for a topic, with its
// min.compaction.lag.ms and delete.retention.ms, on clock.
func (m *Model) Compaction(topic string, lagMs, retainMs int64, clock func() int64) {
	if m.compact == nil {
		m.compact, m.keyst = map[string]*compactRules{}, map[string]*[compactKeys]keyState{}
	}
	m.compact[topic] = &compactRules{lagMs, retainMs}
	ks := &[compactKeys]keyState{}
	for i := range ks {
		ks[i].latest = -1
	}
	m.keyst[topic] = ks
	m.clock = clock
}

// checkable reports whether power losses leave nothing in doubt.
func (m *Model) checkable() bool { return m.strict || m.epoch == 0 }

func (m *Model) sentCompacted(tp TP, first uint64, n int) {
	if ks := m.keyst[tp.Topic]; ks != nil {
		for id := first; id < first+uint64(n); id++ {
			ks[keyIndex(id)].pending++
		}
	}
}

// answered notes that the record id, until now unanswered, is acknowledged,
// rejected or read.
func (m *Model) answered(id uint64) {
	r := &m.recs[id]
	if ks := m.keyst[r.tp.Topic]; ks != nil && r.offset < 0 && (r.state == recPending || r.state == recUnknown) {
		ks[keyIndex(id)].pending--
	}
}

// known notes that the record id sits at off.
func (m *Model) known(id uint64, off int64) {
	if ks := m.keyst[m.recs[id].tp.Topic]; ks != nil {
		k := &ks[keyIndex(id)]
		k.latest = max(k.latest, off)
	}
}

// removalOK checks C3 and C1 for one acknowledged record that a read of
// its partition did not return although the read covered its offset.
func (m *Model) removalOK(tp TP, off int64, id uint64) error {
	rules, ks := m.compact[tp.Topic], m.keyst[tp.Topic]
	r := &m.recs[id]
	if r.state != recAcked || !m.checkable() || off < m.part(tp).start {
		return nil
	}
	age := m.clock() - r.sent
	if age < rules.lagMs { // inside the lag nothing may be compacted, whatever replaced it
		return fmt.Errorf("C3: %v offset %d (record %d) was removed at age %d ms, inside min.compaction.lag.ms %d", tp, off, id, age, rules.lagMs)
	}
	k := ks[keyIndex(id)]
	switch {
	case k.pending > 0, k.latest > off:
	case Tombstone(id) && age >= rules.retainMs:
	default:
		return fmt.Errorf("C1: %v offset %d (record %d, key %s) is the latest record of its key, not an old tombstone, and is gone", tp, off, id, Key(id))
	}
	return nil
}

// observeCompacted checks one batch of a compacted topic that was read, as
// observe does for the others, and returns the offsets it holds.
func (m *Model) observeCompacted(tp TP, base int64, recs []wire.StoredRecord) (present map[int64]bool, err error) {
	p := m.part(tp)
	present = map[int64]bool{}
	for _, got := range recs {
		off := base + int64(got.OffsetDelta)
		present[off] = true
		var id uint64
		switch {
		case got.Value != nil:
			if len(got.Value) < 8 {
				return nil, fmt.Errorf("I4: %v offset %d holds a record nobody sent", tp, off)
			}
			id = binary.BigEndian.Uint64(got.Value)
			if id == 0 || id >= uint64(len(m.recs)) || !bytes.Equal(got.Value, Value(id)) || Tombstone(id) {
				return nil, fmt.Errorf("I4: %v offset %d holds a record nobody sent, or a damaged one", tp, off)
			}
		case off < int64(len(p.at)):
			// A tombstone says nothing of itself: the model knows where it
			// was acknowledged. A slot holding something else may be left
			// over from before a power loss.
			if id = p.at[off].id; id != 0 && !Tombstone(id) {
				id = 0
			}
		}
		if id == 0 {
			if !bytes.HasPrefix(got.Key, []byte("key-")) {
				return nil, fmt.Errorf("I4: %v offset %d holds a tombstone with an unknown key %q", tp, off, got.Key)
			}
			// Not acknowledged: it cannot be checked or placed, but the final
			// read still counts it as something left of its key.
			if m.finalPass > 0 {
				var k int
				if _, err := fmt.Sscanf(string(got.Key), "key-%d", &k); err == nil && k >= 0 && k < compactKeys {
					m.blind = append(m.blind, blindRecord{tp.Topic, k, off, m.finalPass})
				}
			}
			continue
		}
		r := &m.recs[id]
		switch {
		case !bytes.Equal(got.Key, Key(id)):
			return nil, fmt.Errorf("C2: %v offset %d: record %d was sent with key %q and read with key %q", tp, off, id, Key(id), got.Key)
		case r.tp != tp:
			return nil, fmt.Errorf("I4: record %d was sent to %v but read from %v", id, r.tp, tp)
		case r.state == recRejected:
			return nil, fmt.Errorf("I5: record %d was rejected but is readable at %v offset %d", id, tp, off)
		}
		m.answered(id)
		if err := m.place(id, off); err != nil {
			return nil, err
		}
		m.known(id, off)
	}
	return present, nil
}

// checkGap checks the offsets from..to-1 of tp, which a read skipped: every
// acknowledged record there is one the broker may have removed.
func (m *Model) checkGap(tp TP, from, to int64, present map[int64]bool) error {
	p := m.part(tp)
	for o := max(from, p.start); o < min(to, int64(len(p.at))); o++ {
		if id := p.at[o].id; id != 0 && !present[o] {
			if err := m.removalOK(tp, o, id); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkCompactedFinal checks C1 and C5 after the final read, which saw every
// record the broker still holds above the log start. A record acknowledged
// and not seen there was removed, which is allowed only if a record of its
// key with a higher offset was seen (it replaced the one that is gone, and
// whatever replaced that), or if its key ended in a tombstone that was old
// enough to go, which takes everything older with it. Unanswered records
// need no special care: only what was seen or acknowledged is used.
func (m *Model) checkCompactedFinal() error {
	if !m.checkable() {
		return nil
	}
	topics := make([]string, 0, len(m.compact))
	for t := range m.compact {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	for _, topic := range topics {
		rules := m.compact[topic]
		if t := m.topics[topic]; t != nil && t.deleting {
			continue
		}
		var seen [compactKeys]int64 // the highest offset seen of each key
		for i := range seen {
			seen[i] = -1
		}
		for id := 1; id < len(m.recs); id++ {
			if r := &m.recs[id]; r.tp.Topic == topic && r.final == m.finalPass && r.offset >= 0 {
				seen[keyIndex(uint64(id))] = max(seen[keyIndex(uint64(id))], r.offset)
			}
		}
		for _, b := range m.blind {
			if b.topic == topic && b.pass == m.finalPass {
				seen[b.key] = max(seen[b.key], b.off)
			}
		}
		now := m.clock()
		// A tombstone of key k old enough to have gone takes everything older
		// with it: oldTombAt[k] is the highest offset of one whose offset is
		// known, oldTombID[k] the highest id of one whose offset is not.
		var oldTombAt [compactKeys]int64
		var oldTombID [compactKeys]uint64
		for i := range oldTombAt {
			oldTombAt[i] = -1
		}
		for id := 1; id < len(m.recs); id++ {
			r := &m.recs[id]
			if r.tp.Topic != topic || !Tombstone(uint64(id)) || r.state == recRejected || now-r.sent < max(rules.retainMs, rules.lagMs) {
				continue
			}
			k := keyIndex(uint64(id))
			if r.offset >= 0 {
				oldTombAt[k] = max(oldTombAt[k], r.offset)
			} else {
				oldTombID[k] = uint64(id)
			}
		}
		for id := 1; id < len(m.recs); id++ {
			r := &m.recs[id]
			if r.tp.Topic != topic || r.state != recAcked || (!m.strict && r.ackEpoch != m.epoch) || r.final == m.finalPass || r.offset < m.part(r.tp).start {
				continue
			}
			k := keyIndex(uint64(id))
			if seen[k] > r.offset || oldTombAt[k] >= r.offset || oldTombID[k] > uint64(id) { // replaced, or an old tombstone itself or after it
				continue
			}
			return fmt.Errorf("C1: record %d, acknowledged at %v offset %d with key %s, is gone, and nothing later of the key is left to replace it", id, r.tp, r.offset, Key(uint64(id)))
		}
		// C5: a tombstone that went, with nothing after it, took everything older.
		for id := 1; id < len(m.recs); id++ {
			r := &m.recs[id]
			if r.tp.Topic != topic || !Tombstone(uint64(id)) || r.state != recAcked || r.final == m.finalPass || r.offset < m.part(r.tp).start {
				continue
			}
			k := keyIndex(uint64(id))
			if seen[k] > r.offset || seen[k] < 0 {
				continue
			}
			// The latest record of key k that is left is older than the tombstone.
			return fmt.Errorf("C5: the tombstone for key %s at %v offset %d is gone, but a record of the key at offset %d, older, is still there", Key(uint64(id)), r.tp, r.offset, seen[k])
		}
	}
	return nil
}

// readCompacted checks the batches of a fetch of a compacted topic, in which
// offsets may be missing, and returns the offset after the last one.
func (m *Model) readCompacted(f Fetch) (next int64, err error) {
	next, cursor := -1, f.Offset
	for rest := f.Records; len(rest) > 0; {
		info, code := wire.ParseCompacted(rest, len(rest))
		if code != wire.ErrNone {
			return -1, fmt.Errorf("I4: %v returned a malformed batch (code %d)", f.TP, code)
		}
		b := rest[:info.Size]
		recs, err := wire.SplitBatch(b)
		if err != nil {
			return -1, fmt.Errorf("I4: %v returned an undecodable batch: %w", f.TP, err)
		}
		base := wire.BatchBaseOffset(b)
		end := base + int64(wire.BatchLastOffsetDelta(b)) + 1
		switch {
		case next == -1 && end <= f.Offset:
			return -1, fmt.Errorf("I2: %v fetch at %d began with a batch covering %d..%d", f.TP, f.Offset, base, end-1)
		case next != -1 && base < next:
			return -1, fmt.Errorf("I2: %v batch at %d follows a batch ending at %d", f.TP, base, next-1)
		}
		present, err := m.observeCompacted(f.TP, base, recs)
		if err != nil {
			return -1, err
		}
		if err := m.checkGap(f.TP, cursor, end, present); err != nil {
			return -1, err
		}
		cursor, next, rest = end, end, rest[info.Size:]
	}
	return next, nil
}
