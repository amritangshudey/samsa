package broker

import (
	"fmt"
	"slices"

	"github.com/amritangshudey/samsa/internal/wire"
)

// snapshotVersion leads every encoded state, so that a newer build can tell
// an older snapshot apart.
// Version 2 adds the log start and each partition's start offset; version 3
// each partition's idempotent producers; version 4 each producer's last
// log index.
const snapshotVersion = 4

// encode returns the whole state machine as bytes, for a Raft snapshot
// (docs/design/a1-snapshots.md). Maps are walked in sorted order, so equal
// states encode to equal bytes. Record data is not included: it stays in
// the log, and batches are referred to by log index.
func (s *state) encode() []byte {
	w := wire.Writer{B: []byte{snapshotVersion}}
	w.Int64(s.logStart)
	w.ArrayLen(false, len(s.names))
	for _, name := range s.names {
		t := s.topics[name]
		w.String(false, name)
		w.ArrayLen(false, len(t.parts))
		for _, p := range t.parts {
			w.Int64(p.next)
			w.Int64(p.start)
			w.ArrayLen(false, len(p.batches))
			for _, b := range p.batches {
				w.Int64(b.base)
				w.Int64(b.logIndex)
				w.Int64(b.maxTimestamp)
			}
			ids := sortedKeys(p.producers)
			w.ArrayLen(false, len(ids))
			for _, id := range ids {
				ps := p.producers[id]
				w.Int64(id)
				w.Int16(ps.epoch)
				w.Int64(ps.index)
				w.ArrayLen(false, len(ps.batches))
				for _, b := range ps.batches {
					w.Int32(b.first)
					w.Int32(b.last)
					w.Int64(b.base)
				}
			}
		}
	}
	names := sortedKeys(s.groups)
	w.ArrayLen(false, len(names))
	for _, name := range names {
		g := s.groups[name]
		w.String(false, name)
		w.Int32(g.generation)
		w.String(false, g.protocolType)
		w.String(false, g.protocol)
		w.String(false, g.leader)
		w.Bool(g.assigned)
		w.ArrayLen(false, len(g.members))
		for _, m := range g.members {
			w.String(false, m.id)
			w.Int32(m.sessionTimeout)
			w.Int32(m.rebalanceTimeout)
			w.ArrayLen(false, len(m.protocols))
			for _, p := range m.protocols {
				w.String(false, p.Name)
				w.Bytes(false, p.Metadata)
			}
			w.Bytes(false, m.assignment)
		}
		topics := sortedKeys(g.offsets)
		w.ArrayLen(false, len(topics))
		for _, topic := range topics {
			parts := sortedKeys(g.offsets[topic])
			w.String(false, topic)
			w.ArrayLen(false, len(parts))
			for _, part := range parts {
				o := g.offsets[topic][part]
				w.Int32(part)
				w.Int64(o.offset)
				w.Int32(o.leaderEpoch)
				w.String(false, o.metadata)
				w.Int64(o.time)
			}
		}
	}
	return w.B
}

// decodeState is the inverse of encode. Every length is checked against the
// input, as for network input (roadmap rule R7); nothing in the result
// aliases b.
func decodeState(b []byte) (*state, error) {
	if len(b) == 0 || b[0] < 1 || b[0] > snapshotVersion {
		return nil, fmt.Errorf("%w: snapshot of unknown version", errBadCommand)
	}
	v := b[0]
	r := wire.NewReader(b[1:])
	s := newState()
	if v >= 2 {
		s.logStart = r.Int64()
	}
	bad := false // a structure the encoder could not have written
	wire.Array(r, false, func() struct{} {
		t := &topic{name: r.String(false)}
		t.parts = wire.Array(r, false, func() *partition {
			p := &partition{next: r.Int64()}
			if v >= 2 {
				p.start = r.Int64()
			}
			p.batches = wire.Array(r, false, func() batchRef {
				return batchRef{base: r.Int64(), logIndex: r.Int64(), maxTimestamp: r.Int64()}
			})
			if p.start < 0 || p.start > p.next || len(p.batches) > 0 && (p.batches[0].base != p.start || p.batches[0].logIndex < s.logStart) {
				bad = true
			}
			if v >= 3 {
				last := int64(-1) // ids ascend, as the encoder writes them
				wire.Array(r, false, func() struct{} {
					id, ps := r.Int64(), &producer{epoch: r.Int16()}
					if v >= 4 {
						ps.index = r.Int64()
						bad = bad || ps.index < 0
					} else {
						// Version 3 did not keep it. The newest batch's index,
						// or the log start, is at or after the real one, so
						// expiry is late, never early.
						ps.index = s.logStart
						if len(p.batches) > 0 {
							ps.index = p.batches[len(p.batches)-1].logIndex
						}
					}
					ps.batches = wire.Array(r, false, func() sentBatch { return sentBatch{r.Int32(), r.Int32(), r.Int64()} })
					if id <= last || len(ps.batches) == 0 || len(ps.batches) > maxProducerBatches {
						bad = true
					}
					for _, b := range ps.batches {
						bad = bad || b.first < 0 || b.last < 0 || b.base < 0 || b.base >= p.next
					}
					if p.producers == nil {
						p.producers = map[int64]*producer{}
					}
					p.producers[id], last = ps, id
					return struct{}{}
				})
			}
			return p
		})
		if r.Err() == nil && (s.topics[t.name] != nil || len(t.parts) == 0 || len(s.names) > 0 && s.names[len(s.names)-1] >= t.name) {
			bad = true
		}
		s.topics[t.name] = t
		s.names = append(s.names, t.name)
		return struct{}{}
	})
	wire.Array(r, false, func() struct{} {
		name := r.String(false)
		g := &group{generation: r.Int32(), protocolType: r.String(false), protocol: r.String(false), leader: r.String(false),
			assigned: r.Bool(), offsets: map[string]map[int32]committedOffset{}}
		g.members = wire.Array(r, false, func() *member {
			m := &member{id: r.String(false), sessionTimeout: r.Int32(), rebalanceTimeout: r.Int32()}
			m.protocols = wire.Array(r, false, func() wire.GroupProtocol {
				return wire.GroupProtocol{Name: r.String(false), Metadata: slices.Clone(r.Bytes(false))}
			})
			m.assignment = slices.Clone(r.Bytes(false))
			return m
		})
		wire.Array(r, false, func() struct{} {
			topic := r.String(false)
			parts := map[int32]committedOffset{}
			wire.Array(r, false, func() struct{} {
				part := r.Int32()
				parts[part] = committedOffset{offset: r.Int64(), leaderEpoch: r.Int32(), metadata: r.String(false), time: r.Int64()}
				return struct{}{}
			})
			g.offsets[topic] = parts
			return struct{}{}
		})
		if r.Err() == nil && s.groups[name] != nil {
			bad = true
		}
		s.groups[name] = g
		return struct{}{}
	})
	if r.Err() != nil || r.Len() != 0 || bad {
		return nil, fmt.Errorf("%w: snapshot state: %w, %d bytes left, malformed %v", errBadCommand, r.Err(), r.Len(), bad)
	}
	return s, nil
}
