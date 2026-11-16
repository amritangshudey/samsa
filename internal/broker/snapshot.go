package broker

import (
	"fmt"
	"slices"

	"github.com/amritangshudey/samsa/internal/scram"
	"github.com/amritangshudey/samsa/internal/wire"
)

// snapshotVersion leads every encoded state, so that a newer build can tell
// an older snapshot apart.
// Version 2 adds the log start and each partition's start offset; version 3
// each partition's idempotent producers; version 4 each producer's last
// log index; version 5 the ACLs; version 6 the SCRAM credentials; version
// 7 the format level; version 8 topic configurations and batch sizes
// (design C2); version 9 topic IDs (ADR 0032); version 10 consumer-protocol
// groups (ADR 0033); version 11 share groups (ADR 0034); version 12 transactions (ADR 0035). Each later field comes last,
// so the encoder writes the oldest version that holds the state: an older
// build in the cluster during a rolling upgrade can still read it (design G7).
const snapshotVersion = 12

// encode returns the whole state machine as bytes, for a Raft snapshot
// (docs/design/a1-snapshots.md). Maps are walked in sorted order, so equal
// states encode to equal bytes. Record data is not included: it stays in
// the log, and batches are referred to by log index.
func (s *state) encode() []byte {
	version := byte(4)
	switch {
	case s.level >= 7:
		version = 12
	case s.level >= 6:
		version = 11
	case s.level >= 5:
		version = 10
	case s.level >= 4:
		version = 9
	case s.level >= 2:
		version = 8
	case s.level > 0:
		version = 7
	case len(s.acls) > 0 || len(s.creds) > 0:
		version = 6
	}
	w := wire.Writer{B: []byte{version}}
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
	if version < 5 {
		return w.B
	}
	w.ArrayLen(false, len(s.acls))
	for _, a := range s.acls {
		writeACL(&w, a)
	}
	users := sortedKeys(s.creds)
	w.ArrayLen(false, len(users))
	for _, user := range users {
		w.String(false, user)
		w.ArrayLen(false, len(s.creds[user]))
		for _, c := range s.creds[user] {
			w.String(false, c.String())
		}
	}
	if version >= 7 {
		w.Int8(int8(s.level))
	}
	if version >= 8 {
		var configured []string
		for _, name := range s.names {
			if s.topics[name].configs != nil {
				configured = append(configured, name)
			}
		}
		w.ArrayLen(false, len(configured))
		for _, name := range configured {
			w.String(false, name)
			writeConfigs(&w, s.topics[name].configs)
		}
		// Sizes in the order of the batches above.
		for _, name := range s.names {
			for _, p := range s.topics[name].parts {
				for _, b := range p.batches {
					w.Int32(b.size)
				}
			}
		}
	}
	if version >= 9 {
		for _, name := range s.names {
			w.UUID(s.topics[name].id)
		}
	}
	if version >= 10 {
		var cgs []string
		for _, name := range names {
			if s.groups[name].cg != nil {
				cgs = append(cgs, name)
			}
		}
		w.ArrayLen(false, len(cgs))
		for _, name := range cgs {
			cg := s.groups[name].cg
			w.String(false, name)
			w.Int32(cg.epoch)
			w.Int32(cg.targetEpoch)
			w.String(false, cg.assignor)
			w.ArrayLen(false, len(cg.meta))
			for _, tm := range cg.meta {
				w.UUID(tm.id)
				w.Int32(tm.parts)
			}
			w.ArrayLen(false, len(cg.members))
			for _, m := range cg.members {
				w.String(false, m.id)
				w.Int32(m.epoch)
				w.Int32(m.prevEpoch)
				w.String(false, m.rack)
				w.String(false, m.clientID)
				w.String(false, m.host)
				w.Int32(m.rebalanceMs)
				w.String(false, m.assignor)
				w.ArrayLen(false, len(m.subscribed))
				for _, sub := range m.subscribed {
					w.String(false, sub)
				}
				w.ArrayLen(false, len(m.owned))
				for _, p := range m.owned {
					w.UUID(p.id)
					w.Int32(p.part)
				}
				w.ArrayLen(false, len(m.target))
				for _, p := range m.target {
					w.UUID(p.id)
					w.Int32(p.part)
				}
			}
		}
	}
	if version >= 11 {
		var sgs []string
		for _, name := range names {
			if s.groups[name].sg != nil {
				sgs = append(sgs, name)
			}
		}
		w.ArrayLen(false, len(sgs))
		for _, name := range sgs {
			sg := s.groups[name].sg
			w.String(false, name)
			w.Int32(sg.epoch)
			w.ArrayLen(false, len(sg.meta))
			for _, tm := range sg.meta {
				w.UUID(tm.id)
				w.Int32(tm.parts)
			}
			w.ArrayLen(false, len(sg.members))
			for _, m := range sg.members {
				w.String(false, m.id)
				w.Int32(m.epoch)
				w.Int32(m.prevEpoch)
				w.String(false, m.rack)
				w.String(false, m.clientID)
				w.String(false, m.host)
				w.ArrayLen(false, len(m.subscribed))
				for _, sub := range m.subscribed {
					w.String(false, sub)
				}
				w.ArrayLen(false, len(m.assigned))
				for _, p := range m.assigned {
					w.UUID(p.id)
					w.Int32(p.part)
				}
			}
			tps := sortedTPKeys(sg.parts)
			w.ArrayLen(false, len(tps))
			for _, tp := range tps {
				sp := sg.parts[tp]
				w.UUID(tp.id)
				w.Int32(tp.part)
				w.Int64(sp.spso)
				w.Int64(sp.speo)
				offs := sortedKeys(sp.locks)
				w.ArrayLen(false, len(offs))
				for _, off := range offs {
					l := sp.locks[off]
					w.Int64(off)
					w.Int8(int8(l.state))
					w.Int16(l.delivery)
					w.String(false, l.owner)
					w.Int64(l.lockUntil)
				}
			}
		}
	}
	if version >= 12 {
		txnIDs := sortedKeys(s.txns)
		w.ArrayLen(false, len(txnIDs))
		for _, id := range txnIDs {
			tx := s.txns[id]
			w.String(false, tx.id)
			w.Int64(tx.pid)
			w.Int16(tx.epoch)
			w.Int64(tx.prevPID)
			w.Int16(tx.prevEpoch)
			w.Int8(int8(tx.phase))
			w.Int32(tx.timeoutMs)
			w.Int64(tx.startedMs)
			w.ArrayLen(false, len(tx.parts))
			for _, tp := range tx.parts {
				w.String(false, tp.topic)
				w.Int32(tp.part)
			}
			w.ArrayLen(false, len(tx.groups))
			for _, g := range tx.groups {
				w.String(false, g)
			}
			gnames := sortedKeys(tx.pendingOffsets)
			w.ArrayLen(false, len(gnames))
			for _, g := range gnames {
				w.String(false, g)
				tps := sortedTPNames(tx.pendingOffsets[g])
				w.ArrayLen(false, len(tps))
				for _, tp := range tps {
					o := tx.pendingOffsets[g][tp]
					w.String(false, tp.topic)
					w.Int32(tp.part)
					w.Int64(o.offset)
					w.Int32(o.leaderEpoch)
					w.String(false, o.metadata)
					w.Int64(o.time)
				}
			}
		}
		for _, name := range s.names {
			for _, p := range s.topics[name].parts {
				w.ArrayLen(false, len(p.batches))
				for _, b := range p.batches {
					w.Int8(int8(b.control))
					w.Int64(b.controlPID)
					w.Int16(b.controlEpoch)
				}
				pids := sortedKeys(p.openTxns)
				w.ArrayLen(false, len(pids))
				for _, pid := range pids {
					ot := p.openTxns[pid]
					w.Int64(pid)
					w.Int16(ot.epoch)
					w.Int64(ot.firstOffset)
				}
				w.ArrayLen(false, len(p.aborted))
				for _, ab := range p.aborted {
					w.Int64(ab.pid)
					w.Int64(ab.firstOffset)
					w.Int64(ab.lastOffset)
				}
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
			// The first batch holds the start: DeleteRecords can put it inside.
			// Not when compaction removed the batches around the start
			// (the first batch may begin after it).
			if p.start < 0 || p.start > p.next || len(p.batches) > 0 && (p.start == p.next ||
				len(p.batches) > 1 && p.batches[1].base <= p.start || slices.ContainsFunc(p.batches, func(b batchRef) bool { return b.logIndex < s.logStart })) {
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
	if v >= 5 {
		wire.Array(r, false, func() struct{} {
			a := readACL(r)
			if r.Err() == nil && (validACL(a) != "" || slices.Contains(s.acls, a)) {
				bad = true
			}
			s.acls = append(s.acls, a)
			return struct{}{}
		})
	}
	if v >= 6 {
		last := ""
		wire.Array(r, false, func() struct{} {
			user := r.String(false)
			var cs []scram.Credential
			wire.Array(r, false, func() struct{} {
				c, err := scram.ParseCredential(r.String(false))
				// one per mechanism, sorted, as apply keeps them
				bad = bad || (r.Err() == nil && (err != nil || len(cs) > 0 && cs[len(cs)-1].Mechanism >= c.Mechanism))
				cs = append(cs, c)
				return struct{}{}
			})
			bad = bad || (r.Err() == nil && (len(cs) == 0 || len(s.creds) > 0 && user <= last))
			s.creds[user], last = cs, user
			return struct{}{}
		})
	}
	if v >= 7 {
		level := r.Int8()
		bad = bad || (r.Err() == nil && (level < 1 || level > formatLevel))
		s.level = byte(level)
	}
	if v >= 8 {
		last := ""
		wire.Array(r, false, func() struct{} {
			name := r.String(false)
			configs, ok := readConfigs(r)
			t := s.topics[name]
			bad = bad || r.Err() == nil && (!ok || len(configs) == 0 || t == nil || name <= last)
			if t != nil {
				t.configs = configs
			}
			last = name
			return struct{}{}
		})
		for _, name := range s.names {
			for _, p := range s.topics[name].parts {
				for i := range p.batches {
					p.batches[i].size = r.Int32()
					bad = bad || p.batches[i].size < 0
				}
			}
		}
	}
	bad = bad || (v >= 9) != (s.level >= 4)
	if v >= 9 {
		for _, name := range s.names {
			id := r.UUID()
			bad = bad || r.Err() == nil && (id == (wire.UUID{}) || s.byID[id] != "")
			s.topics[name].id = id
			s.byID[id] = name
		}
	}
	bad = bad || (v >= 10) != (s.level >= 5)
	if v >= 10 {
		last := ""
		wire.Array(r, false, func() struct{} {
			name := r.String(false)
			cg := &cgroup{epoch: r.Int32(), targetEpoch: r.Int32(), assignor: r.String(false)}
			cg.meta = wire.Array(r, false, func() topicMeta { return topicMeta{id: r.UUID(), parts: r.Int32()} })
			cg.members = wire.Array(r, false, func() *cmember {
				m := &cmember{
					id: r.String(false), epoch: r.Int32(), prevEpoch: r.Int32(),
					rack: r.String(false), clientID: r.String(false), host: r.String(false),
					rebalanceMs: r.Int32(), assignor: r.String(false),
				}
				m.subscribed = wire.Array(r, false, func() string { return r.String(false) })
				m.owned = wire.Array(r, false, func() tpKey { return tpKey{r.UUID(), r.Int32()} })
				m.target = wire.Array(r, false, func() tpKey { return tpKey{r.UUID(), r.Int32()} })
				return m
			})
			g := s.groups[name]
			bad = bad || r.Err() == nil && (g == nil || g.cg != nil || len(g.members) > 0 || name <= last)
			if g != nil {
				g.cg = cg
			}
			last = name
			return struct{}{}
		})
	}
	bad = bad || (v >= 11) != (s.level >= 6)
	if v >= 11 {
		last := ""
		wire.Array(r, false, func() struct{} {
			name := r.String(false)
			sg := newShareGroup()
			sg.epoch = r.Int32()
			sg.meta = wire.Array(r, false, func() topicMeta { return topicMeta{id: r.UUID(), parts: r.Int32()} })
			sg.members = wire.Array(r, false, func() *sgMember {
				m := &sgMember{
					id: r.String(false), epoch: r.Int32(), prevEpoch: r.Int32(),
					rack: r.String(false), clientID: r.String(false), host: r.String(false),
				}
				m.subscribed = wire.Array(r, false, func() string { return r.String(false) })
				m.assigned = wire.Array(r, false, func() tpKey { return tpKey{r.UUID(), r.Int32()} })
				return m
			})
			wire.Array(r, false, func() struct{} {
				tp := tpKey{r.UUID(), r.Int32()}
				sp := &sharePart{spso: r.Int64(), speo: r.Int64(), locks: map[int64]*shareLock{}}
				wire.Array(r, false, func() struct{} {
					off := r.Int64()
					sp.locks[off] = &shareLock{
						state:     shareRecState(r.Int8()),
						delivery:  r.Int16(),
						owner:     r.String(false),
						lockUntil: r.Int64(),
					}
					return struct{}{}
				})
				sg.parts[tp] = sp
				return struct{}{}
			})
			g := s.groups[name]
			bad = bad || r.Err() == nil && (g == nil || g.sg != nil || g.cg != nil || len(g.members) > 0 || name <= last)
			if g != nil {
				g.sg = sg
			}
			last = name
			return struct{}{}
		})
	}
	bad = bad || (v >= 12) != (s.level >= 7)
	if v >= 12 {
		last := ""
		wire.Array(r, false, func() struct{} {
			id := r.String(false)
			tx := &txnState{
				id:        id,
				pid:       r.Int64(),
				epoch:     r.Int16(),
				prevPID:   r.Int64(),
				prevEpoch: r.Int16(),
				phase:     txnPhase(r.Int8()),
				timeoutMs: r.Int32(),
				startedMs: r.Int64(),
			}
			tx.parts = wire.Array(r, false, func() tpName {
				return tpName{topic: r.String(false), part: r.Int32()}
			})
			tx.groups = wire.Array(r, false, func() string { return r.String(false) })
			wire.Array(r, false, func() struct{} {
				g := r.String(false)
				if tx.pendingOffsets == nil {
					tx.pendingOffsets = map[string]map[tpName]committedOffset{}
				}
				m := map[tpName]committedOffset{}
				wire.Array(r, false, func() struct{} {
					tp := tpName{topic: r.String(false), part: r.Int32()}
					m[tp] = committedOffset{offset: r.Int64(), leaderEpoch: r.Int32(), metadata: r.String(false), time: r.Int64()}
					return struct{}{}
				})
				tx.pendingOffsets[g] = m
				return struct{}{}
			})
			bad = bad || r.Err() == nil && (len(s.txns) > 0 && id <= last)
			s.txns[id] = tx
			last = id
			return struct{}{}
		})
		for _, name := range s.names {
			for _, p := range s.topics[name].parts {
				ctrls := wire.Array(r, false, func() struct {
					c uint8
					p int64
					e int16
				} {
					return struct {
						c uint8
						p int64
						e int16
					}{c: uint8(r.Int8()), p: r.Int64(), e: r.Int16()}
				})
				bad = bad || r.Err() == nil && len(ctrls) != len(p.batches)
				if len(ctrls) == len(p.batches) {
					for i := range p.batches {
						p.batches[i].control = ctrls[i].c
						p.batches[i].controlPID = ctrls[i].p
						p.batches[i].controlEpoch = ctrls[i].e
					}
				}
				wire.Array(r, false, func() struct{} {
					if p.openTxns == nil {
						p.openTxns = map[int64]txnOpen{}
					}
					pid := r.Int64()
					p.openTxns[pid] = txnOpen{epoch: r.Int16(), firstOffset: r.Int64()}
					return struct{}{}
				})
				p.aborted = wire.Array(r, false, func() abortedTxn {
					return abortedTxn{pid: r.Int64(), firstOffset: r.Int64(), lastOffset: r.Int64()}
				})
			}
		}
	}
	if r.Err() != nil || r.Len() != 0 || bad {
		return nil, fmt.Errorf("%w: snapshot state: %w, %d bytes left, malformed %v", errBadCommand, r.Err(), r.Len(), bad)
	}
	return s, nil
}
