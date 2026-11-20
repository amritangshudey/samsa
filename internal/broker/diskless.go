package broker

import (
	"fmt"
	"hash/crc32"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Diskless topics (Item 21, docs/design/c21-diskless-topics.md, ADR 0038):
// any broker uploads produced batches directly to ObjectStore and commits only
// a compact cmdAppendRef (~90 bytes) through Raft.

func isDiskless(t *topic) bool {
	return t != nil && t.configs["diskless.enabled"] == "true"
}

type appendRefBatch struct {
	count         int32
	pid           int64
	epoch         int16
	seq           int32
	maxTs         int64
	transactional bool
	objOffset     int32
	objLen        int32
	objCRC        uint32
}

type appendRefCmd struct {
	topic   string
	part    int32
	objKey  string
	batches []appendRefBatch
}

func encodeAppendRef(topic string, part int32, objKey string, batches []appendRefBatch) []byte {
	w := wire.Writer{B: []byte{cmdAppendRef}}
	w.String(false, topic)
	w.Int32(part)
	w.String(false, objKey)
	w.ArrayLen(false, len(batches))
	for _, b := range batches {
		w.Int32(b.count)
		w.Int64(b.pid)
		w.Int16(b.epoch)
		w.Int32(b.seq)
		w.Int64(b.maxTs)
		w.Bool(b.transactional)
		w.Int32(b.objOffset)
		w.Int32(b.objLen)
		w.Int32(int32(b.objCRC))
	}
	return w.B
}

func decodeAppendRef(r *wire.Reader) (appendRefCmd, bool) {
	c := appendRefCmd{
		topic:  r.String(false),
		part:   r.Int32(),
		objKey: r.String(false),
	}
	c.batches = wire.Array(r, false, func() appendRefBatch {
		return appendRefBatch{
			count:         r.Int32(),
			pid:           r.Int64(),
			epoch:         r.Int16(),
			seq:           r.Int32(),
			maxTs:         r.Int64(),
			transactional: r.Bool(),
			objOffset:     r.Int32(),
			objLen:        r.Int32(),
			objCRC:        uint32(r.Int32()),
		}
	})
	if r.Err() != nil || r.Len() != 0 || c.objKey == "" || len(c.batches) == 0 {
		return c, false
	}
	for _, b := range c.batches {
		if b.count <= 0 || b.objOffset < 0 || b.objLen <= 0 {
			return c, false
		}
	}
	return c, true
}

func (s *state) applyAppendRef(r *wire.Reader, index int64) (result, error) {
	if s.level < 9 {
		return result{}, fmt.Errorf("%w: appendRef at index %d requires level 9", errBadCommand, index)
	}
	c, ok := decodeAppendRef(r)
	if !ok {
		return result{}, fmt.Errorf("%w: appendRef at index %d", errBadCommand, index)
	}
	p := s.partition(c.topic, c.part)
	if p == nil {
		return result{errCode: wire.ErrUnknownTopicOrPartition, baseOffset: -1}, nil
	}
	// Deduplicate forwarded cmdAppendRef retries/duplicates by unique objKey.
	for i := len(p.batches) - 1; i >= 0 && i >= len(p.batches)-64; i-- {
		if p.batches[i].objKey == c.objKey {
			first := p.batches[i].base
			for j := i - 1; j >= 0 && p.batches[j].objKey == c.objKey; j-- {
				first = p.batches[j].base
			}
			return result{baseOffset: first}, nil
		}
	}
	firstBase := int64(-1)
	for k, b := range c.batches {
		info := wire.BatchInfo{
			Count:         b.count,
			ProducerID:    b.pid,
			ProducerEpoch: b.epoch,
			BaseSequence:  b.seq,
			MaxTimestamp:  b.maxTs,
			Transactional: b.transactional,
		}
		idempotent := info.ProducerID >= 0 && !s.noSequences
		if info.Transactional {
			txn := s.txnByPID(info.ProducerID)
			switch {
			case txn == nil || txn.phase != txnOngoing:
				return result{errCode: wire.ErrInvalidTxnState, baseOffset: -1}, nil
			case txn.epoch != info.ProducerEpoch:
				return result{errCode: wire.ErrInvalidProducerEpoch, baseOffset: -1}, nil
			}
		}
		if idempotent {
			if dup, code := p.admit(info); code != wire.ErrNone {
				return result{errCode: code, baseOffset: -1}, nil
			} else if dup >= 0 {
				if k == 0 {
					firstBase = dup
				}
				continue
			}
		}
		base := p.next
		if k == 0 {
			firstBase = base
		}
		p.next += int64(b.count)
		p.batches = append(p.batches, batchRef{
			base:         base,
			logIndex:     0,
			maxTimestamp: b.maxTs,
			size:         b.objLen,
			objKey:       c.objKey,
			objOffset:    b.objOffset,
			objLen:       b.objLen,
			objCRC:       b.objCRC,
		})
		if info.Transactional {
			if p.openTxns == nil {
				p.openTxns = map[int64]txnOpen{}
			}
			if _, exists := p.openTxns[info.ProducerID]; !exists {
				p.openTxns[info.ProducerID] = txnOpen{epoch: info.ProducerEpoch, firstOffset: base}
			}
		}
		if idempotent {
			p.remember(info, base, index)
		}
	}
	return result{baseOffset: firstBase}, nil
}

// canServeDisklessProduce reports whether this follower can accept req directly:
// object storage is enabled, the cluster is at level 9+, a leader is known for
// metadata ordering, and every topic in req is a diskless topic.
func (n *Node) canServeDisklessProduce(req *wire.ProduceRequest) bool {
	if n.obj == nil || n.st.level < 9 || len(req.Topics) == 0 {
		return false
	}
	leader, ok := n.raft.Leader()
	if !ok || leader == sys.NodeID(n.cfg.NodeID) {
		return false
	}
	for _, t := range req.Topics {
		if !isDiskless(n.st.topics[t.Name]) {
			return false
		}
	}
	return true
}

const maxObjPutRetries = 16

// produceDisklessPartition uploads all batches of one partition in a ProduceRequest
// as a single object and commits them atomically in one cmdAppendRef entry so
// offsets are strictly contiguous even under concurrent producers.
func (n *Node) produceDisklessPartition(topic string, part int32, raw []byte, sizes []int, infos []wire.BatchInfo, done func(baseOffset int64, errCode int16)) {
	n.disklessSeq++
	key := fmt.Sprintf("diskless/%d-%d-%016x.obj", n.cfg.NodeID, n.raft.Term(), n.rand.Uint64())
	cp := append([]byte(nil), raw...)

	batches := make([]appendRefBatch, len(sizes))
	var off int32
	for k, sz := range sizes {
		info := infos[k]
		batches[k] = appendRefBatch{
			count:         info.Count,
			pid:           info.ProducerID,
			epoch:         info.ProducerEpoch,
			seq:           info.BaseSequence,
			maxTs:         info.MaxTimestamp,
			transactional: info.Transactional,
			objOffset:     off,
			objLen:        int32(sz),
			objCRC:        crc32.Checksum(cp[off:off+int32(sz)], castagnoliTable),
		}
		off += int32(sz)
	}

	var tryPut func(attempt int)
	tryPut = func(attempt int) {
		n.objPut(key, cp, func(err error) {
			if n.fatal != nil {
				done(-1, wire.ErrNotLeaderOrFollower)
				return
			}
			if err != nil {
				if attempt < maxObjPutRetries {
					tryPut(attempt + 1)
					return
				}
				done(-1, wire.ErrNotLeaderOrFollower)
				return
			}
			for _, b := range batches {
				ref := batchRef{objKey: key, objOffset: b.objOffset, objLen: b.objLen, objCRC: b.objCRC}
				n.objCache.put(ref, cp[b.objOffset:b.objOffset+b.objLen])
			}
			cmd := encodeAppendRef(topic, part, key, batches)
			n.proposeOrForwardTimeout(cmd, 2*time.Second, func(r result) {
				if r.errCode == wire.ErrNotController {
					r.errCode = wire.ErrNotLeaderOrFollower
				}
				if r.errCode != wire.ErrNone {
					done(-1, r.errCode)
					return
				}
				n.uploadedObjs[key] = true
				n.stats.disklessBatches += int64(len(batches))
				n.stats.disklessBytes += int64(len(cp))
				done(r.baseOffset, wire.ErrNone)
			})
		})
	}
	tryPut(0)
}
