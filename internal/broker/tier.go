package broker

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Unified object storage (tiered storage, Item 20; diskless foundation, Item 21).
// See docs/design/c20-object-storage.md and ADR 0037.

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type tierEntry struct {
	topic       string
	part        int32
	base        int64
	oldLogIndex int64
	objOffset   int32
	objLen      int32
	objCRC      uint32
}

func encodeTier(objKey string, entries []tierEntry) []byte {
	w := wire.Writer{B: []byte{cmdTier}}
	w.String(false, objKey)
	w.ArrayLen(false, len(entries))
	for _, e := range entries {
		w.String(false, e.topic)
		w.Int32(e.part)
		w.Int64(e.base)
		w.Int64(e.oldLogIndex)
		w.Int32(e.objOffset)
		w.Int32(e.objLen)
		w.Int32(int32(e.objCRC))
	}
	return w.B
}

// tierEntryBytes is what one entry adds to encodeTier's output.
func tierEntryBytes(topic string) int { return 2 + len(topic) + 4 + 8 + 8 + 4 + 4 + 4 }

func (s *state) applyTier(r *wire.Reader, index int64) (result, error) {
	if s.level < 8 {
		return result{}, fmt.Errorf("%w: tier at index %d requires level 8", errBadCommand, index)
	}
	objKey := r.String(false)
	entries := wire.Array(r, false, func() tierEntry {
		return tierEntry{
			topic:       r.String(false),
			part:        r.Int32(),
			base:        r.Int64(),
			oldLogIndex: r.Int64(),
			objOffset:   r.Int32(),
			objLen:      r.Int32(),
			objCRC:      uint32(r.Int32()),
		}
	})
	if r.Err() != nil || r.Len() != 0 || objKey == "" {
		return result{}, fmt.Errorf("%w: tier at index %d", errBadCommand, index)
	}
	for _, e := range entries {
		p := s.partition(e.topic, e.part)
		if p == nil {
			continue
		}
		i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].base >= e.base })
		if i < len(p.batches) && p.batches[i].base == e.base &&
			p.batches[i].logIndex == e.oldLogIndex && p.batches[i].objKey == "" {
			p.batches[i].logIndex = 0
			p.batches[i].objKey = objKey
			p.batches[i].objOffset = e.objOffset
			p.batches[i].objLen = e.objLen
			p.batches[i].objCRC = e.objCRC
		}
	}
	return result{}, nil
}

// --- Bounded in-memory cache for remote batches ---

type objCacheKey struct {
	key string
	off int32
	len int32
}

type objCache struct {
	limit int64
	used  int64
	order []objCacheKey
	items map[objCacheKey][]byte
}

func newObjCache(limit int64) *objCache {
	if limit <= 0 {
		limit = 32 << 20
	}
	return &objCache{limit: limit, items: map[objCacheKey][]byte{}}
}

func (c *objCache) get(ref batchRef) ([]byte, bool) {
	b, ok := c.items[objCacheKey{ref.objKey, ref.objOffset, ref.objLen}]
	return b, ok
}

func (c *objCache) put(ref batchRef, data []byte) {
	k := objCacheKey{ref.objKey, ref.objOffset, ref.objLen}
	if _, ok := c.items[k]; ok {
		return
	}
	cp := append([]byte(nil), data...)
	c.items[k] = cp
	c.order = append(c.order, k)
	c.used += int64(len(cp))
	for c.used > c.limit && len(c.order) > 1 {
		old := c.order[0]
		c.order = c.order[1:]
		if b, ok := c.items[old]; ok {
			c.used -= int64(len(b))
			delete(c.items, old)
		}
	}
}

// --- Async ObjectStore dispatch on Node ---

// OnObjectDone implements sys.ObjectHandler.
func (n *Node) OnObjectDone(id sys.ObjOpID, data []byte, err error) error {
	if n.fatal != nil {
		return n.fatal
	}
	fn, ok := n.objOps[id]
	if !ok {
		return nil
	}
	delete(n.objOps, id)
	fn(data, err)
	return n.settle()
}

func (n *Node) objPut(key string, data []byte, onDone func(error)) {
	n.nextObjOp++
	id := n.nextObjOp
	n.objOps[id] = func(_ []byte, err error) { onDone(err) }
	n.obj.Put(id, key, data)
}

func (n *Node) objGet(key string, off, length int64, onDone func([]byte, error)) {
	n.nextObjOp++
	id := n.nextObjOp
	n.objOps[id] = onDone
	n.obj.Get(id, key, off, length)
}

func (n *Node) objDelete(key string) {
	if n.obj == nil {
		return
	}
	n.nextObjOp++
	id := n.nextObjOp
	n.objOps[id] = func([]byte, error) {}
	n.obj.Delete(id, key)
}

// --- Tiering closed segments ---

// tierSegment packs live local batches in [start, end) into one object,
// uploads it to ObjectStore, and proposes cmdTier once confirmed; what does
// not fit one object goes in the next. Returns true if tiering work was
// started (or is already in flight).
func (n *Node) tierSegment(start, end int64) bool {
	if n.tiering {
		return true
	}
	var buf []byte
	var entries []tierEntry
	entryBytes := 0
	for _, name := range n.st.names {
		if compacts(n.st.topics[name]) && !n.cfg.UnsafeTierCompacted {
			continue // compaction rewrites its batches in place, and an object cannot be (BUGS.md S18)
		}
		for i, p := range n.st.topics[name].parts {
			for _, b := range p.batches {
				if b.objKey != "" || b.control != 0 || b.logIndex < start || b.logIndex >= end {
					continue
				}
				// The cmdTier must fit a log entry: its entries in
				// MaxBatchBytes, and its header and Raft's in
				// commandOverhead, whatever the first entry (BUGS.md S16).
				if len(entries) > 0 && (len(buf) >= cleanBatchBytes || entryBytes+tierEntryBytes(name) > n.cfg.MaxBatchBytes) {
					goto upload
				}
				data, ok := n.readBatch(b)
				if !ok {
					return false
				}
				off := int32(len(buf))
				buf = append(buf, data...)
				entries = append(entries, tierEntry{
					topic:       name,
					part:        int32(i),
					base:        b.base,
					oldLogIndex: b.logIndex,
					objOffset:   off,
					objLen:      int32(len(data)),
					objCRC:      crc32.Checksum(data, castagnoliTable),
				})
				entryBytes += tierEntryBytes(name)
			}
		}
	}
upload:
	if len(entries) == 0 {
		return false
	}
	n.tierSeq++
	key := fmt.Sprintf("tier/%d-%d-%d-%016x.obj", n.raft.Term(), start, end, n.rand.Uint64())
	n.tiering = true

	// Deliberate bug mutation (Item 20): propose cmdTier (allowing local segment
	// trim) BEFORE the object-store upload is confirmed!
	if n.cfg.UnsafeDeleteBeforeUpload {
		n.objPut(key, buf, func(error) {}) // fire-and-forget: ignored even if lost/slow
		n.cleaning++
		n.propose(encodeTier(key, entries), func(result) {
			n.cleaning--
			n.tiering = false
		})
		return true
	}

	n.objPut(key, buf, func(err error) {
		if err != nil {
			n.log.Warn("tier upload failed; will retry", "key", key, "err", err)
			n.tiering = false
			return
		}
		if !n.serving || n.fatal != nil {
			n.tiering = false
			return
		}
		// Cache the uploaded batches locally so immediate reads don't round-trip.
		for _, e := range entries {
			ref := batchRef{objKey: key, objOffset: e.objOffset, objLen: e.objLen, objCRC: e.objCRC}
			n.objCache.put(ref, buf[e.objOffset:e.objOffset+e.objLen])
		}
		n.cleaning++
		n.propose(encodeTier(key, entries), func(res result) {
			n.cleaning--
			n.tiering = false
			n.uploadedObjs[key] = true // referenced, or garbage for gcObjects
			if res.errCode == wire.ErrNone {
				n.stats.tieredBatches += int64(len(entries))
				n.stats.tieredBytes += int64(len(buf))
			}
		})
	})
	return true
}

// gcObjects deletes uploaded objects that are no longer referenced by any batchRef.
// Under non-strict sync policies (SyncInterval / SyncNever), a crash can roll back
// an unsynced DeleteRecords and resurrect a batchRef, so GC only runs under SyncAlways.
func (n *Node) gcObjects() {
	if n.obj == nil || n.tiering || n.cfg.Sync != SyncAlways || len(n.uploadedObjs) == 0 {
		return
	}
	referenced := map[string]bool{}
	for _, name := range n.st.names {
		for _, p := range n.st.topics[name].parts {
			for _, b := range p.batches {
				if b.objKey != "" {
					referenced[b.objKey] = true
				}
			}
		}
	}
	for _, key := range sortedKeys(n.uploadedObjs) {
		if referenced[key] {
			n.uploadedObjs[key] = true
			continue
		}
		// Two-pass grace period (1s): avoid deleting an object that an in-flight
		// remote fetch just started reading before expiry applied.
		if n.uploadedObjs[key] {
			n.uploadedObjs[key] = false
			continue
		}
		delete(n.uploadedObjs, key)
		n.objDelete(key)
	}
}

// --- Async remote batch prefetch for Fetch ---

// missingRemoteBatches scans req's partitions and returns uncached remote batchRefs
// that buildFetch will need.
func (n *Node) missingRemoteBatches(c *conn, v int16, req *wire.FetchRequest) []batchRef {
	if n.obj == nil {
		return nil
	}
	budget := n.cfg.MaxFetchBytes
	if req.MaxBytes > 0 && int(req.MaxBytes) < budget {
		budget = int(req.MaxBytes)
	}
	var missing []batchRef
	seen := map[objCacheKey]bool{}
	for _, t := range req.Topics {
		name := t.Name
		if v >= 13 {
			name = n.st.byID[t.TopicID]
		}
		if name == "" || !n.may(c, opRead, resTopic, name) {
			continue
		}
		for _, q := range t.Partitions {
			p := n.st.partition(name, q.Index)
			if p == nil || q.Offset < p.start || q.Offset >= p.next {
				continue
			}
			maxOff := p.next
			if req.IsolationLevel == 1 && !n.st.readUncommitted {
				maxOff = p.lso()
			}
			if q.Offset >= maxOff {
				continue
			}
			i := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].base > q.Offset }) - 1
			if i < 0 {
				i = 0
			}
			partLimit := min(int(q.MaxBytes), budget)
			partBytes := 0
			for ; i < len(p.batches); i++ {
				ref := p.batches[i]
				if ref.base >= maxOff {
					break
				}
				bLen := int(ref.objLen)
				if bLen <= 0 {
					bLen = int(ref.size)
				}
				if ref.base < q.Offset {
					// The read skips it if q.Offset lies in a gap after it,
					// left by compaction, and starts with the next one
					// whatever its size (BUGS.md S19).
					bLen = 0
				}
				if partBytes > 0 && partBytes+bLen > partLimit {
					break
				}
				partBytes += bLen
				if ref.objKey != "" {
					if _, ok := n.objCache.get(ref); !ok {
						k := objCacheKey{ref.objKey, ref.objOffset, ref.objLen}
						if !seen[k] {
							seen[k] = true
							missing = append(missing, ref)
						}
					}
				}
			}
		}
	}
	return missing
}

const maxObjGetRetries = 16

// fetchRemoteBatches fetches all refs from ObjectStore into n.objCache, verifying
// CRC32C and retrying transient errors or torn reads up to maxObjGetRetries times.
func (n *Node) fetchRemoteBatches(refs []batchRef, done func(ok bool)) {
	pending := len(refs)
	allOK := true
	var fetchOne func(ref batchRef, attempt int)
	fetchOne = func(ref batchRef, attempt int) {
		n.objGet(ref.objKey, int64(ref.objOffset), int64(ref.objLen), func(data []byte, err error) {
			if err == nil && (len(data) != int(ref.objLen) || crc32.Checksum(data, castagnoliTable) != ref.objCRC) {
				err = fmt.Errorf("object %s[%d:%d]: crc or length mismatch", ref.objKey, ref.objOffset, ref.objLen)
			}
			if err != nil {
				n.stats.objReadErrors++
				if !errors.Is(err, sys.ErrNotExist) && attempt < maxObjGetRetries {
					fetchOne(ref, attempt+1)
					return
				}
				n.log.Warn("remote batch read failed", "key", ref.objKey, "err", err)
				allOK = false
			} else {
				n.stats.objReadCount++
				n.objCache.put(ref, data)
			}
			pending--
			if pending == 0 {
				done(allOK)
			}
		})
	}
	for _, ref := range refs {
		fetchOne(ref, 0)
	}
}
