package broker

import (
	"fmt"
	"strconv"
	"time"

	"github.com/amritangshudey/samsa/internal/sys"
)

// Per-topic retention and the copy-forward cleaner
// (docs/design/c2-retention-compaction.md, section 3). Both run on the
// serving leader and only propose commands: expiry is a DeleteRecords, a
// copy is a cmdRelocate, and freeing disk is a Trim. Apply stays pure.

// retainEvery is how often the leader looks at retention. Each look walks
// every batch reference once.
const retainEvery = time.Second

// cleanBatchBytes bounds the bytes one cleaning round copies, so that
// copies never crowd out clients for long.
const cleanBatchBytes = 4 << 20

// retain runs after apply, and every retainEvery, on the serving leader at
// level 2 or above. Expiry and cleaning each wait while their own
// proposals are in flight, so as not to propose the same thing twice. They
// do not wait for each other: an expiry only removes references, and a
// relocation checks at apply that its reference is unchanged.
//
// Each look walks every batch reference, so the leader looks once a
// second, except that while the cleaner has work it starts its next round
// as soon as the last one has applied: one round a second could not keep
// up with a busy log.
func (n *Node) retain() {
	now := n.clock.Now()
	// Over the safety limit it acts at once, as retention did before C2.
	over := n.cfg.RetentionBytes > 0 && n.raft.LogBytesFrom(n.st.logStart) > n.cfg.RetentionBytes
	if now >= n.retainAt || over {
		n.retainAt = now + sys.Instant(retainEvery)
		if n.expiring == 0 {
			n.expireRecords(now)
		}
		n.cleanActive = true // clean decides
		n.compactActive = true
	}
	if n.cleaning == 0 && n.cleanActive {
		n.cleanActive = n.clean()
	}
	if n.compacting == 0 && n.compactActive && n.st.level >= 3 && !n.cfg.CompactionOff {
		n.compactActive = n.compact(now)
	}
}

// topicLimit returns a topic's limit for a config, the broker's default if
// the topic sets none, and -1 for none.
func topicLimit(t *topic, name string, def int64) int64 {
	if v, ok := t.configs[name]; ok {
		limit, _ := strconv.ParseInt(v, 10, 64) // checked at apply
		return limit
	}
	if def <= 0 {
		return -1
	}
	return def
}

// expireRecords proposes to move the start of every partition past what
// its topic's retention.ms and retention.bytes no longer keep. Expiry stops
// at the first batch kept, as Kafka's stops at the first segment kept.
func (n *Node) expireRecords(now sys.Instant) {
	nowMs := int64(now) / int64(time.Millisecond)
	for _, name := range n.st.names {
		t := n.st.topics[name]
		ms := topicLimit(t, "retention.ms", n.cfg.RetentionTime.Milliseconds())
		maxBytes := topicLimit(t, "retention.bytes", n.cfg.RetentionBytesPerPartition)
		if ms < 0 && maxBytes < 0 || !deletes(t) { // a compact-only topic is never expired
			continue
		}
		for i, p := range t.parts {
			cut := p.start
			end := func(j int) int64 { // the offset after batch j
				if j+1 < len(p.batches) {
					return p.batches[j+1].base
				}
				return p.next
			}
			if ms >= 0 {
				// shortcut: a batch without timestamps (-1) never expires by
				// time and holds back those after it; Kafka falls back to the
				// segment's file time. Upgrade if clients without
				// timestamps matter.
				for j, b := range p.batches {
					if b.maxTimestamp < 0 || b.maxTimestamp >= nowMs-ms {
						break
					}
					cut = max(cut, end(j))
				}
			}
			if maxBytes >= 0 {
				var total int64
				for _, b := range p.batches {
					total += int64(b.size)
				}
				for j := 0; j < len(p.batches) && total > maxBytes; j++ {
					total -= int64(p.batches[j].size)
					cut = max(cut, end(j))
				}
			}
			if cut > p.start {
				n.expiring++
				n.propose(encodeDeleteRecords(name, int32(i), cut), func(result) { n.expiring-- })
			}
		}
	}
}

// liveBytes returns the bytes on disk of the log entries holding every
// batch a partition references.
func (n *Node) liveBytes() int64 {
	var live int64
	for _, name := range n.st.names {
		for _, p := range n.st.topics[name].parts {
			for _, b := range p.batches {
				live += int64(b.size)
			}
		}
	}
	return live
}

// clean frees the head segment of the log once the log outgrows its
// budget: it copies the batches still referenced there to the tail, and
// once none is, trims the segment. If live data alone exceeds
// -retention-bytes, copying cannot help, and the oldest live data goes as
// before C2, counted in ForcedRetentionBytes. It reports whether it
// proposed anything, and so whether another round may follow.
func (n *Node) clean() bool {
	start := n.st.logStart
	disk, live := n.raft.LogBytesFrom(start), n.liveBytes()
	segment := n.cfg.SegmentBytes
	if segment <= 0 {
		segment = 64 << 20 // the storage default
	}
	limit := n.cfg.RetentionBytes
	if limit > 0 && disk > limit && live+segment > limit {
		n.forceTrim(limit)
		return false
	}
	// One segment more than the budget: then, either way, more than a
	// segment of the log is dead, and every round frees some of it. Without
	// that margin, a log of live data only could be copied around for ever.
	if float64(disk) <= n.cfg.RetentionSlack*float64(max(live, segment))+float64(segment) && (limit <= 0 || disk <= limit) {
		return false
	}
	end := n.raft.SegmentEnd(start)
	if end < 0 {
		return false // the head is the segment being written
	}
	copied := 0
	for _, name := range n.st.names {
		if n.cfg.UnsafeTrimLive {
			break
		}
		for i, p := range n.st.topics[name].parts {
			for _, b := range p.batches {
				if b.logIndex < start || b.logIndex >= end {
					continue
				}
				if copied >= cleanBatchBytes {
					return true // the rest in the next round
				}
				cmd, _, err := n.raft.Read(b.logIndex)
				if err != nil {
					n.fatal = fmt.Errorf("read committed batch at log index %d to relocate it: %w", b.logIndex, err)
					return false
				}
				data := appendBatch(cmd)
				copied += len(data)
				n.stats.relocatedBytes += int64(len(data))
				n.cleaning++
				n.propose(encodeRelocate(name, int32(i), b.base, b.logIndex, data), func(result) { n.cleaning-- })
			}
		}
	}
	if copied > 0 {
		return true
	}
	if end > n.trimProposed {
		n.trimProposed = end
		n.cleaning++
		n.propose(encodeTrim(end), func(result) { n.cleaning-- })
		return true
	}
	return false
}

// forceTrim is retention before C2: cut the oldest segments, live or not.
func (n *Node) forceTrim(limit int64) {
	cut := n.raft.RetentionCut(limit)
	if cut <= n.st.logStart || cut <= n.trimProposed {
		return
	}
	for _, name := range n.st.names {
		for _, p := range n.st.topics[name].parts {
			for _, b := range p.batches {
				if b.logIndex < cut {
					n.stats.forcedBytes += int64(b.size)
				}
			}
		}
	}
	n.trimProposed = cut
	n.propose(encodeTrim(cut), func(result) {})
}
