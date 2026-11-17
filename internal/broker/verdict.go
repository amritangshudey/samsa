package broker

import (
	"fmt"

	"github.com/amritangshudey/samsa/internal/raft"
	"github.com/amritangshudey/samsa/internal/sys"
	"github.com/amritangshudey/samsa/internal/wire"
)

// Pipeline is an application under test: it reads topic In, as consumer
// group Group if not empty, and writes to topic Out one record or more for
// each input record, with the input record's key
// (docs/design/a5b-app-faults.md, section 3).
type Pipeline struct {
	In, Out, Group string
}

// Verdict is what happened to every input record of a pipeline.
type Verdict struct {
	Inputs, Outputs int
	// Each input record is in exactly one of these: output once, output
	// more than once, never output although the group committed past it
	// (or, without a group, never output), or never output and not yet
	// committed past: the application had not got to it.
	Once, Duplicated, Lost, Pending int
	// Unmatched counts output records whose key is no input's.
	Unmatched int
	// Examples of each finding, at most MaxExamples each, in partition
	// and offset order.
	LostRecords, DuplicatedRecords, PendingRecords []RecordRef
}

// MaxExamples bounds the records a Verdict lists per finding.
const MaxExamples = 20

// RecordRef names one input record.
type RecordRef struct {
	Partition int32
	Offset    int64
	Key       string
	Outputs   int
}

// Judge reads a stopped node's data, as Check does, and says what the
// pipeline did with each input record. Input records are matched to
// output records by key, so input keys must be unique and present. Records
// must be uncompressed: the core has no decompressor.
func Judge(disk sys.Disk, cfg Config, p Pipeline) (Verdict, error) {
	_, r, st, err := load(disk, cfg)
	if err != nil {
		return Verdict{}, err
	}
	in, err := records(r, st, p.In)
	if err != nil {
		return Verdict{}, err
	}
	out, err := records(r, st, p.Out)
	if err != nil {
		return Verdict{}, err
	}
	v := Verdict{Inputs: len(in), Outputs: len(out)}
	first := map[string]rec{}
	for _, x := range in {
		if x.key == nil {
			return v, fmt.Errorf("input %s/%d at offset %d has no key: the verdict matches inputs to outputs by key", p.In, x.part, x.offset)
		}
		if y, ok := first[string(x.key)]; ok {
			return v, fmt.Errorf("input key %q is at %s/%d offset %d and %s/%d offset %d: keys must be unique", x.key, p.In, y.part, y.offset, p.In, x.part, x.offset)
		}
		first[string(x.key)] = x
	}
	count := map[string]int{}
	for _, x := range out {
		if _, ok := first[string(x.key)]; ok {
			count[string(x.key)]++
		} else {
			v.Unmatched++
		}
	}
	var g *group
	var inTopicID [16]byte
	if p.Group != "" {
		g = st.group(p.Group)
		if t := st.topics[p.In]; t != nil {
			inTopicID = t.id
		}
	}
	add := func(list *[]RecordRef, x rec, n int) {
		if len(*list) < MaxExamples {
			*list = append(*list, RecordRef{Partition: x.part, Offset: x.offset, Key: string(x.key), Outputs: n})
		}
	}
	for _, x := range in { // in partition and offset order
		n := count[string(x.key)]
		switch {
		case n == 1:
			v.Once++
		case n > 1:
			v.Duplicated++
			add(&v.DuplicatedRecords, x, n)
		case g == nil || (g.sg != nil && shareDone(g.sg, inTopicID, x)) || (g.sg == nil && committedPast(g, p.In, x)):
			v.Lost++
			add(&v.LostRecords, x, 0)
		default:
			v.Pending++
			add(&v.PendingRecords, x, 0)
		}
	}
	return v, nil
}

func committedPast(g *group, topic string, x rec) bool {
	c, ok := g.offsets[topic][x.part]
	return ok && c.offset > x.offset
}

func shareDone(sg *shareGroup, tid [16]byte, x rec) bool {
	sp := sg.parts[tpKey{tid, x.part}]
	if sp == nil {
		return false
	}
	if x.offset < sp.spso {
		return true
	}
	l := sp.locks[x.offset]
	return l != nil && (l.state == shareAcked || l.state == shareArchived)
}

type rec struct {
	part   int32
	offset int64
	key    []byte
}

func isAbortedRecord(aborted []abortedTxn, pid int64, off int64) bool {
	for _, ab := range aborted {
		if ab.pid == pid && off >= ab.firstOffset && off < ab.lastOffset {
			return true
		}
	}
	return false
}

// records returns every committed data record of topic, by partition, then
// offset. It skips control batches, uncommitted records at or above LSO, and
// aborted transactional records, and reads only what the replayed state
// references so trimmed and deleted batches are never read.
func records(r *raft.Raft, st *state, topic string) ([]rec, error) {
	t := st.topics[topic]
	if t == nil {
		return nil, fmt.Errorf("no topic %q", topic)
	}
	var out []rec
	for i, p := range t.parts {
		if p.start > 0 {
			return nil, fmt.Errorf("%s/%d starts at offset %d: records were deleted, so the verdict cannot see them all", topic, i, p.start)
		}
		lso := p.lso()
		for _, ref := range p.batches {
			if ref.base >= lso {
				break
			}
			cmd, _, err := r.Read(ref.logIndex)
			if err != nil {
				return nil, fmt.Errorf("%s/%d: read log entry %d: %w", topic, i, ref.logIndex, err)
			}
			if len(cmd) == 0 || cmd[0] != cmdAppend {
				continue // control marker batch (e.g. cmdTxnEnd)
			}
			raw := appendBatch(cmd)
			info, ok := wire.PeekBatch(raw)
			if ok && info.Control {
				continue
			}
			_, recs, err := wire.DecodeBatch(raw)
			if err != nil {
				return nil, fmt.Errorf("%s/%d: batch at offset %d is compressed or malformed (the verdict reads uncompressed batches only): %w", topic, i, ref.base, err)
			}
			for j, x := range recs {
				off := ref.base + int64(j)
				if off >= lso || (ok && info.Transactional && isAbortedRecord(p.aborted, info.ProducerID, off)) {
					continue
				}
				out = append(out, rec{part: int32(i), offset: off, key: x.Key})
			}
		}
	}
	return out, nil
}
