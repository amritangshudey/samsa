// Package broker implements the node: a single-threaded sys.Handler that
// speaks the Kafka protocol to clients and keeps all of its state as the
// result of applying one log of commands (ADR 0005).
package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/amritangshudey/samsa/internal/wire"
)

// Command types. The first byte of every command names its type, so a log
// written by an older build stays readable and an unknown type fails loudly.
const (
	cmdCreateTopic byte = 1
	cmdAppend      byte = 2
)

// errBadCommand marks a command in the log that cannot be decoded. The log
// is checksummed, so this means a bug or a newer on-disk format, and the
// node refuses to continue.
var errBadCommand = errors.New("broker: undecodable command in log")

func encodeCreateTopic(name string, partitions int32) []byte {
	w := wire.Writer{B: []byte{cmdCreateTopic}}
	w.String(false, name)
	w.Int32(partitions)
	return w.B
}

// encodeAppend copies batch, which usually aliases a connection buffer.
func encodeAppend(topic string, partition int32, batch []byte) []byte {
	w := wire.Writer{B: make([]byte, 0, 1+2+len(topic)+4+len(batch))}
	w.Int8(int8(cmdAppend))
	w.String(false, topic)
	w.Int32(partition)
	w.B = append(w.B, batch...)
	return w.B
}

// batchRef locates one record batch of a partition in the command log.
type batchRef struct {
	base         int64 // partition offset of the batch's first record
	logIndex     int64 // index of the Append command holding the batch
	maxTimestamp int64
}

type partition struct {
	next    int64 // offset the next record will get; also the high watermark
	batches []batchRef
}

type topic struct {
	name  string
	parts []*partition
}

// state is the broker's state machine. It changes only in apply, which is a
// deterministic function of the commands applied so far: no clock, no
// randomness, no I/O, no map iteration (roadmap rule R1).
type state struct {
	topics map[string]*topic
	names  []string // sorted, for deterministic listing
}

func newState() *state { return &state{topics: map[string]*topic{}} }

// partition returns the named partition, or nil.
func (s *state) partition(name string, index int32) *partition {
	t := s.topics[name]
	if t == nil || index < 0 || int(index) >= len(t.parts) {
		return nil
	}
	return t.parts[index]
}

// result is what applying a command produced, for the request that
// proposed it.
type result struct {
	errCode    int16
	baseOffset int64 // Append: offset assigned to the batch's first record
}

// apply executes the command stored at log index. The error is non-nil only
// for a command that cannot be decoded.
func (s *state) apply(index int64, cmd []byte) (result, error) {
	if len(cmd) == 0 {
		return result{}, fmt.Errorf("%w: empty command at index %d", errBadCommand, index)
	}
	r := wire.NewReader(cmd[1:])
	switch cmd[0] {
	case cmdCreateTopic:
		name, parts := r.String(false), r.Int32()
		if r.Err() != nil || parts < 1 {
			return result{}, fmt.Errorf("%w: create topic at index %d", errBadCommand, index)
		}
		if s.topics[name] != nil {
			// Two creates can both pass validation before either is
			// applied. The second one loses, identically on every replay.
			return result{errCode: wire.ErrTopicAlreadyExists}, nil
		}
		t := &topic{name: name, parts: make([]*partition, parts)}
		for i := range t.parts {
			t.parts[i] = &partition{}
		}
		s.topics[name] = t
		at := sort.SearchStrings(s.names, name)
		s.names = append(s.names, "")
		copy(s.names[at+1:], s.names[at:])
		s.names[at] = name
		return result{}, nil

	case cmdAppend:
		name, part := r.String(false), r.Int32()
		count, maxTime, ok := wire.PeekBatch(r.Rest())
		if r.Err() != nil || !ok {
			return result{}, fmt.Errorf("%w: append at index %d", errBadCommand, index)
		}
		p := s.partition(name, part)
		if p == nil {
			return result{errCode: wire.ErrUnknownTopicOrPartition, baseOffset: -1}, nil
		}
		base := p.next
		p.batches = append(p.batches, batchRef{base: base, logIndex: index, maxTimestamp: maxTime})
		p.next += int64(count)
		return result{baseOffset: base}, nil
	}
	return result{}, fmt.Errorf("%w: unknown type %d at index %d", errBadCommand, cmd[0], index)
}

// appendBatch returns the record batch inside an Append command, or nil.
func appendBatch(cmd []byte) []byte {
	if len(cmd) == 0 || cmd[0] != cmdAppend {
		return nil
	}
	r := wire.NewReader(cmd[1:])
	r.String(false)
	r.Int32()
	if r.Err() != nil {
		return nil
	}
	return r.Rest()
}

// hash returns a checksum of the whole state machine. Two nodes that have
// applied the same commands have the same hash; the simulator uses it to
// check that replay is repeatable and, later, that replicas agree.
func (s *state) hash() uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	var sum uint32
	add := func(vs ...int64) {
		var b [8]byte
		for _, v := range vs {
			binary.BigEndian.PutUint64(b[:], uint64(v))
			sum = crc32.Update(sum, table, b[:])
		}
	}
	for _, name := range s.names { // sorted: never iterate the map
		t := s.topics[name]
		sum = crc32.Update(sum, table, []byte(name))
		add(int64(len(t.parts)))
		for _, p := range t.parts {
			add(p.next, int64(len(p.batches)))
			for _, b := range p.batches {
				add(b.base, b.logIndex, b.maxTimestamp)
			}
		}
	}
	return sum
}
