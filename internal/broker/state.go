// Package broker implements the node: a single-threaded sys.Handler that
// speaks the Kafka protocol to clients and keeps all of its state as the
// result of applying one log of commands (ADR 0005).
package broker

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"sort"

	"github.com/amritangshudey/samsa/internal/wire"
)

// Command types. The first byte of every command names its type, so a log
// written by an older build stays readable and an unknown type fails loudly.
const (
	cmdCreateTopic     byte = 1
	cmdAppend          byte = 2
	cmdOffsetCommit    byte = 3
	cmdGroupGeneration byte = 4
	cmdGroupAssignment byte = 5
	cmdTrim            byte = 6
	cmdExpireProducers byte = 7
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
	start   int64 // first offset still available; below it, trimmed away
	batches []batchRef
	// producers is what the partition remembers of each idempotent
	// producer that wrote to it. It is per partition because a client moves
	// its partitions to a new epoch one at a time.
	// Expired by ExpireProducers (docs/design/a2-idempotent-producer.md,
	// section 7).
	producers map[int64]*producer
}

// maxProducerBatches is how many of a producer's latest batches a partition
// remembers, to recognise a retry: clients keep at most that many in flight
// (ProducerStateEntry.NUM_BATCHES_TO_RETAIN).
const maxProducerBatches = 5

// sentBatch is one batch accepted from a producer.
type sentBatch struct {
	first, last int32 // sequence numbers of its first and last record
	base        int64 // offset it was appended at
}

// producer is one idempotent producer, as one partition knows it.
type producer struct {
	epoch   int16
	index   int64       // log index of its last accepted batch, for expiry
	batches []sentBatch // the latest, oldest first; never empty
}

// admit applies the idempotence rules to a batch that names a producer:
// docs/design/a2-idempotent-producer.md, section 2, copied from Kafka's
// ProducerAppendInfo and ProducerStateEntry. dup is the offset of the batch
// this one repeats, or -1; a code other than ErrNone refuses the batch.
func (p *partition) admit(b wire.BatchInfo) (dup int64, code int16) {
	ps := p.producers[b.ProducerID]
	switch {
	case ps == nil:
		// Kafka requires 0 only on a partition never written, since
		// elsewhere the state may have expired. Accepting any sequence let
		// a batch in ahead of the one before it, which a node had refused
		// as not leader just before it became leader: the earlier batch's
		// retry was then out of order (BUGS.md S6). An expired producer
		// that comes back is refused too, and its client starts a new
		// epoch at 0 (design section 7.1).
		if b.BaseSequence != 0 {
			return -1, wire.ErrOutOfOrderSequenceNumber
		}
		return -1, wire.ErrNone
	case b.ProducerEpoch < ps.epoch:
		return -1, wire.ErrInvalidProducerEpoch
	case b.ProducerEpoch > ps.epoch:
		if b.BaseSequence != 0 {
			return -1, wire.ErrOutOfOrderSequenceNumber
		}
		return -1, wire.ErrNone
	}
	last := wire.AddSequence(b.BaseSequence, b.Count-1)
	for _, k := range ps.batches {
		if k.first == b.BaseSequence && k.last == last {
			return k.base, wire.ErrNone
		}
	}
	if b.BaseSequence != wire.AddSequence(ps.batches[len(ps.batches)-1].last, 1) {
		return -1, wire.ErrOutOfOrderSequenceNumber
	}
	return -1, wire.ErrNone
}

// remember records a batch that admit let in, appended at base by the log
// entry at index.
func (p *partition) remember(b wire.BatchInfo, base, index int64) {
	ps := p.producers[b.ProducerID]
	if ps == nil || ps.epoch != b.ProducerEpoch { // a new epoch forgets the old one's batches
		if p.producers == nil {
			p.producers = map[int64]*producer{}
		}
		ps = &producer{epoch: b.ProducerEpoch}
		p.producers[b.ProducerID] = ps
	}
	ps.index = index
	if len(ps.batches) == maxProducerBatches {
		ps.batches = ps.batches[:copy(ps.batches, ps.batches[1:])]
	}
	ps.batches = append(ps.batches, sentBatch{b.BaseSequence, wire.AddSequence(b.BaseSequence, b.Count-1), base})
}

// encodeTrim is the retention command: every batch stored below log index
// cut is dropped (docs/design/a1-snapshots.md, section 3.1).
// encodeExpireProducers is the expiry command: every producer whose last
// batch is at a log index below before is forgotten, on every partition.
// The leader picks before from its clock (design section 7.1).
func encodeExpireProducers(before int64) []byte {
	w := wire.Writer{B: []byte{cmdExpireProducers}}
	w.Int64(before)
	return w.B
}

func encodeTrim(cut int64) []byte {
	w := wire.Writer{B: []byte{cmdTrim}}
	w.Int64(cut)
	return w.B
}

type topic struct {
	name  string
	parts []*partition
}

// state is the broker's state machine. It changes only in apply, which is a
// deterministic function of the commands applied so far: no clock, no
// randomness, no I/O, no map iteration (roadmap rule R1).
type state struct {
	// logStart is the log index below which nothing is referenced: the
	// cut of the latest Trim applied.
	logStart int64
	topics   map[string]*topic
	names    []string // sorted, for deterministic listing
	groups   map[string]*group
	// noFencing is Config.UnsafeNoFencing, noSequences
	// Config.UnsafeNoSequenceCheck: deliberate bugs.
	noFencing, noSequences bool
	// expired counts the producers this replica forgot since it started or
	// restored a snapshot. Diagnostic only: not hashed, not in snapshots.
	expired int
}

func newState() *state { return &state{topics: map[string]*topic{}, groups: map[string]*group{}} }

// member is one consumer in a group generation.
type member struct {
	id               string
	sessionTimeout   int32 // milliseconds
	rebalanceTimeout int32 // milliseconds
	// protocols are the assignment strategies the member supports, each
	// with its metadata. The broker never looks inside the metadata.
	protocols  []wire.GroupProtocol
	assignment []byte // what the leader gave this member; opaque
}

// committedOffset is where a group will resume reading one partition.
type committedOffset struct {
	offset      int64
	leaderEpoch int32
	metadata    string
	time        int64 // proposer's clock at commit, ms; for offset expiry, which does not exist yet
}

// offsetEntry is one partition's offset inside an OffsetCommit command.
type offsetEntry struct {
	topic     string
	partition int32
	committedOffset
}

// group is the applied state of a consumer group. It is Empty with no
// members, CompletingRebalance with members and no assignment, and Stable
// with both. A rebalance in progress is not applied state: see softGroup.
type group struct {
	generation   int32
	protocolType string
	protocol     string
	leader       string
	members      []*member // in the order of the GroupGeneration command
	assigned     bool
	offsets      map[string]map[int32]committedOffset // topic, then partition
}

// member returns the member with the given id, or nil.
func (g *group) member(id string) *member {
	for _, m := range g.members {
		if m.id == id {
			return m
		}
	}
	return nil
}

// group returns the named group for reading. A group that does not exist
// reads as an empty one at generation 0; the returned value must not be
// modified.
func (s *state) group(name string) *group {
	if g := s.groups[name]; g != nil {
		return g
	}
	return &group{}
}

// forWrite returns the named group, creating it if needed.
func (s *state) forWrite(name string) *group {
	g := s.groups[name]
	if g == nil {
		g = &group{offsets: map[string]map[int32]committedOffset{}}
		s.groups[name] = g
	}
	return g
}

// checkCommit decides whether an offset commit is accepted. It runs in
// apply, so the generation it compares with is the one current at the
// commit's position in the log: this is what fences a zombie (G4).
func (g *group) checkCommit(generation int32, memberID string, noFencing bool) int16 {
	if generation == -1 && memberID == "" {
		// A consumer that assigns partitions itself. It may not write
		// under a group that has members.
		if len(g.members) > 0 {
			return wire.ErrUnknownMemberID
		}
		return wire.ErrNone
	}
	switch {
	case g.member(memberID) == nil:
		return wire.ErrUnknownMemberID
	case generation != g.generation && !noFencing:
		return wire.ErrIllegalGeneration
	case !g.assigned:
		return wire.ErrRebalanceInProgress
	}
	return wire.ErrNone
}

func encodeOffsetCommit(groupID string, generation int32, memberID string, now int64, entries []offsetEntry) []byte {
	w := wire.Writer{B: []byte{cmdOffsetCommit}}
	w.String(false, groupID)
	w.Int32(generation)
	w.String(false, memberID)
	w.Int64(now)
	w.ArrayLen(false, len(entries))
	for _, e := range entries {
		w.String(false, e.topic)
		w.Int32(e.partition)
		w.Int64(e.offset)
		w.Int32(e.leaderEpoch)
		w.String(false, e.metadata)
	}
	return w.B
}

func encodeGroupGeneration(groupID string, g *group) []byte {
	w := wire.Writer{B: []byte{cmdGroupGeneration}}
	w.String(false, groupID)
	w.Int32(g.generation)
	w.String(false, g.protocolType)
	w.String(false, g.protocol)
	w.String(false, g.leader)
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
	}
	return w.B
}

func encodeGroupAssignment(groupID string, generation int32, assignments []wire.MemberAssignment) []byte {
	w := wire.Writer{B: []byte{cmdGroupAssignment}}
	w.String(false, groupID)
	w.Int32(generation)
	w.ArrayLen(false, len(assignments))
	for _, a := range assignments {
		w.String(false, a.MemberID)
		w.Bytes(false, a.Assignment)
	}
	return w.B
}

// sortedKeys returns m's keys in order: the only way the core walks a map.
func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

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
		info, ok := wire.PeekBatch(r.Rest())
		if r.Err() != nil || !ok {
			return result{}, fmt.Errorf("%w: append at index %d", errBadCommand, index)
		}
		p := s.partition(name, part)
		if p == nil {
			return result{errCode: wire.ErrUnknownTopicOrPartition, baseOffset: -1}, nil
		}
		idempotent := info.ProducerID >= 0 && !s.noSequences
		if idempotent {
			// Refused or repeated, the entry stays in the log and changes
			// nothing: only here, after everything before it, is it known
			// which of the two it is.
			if dup, code := p.admit(info); code != wire.ErrNone {
				return result{errCode: code, baseOffset: -1}, nil
			} else if dup >= 0 {
				return result{baseOffset: dup}, nil
			}
		}
		base := p.next
		p.batches = append(p.batches, batchRef{base: base, logIndex: index, maxTimestamp: info.MaxTimestamp})
		p.next += int64(info.Count)
		if idempotent {
			p.remember(info, base, index)
		}
		return result{baseOffset: base}, nil

	case cmdOffsetCommit:
		name, generation, memberID, now := r.String(false), r.Int32(), r.String(false), r.Int64()
		entries := wire.Array(r, false, func() offsetEntry {
			return offsetEntry{r.String(false), r.Int32(), committedOffset{r.Int64(), r.Int32(), r.String(false), now}}
		})
		if r.Err() != nil {
			return result{}, fmt.Errorf("%w: offset commit at index %d", errBadCommand, index)
		}
		if code := s.group(name).checkCommit(generation, memberID, s.noFencing); code != wire.ErrNone {
			return result{errCode: code}, nil
		}
		g := s.forWrite(name)
		for _, e := range entries {
			if g.offsets[e.topic] == nil {
				g.offsets[e.topic] = map[int32]committedOffset{}
			}
			g.offsets[e.topic][e.partition] = e.committedOffset
		}
		return result{}, nil

	case cmdGroupGeneration:
		name := r.String(false)
		next := group{generation: r.Int32(), protocolType: r.String(false), protocol: r.String(false), leader: r.String(false)}
		next.members = wire.Array(r, false, func() *member {
			return &member{id: r.String(false), sessionTimeout: r.Int32(), rebalanceTimeout: r.Int32(),
				protocols: wire.Array(r, false, func() wire.GroupProtocol {
					// Copied: the command may alias a buffer that is reused.
					return wire.GroupProtocol{Name: r.String(false), Metadata: slices.Clone(r.Bytes(false))}
				})}
		})
		if r.Err() != nil {
			return result{}, fmt.Errorf("%w: group generation at index %d", errBadCommand, index)
		}
		// One node has one proposer, so this holds today. Under Raft a
		// deposed leader's proposal can arrive late; this discards it.
		if next.generation != s.group(name).generation+1 {
			return result{errCode: wire.ErrIllegalGeneration}, nil
		}
		g := s.forWrite(name)
		next.offsets = g.offsets
		*g = next
		return result{}, nil

	case cmdTrim:
		cut := r.Int64()
		if r.Err() != nil || cut < 0 || cut > index {
			return result{}, fmt.Errorf("%w: trim at index %d", errBadCommand, index)
		}
		if cut <= s.logStart {
			return result{}, nil // an older proposal, overtaken
		}
		s.logStart = cut
		for _, name := range s.names {
			for _, p := range s.topics[name].parts {
				keep := sort.Search(len(p.batches), func(i int) bool { return p.batches[i].logIndex >= cut })
				p.batches = slices.Delete(p.batches, 0, keep)
				p.start = p.next
				if len(p.batches) > 0 {
					p.start = p.batches[0].base
				}
			}
		}
		return result{}, nil

	case cmdExpireProducers:
		before := r.Int64()
		if r.Err() != nil || before < 0 || before > index {
			return result{}, fmt.Errorf("%w: producer expiry at index %d", errBadCommand, index)
		}
		for _, name := range s.names {
			for _, p := range s.topics[name].parts {
				for id, ps := range p.producers { // deleting is the same in any order
					if ps.index < before {
						delete(p.producers, id)
						s.expired++
					}
				}
			}
		}
		return result{}, nil

	case cmdGroupAssignment:
		name, generation := r.String(false), r.Int32()
		assignments := wire.Array(r, false, func() wire.MemberAssignment {
			return wire.MemberAssignment{MemberID: r.String(false), Assignment: slices.Clone(r.Bytes(false))}
		})
		if r.Err() != nil {
			return result{}, fmt.Errorf("%w: group assignment at index %d", errBadCommand, index)
		}
		g := s.groups[name]
		if g == nil || generation != g.generation || g.assigned {
			return result{errCode: wire.ErrIllegalGeneration}, nil
		}
		for _, a := range assignments {
			if m := g.member(a.MemberID); m != nil {
				m.assignment = a.Assignment
			}
		}
		g.assigned = true
		return result{}, nil
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
	add(s.logStart)
	for _, name := range s.names { // sorted: never iterate the map
		t := s.topics[name]
		sum = crc32.Update(sum, table, []byte(name))
		add(int64(len(t.parts)))
		for _, p := range t.parts {
			add(p.next, p.start, int64(len(p.batches)))
			for _, b := range p.batches {
				add(b.base, b.logIndex, b.maxTimestamp)
			}
			add(int64(len(p.producers)))
			for _, id := range sortedKeys(p.producers) {
				ps := p.producers[id]
				add(id, int64(ps.epoch), ps.index, int64(len(ps.batches)))
				for _, b := range ps.batches {
					add(int64(b.first), int64(b.last), b.base)
				}
			}
		}
	}
	str := func(vs ...string) {
		for _, v := range vs {
			add(int64(len(v)))
			sum = crc32.Update(sum, table, []byte(v))
		}
	}
	for _, name := range sortedKeys(s.groups) {
		g := s.groups[name]
		str(name, g.protocolType, g.protocol, g.leader)
		add(int64(g.generation), int64(len(g.members)))
		for _, m := range g.members {
			str(m.id, string(m.assignment))
			add(int64(m.sessionTimeout), int64(m.rebalanceTimeout), int64(len(m.protocols)))
			for _, p := range m.protocols {
				str(p.Name, string(p.Metadata))
			}
		}
		if g.assigned {
			add(1)
		}
		for _, topic := range sortedKeys(g.offsets) {
			str(topic)
			for _, part := range sortedKeys(g.offsets[topic]) {
				o := g.offsets[topic][part]
				str(o.metadata)
				add(int64(part), o.offset, int64(o.leaderEpoch), o.time)
			}
		}
	}
	return sum
}
